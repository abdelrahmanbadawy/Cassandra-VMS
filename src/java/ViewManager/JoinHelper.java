package ViewManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class JoinHelper {

	static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
					.getString("config.host.localhost"))
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();

	public static void insertStatementUpdateLeft(JSONObject json,String leftJName,String joinTablePk,String colNames,String tuple,String leftList,String nullValues){

		StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
		insertQuery.append((String) json.get("keyspace")).append(".")
		.append(leftJName).append(" (");
		insertQuery.append(joinTablePk).append(", ");
		insertQuery.append(colNames).append(") VALUES (");
		insertQuery.append(tuple).append(", ");
		insertQuery.append(leftList).append(", ");
		insertQuery.append(nullValues);
		insertQuery.append(");");

		System.out.println(insertQuery);

		try {

			Session session = currentCluster.connect();
			session.execute(insertQuery.toString());
			session.close();
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static boolean updateLeftJoinTable(Stream stream,String leftJName, CustomizedRow theRow,
			JSONObject json) {

		// 3. Read Left Join xml, get leftPkName, leftPkType, get pk of join
		// table & type
		// rightPkName, rightPkType

		int position = VmXmlHandler.getInstance().getlJSchema()
				.getList("dbSchema.tableDefinition.name").indexOf(leftJName);

		String colNames = "";
		String joinTablePk = "";

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			joinTablePk = VmXmlHandler.getInstance().getlJSchema()
					.getString(temp + ".primaryKey.name");

			List<String> colName = VmXmlHandler.getInstance().getlJSchema()
					.getList(temp + ".column.name");
			colNames = StringUtils.join(colName, ", ");

			String leftPkName = temp + ".primaryKey.left";
			leftPkName = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkName);

			String leftPkType = temp + ".primaryKey.leftType";
			leftPkType = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkType);

			String leftPkValue = "";

			String rightPkType = temp + ".primaryKey.righType";
			rightPkType = VmXmlHandler.getInstance().getlJSchema()
					.getString(rightPkType);

			// 3.a. get from delta row, the left pk value
			leftPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), leftPkName, leftPkType, "");

			// 3.b. retrieve the left list values for inserton statement
			Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1");
			HashMap<String, String> myMap1 = new HashMap<String, String>();
			myMap1.putAll(tempMapImmutable1);

			String leftList = myMap1.get(leftPkValue);
			leftList = leftList.replaceAll("\\[", "").replaceAll("\\]", "");

			// insert null values if myMap2 has no entries yet

			String tuple = "(" + leftPkValue + "," + 0 + ")";
			int nrRightCol = VmXmlHandler.getInstance().getlJSchema()
					.getInt(temp + ".nrRightCol");

			StringBuilder insertQuery = new StringBuilder();
			for (int i = 0; i < nrRightCol; i++) {
				insertQuery.append("null").append(", ");
			}
			insertQuery.deleteCharAt(insertQuery.length() - 2);

			JoinHelper.insertStatementUpdateLeft(json, leftJName, joinTablePk, colNames, tuple, leftList, insertQuery.toString());

		}

		return true;
	}


	public static boolean removeLeftCrossRight(Stream stream, JSONObject json, String innerJTableName) {

		// 1. get old row updated by reverse join
		CustomizedRow theRow = stream.getReverseJoinUpadteOldRow();

		// 1.a get columns item_2

		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2");

		// 2. retrieve list_item2

		HashMap<String, String> myMap2 = new HashMap<String, String>();
		myMap2.putAll(tempMapImmutable2);

		// 3. Read Left Join xml, get leftPkName, leftPkType, get pk of join
		// table & type
		// rightPkName, rightPkType

		int position = VmXmlHandler.getInstance().getiJSchema()
				.getList("dbSchema.tableDefinition.name")
				.indexOf(innerJTableName);

		String colNames = "";
		String joinTablePk = "";

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			joinTablePk = VmXmlHandler.getInstance().getiJSchema()
					.getString(temp + ".primaryKey.name");

			String leftPkName = temp + ".primaryKey.left";
			leftPkName = VmXmlHandler.getInstance().getiJSchema()
					.getString(leftPkName);

			String leftPkType = temp + ".primaryKey.leftType";
			leftPkType = VmXmlHandler.getInstance().getiJSchema()
					.getString(leftPkType);

			String leftPkValue = "";

			String rightPkType = temp + ".primaryKey.rightType";
			rightPkType = VmXmlHandler.getInstance().getiJSchema()
					.getString(rightPkType);

			// 3.a. get from delta row, the left pk value
			leftPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), leftPkName, leftPkType, "");

			String rightPkValue = "";
			// 4. for each entry in item_list2, create insert statement for each
			// entry to add a new row
			for (Map.Entry<String, String> entry : myMap2.entrySet()) {

				switch (rightPkType) {

				case "text":
					rightPkValue = "'" + entry.getKey() + "'";
					break;

				case "varchar":
					rightPkValue = "'" + entry.getKey() + "'";
					break;

				default:
					rightPkValue = entry.getKey();
					break;

				}

				String tuple = "(" + leftPkValue + "," + rightPkValue + ")";

				Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJTableName, joinTablePk, tuple);

			}
		}

		return true;
	}


	public static boolean addAllToRightJoinTable(String rightJName,
			Map<String, String> myMap2, JSONObject json) {

	
		
		int position = VmXmlHandler.getInstance().getrJSchema()
				.getList("dbSchema.tableDefinition.name").indexOf(rightJName);

		String colNames = "";
		String joinTablePk = "";

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			joinTablePk = VmXmlHandler.getInstance().getlJSchema()
					.getString(temp + ".primaryKey.name");

			List<String> colName = VmXmlHandler.getInstance().getlJSchema()
					.getList(temp + ".column.name");
			colNames = StringUtils.join(colName, ", ");

			String rightPkName = temp + ".primaryKey.right";
			rightPkName = VmXmlHandler.getInstance().getlJSchema()
					.getString(rightPkName);

			String rightPkType = temp + ".primaryKey.righType";
			rightPkType = VmXmlHandler.getInstance().getlJSchema()
					.getString(rightPkType);

			String rightPkValue = "";

			String leftPkType = temp + ".primaryKey.leftType";
			leftPkType = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkType);

			for (Map.Entry<String, String> entry : myMap2.entrySet()) {
				switch (rightPkValue) {

				case "text":
					rightPkValue = "'" + entry.getKey() + "'";
					break;

				case "varchar":
					rightPkValue = "'" + entry.getKey() + "'";
					break;

				default:
					rightPkValue = entry.getKey();
					break;

				}

				String rightList = myMap2.get(rightPkValue);
				rightList = rightList.replaceAll("\\[", "").replaceAll("\\]",
						"");

				// insert null values if myMap2 has no entries yet

				String tuple = "(" + 0 + "," + rightPkValue + ")";
				int nrLeftCol = VmXmlHandler.getInstance()
						.getlJSchema().getInt(temp + ".nrLeftCol");

				StringBuilder insertQuery = new StringBuilder();
				for (int i = 0; i < nrLeftCol; i++) {
					insertQuery.append("null").append(", ");
				}
				
				insertQuery.deleteCharAt(insertQuery.length() - 2);
				

				JoinHelper.insertStatementUpdateLeft(json, rightJName, joinTablePk, colNames, tuple,insertQuery.toString(),rightList);

			}

		}
		return true;
	}

	public static boolean updateRightJoinTable(Stream stream,String rightJName, CustomizedRow theRow,
			JSONObject json) {

		int position = VmXmlHandler.getInstance().getrJSchema()
				.getList("dbSchema.tableDefinition.name").indexOf(rightJName);

		String colNames = "";
		String joinTablePk = "";

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			joinTablePk = VmXmlHandler.getInstance().getrJSchema()
					.getString(temp + ".primaryKey.name");

			List<String> colName = VmXmlHandler.getInstance().getrJSchema()
					.getList(temp + ".column.name");
			colNames = StringUtils.join(colName, ", ");

			String rightPkName = temp + ".primaryKey.right";
			rightPkName = VmXmlHandler.getInstance().getrJSchema()
					.getString(rightPkName);

			String rightPkType = temp + ".primaryKey.rightType";
			rightPkType = VmXmlHandler.getInstance().getrJSchema()
					.getString(rightPkType);

			String rightPkValue = "";

			// 3.a. get from delta row, the left pk value
			rightPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), rightPkName, rightPkType, "");
			
			// 3.b. retrieve the left list values for inserton statement
			Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2");
			HashMap<String, String> myMap2 = new HashMap<String, String>();
			myMap2.putAll(tempMapImmutable2);

			String rightList = myMap2.get(rightPkValue);
			rightList = rightList.replaceAll("\\[", "").replaceAll("\\]", "");

			// insert null values if myMap2 has no entries yet

			String tuple = "(" + 0 + "," + rightPkValue + ")";
			int nrLeftCol = VmXmlHandler.getInstance().getlJSchema()
					.getInt(temp + ".nrLeftCol");

			StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
			insertQuery.append((String) json.get("keyspace")).append(".")
			.append(rightJName).append(" (");
			insertQuery.append(joinTablePk).append(", ");
			insertQuery.append(colNames).append(") VALUES (");
			insertQuery.append(tuple).append(", ");

			for (int i = 0; i < nrLeftCol; i++) {
				insertQuery.append("null").append(", ");
			}

			insertQuery.append(rightList).append(", ");

			insertQuery.deleteCharAt(insertQuery.length() - 2);
			insertQuery.append(");");

			System.out.println(insertQuery);

			try {

				Session session = currentCluster.connect();
				session.execute(insertQuery.toString());
				session.close();
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

		return true;
	}

	public static boolean removeRightCrossLeft(Stream stream,JSONObject json, String innerJTableName) {

		// 1. get row updated by reverse join
		CustomizedRow theRow = stream.getReverseJoinUpadteOldRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1");

		// 2. retrieve list_item1, list_item2
		HashMap<String, String> myMap1 = new HashMap<String, String>();
		myMap1.putAll(tempMapImmutable1);

		if (myMap1.size() == 0) {
			return true;
		}

		// 3. Read Left Join xml, get leftPkName, leftPkType, get pk of join
		// table & type
		// rightPkName, rightPkType

		int position = VmXmlHandler.getInstance().getiJSchema()
				.getList("dbSchema.tableDefinition.name")
				.indexOf(innerJTableName);

		String colNames = "";
		String joinTablePk = "";

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			joinTablePk = VmXmlHandler.getInstance().getiJSchema()
					.getString(temp + ".primaryKey.name");

			String rightPkName = temp + ".primaryKey.right";
			rightPkName = VmXmlHandler.getInstance().getiJSchema()
					.getString(rightPkName);

			String rightPkType = temp + ".primaryKey.rightType";
			rightPkType = VmXmlHandler.getInstance().getiJSchema()
					.getString(rightPkType);

			String rightPkValue = "";

			String leftPkType = temp + ".primaryKey.leftType";
			leftPkType = VmXmlHandler.getInstance().getiJSchema()
					.getString(leftPkType);

			// 3.a. get from delta row, the left pk value
			rightPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), rightPkName, rightPkType, "");
			
			String leftPkValue = "";

			// 4. for each entry in item_list2, create insert statement for each
			// entry to add a new row
			for (Map.Entry<String, String> entry : myMap1.entrySet()) {

				switch (leftPkType) {

				case "text":
					leftPkValue = "'" + entry.getKey() + "'";
					break;

				case "varchar":
					leftPkValue = "'" + entry.getKey() + "'";
					break;

				default:
					leftPkValue = entry.getKey();
					break;

				}

				String tuple = "(" + leftPkValue + "," + rightPkValue + ")";

				StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
				deleteQuery.append((String) json.get("keyspace")).append(".")
				.append(innerJTableName).append(" WHERE ");
				deleteQuery.append(joinTablePk).append(" = ");
				deleteQuery.append(tuple).append(";");

				System.out.println(deleteQuery);

				try {

					Session session = currentCluster.connect();
					session.execute(deleteQuery.toString());
					session.close();
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}

		return true;
	}
	
	public static boolean addAllToLeftJoinTable(String leftJName,
			Map<String, String> myMap1, JSONObject json) {

		int position = VmXmlHandler.getInstance().getlJSchema()
				.getList("dbSchema.tableDefinition.name").indexOf(leftJName);

		String colNames = "";
		String joinTablePk = "";

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			joinTablePk = VmXmlHandler.getInstance().getlJSchema()
					.getString(temp + ".primaryKey.name");

			List<String> colName = VmXmlHandler.getInstance().getlJSchema()
					.getList(temp + ".column.name");
			colNames = StringUtils.join(colName, ", ");

			String leftPkName = temp + ".primaryKey.left";
			leftPkName = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkName);

			String leftPkType = temp + ".primaryKey.leftType";
			leftPkType = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkType);

			String leftPkValue = "";

			String rightPkType = temp + ".primaryKey.righType";
			rightPkType = VmXmlHandler.getInstance().getlJSchema()
					.getString(rightPkType);

			for (Map.Entry<String, String> entry : myMap1.entrySet()) {
				switch (leftPkType) {

				case "text":
					leftPkValue = "'" + entry.getKey() + "'";
					break;

				case "varchar":
					leftPkValue = "'" + entry.getKey() + "'";
					break;

				default:
					leftPkValue = entry.getKey();
					break;

				}

				String leftList = myMap1.get(leftPkValue);
				leftList = leftList.replaceAll("\\[", "").replaceAll("\\]", "");

				// insert null values if myMap2 has no entries yet

				String tuple = "(" + leftPkValue + "," + 0 + ")";
				int nrRightCol = VmXmlHandler.getInstance()
						.getlJSchema().getInt(temp + ".nrRightCol");

				StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
				insertQuery.append((String) json.get("keyspace")).append(".")
				.append(leftJName).append(" (");
				insertQuery.append(joinTablePk).append(", ");
				insertQuery.append(colNames).append(") VALUES (");
				insertQuery.append(tuple).append(", ");
				insertQuery.append(leftList).append(", ");

				for (int i = 0; i < nrRightCol; i++) {
					insertQuery.append("null").append(", ");
				}

				insertQuery.deleteCharAt(insertQuery.length() - 2);
				insertQuery.append(");");

				System.out.println(insertQuery);

				try {

					Session session = currentCluster.connect();
					session.execute(insertQuery.toString());
					session.close();
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}

		}

		return true;

	}

	
	
	
}
