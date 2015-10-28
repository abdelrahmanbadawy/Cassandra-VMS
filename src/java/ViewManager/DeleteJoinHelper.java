package ViewManager;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class DeleteJoinHelper {

	static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
					.getString("config.host.localhost"))
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();


	public static boolean deleteFromLeftJoinTable(HashMap<String, String> myMap1,
			String leftJName, JSONObject json, boolean fromDelete) {

		int position = VmXmlHandler.getInstance().getlJSchema()
				.getList("dbSchema.tableDefinition.name").indexOf(leftJName);

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			String leftPkName = temp + ".primaryKey.left";
			leftPkName = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkName);

			String leftPkType = temp + ".primaryKey.leftType";
			leftPkType = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkType);

			String leftPkValue = "";

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

				String tuple = "(" + leftPkValue + "," + 0 + ")";

				Utils.deleteEntireRowWithPK((String) json.get("keyspace"), leftJName, leftPkName, tuple);
			}
		}
		return true;
	}


	public static boolean deleteElementFromRightJoinTable(Stream stream,HashMap<String, String> myMap2,
			String rightJName, JSONObject json, boolean fromDelete) {

		int position = VmXmlHandler.getInstance().getrJSchema()
				.getList("dbSchema.tableDefinition.name").indexOf(rightJName);

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			String rightPkName = temp + ".primaryKey.right";
			rightPkName = VmXmlHandler.getInstance().getrJSchema()
					.getString(rightPkName);

			String rightPkType = temp + ".primaryKey.rightType";
			rightPkType = VmXmlHandler.getInstance().getrJSchema()
					.getString(rightPkType);

			String rightPkValue = "";

			// 4. for each entry in item_list2, create insert statement for each
			// entry to add a new row

			rightPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), rightPkName, rightPkType, "");

			String tuple = "(" + 0 + "," + rightPkValue + ")";

			Utils.deleteEntireRowWithPK((String) json.get("keyspace"), rightJName, rightPkName, tuple);

		}
		return true;
	}


	public static boolean deleteElementFromLeftJoinTable(Stream stream,HashMap<String, String> myMap1,
			String leftJName, JSONObject json, boolean fromDelete) {

		int position = VmXmlHandler.getInstance().getlJSchema()
				.getList("dbSchema.tableDefinition.name").indexOf(leftJName);

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			String leftPkName = temp + ".primaryKey.left";
			leftPkName = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkName);

			String leftPkType = temp + ".primaryKey.leftType";
			leftPkType = VmXmlHandler.getInstance().getlJSchema()
					.getString(leftPkType);

			String leftPkValue = "";

			leftPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), leftPkName, leftPkType, "");

			String tuple = "(" + leftPkValue + "," + 0 + ")";

			Utils.deleteEntireRowWithPK((String) json.get("keyspace"), leftJName, leftPkName, tuple);

		}
		return true;
	}


	public static boolean deleteFromRightJoinTable(Stream stream,HashMap<String, String> myMap2,
			String rightJName, JSONObject json, boolean fromDelete) {

		int position = VmXmlHandler.getInstance().getrJSchema()
				.getList("dbSchema.tableDefinition.name").indexOf(rightJName);

		if (position != -1) {

			String temp = "dbSchema.tableDefinition(";
			temp += Integer.toString(position);
			temp += ")";

			String rightPkName = temp + ".primaryKey.right";
			rightPkName = VmXmlHandler.getInstance().getrJSchema()
					.getString(rightPkName);

			String rightPkType = temp + ".primaryKey.rightType";
			rightPkType = VmXmlHandler.getInstance().getrJSchema()
					.getString(rightPkType);

			String rightPkValue = "";

			// 4. for each entry in item_list2, create insert statement for each
			// entry to add a new row


			rightPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), rightPkName, rightPkType, "");


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

				String tuple = "(" + 0 + "," + rightPkValue + ")";

				Utils.deleteEntireRowWithPK((String) json.get("keyspace"), rightJName, rightPkName, tuple);
			}

		}
		return true;
	}


	public static boolean removeDeleteLeftCrossRight(Stream stream,JSONObject json,
			String innerJName, Map<String, String> myMap2) {

		String type = stream.getDeltaDeletedRow().getType(0);
		String name =  stream.getDeltaDeletedRow().getName(0);
		String leftPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), name, type, "");

		for (Map.Entry<String, String> entry : myMap2.entrySet()) {
			String tuple = "(" + leftPkValue + "," + entry.getKey() + ")";

			int position = VmXmlHandler.getInstance().getiJSchema()
					.getList("dbSchema.tableDefinition.name")
					.indexOf(innerJName);

			String joinTablePk = "";

			if (position != -1) {

				String temp = "dbSchema.tableDefinition(";
				temp += Integer.toString(position);
				temp += ")";

				joinTablePk = VmXmlHandler.getInstance().getrJSchema()
						.getString(temp + ".primaryKey.name");

			}

			Utils.deleteEntireRowWithPK((String)json.get("keyspace"), innerJName, joinTablePk, tuple);

		}
		return true;
	}


	public static boolean removeDeleteRightCrossLeft(Stream stream,JSONObject json,
			String innerJName, Map<String, String> myMap1) {

		String type = stream.getDeltaDeletedRow().getType(0);
		String name =  stream.getDeltaDeletedRow().getName(0);
		String rigthPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), name, type, "");


		for (Map.Entry<String, String> entry : myMap1.entrySet()) {
			String tuple = "(" + entry.getKey() + "," + rigthPkValue + ")";

			int position = VmXmlHandler.getInstance().getiJSchema()
					.getList("dbSchema.tableDefinition.name")
					.indexOf(innerJName);

			String joinTablePk = "";

			if (position != -1) {

				String temp = "dbSchema.tableDefinition(";
				temp += Integer.toString(position);
				temp += ")";

				joinTablePk = VmXmlHandler.getInstance().getrJSchema()
						.getString(temp + ".primaryKey.name");

			}

			Utils.deleteEntireRowWithPK((String)json.get("keyspace"), innerJName, joinTablePk, tuple);
		}

		return true;
	}


}
