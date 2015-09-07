package ViewManager;

import java.math.BigInteger;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

import ch.qos.logback.core.joran.action.NewRuleAction;
import client.client.Client;
import client.client.XmlHandler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class ViewManager {

	Cluster currentCluster = null;
	private Row deltaUpdatedRow;
	private Row deltaDeletedRow;

	private String reverseJoinTableName;

	private Row deletePreaggRowDeleted;
	private Row deletePreaggRow;

	private Row updatedPreaggRow;
	private Row updatedPreaggRowDeleted;
	private Row updatedPreaggRowChangeAK;

	private Row reverseJoinUpdateNewRow;
	private Row reverseJoinUpadteOldRow;

	private Row reverseJoinUpdatedOldRow_changeJoinKey;

	private Row reverseJoinDeleteNewRow;
	private Row revereJoinDeleteOldRow;

	private Row joinAggRow;
	private Row joinAggRowChangeAK;
	private Row joinAggDeletedRow;

	private Row deleteRowJoinAggDeleted;
	private Row deleteRowJoinAgg;

	public ViewManager(Cluster currenCluster) {
		this.currentCluster = currenCluster;
	}

	public Row getDeltaUpdatedRow() {
		return deltaUpdatedRow;
	}

	private void setDeltaUpdatedRow(Row row) {
		deltaUpdatedRow = row;
	}

	public Row getrjUpdatedRow() {
		return reverseJoinUpdateNewRow;
	}

	private void setReverseJoinName(String s) {
		reverseJoinTableName = s;
	}

	public String getReverseJoinName() {
		return reverseJoinTableName;
	}

	private void setReverseJoinUpdatedNewRow(Row row) {
		reverseJoinUpdateNewRow = row;
	}

	public Row getReverseJoinUpdatedNewRow() {
		return reverseJoinUpdateNewRow;
	}

	public Row getUpdatedPreaggRow() {
		return updatedPreaggRow;
	}

	private void setUpdatedPreaggRow(Row row) {
		updatedPreaggRow = row;
	}

	public Row getUpdatedPreaggRowChangeAK() {
		return updatedPreaggRowChangeAK;
	}

	private void setUpdatedPreaggRowChangeAK(Row row) {
		updatedPreaggRowChangeAK = row;
	}

	public boolean updateDelta(Stream stream,JSONObject json, int indexBaseTableName,
			String baseTablePrimaryKey) {

		// retrieve values from json
		String table = (String) json.get("table");
		String keyspace = (String) json.get("keyspace");
		JSONObject data = (JSONObject) json.get("data");

		Set<?> keySet = data.keySet();
		Iterator<?> keySetIterator = keySet.iterator();

		List<String> selectStatement_new_values = new ArrayList<>();
		StringBuilder selectStatement_new = new StringBuilder();
		StringBuilder selectStatement_old = new StringBuilder();

		while (keySetIterator.hasNext()) {

			String key = (String) keySetIterator.next();

			if (key.contains(baseTablePrimaryKey)) {
				selectStatement_new.append(key);
				selectStatement_new.append(", ");
				selectStatement_new_values.add(data.get(key).toString());

			} else {
				selectStatement_new.append(key + "_new ,");
				selectStatement_old.append(key + "_old ,");
				selectStatement_new_values.add(data.get(key).toString());
			}
		}

		selectStatement_new.deleteCharAt(selectStatement_new.length() - 1);
		selectStatement_old.deleteCharAt(selectStatement_old.length() - 1);

		// 1. Retrieve from the delta table the _new column values of columns
		// retrieved from json having the baseTablePrimaryKey

		ResultSet result = Utils.selectStatement(selectStatement_new.toString(), keyspace, table, baseTablePrimaryKey, data.get(baseTablePrimaryKey).toString());
		
		Row theRow = result.one();
		StringBuilder insertQueryAgg = new StringBuilder();

		// 2. If the row retrieved is empty, then this is a new insertion
		// 2.a Insert into delta, values from json in the appropriate _new
		// columns
		// 2.b the _old columns will have nulls as values

		if (theRow == null) {

			// 3. Execute insertion statement in delta
			org.apache.commons.lang.StringUtils.join(selectStatement_new_values,", ");
			Utils.insertStatement(keyspace,"delta_"+ table,selectStatement_new.toString(), selectStatement_new_values.toString().replace("[", "").replace("]",""));

		} else {

			// 3. If retrieved row is not empty, then this is an update
			// operation, values of row are retrieved
			// 3.a Insert into delta, values from json in the appropriate _new
			// columns
			// 3.b the _old columns will have the retrieved values from 3.

			Iterator<?> keySetIterator1 = keySet.iterator();

			while (keySetIterator1.hasNext()) {
				String key = (String) keySetIterator1.next();
				insertQueryAgg.append(data.get(key).toString()).append(", ");
			}
			insertQueryAgg.deleteCharAt(insertQueryAgg.length() - 2);

			
			int nrColumns = theRow.getColumnDefinitions().size();

			for (int i = 0; i < nrColumns; i++) {

				switch (theRow.getColumnDefinitions().getType(i).toString()) {

				case "text":
					if (!theRow.getColumnDefinitions().getName(i)
							.equals(baseTablePrimaryKey)) {
						insertQueryAgg
						.append(", '" + theRow.getString(i) + "'");
					}
					break;
				case "int":
					if (!theRow.getColumnDefinitions().getName(i)
							.equals(baseTablePrimaryKey)) {
						insertQueryAgg.append(", " + theRow.getInt(i));
					}
					break;
				case "varint":
					if (!theRow.getColumnDefinitions().getName(i)
							.equals(baseTablePrimaryKey)) {
						insertQueryAgg.append(", " + theRow.getVarint(i));
					}
					break;

				case "varchar":
					if (!theRow.getColumnDefinitions().getName(i)
							.equals(baseTablePrimaryKey)) {
						insertQueryAgg
						.append(", '" + theRow.getString(i) + "'");
					}
					break;

				case "float":
					if (!theRow.getColumnDefinitions().getName(i)
							.equals(baseTablePrimaryKey)) {
						insertQueryAgg.append(", " + theRow.getFloat(i));
					}
					break;

				}
			}
			
			// 4. Execute insertion statement in delta
			Utils.insertStatement(keyspace,"delta_"+ table,selectStatement_new.toString()+", "+selectStatement_old, insertQueryAgg.toString());
			
		}
		
		System.out.println("Done Delta update");

		// 5. get the entire row from delta where update has happened
		// 5.a save the row and send bk to controller

		Row row = Utils.selectAllStatement(keyspace, "delta_"+table, baseTablePrimaryKey, data.get(baseTablePrimaryKey).toString());
		stream.setDeltaUpdatedRow(row);
		
		//TO BE REMOVED
		setDeltaUpdatedRow(row);
		
		return true;
	}

	public boolean deleteRowDelta(Stream stream,JSONObject json) {

		JSONObject condition = (JSONObject) json.get("condition");
		Object[] hm = condition.keySet().toArray();

		// 1. retrieve the row to be deleted from delta table
		Row row = Utils.selectAllStatement((String)json.get("keyspace"), "delta_" + json.get("table"), hm[0].toString(), condition.get(hm[0]).toString());
		
		// 2. set DeltaDeletedRow variable for streaming
		stream.setDeltaDeletedRow(row);
		//TO BE REMOVED
		setDeltaDeletedRow(row);

		// 3. delete row from delta
		Utils.deleteEntireRowWithPK((String)json.get("keyspace"), "delta_" + json.get("table"),  hm[0].toString(), condition.get(hm[0]).toString());
		
		System.out.println("Delete Successful from Delta Table");

		return true;

	}

	public void setDeltaDeletedRow(Row one) {
		deltaDeletedRow = one;
	}

	public Row getDeltaDeletedRow() {
		return deltaDeletedRow;
	}

	public boolean deleteRowPreaggAgg(Row deltaDeletedRow,
			String baseTablePrimaryKey, JSONObject json, String preaggTable,
			String aggKey, String aggKeyType, String aggCol, String aggColType) {

		float count = 0;
		float sum = 0;
		float average = 0;
		float min = Float.MAX_VALUE;
		float max = -Float.MAX_VALUE;

		// 1. retrieve agg key value from delta stream to retrieve the correct
		// row from preagg
		String aggKeyValue = "";
		float aggColValue = 0;

		switch (aggKeyType) {

		case "text":
			aggKeyValue = "'" + deltaDeletedRow.getString(aggKey + "_new")
			+ "'";
			break;

		case "int":
			aggKeyValue = "" + deltaDeletedRow.getInt(aggKey + "_new") + "";
			break;

		case "varint":
			aggKeyValue = "" + deltaDeletedRow.getVarint(aggKey + "_new") + "";
			break;

		case "float":
			aggKeyValue = "" + deltaDeletedRow.getFloat(aggKey + "_new") + "";
			break;
		}

		// 1.b Retrieve row key from delta stream
		String pk = "";
		switch (deltaDeletedRow.getColumnDefinitions().asList().get(0)
				.getType().toString()) {

				case "text":
					pk = deltaDeletedRow.getString(0);
					break;

				case "int":
					pk = Integer.toString(deltaDeletedRow.getInt(0));
					break;

				case "varint":
					pk = deltaDeletedRow.getVarint(0).toString();
					break;

				case "varchar":
					pk = deltaDeletedRow.getString(0);
					break;

				case "float":
					pk = Float.toString(deltaDeletedRow.getFloat(0));
					break;
		}

		// 2. select row with aggkeyValue from delta stream
		StringBuilder selectPreaggQuery1 = new StringBuilder("SELECT ")
		.append(aggKey + ", list_item, ").append("sum, ")
		.append("count, ").append("average, min, max ");
		selectPreaggQuery1.append(" FROM ")
		.append((String) json.get("keyspace")).append(".")
		.append(preaggTable).append(" where ").append(aggKey + " = ")
		.append(aggKeyValue).append(";");

		System.out.println(selectPreaggQuery1);

		// 2.b execute select statement
		ResultSet PreAggMap;
		try {

			Session session = currentCluster.connect();
			PreAggMap = session.execute(selectPreaggQuery1.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		Row theRow = PreAggMap.one();
		if (theRow != null) {

			// 3.check size of map for given agg key
			// if map.size is 1 then whole row can be deleted
			// if map.size is larger than 1 then iterate over map & delete
			// desired entry with the correct pk as key

			Map<String, String> tempMapImmutable = theRow.getMap("list_item",
					String.class, String.class);

			System.out.println(tempMapImmutable);
			Map<String, String> myMap = new HashMap<String, String>();
			myMap.putAll(tempMapImmutable);

			if (myMap.size() == 1) {
				// 4. delete the whole row

				setDeletePreaggRowDeleted(theRow);

				deleteEntireRowWithPK((String) json.get("keyspace"),
						preaggTable, aggKey, aggKeyValue);
			} else {

				// 5.a remove entry from map with that pk
				myMap.remove(pk);

				// 5.b retrieve aggCol value
				switch (aggColType) {

				case "int":
					aggColValue = deltaDeletedRow.getInt(aggCol + "_new");
					break;

				case "varint":
					aggColValue = deltaDeletedRow.getVarint(aggCol + "_new")
					.floatValue();
					break;

				case "float":
					aggColValue = deltaDeletedRow.getFloat(aggCol + "_new");
					break;
				}

				// 5.c adjust sum,count,average values
				count = myMap.size();
				sum = theRow.getFloat("sum") - aggColValue;
				average = sum / count;

				max = -Float.MAX_VALUE;
				min = Float.MAX_VALUE;

				List<Definition> def = deltaDeletedRow.getColumnDefinitions()
						.asList();

				int aggColIndexInList = 0;

				for (int i = 0; i < def.size(); i++) {
					if (def.get(i).getName().contentEquals(aggCol + "_new")) {
						break;
					}
					if (def.get(i).getName().contains("_new"))
						aggColIndexInList++;
				}

				for (Map.Entry<String, String> entry : myMap.entrySet()) {

					String list = entry.getValue().replaceAll("\\[", "")
							.replaceAll("\\]", "");
					String[] listArray = list.split(",");
					if (Float.valueOf(listArray[aggColIndexInList]) < min)
						min = Float.valueOf(listArray[aggColIndexInList]);

					if (Float.valueOf(listArray[aggColIndexInList]) > max)
						max = Float.valueOf(listArray[aggColIndexInList]);
				}

				// 6. Execute insertion statement of the row with the
				// aggKeyValue_old to refelect changes

				StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
				insertQueryAgg.append((String) json.get("keyspace"))
				.append(".").append(preaggTable).append(" ( ")
				.append(aggKey + ", ").append("list_item, ")
				.append("sum, count, average, min, max")
				.append(") VALUES (").append(aggKeyValue + ", ")
				.append("?, ?, ?, ?, ?, ?);");

				Session session1 = currentCluster.connect();

				PreparedStatement statement1 = session1.prepare(insertQueryAgg
						.toString());
				BoundStatement boundStatement = new BoundStatement(statement1);
				session1.execute(boundStatement.bind(myMap, sum, (int) count,
						average, min, max));
				System.out.println(boundStatement.toString());

				// Selection to set DeleteRowDelete variable

				StringBuilder selectPreaggQuery2 = new StringBuilder("SELECT ")
				.append(aggKey + ",").append("list_item, sum, ")
				.append("count, ").append("average, min, max ");
				selectPreaggQuery2.append(" FROM ")
				.append((String) json.get("keyspace")).append(".")
				.append(preaggTable).append(" where ")
				.append(aggKey + " = ").append(aggKeyValue).append(";");

				System.out.println(selectPreaggQuery2);

				try {

					Session session = currentCluster.connect();
					setDeletePreaggRow((session.execute(selectPreaggQuery2
							.toString()).one()));
					setDeletePreaggRowDeleted(null);

				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}

			}
		}

		System.out.println("Done deleting from preagg and agg table");

		return true;
	}

	public boolean deleteEntireRowWithPK(String keyspace, String tableName,
			String pk, String pkValue) {

		StringBuilder deleteQuery = new StringBuilder("delete from ");
		deleteQuery.append(keyspace).append(".").append(tableName)
		.append(" WHERE ").append(pk + " = ").append(pkValue)
		.append(";");

		System.out.println(deleteQuery.toString());
		try {

			Session session = currentCluster.connect();
			session.execute(deleteQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public boolean updatePreaggregation(Row deltaUpdatedRow, String aggKey,
			String aggKeyType, JSONObject json, String preaggTable,
			String baseTablePrimaryKey, String aggCol, String aggColType,
			boolean override, boolean mapSize1) {

		// 1. check if the aggKey has been updated or not from delta Stream
		// given as input
		// sameKeyValue = true , if 1) aggkey hasnt been updated or 2) for first
		// insertion where _old value is null
		// 1.b save aggKeyValue in loop

		boolean sameKeyValue = false;
		String aggKeyValue = "";
		String aggKeyValue_old = "";
		ColumnDefinitions colDef = null;
		float average = 0;
		float sum = 0;
		float count = 0;
		float aggColValue = 0;
		float min = Float.MAX_VALUE;
		float max = -Float.MAX_VALUE;

		if (deltaUpdatedRow != null) {
			colDef = deltaUpdatedRow.getColumnDefinitions();
			int indexNew = colDef.getIndexOf(aggKey + "_new");
			int indexOld = colDef.getIndexOf(aggKey + "_old");

			switch (aggKeyType) {

			case "text":
				if (deltaUpdatedRow.getString(indexNew).equals(
						deltaUpdatedRow.getString(indexOld))
						|| deltaUpdatedRow.isNull(indexOld)) {
					sameKeyValue = true;
				}
				aggKeyValue = "'" + deltaUpdatedRow.getString(indexNew) + "'";
				aggKeyValue_old = "'" + deltaUpdatedRow.getString(indexOld)
						+ "'";
				break;

			case "int":
				if (deltaUpdatedRow.getInt(indexNew) == (deltaUpdatedRow
						.getInt(indexOld)) || deltaUpdatedRow.isNull(indexOld)) {
					sameKeyValue = true;
				}
				aggKeyValue = "" + deltaUpdatedRow.getInt(indexNew) + "";
				aggKeyValue_old = "" + deltaUpdatedRow.getInt(indexOld) + "";
				break;

			case "varint":
				if (deltaUpdatedRow.getVarint(indexNew) == (deltaUpdatedRow
						.getVarint(indexOld))
						|| deltaUpdatedRow.isNull(indexOld)) {
					sameKeyValue = true;
				}
				aggKeyValue = "" + deltaUpdatedRow.getVarint(indexNew) + "";
				aggKeyValue_old = "" + deltaUpdatedRow.getVarint(indexOld) + "";
				break;

			case "varchar":
				if (deltaUpdatedRow.getString(indexNew).equals(
						deltaUpdatedRow.getString(indexOld))
						|| deltaUpdatedRow.isNull(indexOld)) {
					sameKeyValue = true;
				}
				aggKeyValue = "'" + deltaUpdatedRow.getString(indexNew) + "'";
				aggKeyValue_old = "'" + deltaUpdatedRow.getString(indexOld)
						+ "'";
				break;

			case "float":
				if (deltaUpdatedRow.getFloat(indexNew) == (deltaUpdatedRow
						.getFloat(indexOld))
						|| deltaUpdatedRow.isNull(indexOld)) {
					sameKeyValue = true;
				}
				aggKeyValue = "" + deltaUpdatedRow.getFloat(indexNew) + "";
				aggKeyValue_old = "" + deltaUpdatedRow.getFloat(indexOld) + "";

				break;
			}

		}

		// 1.b get all column values (_new) from delta table + including pk
		// & save them in myList

		ArrayList<String> myList = new ArrayList<String>();
		List<Definition> def = colDef.asList();

		int aggColIndexInList = 0;

		for (int i = 0; i < def.size(); i++) {
			if (def.get(i).getName().contentEquals(aggCol + "_new")) {
				aggColIndexInList = i;
				break;
			}
		}

		for (int i = 0; i < def.size(); i++) {

			switch (def.get(i).getType().toString()) {

			case "text":
				if (!def.get(i).getName().contains("_old")) {
					myList.add(deltaUpdatedRow.getString(i));
				}

				break;

			case "int":
				if (!def.get(i).getName().contains("_old")) {
					myList.add("" + deltaUpdatedRow.getInt(i) + "");
				}
				break;

			case "varint":
				if (!def.get(i).getName().contains("_old")) {
					myList.add("" + deltaUpdatedRow.getVarint(i) + "");
				}
				break;

			case "varchar":
				if (!def.get(i).getName().contains("_old")) {
					myList.add(deltaUpdatedRow.getString(i));
				}
				break;

			case "float":
				if (!def.get(i).getName().contains("_old")) {
					myList.add("" + deltaUpdatedRow.getFloat(i) + "");
				}
				break;
			}

		}

		// 1.c Get the old and new aggCol value from delta table (_new), (_old)

		int aggColIndexNew;
		int aggColIndexOld;
		float aggColValue_old = 0;

		if (deltaUpdatedRow != null) {
			colDef = deltaUpdatedRow.getColumnDefinitions();
			aggColIndexNew = colDef.getIndexOf(aggCol + "_new");
			aggColIndexOld = colDef.getIndexOf(aggCol + "_old");

			switch (aggColType) {

			case "int":
				aggColValue = deltaUpdatedRow.getInt(aggColIndexNew);
				aggColValue_old = deltaUpdatedRow.getInt(aggColIndexOld);

				break;

			case "varint":
				aggColValue = deltaUpdatedRow.getVarint(aggColIndexNew)
				.floatValue();
				BigInteger temp = deltaUpdatedRow.getVarint(aggColIndexOld);

				if (temp != null) {
					aggColValue_old = temp.floatValue();
				} else {
					aggColValue_old = 0;
				}

				break;

			case "float":
				aggColValue = deltaUpdatedRow.getFloat(aggColIndexNew);
				aggColValue_old = deltaUpdatedRow.getFloat(aggColIndexOld);
				break;
			}
		}

		// 2. if AggKey hasnt been updated or first insertion
		if (sameKeyValue || override) {

			// 2.a select from preagg table row with AggKey as PK

			StringBuilder selectPreaggQuery1 = new StringBuilder("SELECT ")
			.append("list_item, ").append("sum, ").append("count, ")
			.append("average, min, max ");
			selectPreaggQuery1.append(" FROM ")
			.append((String) json.get("keyspace")).append(".")
			.append(preaggTable).append(" where ")
			.append(aggKey + " = ").append(aggKeyValue).append(";");

			System.out.println(selectPreaggQuery1);

			// 2.b execute select statement
			ResultSet PreAggMap;
			try {

				Session session = currentCluster.connect();
				PreAggMap = session.execute(selectPreaggQuery1.toString());

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

			Row theRow1 = PreAggMap.one();

			HashMap<String, String> myMap = new HashMap<>();
			// 2.c If row retrieved is null, then this is the first insertion
			// for this given Agg key
			if (theRow1 == null) {

				// 2.c.1 create a map, add pk and list with delta _new values
				String pk = myList.get(0);
				myList.remove(0);
				myMap.put(pk, myList.toString());

				// 2.c.2 set the agg col values
				sum += aggColValue;
				count = 1;
				average = sum / count;
				min = aggColValue;
				max = aggColValue;

			} else {

				// 2.d If row is not null, then this is not the first insertion
				// for this agg Key
				Map<String, String> tempMapImmutable = theRow1.getMap(
						"list_item", String.class, String.class);

				System.out.println(tempMapImmutable);
				myMap.putAll(tempMapImmutable);

				int prev_count = myMap.keySet().size();

				String pk = myList.get(0);
				myList.remove(0);
				myMap.put(pk, myList.toString());

				// 2.e set agg col values

				if (myMap.size() != 1) {
					count = myMap.keySet().size();

					if (count > prev_count)
						sum = theRow1.getFloat("sum") + aggColValue;
					else
						sum = theRow1.getFloat("sum") - aggColValue_old
						+ aggColValue;

					average = sum / count;


					max = -Float.MAX_VALUE;
					min = Float.MAX_VALUE;

					for (Map.Entry<String, String> entry : myMap.entrySet()) {
						String list = entry.getValue().replaceAll("\\[", "")
								.replaceAll("\\]", "");
						String[] listArray = list.split(",");
						if (Float.valueOf(listArray[aggColIndexInList - 1]) < min)
							min = Float
							.valueOf(listArray[aggColIndexInList - 1]);

						if (Float.valueOf(listArray[aggColIndexInList - 1]) > max)
							max = Float
							.valueOf(listArray[aggColIndexInList - 1]);
					}

				} else {
					// 2.c.2 set the agg col values
					sum = aggColValue;
					count = 1;
					average = sum / count;
					min = aggColValue;
					max = aggColValue;
				}

			}
			try {

				// 3. execute the insertion
				StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
				insertQueryAgg.append((String) json.get("keyspace"))
				.append(".").append(preaggTable).append(" ( ")
				.append(aggKey + ", ").append("list_item, ")
				.append("sum, count, average, min, max")
				.append(") VALUES (").append(aggKeyValue + ", ")
				.append("?, ?, ?, ?, ?, ?);");

				Session session1 = currentCluster.connect();

				PreparedStatement statement1 = session1.prepare(insertQueryAgg
						.toString());
				BoundStatement boundStatement = new BoundStatement(statement1);
				session1.execute(boundStatement.bind(myMap, sum, (int) count,
						average, min, max));
				System.out.println(boundStatement.toString());

				// =======================================
				// 4.Retrieve row from preagg

				StringBuilder selectPreaggQuery2 = new StringBuilder("SELECT ")
				.append(aggKey + ", ").append("list_item, sum, ")
				.append("count, ").append("average, min, max ");
				selectPreaggQuery2.append(" from ")
				.append((String) json.get("keyspace")).append(".")
				.append(preaggTable).append(" where ")
				.append(aggKey + " = ").append(aggKeyValue).append(";");

				System.out.println(selectPreaggQuery2);

				try {
					Session session = currentCluster.connect();
					setUpdatedPreaggRow(session.execute(
							selectPreaggQuery2.toString()).one());
					if (!override) {
						setUpdatedPreaggRowChangeAK(null);
						setUpdatedPreaggRowDeleted(null);
					}
					if (!mapSize1) {
						setUpdatedPreaggRowDeleted(null);
					}
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		} else if ((!sameKeyValue && !override)) {

			// 1. retrieve old agg key value from delta stream to retrieve the
			// correct row from preagg
			// was retrieved above in aggKeyValue_old variable

			// 2. select row with old aggkeyValue from delta stream
			StringBuilder selectPreaggQuery1 = new StringBuilder("SELECT ")
			.append("list_item, ").append("sum, ").append("count, ")
			.append("average, min, max");
			selectPreaggQuery1.append(" FROM ")
			.append((String) json.get("keyspace")).append(".")
			.append(preaggTable).append(" where ")
			.append(aggKey + " = ").append(aggKeyValue_old).append(";");

			System.out.println(selectPreaggQuery1);

			// 2.b execute select statement
			ResultSet PreAggMap;
			try {

				Session session = currentCluster.connect();
				PreAggMap = session.execute(selectPreaggQuery1.toString());

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

			Row theRow = PreAggMap.one();
			if (theRow != null) {

				// 3.check size of map for given old agg key
				// if map.size is 1 then whole row can be deleted
				// if map.size is larger than 1 then iterate over map & delete
				// desired entry with the correct pk as key

				Map<String, String> tempMapImmutable = theRow.getMap(
						"list_item", String.class, String.class);

				System.out.println(tempMapImmutable);
				Map<String, String> myMap = new HashMap<String, String>();
				myMap.putAll(tempMapImmutable);

				if (myMap.size() == 1) {

					// Selection to set PreaggRowDeleted

					StringBuilder selectPreaggQuery2 = new StringBuilder(
							"SELECT ").append(aggKey + ",")
							.append("list_item, sum, ").append("count, ")
							.append("average, min, max ");
					selectPreaggQuery2.append(" FROM ")
					.append((String) json.get("keyspace")).append(".")
					.append(preaggTable).append(" where ")
					.append(aggKey + " = ").append(aggKeyValue_old)
					.append(";");

					System.out.println(selectPreaggQuery2);

					try {

						Session session = currentCluster.connect();
						setUpdatedPreaggRowDeleted((session
								.execute(selectPreaggQuery2.toString()).one()));

					} catch (Exception e) {
						e.printStackTrace();
						return false;
					}

					// 4. delete the whole row
					deleteEntireRowWithPK((String) json.get("keyspace"),
							preaggTable, aggKey, aggKeyValue_old);

					// 4.a perform a new insertion with new values
					updatePreaggregation(deltaUpdatedRow, aggKey, aggKeyType,
							json, preaggTable, baseTablePrimaryKey, aggCol,
							aggColType, true, true);

				} else {

					// 5. retrieve the pk value that has to be removed from the
					// map
					String pk = myList.get(0);
					myList.remove(0);

					// 5.a remove entry from map with that pk
					myMap.remove(pk);

					// 5.c adjust sum,count,average values
					count = myMap.size();
					sum = theRow.getFloat("sum") - aggColValue_old;
					average = sum / count;

					max = -Float.MAX_VALUE;
					min = Float.MAX_VALUE;

					for (Map.Entry<String, String> entry : myMap.entrySet()) {
						String list = entry.getValue().replaceAll("\\[", "")
								.replaceAll("\\]", "");
						String[] listArray = list.split(",");
						if (Float.valueOf(listArray[aggColIndexInList - 1]) < min)
							min = Float
							.valueOf(listArray[aggColIndexInList - 1]);

						if (Float.valueOf(listArray[aggColIndexInList - 1]) > max)
							max = Float
							.valueOf(listArray[aggColIndexInList - 1]);
					}

					// 6. Execute insertion statement of the row with the
					// aggKeyValue_old to refelect changes

					StringBuilder insertQueryAgg = new StringBuilder(
							"INSERT INTO ");
					insertQueryAgg.append((String) json.get("keyspace"))
					.append(".").append(preaggTable).append(" ( ")
					.append(aggKey + ", ").append("list_item, ")
					.append("sum, count, average, min, max")
					.append(") VALUES (")
					.append(aggKeyValue_old + ", ")
					.append("?, ?, ?, ?, ?, ?);");

					Session session1 = currentCluster.connect();

					PreparedStatement statement1 = session1
							.prepare(insertQueryAgg.toString());
					BoundStatement boundStatement = new BoundStatement(
							statement1);
					session1.execute(boundStatement.bind(myMap, sum,
							(int) count, average, min, max));
					System.out.println(boundStatement.toString());

					// perform a new insertion for the new aggkey given in json
					updatePreaggregation(deltaUpdatedRow, aggKey, aggKeyType,
							json, preaggTable, baseTablePrimaryKey, aggCol,
							aggColType, true, false);

					// Selection to set updatedRow

					StringBuilder selectPreaggQuery2 = new StringBuilder(
							"SELECT ").append(aggKey + ",")
							.append("list_item, sum, ").append("count, ")
							.append("average, min, max ");
					selectPreaggQuery2.append(" FROM ")
					.append((String) json.get("keyspace")).append(".")
					.append(preaggTable).append(" where ")
					.append(aggKey + " = ").append(aggKeyValue_old)
					.append(";");

					System.out.println(selectPreaggQuery2);

					try {

						Session session = currentCluster.connect();
						setUpdatedPreaggRowChangeAK((session
								.execute(selectPreaggQuery2.toString()).one()));

					} catch (Exception e) {
						e.printStackTrace();
						return false;
					}
				}

			}

		}

		return true;
	}

	public void updateReverseJoin(JSONObject json, int cursor, int nrOfTables,
			String joinTable, List<String> baseTables, String joinKeyName,
			String tableName, String keyspace, String joinKeyType, int column) {

		setReverseJoinName(joinTable);
		setUpdatedPreaggRowDeleted(null);

		StringBuilder selectQuery = new StringBuilder();

		JSONObject data;

		// insert
		if (json.get("data") != null) {
			data = (JSONObject) json.get("data");
			// update
		} else {
			data = (JSONObject) json.get("set_data");
		}

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_old");

		selectQuery.append("SELECT * FROM ").append(keyspace).append(".")
		.append(joinTable).append(" WHERE ").append(joinKeyName)
		.append(" = ").append(joinKeyValue).append(";");

		System.out.println(selectQuery);

		Session session = null;
		ResultSet queryResults = null;

		try {

			session = currentCluster.connect();
			queryResults = session.execute(selectQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();

		}

		Row theRow = queryResults.one();

		StringBuilder insertQuery = new StringBuilder("INSERT INTO ")
		.append(keyspace).append(".").append(joinTable).append(" (")
		.append(joinKeyName).append(", ").append("list_item" + column)
		.append(") VALUES (").append(joinKeyValue).append(", ?);");

		ArrayList<String> myList = new ArrayList<String>();

		List<Definition> def = deltaUpdatedRow.getColumnDefinitions().asList();

		for (int i = 0; i < def.size(); i++) {

			switch (def.get(i).getType().toString()) {

			case "text":
				if (!def.get(i).getName().contains("_old")) {
					myList.add("'" + deltaUpdatedRow.getString(i) + "'");
				}

				break;

			case "int":
				if (!def.get(i).getName().contains("_old")) {
					myList.add("" + deltaUpdatedRow.getInt(i));
				}
				break;

			case "varint":
				if (!def.get(i).getName().contains("_old")) {
					myList.add("" + deltaUpdatedRow.getVarint(i));
				}
				break;

			case "varchar":
				if (!def.get(i).getName().contains("_old")) {
					myList.add("'" + deltaUpdatedRow.getString(i) + "'");
				}
				break;

			case "float":
				if (!def.get(i).getName().contains("_old")) {
					myList.add("" + deltaUpdatedRow.getFloat(i));
				}
				break;
			}

		}

		HashMap<String, String> myMap = new HashMap<String, String>();
		String pk = myList.get(0);
		myList.remove(0);
		// myList.remove(joinKeyValue);
		myMap.put(pk, myList.toString());

		// already exists
		if (theRow != null) {

			Map<String, String> tempMapImmutable = theRow.getMap("list_item"
					+ column, String.class, String.class);

			System.out.println(tempMapImmutable);

			myMap.putAll(tempMapImmutable);
			myMap.put(pk, myList.toString());

		}

		// change in join key value
		if (!oldJoinKeyValue.equals("null")
				&& !oldJoinKeyValue.equals("'null'")
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			StringBuilder selectQuery1 = new StringBuilder();
			selectQuery1.append("SELECT * FROM ").append(keyspace).append(".")
			.append(joinTable).append(" WHERE ").append(joinKeyName)
			.append(" = ").append(oldJoinKeyValue).append(";");

			session = currentCluster.connect();
			queryResults = session.execute(selectQuery1.toString());

			// The row that contains the old join key value
			Row row_old_join_value = queryResults.one();

			setReverseJoinOldUpdateRow(row_old_join_value);
			StringBuilder insertQuery2 = new StringBuilder("INSERT INTO ")
			.append(keyspace).append(".").append(joinTable)
			.append(" (").append(joinKeyName).append(", ")
			.append("list_item" + column).append(") VALUES (")
			.append(oldJoinKeyValue).append(", ?);");

			Map<String, String> tempMapImmutable2 = row_old_join_value.getMap(
					"list_item" + column, String.class, String.class);

			HashMap<String, String> myMap2 = new HashMap<String, String>();

			myMap2.putAll(tempMapImmutable2);

			// delete this from the other row
			myMap2.remove(pk);

			PreparedStatement statement = session.prepare(insertQuery2
					.toString());
			BoundStatement boundStatement = new BoundStatement(statement);
			session.execute(boundStatement.bind(myMap2));

			// retrieve and set update old row
			StringBuilder selectQuery2 = new StringBuilder();
			selectQuery2.append("SELECT * FROM ").append(keyspace).append(".")
			.append(joinTable).append(" WHERE ").append(joinKeyName)
			.append(" = ").append(oldJoinKeyValue).append(";");

			session = currentCluster.connect();
			queryResults = session.execute(selectQuery2.toString());

			// The old row that contains the old join key value after being
			// updated
			Row row_after_change_join_value = queryResults.one();

			setReverseJoinUpdatedOldRow_changeJoinKey(row_after_change_join_value);

			// check if all maps are empty --> remove the row
			boolean allNull = true;

			if (myMap2.size() == 0) {

				for (int k = 0; k < baseTables.size() && allNull; k++) {
					if (column != k + 1
							&& row_old_join_value.getMap("list_item" + (k + 1),
									String.class, String.class).size() != 0) {
						allNull = false;

					}
				}
			} else
				allNull = false;

			// all entries are nulls
			if (allNull) {
				StringBuilder deleteQuery = new StringBuilder("delete from ");
				deleteQuery.append(keyspace).append(".").append(joinTable)
				.append(" WHERE ").append(joinKeyName + " = ")
				.append(oldJoinKeyValue).append(";");

				System.out.println(deleteQuery);

				session = currentCluster.connect();
				session.execute(deleteQuery.toString());
			}

		}

		else
			setReverseJoinOldUpdateRow(theRow);

		try {

			session = currentCluster.connect();

			System.out.println(insertQuery);
			PreparedStatement statement = session.prepare(insertQuery
					.toString());
			BoundStatement boundStatement = new BoundStatement(statement);
			session.execute(boundStatement.bind(myMap));

		} catch (Exception e) {
			e.printStackTrace();

		}

		// Set the rj updated row for join updates
		StringBuilder selectQuery1 = new StringBuilder();
		selectQuery1.append("SELECT * FROM ").append(keyspace).append(".")
		.append(joinTable).append(" WHERE ").append(joinKeyName)
		.append(" = ").append(joinKeyValue).append(";");

		System.out.println(selectQuery1);

		session = null;

		try {
			session = currentCluster.connect();
			setReverseJoinUpdatedNewRow(session
					.execute(selectQuery1.toString()).one());

		} catch (Exception e) {
			e.printStackTrace();

		}

		// System.out.println("Old RJ Update Row: ");
		// if(getReverseJoinUpdateOldRow()!=null){
		// System.out.println("join key: "+getReverseJoinUpdateOldRow().getString(joinKeyName));
		// System.out.println("listitem1: "+getReverseJoinUpdateOldRow().getMap(
		// "list_item1", String.class, String.class).toString());
		// System.out.println("listitem2: "+getReverseJoinUpdateOldRow().getMap(
		// "list_item2", String.class, String.class).toString());
		// }
		// else
		// System.out.println("null");
		//
		//
		// System.out.println("New RJ Update Row: ");
		// if(getrjUpdatedRow()!=null){
		// System.out.println("join key: "+getrjUpdatedRow().getString(joinKeyName));
		// System.out.println("listitem1: "+getrjUpdatedRow().getMap(
		// "list_item1", String.class, String.class).toString());
		// System.out.println("listitem2: "+getrjUpdatedRow().getMap(
		// "list_item2", String.class, String.class).toString());
		// }
		// else
		// System.out.println("null");
		//

	}

	public boolean updateJoinController(Row deltaUpdatedRow, String innerJName,
			String leftJName, String rightJName, JSONObject json,
			Boolean updateLeft, Boolean updateRight, String joinKeyType,
			String joinKeyName, String pkName) {

		// 1. get row updated by reverse join
		Row theRow = getrjUpdatedRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1",
				String.class, String.class);
		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2",
				String.class, String.class);

		// 2. retrieve list_item1, list_item2
		HashMap<String, String> myMap1 = new HashMap<String, String>();
		myMap1.putAll(tempMapImmutable1);

		HashMap<String, String> myMap2 = new HashMap<String, String>();
		myMap2.putAll(tempMapImmutable2);

		// 3. Check if the join key value changed

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_old");

		boolean changeJoinKey = !oldJoinKeyValue.equals("null")
				&& !oldJoinKeyValue.equals("'null'")
				&& !joinKeyValue.equals(oldJoinKeyValue);

		// Case 1 : update left join table
		// !leftJName.equals(false) meaning : no left join wanted, only right
		if (updateLeft && myMap2.size() == 0 && !leftJName.equals("false")) {
			updateLeftJoinTable(leftJName, theRow, json);

			if (changeJoinKey && getReverseJoinUpdateOldRow() != null) {
				removeLeftCrossRight(json, innerJName);
				if (!rightJName.equals("false")
						&& getReverseJoinUpdateOldRow().getMap("list_item2",
								String.class, String.class).size() > 0) {

					addAllToRightJoinTable(
							rightJName,
							getReverseJoinUpdateOldRow().getMap("list_item2",
									String.class, String.class), json);
				}
			}

			return true;
		}

		// Case 2: update right join table
		// !rightName.equals(false) meaning : no right join wanted, only left
		if (updateRight && myMap1.size() == 0 && !rightJName.equals("false")) {
			updateRightJoinTable(rightJName, theRow, json);

			if (changeJoinKey && getReverseJoinUpdateOldRow() != null) {
				removeRightCrossLeft(json, innerJName);
				if (!leftJName.equals("false")
						&& getReverseJoinUpdateOldRow().getMap("list_item1",
								String.class, String.class).size() > 0) {

					addAllToLeftJoinTable(
							leftJName,
							getReverseJoinUpdateOldRow().getMap("list_item1",
									String.class, String.class), json);
				}
			}

			return true;
		}

		// Case 6: change in join key value and update in left
		if (changeJoinKey && updateLeft) {
			removeLeftCrossRight(json, innerJName);
			if (!rightJName.equals("false")
					&& getReverseJoinUpdateOldRow().getMap("list_item1",
							String.class, String.class).size() == 1
							&& getReverseJoinUpdateOldRow().getMap("list_item2",
									String.class, String.class).size() > 0) {

				addAllToRightJoinTable(rightJName, getReverseJoinUpdateOldRow()
						.getMap("list_item2", String.class, String.class), json);
			}

			// remove old from left join
			if (!leftJName.equals("false")
					&& getReverseJoinUpdateOldRow().getMap("list_item2",
							String.class, String.class).size() == 0
							&& myMap2.size() > 0) {

				JSONObject data;
				if (json.get("type").equals("insert")) {
					data = (JSONObject) json.get("data");
				} else
					data = (JSONObject) json.get("set_data");

				String pkValue = "(" + data.get(pkName) + ",0)";

				int position = VmXmlHandler.getInstance().getlJSchema()
						.getList("dbSchema.tableDefinition.name")
						.indexOf(leftJName);

				String temp = "dbSchema.tableDefinition(";
				temp += Integer.toString(position);
				temp += ")";

				String joinTablePk = VmXmlHandler.getInstance().getlJSchema()
						.getString(temp + ".primaryKey.name");

				deleteEntireRowWithPK(json.get("keyspace").toString(),
						leftJName, joinTablePk, pkValue);

			}

		}

		// Case 7: change in join key value and update in right
		if (changeJoinKey && updateRight) {
			removeRightCrossLeft(json, innerJName);
			if (!leftJName.equals("false")
					&& getReverseJoinUpdateOldRow().getMap("list_item2",
							String.class, String.class).size() == 1
							&& getReverseJoinUpdateOldRow().getMap("list_item1",
									String.class, String.class).size() > 0) {

				addAllToLeftJoinTable(leftJName, getReverseJoinUpdateOldRow()
						.getMap("list_item1", String.class, String.class), json);
			}

			// remove old from right join
			if (!rightJName.equals("false")
					&& getReverseJoinUpdateOldRow().getMap("list_item1",
							String.class, String.class).size() == 0
							&& myMap1.size() > 0) {

				JSONObject data;
				if (json.get("type").equals("insert")) {
					data = (JSONObject) json.get("data");
				} else
					data = (JSONObject) json.get("set_data");

				String pkValue = "(0," + data.get(pkName) + ")";

				int position = VmXmlHandler.getInstance().getrJSchema()
						.getList("dbSchema.tableDefinition.name")
						.indexOf(rightJName);

				String temp = "dbSchema.tableDefinition(";
				temp += Integer.toString(position);
				temp += ")";

				String joinTablePk = VmXmlHandler.getInstance().getrJSchema()
						.getString(temp + ".primaryKey.name");

				deleteEntireRowWithPK(json.get("keyspace").toString(),
						rightJName, joinTablePk, pkValue);

			}
		}

		// Case 3: create cross product & save in inner join table
		if (updateLeft && myMap2.size() != 0)
			leftCrossRight(json, innerJName);

		if (updateRight && myMap1.size() != 0)
			rightCrossLeft(json, innerJName);

		// Case 4 : delete row from left join if no longer valid
		if (updateLeft && myMap1.size() == 1)
			deleteFromRightJoinTable(myMap2, rightJName, json, false);

		// Case 5: delete row from left join if no longer valid
		if (updateRight && myMap2.size() == 1)
			deleteFromLeftJoinTable(myMap1, leftJName, json, false);

		return true;
	}

	private boolean deleteFromRightJoinTable(HashMap<String, String> myMap2,
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

				if (fromDelete) {
					JSONObject condition = (JSONObject) json.get("condition");
					Object[] hm = condition.keySet().toArray();

					if (!rightPkValue.equals(condition.get(hm[0]))) {
						myMap2.remove(entry.getKey());
						continue;
					}
				}

				String tuple = "(" + 0 + "," + rightPkValue + ")";

				StringBuilder insertQuery = new StringBuilder("Delete from ");
				insertQuery.append((String) json.get("keyspace")).append(".")
				.append(rightJName).append(" WHERE ");
				insertQuery.append(rightPkName).append(" = ");
				insertQuery.append(tuple).append(";");

				System.out.println(insertQuery);

				try {

					Session session = currentCluster.connect();
					session.execute(insertQuery.toString());
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}

		return true;

	}

	private boolean deleteFromLeftJoinTable(HashMap<String, String> myMap1,
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

				if (fromDelete) {
					JSONObject condition = (JSONObject) json.get("condition");
					Object[] hm = condition.keySet().toArray();

					if (!leftPkValue.equals(condition.get(hm[0]))) {
						myMap1.remove(entry.getKey());
						continue;
					}
				}

				String tuple = "(" + leftPkValue + "," + 0 + ")";

				StringBuilder insertQuery = new StringBuilder("Delete from ");
				insertQuery.append((String) json.get("keyspace")).append(".")
				.append(leftJName).append(" WHERE ");
				insertQuery.append(leftPkName).append(" = ");
				insertQuery.append(tuple).append(";");

				System.out.println(insertQuery);

				try {

					Session session = currentCluster.connect();
					session.execute(insertQuery.toString());
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}
		return true;

	}

	private boolean updateRightJoinTable(String rightJName, Row theRow,
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

			//String rightPkValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, rightPkName, rightPkType, "");

			// 3.a. get from delta row, the left pk value
			switch (rightPkType) {

			case "int":
				rightPkValue = Integer.toString(deltaUpdatedRow
						.getInt(rightPkName));
				break;

			case "float":
				rightPkValue = Float.toString(deltaUpdatedRow
						.getFloat(rightPkName));
				break;

			case "varint":
				rightPkValue = deltaUpdatedRow.getVarint(rightPkName)
				.toString();
				break;

			case "varchar":
				rightPkValue = deltaUpdatedRow.getString(rightPkName);
				break;

			case "text":
				rightPkValue = deltaUpdatedRow.getString(rightPkName);
				break;
			}

			// 3.b. retrieve the left list values for inserton statement
			Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2",
					String.class, String.class);
			HashMap<String, String> myMap2 = new HashMap<String, String>();
			myMap2.putAll(tempMapImmutable2);

			String rightList = myMap2.get(rightPkValue);
			rightList = rightList.replaceAll("\\[", "").replaceAll("\\]", "");

			// insert null values if myMap2 has no entries yet

			String tuple = "(" + 0 + "," + rightPkValue + ")";
			int nrLeftCol = XmlHandler.getInstance().getLeftJoinViewConfig()
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
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

		return true;
	}

	private boolean updateLeftJoinTable(String leftJName, Row theRow,
			JSONObject json) {

		// 3. Read Left Join xml, get leftPkName, leftPkType, get pk of join
		// table & type
		// rightPkName, rightPkType

		int position = XmlHandler.getInstance().getLeftJoinViewConfig()
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
			switch (leftPkType) {

			case "int":
				leftPkValue = Integer.toString(deltaUpdatedRow
						.getInt(leftPkName));
				break;

			case "float":
				leftPkValue = Float.toString(deltaUpdatedRow
						.getFloat(leftPkName));
				break;

			case "varint":
				leftPkValue = deltaUpdatedRow.getVarint(leftPkName).toString();
				break;

			case "varchar":
				leftPkValue = deltaUpdatedRow.getString(leftPkName);
				break;

			case "text":
				leftPkValue = deltaUpdatedRow.getString(leftPkName);
				break;
			}

			// 3.b. retrieve the left list values for inserton statement
			Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1",
					String.class, String.class);
			HashMap<String, String> myMap1 = new HashMap<String, String>();
			myMap1.putAll(tempMapImmutable1);

			String leftList = myMap1.get(leftPkValue);
			leftList = leftList.replaceAll("\\[", "").replaceAll("\\]", "");

			// insert null values if myMap2 has no entries yet

			String tuple = "(" + leftPkValue + "," + 0 + ")";
			int nrRightCol = XmlHandler.getInstance().getLeftJoinViewConfig()
					.getInt(temp + ".nrRightCol");

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
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

		return true;
	}

	private boolean addAllToLeftJoinTable(String leftJName,
			Map<String, String> myMap1, JSONObject json) {

		int position = XmlHandler.getInstance().getLeftJoinViewConfig()
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
				int nrRightCol = XmlHandler.getInstance()
						.getLeftJoinViewConfig().getInt(temp + ".nrRightCol");

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
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}

		}

		return true;

	}

	private boolean addAllToRightJoinTable(String rightJName,
			Map<String, String> myMap2, JSONObject json) {

		int position = XmlHandler.getInstance().getRightJoinViewConfig()
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
				int nrLeftCol = XmlHandler.getInstance()
						.getLeftJoinViewConfig().getInt(temp + ".nrLeftCol");

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
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}

		}
		return true;

	}

	public boolean updateSelection(Row row, String keyspace, String selecTable,
			String selColName) {

		// 1. get column names of delta table
		// 1.b save column values from delta stream too

		StringBuilder insertion = new StringBuilder();
		StringBuilder insertionValues = new StringBuilder();

		List<Definition> colDef = row.getColumnDefinitions().asList();

		for (int i = 0; i < colDef.size(); i++) {

			switch (colDef.get(i).getType().toString()) {

			case "text":
				if (!row.getColumnDefinitions().getName(i).contains("_old")) {
					if (row.getColumnDefinitions().getName(i).contains("_new")) {
						String[] split = row.getColumnDefinitions().getName(i)
								.split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getColumnDefinitions().getName(i)
								+ ", ");
					}

					insertionValues.append("'"
							+ row.getString(row.getColumnDefinitions().getName(
									i)) + "', ");
				}
				break;

			case "int":
				if (!row.getColumnDefinitions().getName(i).contains("_old")) {

					if (row.getColumnDefinitions().getName(i).contains("_new")) {
						String[] split = row.getColumnDefinitions().getName(i)
								.split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getColumnDefinitions().getName(i)
								+ ", ");
					}

					insertionValues.append(row.getInt(row
							.getColumnDefinitions().getName(i)) + ", ");
				}
				break;

			case "varint":
				if (!row.getColumnDefinitions().getName(i).contains("_old")) {

					if (row.getColumnDefinitions().getName(i).contains("_new")) {
						String[] split = row.getColumnDefinitions().getName(i)
								.split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getColumnDefinitions().getName(i)
								+ ", ");
					}
					insertionValues.append(row.getVarint(row
							.getColumnDefinitions().getName(i)) + ", ");
				}
				break;

			case "varchar":
				if (!row.getColumnDefinitions().getName(i).contains("_old")) {

					if (row.getColumnDefinitions().getName(i).contains("_new")) {
						String[] split = row.getColumnDefinitions().getName(i)
								.split("_");
						insertion.append(split[0] + ", ");
					} else {

						insertion.append(row.getColumnDefinitions().getName(i)
								+ ", ");
					}

					insertionValues.append("'"
							+ row.getString(row.getColumnDefinitions().getName(
									i)) + "', ");
				}
				break;

			}
		}

		insertion.deleteCharAt(insertion.length() - 2);
		insertionValues.deleteCharAt(insertionValues.length() - 2);

		try {

			// 1. execute the insertion
			StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
			insertQuery.append(keyspace).append(".").append(selecTable)
			.append(" (").append(insertion).append(") VALUES (")
			.append(insertionValues).append(");");

			System.out.println(insertQuery);

			Session session1 = currentCluster.connect();
			session1.execute(insertQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public boolean deleteRowSelection(Row deltaDeletedRow2, String keyspace,
			String selecTable, String baseTablePrimaryKey, JSONObject json) {

		StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
		deleteQuery.append(keyspace).append(".").append(selecTable)
		.append(" WHERE ").append(baseTablePrimaryKey).append(" = ");

		if (json.containsKey("condition")) {

			JSONObject condition = (JSONObject) json.get("condition");
			Object[] hm = condition.keySet().toArray();
			deleteQuery.append(condition.get(hm[0]));
			deleteQuery.append(";");

		} else {
			JSONObject data = (JSONObject) json.get("data");
			deleteQuery.append(data.get(baseTablePrimaryKey)).append(" ;");
		}

		System.out.println(deleteQuery);

		try {

			Session session = currentCluster.connect();
			session.execute(deleteQuery.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		System.out.println("Possible Deletes Successful from Selection View");

		return true;

	}

	private boolean rightCrossLeft(JSONObject json, String innerJTableName) {

		// 1. get row updated by reverse join
		Row theRow = getrjUpdatedRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1",
				String.class, String.class);
		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2",
				String.class, String.class);

		// 2. retrieve list_item1, list_item2
		HashMap<String, String> myMap1 = new HashMap<String, String>();
		myMap1.putAll(tempMapImmutable1);

		HashMap<String, String> myMap2 = new HashMap<String, String>();
		myMap2.putAll(tempMapImmutable2);

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

			List<String> colName = VmXmlHandler.getInstance().getiJSchema()
					.getList(temp + ".column.name");
			colNames = StringUtils.join(colName, ", ");

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
			switch (rightPkType) {

			case "int":
				rightPkValue = Integer.toString(deltaUpdatedRow
						.getInt(rightPkName));
				break;

			case "float":
				rightPkValue = Float.toString(deltaUpdatedRow
						.getFloat(rightPkName));
				break;

			case "varint":
				rightPkValue = deltaUpdatedRow.getVarint(rightPkName)
				.toString();
				break;

			case "varchar":
				rightPkValue = deltaUpdatedRow.getString(rightPkName);
				break;

			case "text":
				rightPkValue = deltaUpdatedRow.getString(rightPkName);
				break;
			}

			// 3.b. retrieve the left list values for inserton statement
			String rightList = myMap2.get(rightPkValue);
			rightList = rightList.replaceAll("\\[", "").replaceAll("\\]", "");

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

				StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
				insertQuery.append((String) json.get("keyspace")).append(".")
				.append(innerJTableName).append(" (");
				insertQuery.append(joinTablePk).append(", ");
				insertQuery.append(colNames).append(") VALUES (");
				insertQuery.append(tuple).append(", ");

				String list_item_1 = myMap1.get(entry.getKey())
						.replaceAll("\\[", "").replaceAll("\\]", "");
				insertQuery.append(list_item_1).append(", ");

				insertQuery.append(rightList).append(");");

				System.out.println(insertQuery);

				try {

					Session session = currentCluster.connect();
					session.execute(insertQuery.toString());
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}

		return true;
	}

	private boolean removeRightCrossLeft(JSONObject json, String innerJTableName) {

		// 1. get row updated by reverse join
		Row theRow = getReverseJoinUpdateOldRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1",
				String.class, String.class);

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
			switch (rightPkType) {

			case "int":
				rightPkValue = Integer.toString(deltaUpdatedRow
						.getInt(rightPkName));
				break;

			case "float":
				rightPkValue = Float.toString(deltaUpdatedRow
						.getFloat(rightPkName));
				break;

			case "varint":
				rightPkValue = deltaUpdatedRow.getVarint(rightPkName)
				.toString();
				break;

			case "varchar":
				rightPkValue = deltaUpdatedRow.getString(rightPkName);
				break;

			case "text":
				rightPkValue = deltaUpdatedRow.getString(rightPkName);
				break;
			}

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
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}

		return true;
	}

	private boolean leftCrossRight(JSONObject json, String innerJTableName) {

		// 1. get row updated by reverse join
		Row theRow = getrjUpdatedRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1",
				String.class, String.class);
		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2",
				String.class, String.class);

		// 2. retrieve list_item1, list_item2
		HashMap<String, String> myMap1 = new HashMap<String, String>();
		myMap1.putAll(tempMapImmutable1);

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

			List<String> colName = VmXmlHandler.getInstance().getiJSchema()
					.getList(temp + ".column.name");
			colNames = StringUtils.join(colName, ", ");

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
			switch (leftPkType) {

			case "int":
				leftPkValue = Integer.toString(deltaUpdatedRow
						.getInt(leftPkName));
				break;

			case "float":
				leftPkValue = Float.toString(deltaUpdatedRow
						.getFloat(leftPkName));
				break;

			case "varint":
				leftPkValue = deltaUpdatedRow.getVarint(leftPkName).toString();
				break;

			case "varchar":
				leftPkValue = deltaUpdatedRow.getString(leftPkName);
				break;

			case "text":
				leftPkValue = deltaUpdatedRow.getString(leftPkName);
				break;
			}

			// 3.b. retrieve the left list values for inserton statement
			String leftList = myMap1.get(leftPkValue);
			leftList = leftList.replaceAll("\\[", "").replaceAll("\\]", "");

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

				StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
				insertQuery.append((String) json.get("keyspace")).append(".")
				.append(innerJTableName).append(" (");
				insertQuery.append(joinTablePk).append(", ");
				insertQuery.append(colNames).append(") VALUES (");
				insertQuery.append(tuple).append(", ");
				insertQuery.append(leftList).append(", ");

				String list_item_2 = myMap2.get(entry.getKey())
						.replaceAll("\\[", "").replaceAll("\\]", "");
				insertQuery.append(list_item_2).append(");");

				System.out.println(insertQuery);

				try {

					Session session = currentCluster.connect();
					session.execute(insertQuery.toString());
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}

		return true;
	}

	private boolean removeLeftCrossRight(JSONObject json, String innerJTableName) {

		// 1. get old row updated by reverse join
		Row theRow = getReverseJoinUpdateOldRow();

		// 1.a get columns item_2

		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2",
				String.class, String.class);

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
			switch (leftPkType) {

			case "int":
				leftPkValue = Integer.toString(deltaUpdatedRow
						.getInt(leftPkName));
				break;

			case "float":
				leftPkValue = Float.toString(deltaUpdatedRow
						.getFloat(leftPkName));
				break;

			case "varint":
				leftPkValue = deltaUpdatedRow.getVarint(leftPkName).toString();
				break;

			case "varchar":
				leftPkValue = deltaUpdatedRow.getString(leftPkName);
				break;

			case "text":
				leftPkValue = deltaUpdatedRow.getString(leftPkName);
				break;
			}

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

				StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
				deleteQuery.append((String) json.get("keyspace")).append(".")
				.append(innerJTableName).append(" WHERE ");
				deleteQuery.append(joinTablePk).append(" = ");
				deleteQuery.append(tuple).append(";");

				System.out.println(deleteQuery);

				try {

					Session session = currentCluster.connect();
					session.execute(deleteQuery.toString());
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}

		return true;
	}

	// JSONObject json, int cursor, int nrOfTables, String joinTable,
	// List<String> baseTables, String joinKeyName,
	// String tableName, String keyspace, String aggKeyType, int column
	public void deleteReverseJoin(JSONObject json, int cursor, int nrOfTables,
			String joinTable, List<String> baseTables, String joinKeyName,
			String tableName, String keyspace, String joinKeyType, int column) {

		setReverseJoinName(joinTable);

		StringBuilder selectQuery = new StringBuilder();

		String joinKeyValue = null;

		switch (joinKeyType) {
		case "text":

			joinKeyValue = ("'"
					+ deltaDeletedRow.getString(joinKeyName + "_new") + "'");

			break;

		case "int":

			joinKeyValue = ("" + deltaDeletedRow.getInt(joinKeyName + "_new"));

			break;

		case "varint":

			joinKeyValue = ("" + deltaDeletedRow
					.getVarint(joinKeyName + "_new"));

			break;

		case "varchar":

			joinKeyValue = ("" + deltaDeletedRow
					.getString(joinKeyName + "_new"));

			break;

		case "float":

			joinKeyValue = ("" + deltaDeletedRow.getFloat(joinKeyName + "_new"));

			break;
		}

		selectQuery.append("SELECT * FROM ").append(keyspace).append(".")
		.append(joinTable).append(" WHERE ").append(joinKeyName)
		.append(" = ").append(joinKeyValue).append(";");

		System.out.println(selectQuery);

		Session session = null;
		ResultSet queryResults = null;

		try {

			session = currentCluster.connect();
			queryResults = session.execute(selectQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();

		}

		Row theRow = queryResults.one();

		setRevereJoinDeleteOldRow(theRow);

		StringBuilder insertQuery = new StringBuilder("INSERT INTO ")
		.append(keyspace).append(".").append(joinTable).append(" (")
		.append(joinKeyName).append(", ").append("list_item" + column)
		.append(") VALUES (").append(joinKeyValue).append(", ?);");

		HashMap<String, String> myMap = null;
		String pk = "";
		switch (deltaDeletedRow.getColumnDefinitions().asList().get(0)
				.getType().toString()) {

				case "text":
					pk = deltaDeletedRow.getString(0);
					break;

				case "int":
					pk = Integer.toString(deltaDeletedRow.getInt(0));
					break;

				case "varint":
					pk = deltaDeletedRow.getVarint(0).toString();
					break;

				case "varchar":
					pk = deltaDeletedRow.getString(0);
					break;

				case "float":
					pk = Float.toString(deltaDeletedRow.getFloat(0));
					break;
		}

		// already exists
		if (theRow != null) {

			Map<String, String> tempMapImmutable = theRow.getMap("list_item"
					+ column, String.class, String.class);

			System.out.println(tempMapImmutable);

			if (tempMapImmutable.size() > 1) {
				myMap = new HashMap<String, String>();
				myMap.putAll(tempMapImmutable);
				myMap.remove(pk);
			}

			try {

				session = currentCluster.connect();

				System.out.println(insertQuery);

				PreparedStatement statement = session.prepare(insertQuery
						.toString());
				BoundStatement boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(myMap));

			} catch (Exception e) {
				e.printStackTrace();

			}

			// checking if all list items are null --> delete the whole row
			boolean allNull = true;
			if (myMap == null) {

				for (int k = 0; k < baseTables.size() && allNull; k++) {
					if (column != k + 1
							&& theRow.getMap("list_item" + (k + 1),
									String.class, String.class).size() != 0) {
						allNull = false;

					}
				}
			} else {
				allNull = false;
			}

			// all entries are nulls
			if (allNull) {
				StringBuilder deleteQuery = new StringBuilder("delete from ");
				deleteQuery.append(keyspace).append(".").append(joinTable)
				.append(" WHERE ").append(joinKeyName + " = ")
				.append(joinKeyValue).append(";");

				System.out.println(deleteQuery);

				session = currentCluster.connect();
				session.execute(deleteQuery.toString());
			}

		}

		// get new deleted row from rj
		StringBuilder selectQuery1 = new StringBuilder();
		selectQuery1.append("SELECT * FROM ").append(keyspace).append(".")
		.append(joinTable).append(" WHERE ").append(joinKeyName)
		.append(" = ").append(joinKeyValue).append(";");

		System.out.println(selectQuery1);

		session = null;

		try {
			session = currentCluster.connect();
			setReverseJoinDeleteNewRow(session.execute(selectQuery1.toString())
					.one());

		} catch (Exception e) {
			e.printStackTrace();

		}

		// System.out.println("Old RJ delete Row: ");
		// if(getRevereJoinDeleteOldRow()!=null){
		// System.out.println("join key: "+getRevereJoinDeleteOldRow().getString(joinKeyName));
		// System.out.println("listitem1: "+getRevereJoinDeleteOldRow().getMap(
		// "list_item1", String.class, String.class).toString());
		// System.out.println("listitem2: "+getRevereJoinDeleteOldRow().getMap(
		// "list_item2", String.class, String.class).toString());
		// }
		// else
		// System.out.println("null");
		//
		//
		// System.out.println("New RJ delete Row: ");
		// if(getReverseJoinDeleteNewRow()!=null){
		// System.out.println("join key: "+getReverseJoinDeleteNewRow().getString(joinKeyName));
		// System.out.println("listitem1: "+getReverseJoinDeleteNewRow().getMap(
		// "list_item1", String.class, String.class).toString());
		// System.out.println("listitem2: "+getReverseJoinDeleteNewRow().getMap(
		// "list_item2", String.class, String.class).toString());
		// }
		// else
		// System.out.println("null");
		//

	}

	private void setReverseJoinOldUpdateRow(Row theRow) {
		reverseJoinUpadteOldRow = theRow;
	}

	public Row getReverseJoinUpdateOldRow() {
		return reverseJoinUpadteOldRow;
	}

	public boolean deleteJoinController(Row deltaDeletedRow, String innerJName,
			String leftJName, String rightJName, JSONObject json,
			Boolean updateLeft, Boolean updateRight) {

		// 1. get row updated by reverse join
		Row theRow = getRevereJoinDeleteOldRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1",
				String.class, String.class);
		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2",
				String.class, String.class);

		// 2. retrieve list_item1, list_item2
		HashMap<String, String> myMap1 = new HashMap<String, String>();
		myMap1.putAll(tempMapImmutable1);

		HashMap<String, String> myMap2 = new HashMap<String, String>();
		myMap2.putAll(tempMapImmutable2);

		// Case 1 : delete from left join table if item_list2 is empty
		// !leftJName.equals(false) meaning : no left join wanted, only right
		if (updateLeft && myMap2.size() == 0 && !leftJName.equals("false")) {
			deleteFromLeftJoinTable(myMap1, leftJName, json, true);
			return true;
		}

		// Case 2: delete from right join table if item_list1 is empty
		// !rightName.equals(false) meaning : no right join wanted, only left
		if (updateRight && myMap1.size() == 0 && !rightJName.equals("false")) {
			deleteFromRightJoinTable(myMap2, rightJName, json, true);
			return true;
		}

		Row newDeletedRow = getReverseJoinDeleteNewRow();

		// Case 3: delete happened from left and list_item1 is not empty
		// remove cross product from innerjoin
		if (updateLeft && myMap2.size() > 0) {

			removeDeleteLeftCrossRight(json, innerJName, myMap2);

			// delete happened in left and new list_item 1 is empty
			// add all list_item2 to right join
			if (newDeletedRow.getMap("list_item1", String.class, String.class)
					.size() == 0) {
				addAllToRightJoinTable(rightJName, newDeletedRow.getMap(
						"list_item2", String.class, String.class), json);
			}

		}

		// Case 4: delete happened from rigth and list_item2 is not empty
		// remove cross product from inner join
		if (updateRight && myMap1.size() > 0) {

			// removeRightCrossLeft(json, innerJName);
			removeDeleteRightCrossLeft(json, innerJName, myMap1);

			// delete happened in right and new list_item 2 is empty
			// add all list_item1 to left join
			if (newDeletedRow.getMap("list_item2", String.class, String.class)
					.size() == 0) {
				addAllToLeftJoinTable(leftJName, newDeletedRow.getMap(
						"list_item1", String.class, String.class), json);
			}

		}

		return true;

	}

	public boolean removeDeleteLeftCrossRight(JSONObject json,
			String innerJName, Map<String, String> myMap2) {
		String leftPkValue = "";
		switch (deltaDeletedRow.getColumnDefinitions().asList().get(0)
				.getType().toString()) {

				case "text":
					leftPkValue = deltaDeletedRow.getString(0);
					break;

				case "int":
					leftPkValue = Integer.toString(deltaDeletedRow.getInt(0));
					break;

				case "varint":
					leftPkValue = deltaDeletedRow.getVarint(0).toString();
					break;

				case "varchar":
					leftPkValue = deltaDeletedRow.getString(0);
					break;

				case "float":
					leftPkValue = Float.toString(deltaDeletedRow.getFloat(0));
					break;
		}

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

			StringBuilder deleteQuery = new StringBuilder("delete from ");
			deleteQuery.append(json.get("keyspace")).append(".")
			.append(innerJName).append(" WHERE ")
			.append(joinTablePk + " = ").append(tuple).append(";");

			System.out.println(deleteQuery);

			try {

				Session session = currentCluster.connect();
				session.execute(deleteQuery.toString());

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

		}

		return true;

	}

	public boolean removeDeleteRightCrossLeft(JSONObject json,
			String innerJName, Map<String, String> myMap1) {
		String rigthPkValue = "";
		switch (deltaDeletedRow.getColumnDefinitions().asList().get(0)
				.getType().toString()) {

				case "text":
					rigthPkValue = deltaDeletedRow.getString(0);
					break;

				case "int":
					rigthPkValue = Integer.toString(deltaDeletedRow.getInt(0));
					break;

				case "varint":
					rigthPkValue = deltaDeletedRow.getVarint(0).toString();
					break;

				case "varchar":
					rigthPkValue = deltaDeletedRow.getString(0);
					break;

				case "float":
					rigthPkValue = Float.toString(deltaDeletedRow.getFloat(0));
					break;
		}

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

			StringBuilder deleteQuery = new StringBuilder("delete from ");
			deleteQuery.append(json.get("keyspace")).append(".")
			.append(innerJName).append(" WHERE ")
			.append(joinTablePk + " = ").append(tuple).append(";");

			System.out.println(deleteQuery);

			try {

				Session session = currentCluster.connect();
				session.execute(deleteQuery.toString());

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

		}

		return true;

	}

	public Row getReverseJoinDeleteNewRow() {
		return reverseJoinDeleteNewRow;
	}

	public void setReverseJoinDeleteNewRow(Row reverseJoinDeletedNewRow) {
		this.reverseJoinDeleteNewRow = reverseJoinDeletedNewRow;
	}

	public Row getRevereJoinDeleteOldRow() {
		return revereJoinDeleteOldRow;
	}

	public void setRevereJoinDeleteOldRow(Row revereJoinDeletedOldRow) {
		this.revereJoinDeleteOldRow = revereJoinDeletedOldRow;
	}

	public boolean updateHaving(Row deltaUpdatedRow, String keyspace,
			String havingTable, Row preagRow) {

		Map<String, String> myMap = new HashMap<String, String>();

		myMap.putAll(preagRow.getMap("list_item", String.class, String.class));

		float min = preagRow.getFloat("min");
		float max = preagRow.getFloat("max");
		float average = preagRow.getFloat("average");
		int count = preagRow.getInt("count");
		float sum = preagRow.getFloat("sum");

		List<Definition> def = preagRow.getColumnDefinitions().asList();
		String pkName = def.get(0).getName();
		String pkType = def.get(0).getType().toString();
		String pkVAlue = "";

		switch (pkType) {

		case "int":
			pkVAlue = Integer.toString(preagRow.getInt(pkName));
			break;

		case "float":
			pkVAlue = Float.toString(preagRow.getFloat(pkName));
			break;

		case "varint":
			pkVAlue = preagRow.getVarint(pkName).toString();
			break;

		case "varchar":
			pkVAlue = preagRow.getString(pkName);
			break;

		case "text":
			pkVAlue = preagRow.getString(pkName);
			break;
		}

		try {
			// 1. execute the insertion
			StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
			insertQuery.append(keyspace).append(".").append(havingTable)
			.append(" (").append(pkName + ", ").append("list_item, ")
			.append("sum, ").append("count, average, min, max ")
			.append(") VALUES (").append("?, ?, ?, ?, ?, ?, ?);");

			System.out.println(insertQuery);

			Session session1 = currentCluster.connect();

			PreparedStatement statement1 = session1.prepare(insertQuery
					.toString());
			BoundStatement boundStatement = new BoundStatement(statement1);
			session1.execute(boundStatement.bind(pkVAlue, myMap, sum,
					(int) count, average, min, max));

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public boolean updateJoinHaving(String keyspace, String havingTable,
			Row preagRow) {

		float min = preagRow.getFloat("min");
		float max = preagRow.getFloat("max");
		float average = preagRow.getFloat("average");
		int count = preagRow.getInt("count");
		float sum = preagRow.getInt("sum");

		List<Definition> def = preagRow.getColumnDefinitions().asList();
		String pkName = def.get(0).getName();
		String pkType = def.get(0).getType().toString();
		String pkVAlue = "";

		switch (pkType) {

		case "int":
			pkVAlue = "" + (preagRow.getFloat(pkName));
			break;

		case "float":
			pkVAlue = Float.toString(preagRow.getFloat(pkName));
			break;

		case "varint":
			pkVAlue = preagRow.getVarint(pkName).toString();
			break;

		case "varchar":
			pkVAlue = preagRow.getString(pkName);
			break;

		case "text":
			pkVAlue = preagRow.getString(pkName);
			break;
		}

		try {
			// 1. execute the insertion
			StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
			insertQuery.append(keyspace).append(".").append(havingTable)
			.append(" (").append(pkName + ", ").append("sum, ")
			.append("count, average, min, max ").append(") VALUES (")
			.append("?, ?, ?, ?, ?, ?);");

			System.out.println(insertQuery);

			Session session1 = currentCluster.connect();

			PreparedStatement statement1 = session1.prepare(insertQuery
					.toString());
			BoundStatement boundStatement = new BoundStatement(statement1);
			session1.execute(boundStatement.bind(pkVAlue, sum, (int) count,
					average, min, max));

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public boolean deleteFromJoinAgg(Row deltaDeletedRow, JSONObject json,
			String joinAggTableName, String aggKey, String aggKeyType,
			String aggCol, String aggColType, Row oldReverseRow,
			Row newReverseRow, Boolean leftTable) {

		float count = 0;
		float sum = 0;
		float average = 0;
		float min = 9999999;
		float max = -9999999;

		// 1. retrieve agg key value from delta stream to retrieve the correct
		// row from preagg
		String aggKeyValue = "";
		float aggColValue = 0;

		switch (aggKeyType) {

		case "text":
			aggKeyValue = "'" + deltaDeletedRow.getString(aggKey + "_new")
			+ "'";
			break;

		case "int":
			aggKeyValue = "" + deltaDeletedRow.getInt(aggKey + "_new") + "";
			break;

		case "varint":
			aggKeyValue = "" + deltaDeletedRow.getVarint(aggKey + "_new") + "";
			break;

		case "float":
			aggKeyValue = "" + deltaDeletedRow.getFloat(aggKey + "_new") + "";
			break;
		}

		// 2. select row with aggkeyValue from delta stream
		StringBuilder selectPreaggQuery1 = new StringBuilder("SELECT ")
		.append(aggKey).append(", sum, ").append("count, ")
		.append("average, min, max ");
		selectPreaggQuery1.append(" FROM ")
		.append((String) json.get("keyspace")).append(".")
		.append(joinAggTableName).append(" where ")
		.append(aggKey + " = ").append(aggKeyValue).append(";");

		System.out.println(selectPreaggQuery1);

		// 2.b execute select statement
		ResultSet PreAggMap;
		try {

			Session session = currentCluster.connect();
			PreAggMap = session.execute(selectPreaggQuery1.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		Row theRow = PreAggMap.one();
		if (theRow != null) {

			// 3.check size of map for given agg key
			// if map.size is 1 then whole row can be deleted
			// if map.size is larger than 1 then iterate over map & delete
			// desired entry with the correct pk as key

			count = theRow.getInt("count");

			if (count == 1) {

				setDeleteRowJoinAggDeleted(theRow);

				// 4. delete the whole row
				deleteEntireRowWithPK((String) json.get("keyspace"),
						joinAggTableName, aggKey, aggKeyValue);
			} else {

				// 5.b retrieve aggCol value
				switch (aggColType) {

				case "int":
					aggColValue = deltaDeletedRow.getInt(aggCol + "_new");
					break;

				case "varint":
					aggColValue = deltaDeletedRow.getVarint(aggCol + "_new")
					.floatValue();
					break;

				case "float":
					aggColValue = deltaDeletedRow.getFloat(aggCol + "_new");
					break;
				}

				// 5.c adjust sum,count,average values
				count = count - 1;
				sum = theRow.getInt("sum") - aggColValue;
				average = sum / count;

				max = -99999999;
				min = 999999999;

				List<Definition> def = theRow.getColumnDefinitions().asList();

				int aggColIndexInList = 0;

				for (int i = 0; i < def.size(); i++) {
					if (def.get(i).getName().contentEquals(aggCol + "_new")) {
						aggColIndexInList = i;
						break;
					}
				}

				Map<String, String> myMap = new HashMap<String, String>();

				if (leftTable) {
					myMap.putAll(newReverseRow.getMap("list_item1",
							String.class, String.class));

				} else {
					myMap.putAll(newReverseRow.getMap("list_item2",
							String.class, String.class));
				}

				for (Map.Entry<String, String> entry : myMap.entrySet()) {
					String list = entry.getValue().replaceAll("\\[", "")
							.replaceAll("\\]", "");
					String[] listArray = list.split(",");

					if (Float.valueOf(listArray[aggColIndexInList]) < min)
						min = Float.valueOf(listArray[aggColIndexInList]);

					if (Float.valueOf(listArray[aggColIndexInList]) > max)
						max = Float.valueOf(listArray[aggColIndexInList]);
				}

				// 6. Execute insertion statement of the row with the
				// aggKeyValue_old to refelect changes

				StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
				insertQueryAgg.append((String) json.get("keyspace"))
				.append(".").append(joinAggTableName).append(" ( ")
				.append(aggKey + ", ")
				.append("sum, count, average, min, max")
				.append(") VALUES (").append(aggKeyValue + ", ")
				.append("?, ?, ?, ?, ?);");

				Session session1 = currentCluster.connect();

				PreparedStatement statement1 = session1.prepare(insertQueryAgg
						.toString());
				BoundStatement boundStatement = new BoundStatement(statement1);
				session1.execute(boundStatement.bind((int) sum, (int) count,
						average, min, max));
				System.out.println(boundStatement.toString());

				// Selection to set DeleteRowDelete variable

				StringBuilder selectPreaggQuery2 = new StringBuilder("SELECT ")
				.append(aggKey).append(", sum, ").append("count, ")
				.append("average, min, max ");

				selectPreaggQuery2.append(" FROM ")
				.append((String) json.get("keyspace")).append(".")
				.append(joinAggTableName).append(" where ")
				.append(aggKey + " = ").append(aggKeyValue).append(";");

				System.out.println(selectPreaggQuery2);

				try {

					Session session = currentCluster.connect();
					setDeleteRowJoinAgg((session.execute(selectPreaggQuery2
							.toString()).one()));
					setDeleteRowJoinAggDeleted(null);

				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}

			}

		}

		System.out.println("Done deleting from aggregation Table of join");

		return true;
	}

	public Row getReverseJoinUpdatedOldRow_changeJoinKey() {
		return reverseJoinUpdatedOldRow_changeJoinKey;
	}

	public void setReverseJoinUpdatedOldRow_changeJoinKey(
			Row reverseJoinUpdatedOldRow_changeJoinKey) {
		this.reverseJoinUpdatedOldRow_changeJoinKey = reverseJoinUpdatedOldRow_changeJoinKey;
	}

	public boolean deleteRowHaving(Row deltaUpdatedRow, String keyspace,
			String havingTable, Row preagRow) {

		List<Definition> def = preagRow.getColumnDefinitions().asList();
		String pkName = def.get(0).getName();
		String pkType = def.get(0).getType().toString();
		String pkVAlue = "";

		switch (pkType) {

		case "int":
			pkVAlue = Integer.toString(preagRow.getInt(pkName));
			break;

		case "float":
			pkVAlue = Float.toString(preagRow.getFloat(pkName));
			break;

		case "varint":
			pkVAlue = preagRow.getVarint(pkName).toString();
			break;

		case "varchar":
			pkVAlue = "'" + preagRow.getString(pkName) + "'";
			break;

		case "text":
			pkVAlue = "'" + preagRow.getString(pkName) + "'";
			break;
		}

		StringBuilder deleteQuery = new StringBuilder("delete from ");
		deleteQuery.append(keyspace).append(".").append(havingTable)
		.append(" WHERE ").append(pkName + " = ").append(pkVAlue)
		.append(";");

		System.out.println(deleteQuery);

		try {

			Session session = currentCluster.connect();
			session.execute(deleteQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public boolean deleteRowJoinHaving(String keyspace, String havingTable,
			Row preagRow) {

		List<Definition> def = preagRow.getColumnDefinitions().asList();

		String pkName = def.get(0).getName();
		String pkType = def.get(0).getType().toString();
		String pkVAlue = "";

		switch (pkType) {

		case "int":
			pkVAlue = Integer.toString(preagRow.getInt(pkName));
			break;

		case "float":
			pkVAlue = Float.toString(preagRow.getFloat(pkName));
			break;

		case "varint":
			pkVAlue = preagRow.getVarint(pkName).toString();
			break;

		case "varchar":
			pkVAlue = "'" + preagRow.getString(pkName) + "'";
			break;

		case "text":
			pkVAlue = "'" + preagRow.getString(pkName) + "'";
			break;
		}

		StringBuilder deleteQuery = new StringBuilder("delete from ");
		deleteQuery.append(keyspace).append(".").append(havingTable)
		.append(" WHERE ").append(pkName + " = ").append(pkVAlue)
		.append(";");

		System.out.println(deleteQuery);

		try {

			Session session = currentCluster.connect();
			session.execute(deleteQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public Row getUpdatedPreaggRowDeleted() {
		return updatedPreaggRowDeleted;
	}

	public void setUpdatedPreaggRowDeleted(Row updatedPreaggRowDeleted) {
		this.updatedPreaggRowDeleted = updatedPreaggRowDeleted;
	}

	public Row getDeletePreaggRowDeleted() {
		return deletePreaggRowDeleted;
	}

	public void setDeletePreaggRowDeleted(Row deletePreaggRowDeleted) {
		this.deletePreaggRowDeleted = deletePreaggRowDeleted;
	}

	public Row getDeletePreaggRow() {
		return deletePreaggRow;
	}

	public void setDeletePreaggRow(Row deletePreaggRow) {
		this.deletePreaggRow = deletePreaggRow;
	}

	public Row getJoinAggRowChangeAK() {
		return joinAggRowChangeAK;
	}

	public void setJoinAggRowChangeAK(Row joinAggRowChangeAK) {
		this.joinAggRowChangeAK = joinAggRowChangeAK;
	}

	public Row getJoinAggRow() {
		return joinAggRow;
	}

	public void setJoinAggRow(Row joinAggRow) {
		this.joinAggRow = joinAggRow;
	}

	public Row getJoinAggDeletedRow() {
		return joinAggDeletedRow;
	}

	public void setJoinAggDeletedRow(Row joinAggDeletedRow) {
		this.joinAggDeletedRow = joinAggDeletedRow;
	}

	public Row getDeleteRowJoinAggDeleted() {
		return deleteRowJoinAggDeleted;
	}

	public void setDeleteRowJoinAggDeleted(Row deleteRowJoinAggDeleted) {
		this.deleteRowJoinAggDeleted = deleteRowJoinAggDeleted;
	}

	public Row getDeleteRowJoinAgg() {
		return deleteRowJoinAgg;
	}

	public void setDeleteRowJoinAgg(Row deleteRowJoinAgg) {
		this.deleteRowJoinAgg = deleteRowJoinAgg;
	}

	public boolean updateInnerJoinAgg(Row deltaUpdatedRow, JSONObject json,
			String joinAggTableName, String aggKey, String aggKeyType,
			String aggCol, String aggColType, Row oldReverseRow,
			Row newReverseRow, Boolean leftUpdateHappened, boolean override,
			boolean mapSize1, String basetable) {

		// Select statement
		StringBuilder select = new StringBuilder();
		select.append("Select * FROM ").append((String) (json.get("keyspace")))
		.append(".").append(joinAggTableName).append(" WHERE ")
		.append(aggKey + " = ");

		String aggKeyValue = "";

		switch (aggKeyType) {
		case "int":
			aggKeyValue = "" + newReverseRow.getInt(aggKey);

		case "varint":
			aggKeyValue = "" + newReverseRow.getVarint(aggKey);
		case "float":
			aggKeyValue = "" + newReverseRow.getFloat(aggKey);
		case "varchar":
			aggKeyValue = "'" + newReverseRow.getString(aggKey) + "'";
		case "text":
			aggKeyValue = "'" + newReverseRow.getString(aggKey) + "'";
		}

		select.append(aggKeyValue + " ;");
		Row theRow = null;
		ResultSet rs = null;

		try {

			Session session = currentCluster.connect();
			// I only need to check if there is one row
			// theRow = session.execute(select.toString()).one();
			rs = session.execute(select.toString());
			theRow = rs.one();

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		// no need to update inner join agg table
		if (theRow != null) {
			return true;
		} else {

			List<String> tableNames = VmXmlHandler.getInstance()
					.getInnerJoinMap().getList("mapping.unit.tableName");
			int index = tableNames.indexOf(joinAggTableName);

			if (index != -1) {

				String temp = "mapping.unit(" + index + ")";

				int positionInMaps = VmXmlHandler.getInstance()
						.getInnerJoinMap().getInt(temp + ".positionInMap");

				Map<String, String> myMap = new HashMap<String, String>();

				if (leftUpdateHappened) {
					myMap.putAll(newReverseRow.getMap("list_item2",
							String.class, String.class));
				} else if (!leftUpdateHappened) {
					myMap.putAll(newReverseRow.getMap("list_item1",
							String.class, String.class));
				}

				float sum = 0;
				float max = -99999999;
				float min = 999999999;

				for (Map.Entry<String, String> entry : myMap.entrySet()) {
					String list = entry.getValue().replaceAll("\\[", "")
							.replaceAll("\\]", "");
					String[] listArray = list.split(",");

					sum += Float.valueOf(listArray[positionInMaps]);

					if (Float.valueOf(listArray[positionInMaps]) < min)
						min = Float.valueOf(listArray[positionInMaps]);

					if (Float.valueOf(listArray[positionInMaps]) > max)
						max = Float.valueOf(listArray[positionInMaps]);
				}

				// Insertion
				int count = myMap.size();
				float average = sum / count;

				StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
				insertQueryAgg.append((String) json.get("keyspace"))
				.append(".").append(joinAggTableName).append(" ( ")
				.append(aggKey + ", ")
				.append("sum, count, average, min, max")
				.append(") VALUES (").append(aggKeyValue + ", ")
				.append("?, ?, ?, ?, ?);");

				Session session1 = currentCluster.connect();

				PreparedStatement statement1 = session1.prepare(insertQueryAgg
						.toString());
				BoundStatement boundStatement = new BoundStatement(statement1);
				session1.execute(boundStatement.bind((int) sum, (int) count,
						average, min, max));

				System.out.println(boundStatement.toString());

			}
		}

		return true;

	}

	public boolean updateJoinAgg_UpdateLeft_AggColLeftSide(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String joinKeyType, String joinKeyName, String aggColName,
			String aggColType) {
		// TODO Auto-generated method stub


		Row newRJRow = getrjUpdatedRow();
		Row oldRJRow = getReverseJoinUpdateOldRow();
		Row changeAK = getReverseJoinUpdatedOldRow_changeJoinKey();

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_old");
		String aggColValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, aggColName, aggColType, "_new");
		String oldAggColValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, aggColName, aggColType, "_old");

		// change in join key value
		if (!oldJoinKeyValue.equals("null")
				&& !oldJoinKeyValue.equals("'null'")
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			// a - First update old agg key
			if (oldRJRow.getMap("list_item1", String.class, String.class).size() == 1) {

				if (oldRJRow.getMap("list_item2", String.class, String.class).size() == 0) {
					// remove this key from left join agg, if exits
					if (!leftJoinAggTable.equals("false")) {
						stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, leftJoinAggTable, json));
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), leftJoinAggTable, joinKeyName, oldJoinKeyValue);
					}

				} else {
					// remove this key left and inner join aggs, if they exist

					if (!leftJoinAggTable.equals("false")) {
						stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, leftJoinAggTable, json));
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), leftJoinAggTable, joinKeyName, oldJoinKeyValue);
					}
					if (!innerJoinAggTable.equals("false")) {
						stream.setInnerJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, innerJoinAggTable, json));
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJoinAggTable, joinKeyName, oldJoinKeyValue);
					}
				}

			} else {

				if (!leftJoinAggTable.equals("false")) {
					JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",deltaUpdatedRow, json, leftJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK);
				}

				if (!innerJoinAggTable.equals("false") && !oldRJRow.getMap("list_item2", String.class, String.class).isEmpty()){
					JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",deltaUpdatedRow, json, innerJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK);
				}
			}

			// b - Update new agg key

			if (newRJRow.getMap("list_item1", String.class, String.class).size() == 1) {

				// if(new.list_item2 == 0)
				// add this key to left table if exist [one item only]
				// else
				// add this key to inner and left table if they do exist

				String sum = aggColValue;
				int count = 1;
				String avg = aggColValue;
				String min = aggColValue;
				String max = aggColValue;
				// add this key to left table if exist [one item only]
				if (!leftJoinAggTable.equals("false") && aggColValue != null
						&& !aggColValue.equals("null")&& !aggColValue.equals("'null'")) {

					stream.setLeftOrRightJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));
					JoinAggregationHelper.insertStatement(Float.valueOf(sum),count, Float.valueOf(avg), Float.valueOf(min),Float.valueOf(max), joinKeyName, joinKeyValue, leftJoinAggTable, json);
				}

				// add this key to inner table if exist [one item only]
				if (!newRJRow.getMap("list_item2", String.class, String.class).isEmpty() && !innerJoinAggTable.equals("false") ) {
					stream.setInnerJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
					JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, innerJoinAggTable, json);
				}

			} else {

				if (!leftJoinAggTable.equals("false")) {
					JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, leftJoinAggTable, aggColValue);
				}

				if (!newRJRow.getMap("list_item2", String.class, String.class)
						.isEmpty() && !innerJoinAggTable.equals("false")) {
					JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, innerJoinAggTable, aggColValue);
				}

			}

		} else {
			// Case No change in join Key

			// there is change in agg col values otherwise ignore
			if (!oldAggColValue.equals(aggColValue)) {

				// updates take place in left_join_agg only
				if (newRJRow.getMap("list_item2", String.class, String.class).isEmpty()) {

					if (!leftJoinAggTable.equals("false")) {

						// only one item
						if (newRJRow.getMap("list_item1", String.class,String.class).size() == 1) {

							String sum = aggColValue;
							int count = 1;
							String avg = aggColValue;
							String min = aggColValue;
							String max = aggColValue;

							JoinAggregationHelper.insertStatement(Float.valueOf(sum), count,Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, leftJoinAggTable, json);
							stream.setLeftOrRightJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));
						}
						// more than one item --> update
						else {

							if (!leftJoinAggTable.equals("false")) {
								JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1",deltaUpdatedRow, newRJRow, json, joinKeyName, joinKeyValue, leftJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
							}
						}
					}

				}else {// update takes place in inner and left join aggs
					// insert row with aggkey/joinkey as pk to left and inner
					// join agg, if exists and values for sum,count,..
					// are calculated for this item only

					if (!leftJoinAggTable.equals("false")|| !innerJoinAggTable.equals("false")) {

						// only one item
						if (newRJRow.getMap("list_item1", String.class,String.class).size() == 1) {

							String sum = aggColValue;
							int count = 1;
							String avg = aggColValue;
							String min = aggColValue;
							String max = aggColValue;

							if (!leftJoinAggTable.equals("false")) {
								JoinAggregationHelper.insertStatement(Float.valueOf(sum), count,Float.valueOf(avg),Float.valueOf(min),Float.valueOf(max), joinKeyName, joinKeyValue, leftJoinAggTable, json);
								stream.setLeftOrRightJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));

							}

							if (!innerJoinAggTable.equals("false")) {
								JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg),Float.valueOf(min),Float.valueOf(max), joinKeyName, joinKeyValue, innerJoinAggTable, json);
								stream.setInnerJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));

							}

						}
						// more than one item --> update
						else {

							if (!leftJoinAggTable.equals("false")) {
								JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1",deltaUpdatedRow, newRJRow, json, joinKeyName, joinKeyValue, leftJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
							}

							if (!innerJoinAggTable.equals("false")) {
								JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1",deltaUpdatedRow, newRJRow, json, joinKeyName, joinKeyValue, innerJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
							}

						}
					}
				}

			}
		}

		return true;
	}

	public boolean updateJoinAgg_UpdateRight_AggColRightSide(Stream stream,String innerJoinAggTable, String rightJoinAggTable, JSONObject json,
			String joinKeyType, String joinKeyName, String aggColName,
			String aggColType) {


		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_old");
		String aggColValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, aggColName, aggColType, "_new");
		String oldAggColValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, aggColName, aggColType, "_old");


		Row newRJRow = getrjUpdatedRow();
		Row oldRJRow = getReverseJoinUpdateOldRow();
		Row changeAK = getReverseJoinUpdatedOldRow_changeJoinKey();

		// change in join key value
		if (!oldJoinKeyValue.equals("null")&& !oldJoinKeyValue.equals("'null'")&& !joinKeyValue.equals(oldJoinKeyValue)) {

			// a - First update old agg key
			if (oldRJRow.getMap("list_item2", String.class, String.class).size() == 1) {

				if (oldRJRow.getMap("list_item1", String.class, String.class).size() == 0) {

					// remove this key from right join agg, if exits
					if (!rightJoinAggTable.equals("false")) {
						stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, rightJoinAggTable, json));
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), rightJoinAggTable, joinKeyName, oldJoinKeyValue);
					}
				} else {
					// remove this key right and inner join aggs, if they exist
					if (!rightJoinAggTable.equals("false")) {
						stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, rightJoinAggTable, json));

						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), rightJoinAggTable, joinKeyName, oldJoinKeyValue);
					}

					if (!innerJoinAggTable.equals("false")) {
						stream.setInnerJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, rightJoinAggTable, json));

						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJoinAggTable, joinKeyName, oldJoinKeyValue);
					}
				}

			} else {

				if (!rightJoinAggTable.equals("false")) {
					JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", deltaUpdatedRow, json, rightJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK);
				}

				if (!oldRJRow.getMap("list_item1", String.class, String.class).isEmpty() && !innerJoinAggTable.equals("false")) {
					JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", deltaUpdatedRow, json, innerJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK);
				}
			}

			// b - Update new agg key

			if (newRJRow.getMap("list_item2", String.class, String.class).size() == 1) {

				// if(new.list_item1 == 0)
				// add this key to right table if exist [one item only]
				// else
				// add this key to inner and left table if they do exist

				String sum = aggColValue;
				int count = 1;
				String avg = aggColValue;
				String min = aggColValue;
				String max = aggColValue;

				// add this key to right table if exist [one item only]
				if (!rightJoinAggTable.equals("false") && aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {
					JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, rightJoinAggTable, json);
					stream.setLeftOrRightJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));
				}

				// add this key to inner table if exist [one item only]
				if (!newRJRow.getMap("list_item1", String.class, String.class).isEmpty() && !innerJoinAggTable.equals("false")) {
					JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, innerJoinAggTable, json);
					stream.setInnerJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
				}

			} else {


				if (!rightJoinAggTable.equals("false") && aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {
					JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, rightJoinAggTable, aggColValue);
				}

				// add this key to inner table if exist [one item only]
				if (!newRJRow.getMap("list_item1", String.class, String.class).isEmpty() && !innerJoinAggTable.equals("false")) {
					JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, innerJoinAggTable, aggColValue);
				}
			}

		} else {
			// Case No change in join Key

			// there is change in agg col values otherwise ignore
			if (!oldAggColValue.equals(aggColValue)) {

				// updates take place in right_join_agg only
				if (newRJRow.getMap("list_item1", String.class, String.class).isEmpty()) {

					if (!rightJoinAggTable.equals("false")) {

						// only one item
						if (newRJRow.getMap("list_item2", String.class,String.class).size() == 1) {

							String sum = aggColValue;
							int count = 1;
							String avg = aggColValue;
							String min = aggColValue;
							String max = aggColValue;

							JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, rightJoinAggTable, json);
							stream.setLeftOrRightJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));

						}
						// more than one item --> update
						else {

							JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2", deltaUpdatedRow, newRJRow, json, joinKeyName, joinKeyValue, rightJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
						}
					}
				}
				// update takes place in inner and right join aggs
				else {
					// insert row with aggkey/joinkey as pk to right and inner
					// join agg, if exists and values for sum,count,..
					// are calculated for this item only

					if (!rightJoinAggTable.equals("false")|| !innerJoinAggTable.equals("false")) {

						// only one item
						if (newRJRow.getMap("list_item2", String.class,
								String.class).size() == 1) {

							String sum = aggColValue;
							int count = 1;
							String avg = aggColValue;
							String min = aggColValue;
							String max = aggColValue;

							if(!rightJoinAggTable.equals("false")) {
								JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, rightJoinAggTable, json);
								stream.setLeftOrRightJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));
							}

							if(!innerJoinAggTable.equals("false")) {
								JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, innerJoinAggTable, json);
								stream.setInnerJoinAggNewRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));

							}

						}else { 
							if (!rightJoinAggTable.equals("false")) {
								JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2",deltaUpdatedRow, newRJRow, json, joinKeyName, joinKeyValue, rightJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
							}

							if (!innerJoinAggTable.equals("false")) {
								JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2",deltaUpdatedRow, newRJRow, json, joinKeyName, joinKeyValue, innerJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
							}
						}
					}
				}

			}
		}

		return true;
	}

	public boolean updateJoinAgg_UpdateLeft_AggColRightSide(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType, int aggColIndexInList) {


		String joinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_old");

		Row newRJRow = getrjUpdatedRow();
		Row oldRJRow = getReverseJoinUpdateOldRow();
		Row changeAK = getReverseJoinUpdatedOldRow_changeJoinKey();


		//  change in join/agg Key

		if (!oldJoinKeyValue.equals("null")&& !oldJoinKeyValue.equals("'null'")
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			// if(new.list_tem1 == 1 && new.list_tem2 > 0)
			// add this new key to inner table
			// // u can get from the right join agg table if it exists
			// //otherwise u must loop on new.list_item2

			if (oldRJRow.getMap("list_item1", String.class, String.class).size() == 1 && !oldRJRow.getMap("list_item2", String.class,String.class).isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				stream.setInnerJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, innerJoinAggTable, json));
				Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJoinAggTable, joinKeyName, oldJoinKeyValue);
			}

			// if(new.list_tem1 == 1 && new.list_tem2 > 0)
			// add this new key to inner table
			// u can get from the right join agg table if it exists
			// otherwise u must loop on new.list_item2
			if (newRJRow.getMap("list_item1", String.class, String.class).size() == 1
					&& !newRJRow.getMap("list_item2", String.class,String.class).isEmpty() && !innerJoinAggTable.equals("false") && !rightJoinAggTable.equals("false")) {

				JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,rightJoinAggTable, innerJoinAggTable, joinKeyName, joinKeyValue, json);

			} else if (newRJRow.getMap("list_item1", String.class, String.class).size() == 1
					&& !newRJRow.getMap("list_item2", String.class,String.class).isEmpty() && !innerJoinAggTable.equals("false") && rightJoinAggTable.equals("false")) {
				JoinAggregationHelper.addRowsToInnerJoinAgg(stream,"list_item2", deltaUpdatedRow, newRJRow, aggColIndexInList, innerJoinAggTable, json, joinKeyName, joinKeyValue);
			}

		} else {

			//no change in join key

			if (newRJRow.getMap("list_item1", String.class, String.class).size() == 1
					&& !newRJRow.getMap("list_item2", String.class,String.class).isEmpty()) {
				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2
				if (!innerJoinAggTable.equals("false") && !rightJoinAggTable.equals("false") ) {
					JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,rightJoinAggTable, innerJoinAggTable, joinKeyName, oldJoinKeyValue, json);

				}else {
					JoinAggregationHelper.addRowsToInnerJoinAgg(stream,"list_item1", deltaUpdatedRow, newRJRow, aggColIndexInList, innerJoinAggTable, json, joinKeyName, oldJoinKeyValue);
				}
			}
		}

		return true;

	}

	public boolean updateJoinAgg_UpdateRight_AggColLeftSide(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType, int aggColIndexInList) {


		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_old");
		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_new");

		Row newRJRow = getrjUpdatedRow();
		Row oldRJRow = getReverseJoinUpdateOldRow();
		Row changeAK = getReverseJoinUpdatedOldRow_changeJoinKey();

		//  change in join/agg Key

		if (!oldJoinKeyValue.equals("null") && !oldJoinKeyValue.equals("'null'") && !joinKeyValue.equals(oldJoinKeyValue)) {

			if (oldRJRow.getMap("list_item2", String.class, String.class).size() == 1
					&& !oldRJRow.getMap("list_item1", String.class,String.class).isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				stream.setInnerJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, innerJoinAggTable, json));
				Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJoinAggTable, joinKeyName, oldJoinKeyValue);

			}

			// if(new.list_tem2 == 1 && new.list_tem1 > 0)
			// add this new key to inner table
			// u can get from the left join agg table if it exists
			// otherwise u must loop on new.list_item2
			if (newRJRow.getMap("list_item2", String.class, String.class).size() == 1
					&& !newRJRow.getMap("list_item1", String.class,	String.class).isEmpty()
					&& !innerJoinAggTable.equals("false") && !leftJoinAggTable.equals("false")) {

				JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,leftJoinAggTable, innerJoinAggTable, joinKeyName, joinKeyValue, json);

			} else if(newRJRow.getMap("list_item2", String.class, String.class).size() == 1
					&& !newRJRow.getMap("list_item1", String.class,	String.class).isEmpty()
					&& !innerJoinAggTable.equals("false") && leftJoinAggTable.equals("false")) {

				JoinAggregationHelper.addRowsToInnerJoinAgg(stream,"list_item1", deltaUpdatedRow, newRJRow, aggColIndexInList, innerJoinAggTable, json, joinKeyName, joinKeyValue);
			}

		} else {

			// no change in join key

			if (newRJRow.getMap("list_item2", String.class, String.class).size() == 1
					&& !newRJRow.getMap("list_item1", String.class,String.class).isEmpty()) {
				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2
				if (!innerJoinAggTable.equals("false") && !leftJoinAggTable.equals("false")) {
					JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,leftJoinAggTable, innerJoinAggTable, joinKeyName, joinKeyValue, json);
				}else{
					JoinAggregationHelper.addRowsToInnerJoinAgg(stream,"list_item1", deltaUpdatedRow, newRJRow, aggColIndexInList, innerJoinAggTable, json, joinKeyName, joinKeyValue);
				}
			}
		}

		return true;

	}

	public String getColumnValueFromDeltaStream(Row stream, String name,
			String type, String suffix) {

		String value = "";

		switch (type) {

		case "text":

			value = ("'" + stream.getString(name + suffix) + "'");
			break;

		case "int":

			value = ("" + stream.getInt(name + suffix));
			break;

		case "varint":

			value = ("" + stream.getVarint(name + suffix));
			break;

		case "varchar":

			value = ("'" + stream.getString(name + suffix) + "'");
			break;

		case "float":

			value = ("" + stream.getFloat(name + suffix));
			break;

		}

		return value;

	}

	public Boolean updateJoinAgg_UpdateLeft_AggColLeftSide_GroupBy(
			Stream stream, String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String aggKeyType, String aggKey, String aggColName,
			String aggColType, String joinKeyName, String joinKeyType) {

		String joinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				joinKeyName, joinKeyType, "_old");

		String aggKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggKey, aggKeyType, "_new");
		String oldAggKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggKey, aggKeyType, "_old");

		String aggColValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggColName, aggColType, "_new");
		String oldAggColValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggColName, aggColType, "_old");

		Row newRJRow = getrjUpdatedRow();
		Row oldRJRow = getReverseJoinUpdateOldRow();
		Row changeAK = getReverseJoinUpdatedOldRow_changeJoinKey();

		// change in join key value or agg key value
		if (!(oldJoinKeyValue.equals("'null'"))
				&& (!joinKeyValue.equals(oldJoinKeyValue) || !aggKeyValue
						.equals(oldAggKeyValue))) {

			// Case 1
			if ((!joinKeyValue.equals(oldJoinKeyValue))
					&& aggKeyValue.equals(oldAggKeyValue)) {
				if (oldRJRow.getMap("list_item1", String.class, String.class)
						.size() == 1) {
					if (!innerJoinAggTable.equals("false")) {
						JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream,json,
								innerJoinAggTable, aggKey,
								oldAggKeyValue, oldAggColValue);

						if (!newRJRow.getMap("list_item2", String.class,
								String.class).isEmpty()) {
							JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
									json, innerJoinAggTable, aggKey,
									aggKeyValue, aggColValue, oldAggColValue,
									oldAggKeyValue);
						}
					}
				}
			}

			// Case 2
			if ((!joinKeyValue.equals(oldJoinKeyValue))
					&& !aggKeyValue.equals(oldAggKeyValue)) {

				if (!leftJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, leftJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue);
					JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
							leftJoinAggTable, aggKey, aggKeyValue, aggColValue,
							oldAggColValue, oldAggKeyValue);
				}
				if (!innerJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue);

					if (!newRJRow.getMap("list_item2", String.class,
							String.class).isEmpty()) {
						JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue);
					}
				}
			}

			// Case 3
			if ((joinKeyValue.equals(oldJoinKeyValue))
					&& !aggKeyValue.equals(oldAggKeyValue)) {

				if (!leftJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, leftJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue);
					JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
							leftJoinAggTable, aggKey, aggKeyValue, aggColValue,
							oldAggColValue, oldAggKeyValue);
				}
				if (!innerJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue);

					if (!newRJRow.getMap("list_item2", String.class,
							String.class).isEmpty()) {
						JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue);
					}
				}
			}

		} else {
			// Case No change in join Key or Case of first insertion

			// Case 4 if there is no change in join key values ignore
			if (!aggColValue.equals(oldAggColValue)) {

				// updates take place in left_join_agg only
				if (newRJRow.getMap("list_item2", String.class, String.class)
						.isEmpty()) {
					if (!leftJoinAggTable.equals("false")) {
						JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								leftJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue);
					}

				}
				if (!newRJRow.getMap("list_item2", String.class, String.class)
						.isEmpty()) {
					if (!innerJoinAggTable.equals("false")) {
						JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue);
					}
					if (!leftJoinAggTable.equals("false")) {
						JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								leftJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue);
					}
				}

			}
		}
		return true;
	}

	public Boolean updateJoinAgg_UpdateRight_AggColLeftSide_GroupBy(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String joinKeyType, String joinKey, String aggColName,
			String aggColType, int index, String key, String keyType,
			int aggKeyIndex) {

		String joinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				joinKey, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				joinKey, joinKeyType, "_old");

		Row newRJRow = getrjUpdatedRow();
		Row oldRJRow = getReverseJoinUpdateOldRow();
		Row changeAK = getReverseJoinUpdatedOldRow_changeJoinKey();

		// change in join/agg Key

		if (!(oldJoinKeyValue.equals("'null'"))
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			if (oldRJRow.getMap("list_item2", String.class, String.class)
					.size() == 1
					&& !oldRJRow.getMap("list_item1", String.class,
							String.class).isEmpty()
							&& !innerJoinAggTable.equals("false")) {

				JoinAggGroupByHelper.deleteListItem1FromGroupBy(stream,oldRJRow,
						index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}

			// if(new.list_tem2 == 1 && new.list_tem1 > 0)
			// add this new key to inner table
			// u can get from the left join agg table if it exists
			// otherwise u must loop on new.list_item2
			if (newRJRow.getMap("list_item2", String.class, String.class)
					.size() == 1
					&& !newRJRow.getMap("list_item1", String.class,
							String.class).isEmpty()
							&& !innerJoinAggTable.equals("false")) {

				JoinAggGroupByHelper.addListItem1toInnerJoinGroupBy(stream,
						deltaDeletedRow, aggColName, leftJoinAggTable,
						newRJRow, index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}
		} else {

			// no change in join key or first insertion

			if (newRJRow.getMap("list_item2", String.class, String.class)
					.size() == 1
					&& !newRJRow.getMap("list_item1", String.class,
							String.class).isEmpty()
							&& !innerJoinAggTable.equals("false")) {

				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2

				JoinAggGroupByHelper.addListItem1toInnerJoinGroupBy(stream,
						deltaUpdatedRow, aggColName, leftJoinAggTable,
						newRJRow, index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}
		}

		return true;

	}

	public Boolean updateJoinAgg_UpdateRight_AggColRightSide_GroupBy(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String aggKeyType, String aggKey,
			String aggColName, String aggColType, String joinKeyName,
			String joinKeyType) {

		String joinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				joinKeyName, joinKeyType, "_old");

		String aggKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggKey, aggKeyType, "_new");
		String oldAggKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggKey, aggKeyType, "_old");

		String aggColValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggColName, aggColType, "_new");
		String oldAggColValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggColName, aggColType, "_old");

		Row newRJRow = getrjUpdatedRow();
		Row oldRJRow = getReverseJoinUpdateOldRow();
		Row changeAK = getReverseJoinUpdatedOldRow_changeJoinKey();

		// change in join key value or agg key value
		if (!(oldJoinKeyValue.equals("'null'"))
				&& (!joinKeyValue.equals(oldJoinKeyValue) || !aggKeyValue
						.equals(oldAggKeyValue))) {

			// Case 1
			if ((!joinKeyValue.equals(oldJoinKeyValue))
					&& aggKeyValue.equals(oldAggKeyValue)) {
				if (oldRJRow.getMap("list_item2", String.class, String.class)
						.size() == 1) {
					if (!innerJoinAggTable.equals("false")) {
						JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream,json,
								innerJoinAggTable, aggKey,
								oldAggKeyValue, oldAggColValue);

						if (!newRJRow.getMap("list_item2", String.class,
								String.class).isEmpty()) {
							JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
									json, innerJoinAggTable, aggKey,
									aggKeyValue, aggColValue, oldAggColValue,
									oldAggKeyValue);
						}
					}
				}
			}

			// Case 2
			if ((!joinKeyValue.equals(oldJoinKeyValue))
					&& !aggKeyValue.equals(oldAggKeyValue)) {

				if (!rightJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, rightJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue);
					JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
							rightJoinAggTable, aggKey, aggKeyValue,
							aggColValue, oldAggColValue, oldAggKeyValue);
				}
				if (!innerJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue);

					if (!newRJRow.getMap("list_item1", String.class,
							String.class).isEmpty()) {
						JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue);
					}
				}
			}

			// Case 3
			if ((joinKeyValue.equals(oldJoinKeyValue))
					&& !aggKeyValue.equals(oldAggKeyValue)) {

				if (!rightJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, rightJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue);
					JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
							rightJoinAggTable, aggKey, aggKeyValue,
							aggColValue, oldAggColValue, oldAggKeyValue);
				}
				if (!innerJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue);

					if (!newRJRow.getMap("list_item1", String.class,
							String.class).isEmpty()) {
						JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue);
					}
				}
			}

		} else {
			// Case No change in join Key or Case of first insertion

			// Case 4 if there is no change in join key values ignore
			if (!aggColValue.equals(oldAggColValue)) {

				// updates take place in left_join_agg only
				if (newRJRow.getMap("list_item1", String.class, String.class)
						.isEmpty()) {
					if (!rightJoinAggTable.equals("false")) {
						JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								rightJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue);
					}

					if (!newRJRow.getMap("list_item2", String.class,
							String.class).isEmpty()) {
						if (!rightJoinAggTable.equals("false")) {
							JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
									json, rightJoinAggTable, aggKey,
									aggKeyValue, aggColValue, oldAggColValue,
									oldAggKeyValue);
						}
						if (!innerJoinAggTable.equals("false")) {
							JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
									json, innerJoinAggTable, aggKey,
									aggKeyValue, aggColValue, oldAggColValue,
									oldAggKeyValue);
						}
					}
				}
			}
		}

		return true;

	}

	public Boolean updateJoinAgg_UpdateLeft_AggColRightSide_GroupBy(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType, int index, String key,
			String keyType, int aggKeyIndex) {

		String joinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				joinKeyName, joinKeyType, "_old");
		String aggColValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggColName, aggColType, "_new");
		String oldAggColValue = getColumnValueFromDeltaStream(deltaUpdatedRow,
				aggColName, aggColType, "_old");

		Row newRJRow = getrjUpdatedRow();
		Row oldRJRow = getReverseJoinUpdateOldRow();
		Row changeAK = getReverseJoinUpdatedOldRow_changeJoinKey();

		if (!(oldJoinKeyValue.equals("'null'"))
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			if (oldRJRow.getMap("list_item1", String.class, String.class)
					.size() == 1
					&& !oldRJRow.getMap("list_item2", String.class,
							String.class).isEmpty()
							&& !innerJoinAggTable.equals("false")) {

				JoinAggGroupByHelper.deleteListItem2FromGroupBy(stream,oldRJRow,
						index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}

			// if(new.list_tem2 == 1 && new.list_tem1 > 0)
			// add this new key to inner table
			// u can get from the left join agg table if it exists
			// otherwise u must loop on new.list_item2
			if (newRJRow.getMap("list_item1", String.class, String.class)
					.size() == 1
					&& !newRJRow.getMap("list_item2", String.class,
							String.class).isEmpty()
							&& !innerJoinAggTable.equals("false")) {

				JoinAggGroupByHelper.addListItem2toInnerJoinGroupBy(stream,
						deltaUpdatedRow, aggColName, rightJoinAggTable,
						newRJRow, index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}
		} else {

			// no change in join key or first insertion
			if (newRJRow.getMap("list_item1", String.class, String.class)
					.size() == 1
					&& !newRJRow.getMap("list_item2", String.class,
							String.class).isEmpty()
							&& !innerJoinAggTable.equals("false")) {
				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2
				JoinAggGroupByHelper.addListItem2toInnerJoinGroupBy(stream,
						deltaUpdatedRow, aggColName, rightJoinAggTable,
						newRJRow, index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}
		}

		return true;
	}

	public boolean deleteJoinAgg_DeleteLeft_AggColLeftSide(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String joinKeyType, String joinKeyName, String aggColName,
			String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaDeletedRow, joinKeyName, joinKeyType, "_new");
		String aggColValue = Utils.getColumnValueFromDeltaStream(deltaDeletedRow, aggColName, aggColType, "_new");

		Row newRJRow = getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item2", String.class, String.class).isEmpty()) {

			if (newRJRow.getMap("list_item1", String.class, String.class).isEmpty()) {
				// remove from left
				if (!leftJoinAggTable.equals("false"))
					stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), leftJoinAggTable,joinKeyName, joinKeyValue);
			} else {
				// update left by subtracting
				if (!leftJoinAggTable.equals("false") && aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {
					JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", deltaDeletedRow, json, leftJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
				}
			}

		} else {

			if (newRJRow.getMap("list_item1", String.class, String.class).isEmpty()) {
				// remove from left and inner
				if (!leftJoinAggTable.equals("false")){
					stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));
				}

				if (!innerJoinAggTable.equals("false")){
					stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), innerJoinAggTable,joinKeyName, joinKeyValue);
				}
			} else {
				// update left and inner
				if ((!leftJoinAggTable.equals("false") || !innerJoinAggTable.equals("false"))
						&& aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {

					if (!leftJoinAggTable.equals("false")) {
						JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", deltaDeletedRow, json, leftJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
					}

					if (!innerJoinAggTable.equals("false")) {
						JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", deltaDeletedRow, json, innerJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
					}

				}
			}
		}
		return true;
	}

	public boolean deleteJoinAgg_DeleteRight_AggColRightSide(Stream stream, String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,String aggColName, String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaDeletedRow, joinKeyName, joinKeyType, "_new");
		String aggColValue =  Utils.getColumnValueFromDeltaStream(deltaDeletedRow, aggColName, aggColType, "_new");

		Row newRJRow = getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item1", String.class, String.class).isEmpty()) {

			if (newRJRow.getMap("list_item2", String.class, String.class).isEmpty()) {
				// remove from rightJoinAggTable
				if (!rightJoinAggTable.equals("false")){
					stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), rightJoinAggTable,joinKeyName, joinKeyValue);
				}
			} else {
				// update right by subtracting
				if (!rightJoinAggTable.equals("false") && aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {
					JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", deltaDeletedRow, json, rightJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
				}
			}

		} else {

			if (newRJRow.getMap("list_item2", String.class, String.class).isEmpty()) {
				// remove from left and inner
				if (!rightJoinAggTable.equals("false")){
					stream.setLeftOrRightJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), rightJoinAggTable,joinKeyName, joinKeyValue);
				}

				if (!innerJoinAggTable.equals("false")){
					stream.setInnerJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), innerJoinAggTable, joinKeyName, joinKeyValue);
				}

			} else {
				// update rightJoinAggTable and inner<

				if ((!rightJoinAggTable.equals("false") || !innerJoinAggTable.equals("false"))
						&& aggColValue != null
						&& !aggColValue.equals("null")
						&& !aggColValue.equals("'null'")) {

					if (!rightJoinAggTable.equals("false")) {
						JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", deltaDeletedRow, json, rightJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
					}

					if (!innerJoinAggTable.equals("false")) {
						JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", deltaDeletedRow, json, innerJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
					}
				}
			}

		}

		return true;
	}


	public boolean deleteJoinAgg_DeleteLeft_AggColRightSide(Stream stream, String innerJoinAggTable, JSONObject json, String joinKeyType,
			String joinKeyName, String aggColName, String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaDeletedRow, joinKeyName, joinKeyType, "_new");

		Row newRJRow = getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item1", String.class, String.class).isEmpty() && !newRJRow.getMap("list_item2", String.class, String.class).isEmpty()) {

			// remove from inner
			if (!innerJoinAggTable.equals("false")){
				stream.setInnerJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),innerJoinAggTable, joinKeyName, joinKeyValue);
			}
		}
		return true;

	}


	public boolean deleteJoinAgg_DeleteRight_AggColLeftSide(Stream stream,
			String innerJoinAggTable, JSONObject json, String joinKeyType,
			String joinKeyName, String aggColName, String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaDeletedRow, joinKeyName, joinKeyType, "_new");

		Row newRJRow = getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item2", String.class, String.class).isEmpty() && !newRJRow.getMap("list_item1", String.class, String.class).isEmpty()) {
			// remove from inner
			if (!innerJoinAggTable.equals("false")){
				stream.setInnerJoinAggDeleteRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),innerJoinAggTable, joinKeyName, joinKeyValue);
			}
		}
		return true;

	}


	public Boolean deleteJoinAgg_DeleteLeft_AggColLeftSide_GroupBy(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String aggKeyType, String aggkey, String aggColName,
			String aggColType) {

		String aggColValue = getColumnValueFromDeltaStream(deltaDeletedRow,
				aggColName, aggColType, "_new");
		String aggKeyValue = getColumnValueFromDeltaStream(deltaDeletedRow,
				aggkey, aggKeyType, "_new");

		Row newRJRow = getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item2", String.class, String.class).isEmpty()) {

			// remove from left: if count == 1, then delete entire row, else
			// substract & update row
			if (!leftJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,json,
						leftJoinAggTable, aggkey, aggKeyValue, aggColValue);
			}

		} else {

			if (newRJRow.getMap("list_item1", String.class, String.class)
					.isEmpty()) {
				// remove from left and inner
				if (!leftJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, leftJoinAggTable, aggkey, aggKeyValue,
							aggColValue);
				}
				if (!innerJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggkey, aggKeyValue,
							aggColValue);
				}
			}
		}

		return true;
	}


	public Boolean deleteJoinAgg_DeleteRight_AggColRightSide_GroupBy(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String aggKeyType, String aggKey,
			String aggColName, String aggColType) {

		String aggColValue = getColumnValueFromDeltaStream(deltaDeletedRow,
				aggColName, aggColType, "_new");
		String aggKeyValue = getColumnValueFromDeltaStream(deltaDeletedRow,
				aggKey, aggKeyType, "_new");

		Row newRJRow = getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item1", String.class, String.class).isEmpty()) {

			// remove from left: if count == 1, then delete entire row, else
			// substract & update row
			if (!rightJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,json,
						rightJoinAggTable, aggKey, aggKeyValue, aggColValue);
			}

		} else {

			if (newRJRow.getMap("list_item2", String.class, String.class)
					.isEmpty()) {
				// remove from left and inner
				if (!rightJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, rightJoinAggTable, aggKey, aggKeyValue,
							aggColValue);
				}
				if (!innerJoinAggTable.equals("false")) {
					JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, aggKeyValue,
							aggColValue);
				}
			}
		}

		return true;
	}


	public Boolean deleteJoinAgg_DeleteLeft_AggColRightSide_GroupBy(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String aggKeyType, String aggKey, String aggColName,
			String aggColType, int aggKeyIndex, int index) {

		Row newRJRow = getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item1", String.class, String.class).isEmpty()
				&& !newRJRow.getMap("list_item2", String.class, String.class)
				.isEmpty()) {

			// remove from inner
			if (!innerJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.deleteListItem2FromGroupBy(stream,newRJRow,
						index, aggKeyType, aggKey, json, innerJoinAggTable,
						aggKeyIndex);
			}
		}

		return true;

	}


	public Boolean deleteJoinAgg_DeleteRight_AggColLeftSide_GroupBy(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String aggKeyType, String aggKey, String aggColName,
			String aggColType, int aggKeyIndex, int index) {

		Row newRJRow = getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item2", String.class, String.class).isEmpty()
				&& !newRJRow.getMap("list_item1", String.class, String.class)
				.isEmpty()) {

			// remove from inner
			if (!innerJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.deleteListItem1FromGroupBy(stream,newRJRow,
						index, aggKeyType, aggKey, json, innerJoinAggTable,
						aggKeyIndex);
			}

		}

		return true;

	}

}