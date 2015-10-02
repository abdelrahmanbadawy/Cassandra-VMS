package ViewManager;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


public class ViewManager{

	Cluster currentCluster = null;
	private String reverseJoinTableName;

	public ViewManager(Cluster currenCluster) {
		this.currentCluster = currenCluster;
	}

	public boolean updateDelta(Stream stream,JSONObject json, int indexBaseTableName,
			String baseTablePrimaryKey) {

		// retrieve values from json
		ResultSet setRow = null;
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
		CustomizedRow crow = new CustomizedRow(row);

		//TO BE REMOVED
		//stream.setDeltaUpdatedRow(row);
		stream.setDeltaUpdatedRow(crow);
		stream.setDeltaJSON(json);

		return true;
	}

	public boolean deleteRowDelta(Stream stream,JSONObject json) {

		JSONObject condition = (JSONObject) json.get("condition");
		Object[] hm = condition.keySet().toArray();

		// 1. retrieve the row to be deleted from delta table
		Row row = Utils.selectAllStatement((String)json.get("keyspace"), "delta_" + json.get("table"), hm[0].toString(), condition.get(hm[0]).toString());

		// 2. set DeltaDeletedRow variable for streaming
		CustomizedRow crow = new CustomizedRow(row);
		stream.setDeltaDeletedRow(crow);
		stream.setDeleteOperation(true);

		// 3. delete row from delta
		Utils.deleteEntireRowWithPK((String)json.get("keyspace"), "delta_" + json.get("table"),  hm[0].toString(), condition.get(hm[0]).toString());

		System.out.println("Delete Successful from Delta Table");

		return true;

	}

	public boolean deleteRowPreaggAgg(Stream stream,
			String baseTablePrimaryKey, JSONObject json, String preaggTable,
			String aggKey, String aggKeyType, String aggCol, String aggColType) {

		float count = 0;
		float sum = 0;
		float average = 0;
		float min = Float.MAX_VALUE;
		float max = -Float.MAX_VALUE;

		// 1. retrieve agg key value from delta stream to retrieve the correct
		// row from preagg
		String aggKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), aggKey, aggKeyType, "_new");
		float aggColValue = 0;

		// 1.b Retrieve row key from delta stream
		String pk = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(),stream.getDeltaDeletedRow().getName(0).toString(), stream.getDeltaDeletedRow().getType(0), "");

		Row theRow = PreaggregationHelper.selectStatement(json, preaggTable, aggKey, aggKeyValue).one();

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

				CustomizedRow crow = new CustomizedRow(theRow);
				stream.setDeletePreaggRowDeleted(crow);
				stream.setDeleteOperation(true);
				String blob = Serialize.serializeStream2(stream);
				PreaggregationHelper.insertStatementToDelete(json, preaggTable, aggKey, aggKeyValue, blob);

				deleteEntireRowWithPK((String) json.get("keyspace"),
						preaggTable, aggKey, aggKeyValue);

			} else {

				// 5.a remove entry from map with that pk
				myMap.remove(pk);

				// 5.b retrieve aggCol value
				aggColValue = Float.valueOf(Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), aggCol, aggColType, "_new"));

				// 5.c adjust sum,count,average values
				count = myMap.size();
				sum = theRow.getFloat("sum") - aggColValue;
				average = sum / count;

				max = -Float.MAX_VALUE;
				min = Float.MAX_VALUE;

				int aggColIndexInList = 0;

				for (int i = 0; i < stream.getDeltaDeletedRow().colDefSize; i++) {
					if (stream.getDeltaDeletedRow().getName(i).contentEquals(aggCol + "_new")) {
						break;
					}
					if (stream.getDeltaDeletedRow().getName(i).contains("_new"))
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

				ByteBuffer blob_old = theRow.getBytes("stream");

				CustomizedRow constructedRow = CustomizedRow.constructUpdatedPreaggRow(aggKey,aggKeyValue,myMap,sum,(int)count,average,min,max, Serialize.serializeStream2(stream));
				stream.setDeletePreaggRow(constructedRow);
				String buffer_new = Serialize.serializeStream2(stream);


				while(!PreaggregationHelper.updateStatement(sum, (int)count, average, min, max, myMap, aggKey, aggKeyValue, preaggTable, json, blob_old, buffer_new)){
					Row row = PreaggregationHelper.selectStatement(json, preaggTable, aggKey, aggKeyValue).one();
					blob_old = row.getBytes("stream");
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

	public boolean updatePreaggregation(Stream stream,String aggKey,
			String aggKeyType, JSONObject json, String preaggTable,
			String baseTablePrimaryKey, String aggCol, String aggColType,
			boolean override, boolean mapSize1) {

		// 1. check if the aggKey has been updated or not from delta Stream
		// given as input
		// sameKeyValue = true , if 1) aggkey hasnt been updated or 2) for first
		// insertion where _old value is null
		// 1.b save aggKeyValue in loop
		float aggColValue_old = 0;
		float aggColValue = 0;
		boolean sameKeyValue = false;
		String aggKeyValue = "";
		String aggKeyValue_old = "";


		CustomizedRow deltaUpdatedRow = stream.getDeltaUpdatedRow();

		if (deltaUpdatedRow != null) {

			aggKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow,aggKey,aggKeyType, "_new");
			aggKeyValue_old = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow,aggKey,aggKeyType, "_old");

			if(aggKeyValue.equals(aggKeyValue_old)|| deltaUpdatedRow.isNull(aggKey + "_old")){
				sameKeyValue = true;
			}
		}

		// 1.b get all column values (_new) from delta table + including pk
		// & save them in myList

		ArrayList<String> myList = new ArrayList<String>();

		int aggColIndexInList = 0;

		for (int i = 0; i < deltaUpdatedRow.colDefSize ; i++) {
			if (deltaUpdatedRow.getName(i).contentEquals(aggCol + "_new")) {
				aggColIndexInList = i;
				break;
			}
		}

		for (int i = 0; i < deltaUpdatedRow.colDefSize; i++) {

			switch (deltaUpdatedRow.getType(i)) {

			case "text":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add(deltaUpdatedRow.getString(i));
				}

				break;

			case "int":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add("" + deltaUpdatedRow.getInt(i) + "");
				}
				break;

			case "varint":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add("" + deltaUpdatedRow.getVarint(i) + "");
				}
				break;

			case "varchar":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add(deltaUpdatedRow.getString(i));
				}
				break;

			case "float":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add("" + deltaUpdatedRow.getFloat(i) + "");
				}
				break;
			}

		}

		// 1.c Get the old and new aggCol value from delta table (_new), (_old)
		if (deltaUpdatedRow != null) {

			String temp = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, aggCol, aggColType, "_new");
			if(temp.equals("null"))
				aggColValue = 0;
			else
				aggColValue = Float.valueOf(temp);

			temp = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, aggCol, aggColType, "_old");
			if(temp.equals("null"))
				aggColValue_old = 0;
			else
				aggColValue_old = Float.valueOf(temp);
		}


		// 2. if AggKey hasnt been updated or first insertion
		if (sameKeyValue || override) {

			// 2.a select from preagg table row with AggKey as PK

			boolean loop = false;

			do{

				if (!override) {
					stream.setUpdatedPreaggRowDeleted(null);
				}
				if (!mapSize1) {
					stream.setUpdatedPreaggRowDeleted(null);
				}

				ResultSet rs = PreaggregationHelper.selectStatement(json, preaggTable, aggKey, aggKeyValue);
				Row theRow1 = rs.one();
				HashMap<String, String> myMap = new HashMap<>();

				// 2.c If row retrieved is null, then this is the first insertion
				// for this given Agg key
				if (theRow1 == null) {
					// 2.c.1 create a map, add pk and list with delta _new values
					// 2.c.2 set the agg col values

					if(PreaggregationHelper.firstInsertion(stream,myList,aggColValue,json,preaggTable,aggKey,aggKeyValue)) {
						loop = false;
					}else{
						loop = true;
					}

				} else {
					// 2.d If row is not null, then this is not the first insertion
					// for this agg Key

					ByteBuffer blob_old = theRow1.getBytes("stream");

					if(PreaggregationHelper.updateAggColValue(stream,myList, aggColValue, aggColValue_old, theRow1, aggColIndexInList,json,preaggTable,aggKey,aggKeyValue,blob_old)){
						loop = false;
					}else{
						loop = true;
					}
				}

			}while(loop);

		} else if ((!sameKeyValue && !override)) {

			// 1. retrieve old agg key value from delta stream to retrieve the
			// correct row from preagg
			// was retrieved above in aggKeyValue_old variable

			// 2. select row with old aggkeyValue from delta stream
			ResultSet PreAggMap = PreaggregationHelper.selectStatement(json, preaggTable, aggKey, aggKeyValue_old);

			Row theRow = PreAggMap.one();
			if (theRow != null) {

				// 3.check size of map for given old agg key
				// if map.size is 1 then whole row can be deleted
				// if map.size is larger than 1 then iterate over map & delete
				// desired entry with the correct pk as key

				Map<String, String> tempMapImmutable = theRow.getMap("list_item", String.class, String.class);

				System.out.println(tempMapImmutable);
				Map<String, String> myMap = new HashMap<String, String>();
				myMap.putAll(tempMapImmutable);


				if (myMap.size() == 1) {

					// Selection to set PreaggRowDeleted

					CustomizedRow crow = new CustomizedRow(theRow);
					stream.setUpdatedPreaggRowDeleted(crow);
					stream.setDeleteOperation(true);
					String blob = Serialize.serializeStream2(stream);
					PreaggregationHelper.insertStatementToDelete(json, preaggTable, aggKey, aggKeyValue_old, blob);

					// 4. delete the whole row
					Utils.deleteEntireRowWithPK((String) json.get("keyspace"),
							preaggTable, aggKey, aggKeyValue_old);

					//Reseting the stream
					stream.setDeleteOperation(false);
					stream.setUpdatedPreaggRowDeleted(null);

					// 4.a perform a new insertion with new values
					updatePreaggregation(stream, aggKey, aggKeyType,
							json, preaggTable, baseTablePrimaryKey, aggCol,
							aggColType, true, true);

				} else {

					// 5. retrieve the pk value that has to be removed from the
					// map
					// 5.a remove entry from map with that pk
					// 5.c adjust sum,count,average values
					// 6. Execute insertion statement of the row with the
					// aggKeyValue_old to refelect changes

					ByteBuffer blob_old = theRow.getBytes("blob");

					while(!PreaggregationHelper.subtractOldAggColValue(stream,myList, aggColValue_old, myMap, theRow, aggColIndexInList, json, preaggTable, aggKey, aggKeyValue_old,blob_old)){
						PreAggMap = PreaggregationHelper.selectStatement(json, preaggTable, aggKey, aggKeyValue_old);
						theRow = PreAggMap.one();
						blob_old = theRow.getBytes("blob");
					}

					// perform a new insertion for the new aggkey given in json
					updatePreaggregation(stream, aggKey, aggKeyType,
							json, preaggTable, baseTablePrimaryKey, aggCol,
							aggColType, true, false);
				}
			}

			// perform a new insertion for the new aggkey given in json
			updatePreaggregation(stream, aggKey, aggKeyType,
					json, preaggTable, baseTablePrimaryKey, aggCol,
					aggColType, true, false);
		}

		return true;
	}

	public void updateReverseJoin(Stream stream,JSONObject json, int cursor, int nrOfTables,
			String joinTable, List<String> baseTables, String joinKeyName,
			String tableName, String keyspace, String joinKeyType, int column) {

		setReverseJoinTableName(joinTable);


		JSONObject data;
		// insert
		if (json.get("data") != null) {
			data = (JSONObject) json.get("data");
			// update
		} else {
			data = (JSONObject) json.get("set_data");
		}

		CustomizedRow deltaUpdatedRow = stream.getDeltaUpdatedRow();

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, joinKeyName, joinKeyType, "_old");

		Row theRow = Utils.selectAllStatement(keyspace, joinTable, joinKeyName, joinKeyValue);

		ArrayList<String> myList = new ArrayList<String>();


		for (int i = 0; i < deltaUpdatedRow.colDefSize; i++) {

			switch (deltaUpdatedRow.getType(i)) {

			case "text":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add("'" + deltaUpdatedRow.getString(i) + "'");
				}

				break;

			case "int":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add("" + deltaUpdatedRow.getInt(i));
				}
				break;

			case "varint":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add("" + deltaUpdatedRow.getVarint(i));
				}
				break;

			case "varchar":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
					myList.add("'" + deltaUpdatedRow.getString(i) + "'");
				}
				break;

			case "float":
				if (!deltaUpdatedRow.getName(i).contains("_old")) {
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

			// The row that contains the old join key value
			Row row_old_join_value = Utils.selectAllStatement(keyspace, joinTable, joinKeyName, oldJoinKeyValue);
			CustomizedRow crow = new CustomizedRow(row_old_join_value);

			stream.setReverseJoinUpadteOldRow(crow);

			Map<String, String> tempMapImmutable2 = row_old_join_value.getMap(
					"list_item" + column, String.class, String.class);

			HashMap<String, String> myMap2 = new HashMap<String, String>();

			myMap2.putAll(tempMapImmutable2);
			// delete this from the other row
			myMap2.remove(pk);

			ReverseJoinHelper.insertStatement(joinTable, keyspace, joinKeyName, oldJoinKeyValue, column, myMap2, stream);

			// retrieve and set update old row
			Row row_after_change_join_value = Utils.selectAllStatement(keyspace, joinTable, joinKeyName, oldJoinKeyValue);

			CustomizedRow crow1 = new CustomizedRow(row_after_change_join_value);
			// The old row that contains the old join key value after being
			// updated
			stream.setReverseJoinUpdatedOldRow_changeJoinKey(crow1);

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
				Utils.deleteEntireRowWithPK(keyspace, joinTable, joinKeyName, oldJoinKeyValue);
			}
		}else{
			CustomizedRow crow2 = new CustomizedRow(theRow);
			stream.setReverseJoinUpadteOldRow(crow2);
		}

		ReverseJoinHelper.insertStatement(joinTable, keyspace, joinKeyName, joinKeyValue, column, myMap, stream);

		// Set the rj updated row for join updates
		Row result = Utils.selectAllStatement(keyspace, joinTable, joinKeyName, joinKeyValue);
		CustomizedRow crow = new CustomizedRow(result);
		stream.setReverseJoinUpdateNewRow(crow);

	}

	public boolean updateJoinController(Stream stream, String innerJName,
			String leftJName, String rightJName, JSONObject json,
			Boolean updateLeft, Boolean updateRight, String joinKeyType,
			String joinKeyName, String pkName) {

		CustomizedRow deltaUpdatedRow = stream.getDeltaUpdatedRow();

		// 1. get row updated by reverse join
		CustomizedRow theRow = stream.getReverseJoinUpdateNewRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1");
		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2");

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


		CustomizedRow reverseJoinUpadteOldRow = stream.getReverseJoinUpadteOldRow();

		// Case 1 : update left join table
		// !leftJName.equals(false) meaning : no left join wanted, only right
		if (updateLeft && myMap2.size() == 0 && !leftJName.equals("false")) {
			JoinHelper.updateLeftJoinTable(stream,leftJName, theRow, json);

			if (changeJoinKey && reverseJoinUpadteOldRow != null) {

				JoinHelper.removeLeftCrossRight(stream,json, innerJName);
				if (!rightJName.equals("false") && reverseJoinUpadteOldRow.getMap("list_item1").size() == 1
						&& reverseJoinUpadteOldRow.getMap("list_item2").size() > 0) {

					JoinHelper.addAllToRightJoinTable(
							rightJName,
							reverseJoinUpadteOldRow.getMap("list_item2"), json);
				}
			}

			return true;
		}

		// Case 2: update right join table
		// !rightName.equals(false) meaning : no right join wanted, only left
		if (updateRight && myMap1.size() == 0 && !rightJName.equals("false")) {
			JoinHelper.updateRightJoinTable(stream,rightJName, theRow, json);

			if (changeJoinKey && reverseJoinUpadteOldRow != null) {
				JoinHelper.removeRightCrossLeft(stream,json, innerJName);
				if (!leftJName.equals("false") && reverseJoinUpadteOldRow.getMap("list_item2").size() == 1
						&& reverseJoinUpadteOldRow.getMap("list_item1").size() > 0) {

					JoinHelper.addAllToLeftJoinTable(
							leftJName,
							reverseJoinUpadteOldRow.getMap("list_item1"), json);
				}
			}

			return true;
		}

		// Case 6: change in join key value and update in left
		if (changeJoinKey && updateLeft) {
			JoinHelper.removeLeftCrossRight(stream,json, innerJName);
			if (!rightJName.equals("false")
					&& reverseJoinUpadteOldRow.getMap("list_item1").size() == 1
					&& reverseJoinUpadteOldRow.getMap("list_item2").size() > 0) {

				JoinHelper.addAllToRightJoinTable(rightJName, reverseJoinUpadteOldRow.getMap("list_item2"), json);
			}

			// remove old from left join
			if (!leftJName.equals("false")
					&& reverseJoinUpadteOldRow.getMap("list_item2").size() == 0
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

				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						leftJName, joinTablePk, pkValue);

			}

		}

		// Case 7: change in join key value and update in right
		if (changeJoinKey && updateRight) {
			JoinHelper.removeRightCrossLeft(stream,json, innerJName);
			if (!leftJName.equals("false")
					&& reverseJoinUpadteOldRow.getMap("list_item2").size() == 1
					&& reverseJoinUpadteOldRow.getMap("list_item1").size() > 0) {

				JoinHelper.addAllToLeftJoinTable(leftJName, reverseJoinUpadteOldRow.getMap("list_item1"), json);
			}

			// remove old from right join
			if (!rightJName.equals("false")
					&& reverseJoinUpadteOldRow.getMap("list_item1").size() == 0
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

				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						rightJName, joinTablePk, pkValue);

			}
		}

		// Case 3: create cross product & save in inner join table
		if (updateLeft && myMap2.size() != 0)
			leftCrossRight(stream,json, innerJName);

		if (updateRight && myMap1.size() != 0)
			rightCrossLeft(stream,json, innerJName);

		// Case 4 : delete row from left join if no longer valid
		if (updateLeft && myMap1.size() == 1)
			DeleteJoinHelper.deleteFromRightJoinTable(myMap2, rightJName, json, false);

		// Case 5: delete row from left join if no longer valid
		if (updateRight && myMap2.size() == 1)
			DeleteJoinHelper.deleteFromLeftJoinTable(myMap1, leftJName, json, false);

		return true;
	}


	public boolean updateSelection(CustomizedRow row, String keyspace, String selecTable,
			String selColName) {

		// 1. get column names of delta table
		// 1.b save column values from delta stream too

		StringBuilder insertion = new StringBuilder();
		StringBuilder insertionValues = new StringBuilder();

		for (int i = 0; i < row.colDefSize; i++) {

			switch (row.getType(i)) {

			case "text":
				if (!row.getName(i).contains("_old")) {
					if (row.getName(i).contains("_new")) {
						String[] split = row.getName(i)
								.split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getName(i)
								+ ", ");
					}

					insertionValues.append("'"
							+ row.getString(row.getName(
									i)) + "', ");
				}
				break;

			case "int":
				if (!row.getName(i).contains("_old")) {

					if (row.getName(i).contains("_new")) {
						String[] split = row.getName(i)
								.split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getName(i)
								+ ", ");
					}

					insertionValues.append(row.getInt(row.getName(i)) + ", ");
				}
				break;

			case "varint":
				if (!row.getName(i).contains("_old")) {

					if (row.getName(i).contains("_new")) {
						String[] split = row.getName(i)
								.split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getName(i)
								+ ", ");
					}
					insertionValues.append(row.getVarint(row.getName(i)) + ", ");
				}
				break;

			case "varchar":
				if (!row.getName(i).contains("_old")) {

					if (row.getName(i).contains("_new")) {
						String[] split = row.getName(i)
								.split("_");
						insertion.append(split[0] + ", ");
					} else {

						insertion.append(row.getName(i)
								+ ", ");
					}

					insertionValues.append("'"
							+ row.getString(row.getName(
									i)) + "', ");
				}
				break;

			}
		}

		insertion.deleteCharAt(insertion.length() - 2);
		insertionValues.deleteCharAt(insertionValues.length() - 2);

		Utils.insertStatement(keyspace, selecTable, insertion.toString(), insertionValues.toString());


		return true;
	}

	public boolean deleteRowSelection(String keyspace,
			String selecTable, String baseTablePrimaryKey, JSONObject json) {

		if (json.containsKey("condition")) {

			JSONObject condition = (JSONObject) json.get("condition");
			Object[] hm = condition.keySet().toArray();
			Utils.deleteEntireRowWithPK(keyspace, selecTable, baseTablePrimaryKey,condition.get(hm[0]).toString());
		} else {
			JSONObject data = (JSONObject) json.get("data");
			Utils.deleteEntireRowWithPK(keyspace, selecTable, baseTablePrimaryKey, data.get(baseTablePrimaryKey).toString());
		}

		System.out.println("Possible Deletes Successful from Selection View");

		return true;

	}

	private boolean rightCrossLeft(Stream stream, JSONObject json, String innerJTableName) {

		// 1. get row updated by reverse join
		CustomizedRow theRow = stream.getReverseJoinUpdateNewRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1");
		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2");

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

			String leftPkType = temp + ".primaryKey.leftType";
			leftPkType = VmXmlHandler.getInstance().getiJSchema()
					.getString(leftPkType);

			// 3.a. get from delta row, the left pk value

			String rightPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), rightPkName, rightPkType, "");

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


	private boolean leftCrossRight(Stream stream, JSONObject json, String innerJTableName) {

		// 1. get row updated by reverse join
		CustomizedRow theRow = stream.getReverseJoinUpdateNewRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1");
		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2");

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


			String rightPkType = temp + ".primaryKey.rightType";
			rightPkType = VmXmlHandler.getInstance().getiJSchema()
					.getString(rightPkType);

			// 3.a. get from delta row, the left pk value
			String leftPkValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), leftPkName, leftPkType, "");

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


	// JSONObject json, int cursor, int nrOfTables, String joinTable,
	// List<String> baseTables, String joinKeyName,
	// String tableName, String keyspace, String aggKeyType, int column
	public void deleteReverseJoin(Stream stream, JSONObject json, int cursor, int nrOfTables,
			String joinTable, List<String> baseTables, String joinKeyName,
			String tableName, String keyspace, String joinKeyType, int column) {

		setReverseJoinTableName(joinTable);

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");

		Row theRow  = Utils.selectAllStatement(keyspace, joinTable, joinKeyName, joinKeyValue);

		CustomizedRow crow = new CustomizedRow(theRow);
		stream.setRevereJoinDeleteOldRow(crow);

		HashMap<String, String> myMap = null;

		String pkType = stream.getDeltaDeletedRow().getType(0);
		String pkName = stream.getDeltaDeletedRow().getName(0);
		String pk = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), pkName, pkType, "");

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

			ReverseJoinHelper.insertStatement(joinTable, keyspace, joinKeyName, joinKeyValue, column, myMap, stream);


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
				Utils.deleteEntireRowWithPK(keyspace, joinTable, joinKeyName, joinKeyValue);
			}

		}

		// get new deleted row from rj
		Row row = Utils.selectAllStatement(keyspace, joinTable, joinKeyName, joinKeyValue);
		CustomizedRow crow1 = new CustomizedRow(row);
		stream.setReverseJoinDeleteNewRow(crow1);

	}


	public boolean deleteJoinController(Stream stream,CustomizedRow deltaDeletedRow, String innerJName,
			String leftJName, String rightJName, JSONObject json,
			Boolean updateLeft, Boolean updateRight) {

		// 1. get row updated by reverse join
		CustomizedRow theRow = stream.getRevereJoinDeleteOldRow();

		// 1.a get columns item_1, item_2
		Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1");
		Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2");

		// 2. retrieve list_item1, list_item2
		HashMap<String, String> myMap1 = new HashMap<String, String>();
		myMap1.putAll(tempMapImmutable1);

		HashMap<String, String> myMap2 = new HashMap<String, String>();
		myMap2.putAll(tempMapImmutable2);

		// Case 1 : delete from left join table if item_list2 is empty
		// !leftJName.equals(false) meaning : no left join wanted, only right
		if (updateLeft && myMap2.size() == 0 && !leftJName.equals("false")) {
			DeleteJoinHelper.deleteFromLeftJoinTable(myMap1, leftJName, json, true);
			return true;
		}

		// Case 2: delete from right join table if item_list1 is empty
		// !rightName.equals(false) meaning : no right join wanted, only left
		if (updateRight && myMap1.size() == 0 && !rightJName.equals("false")) {
			DeleteJoinHelper.deleteFromRightJoinTable(myMap2, rightJName, json, true);
			return true;
		}

		CustomizedRow newDeletedRow = stream.getReverseJoinDeleteNewRow();

		// Case 3: delete happened from left and list_item1 is not empty
		// remove cross product from innerjoin
		if (updateLeft && myMap2.size() > 0) {

			DeleteJoinHelper.removeDeleteLeftCrossRight(stream,json, innerJName, myMap2);

			// delete happened in left and new list_item 1 is empty
			// add all list_item2 to right join
			if (newDeletedRow.getMap("list_item1")
					.size() == 0) {
				JoinHelper.addAllToRightJoinTable(rightJName, newDeletedRow.getMap(
						"list_item2"), json);
			}

		}

		// Case 4: delete happened from rigth and list_item2 is not empty
		// remove cross product from inner join
		if (updateRight && myMap1.size() > 0) {

			// removeRightCrossLeft(json, innerJName);
			DeleteJoinHelper.removeDeleteRightCrossLeft(stream,json, innerJName, myMap1);

			// delete happened in right and new list_item 2 is empty
			// add all list_item1 to left join
			if (newDeletedRow.getMap("list_item2")
					.size() == 0) {
				JoinHelper.addAllToLeftJoinTable(leftJName, newDeletedRow.getMap(
						"list_item1"), json);
			}

		}

		return true;
	}


	public boolean updateHaving(CustomizedRow deltaUpdatedRow, JSONObject json,
			String havingTable, CustomizedRow preagRow) {

		Map<String, String> myMap = new HashMap<String, String>();

		myMap.putAll(preagRow.getMap("list_item"));

		float min = preagRow.getFloat("min");
		float max = preagRow.getFloat("max");
		float average = preagRow.getFloat("average");
		int count = preagRow.getInt("count");
		float sum = preagRow.getFloat("sum");


		String pkName = preagRow.getName(0);
		String pkType = preagRow.getType(0);
		String pkVAlue = Utils.getColumnValueFromDeltaStream(preagRow, pkName, pkType, "");

		PreaggregationHelper.insertStatement(json, havingTable, pkName, pkVAlue, myMap, sum, count, min, max, average);

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


	public boolean deleteRowHaving(String keyspace,
			String havingTable, CustomizedRow preagRow) {

		String pkName = preagRow.getName(0);
		String pkType = preagRow.getType(0);
		String pkVAlue = Utils.getColumnValueFromDeltaStream(preagRow, pkName, pkType, "");

		Utils.deleteEntireRowWithPK(keyspace, havingTable, pkName, pkVAlue);

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

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();
		CustomizedRow changeAK = stream.getReverseJoinUpdatedOldRow_changeJoinKey();

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_old");
		String aggColValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), aggColName, aggColType, "_new");
		String oldAggColValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), aggColName, aggColType, "_old");

		// change in join key value
		if (!oldJoinKeyValue.equals("null")
				&& !oldJoinKeyValue.equals("'null'")
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			// a - First update old agg key
			if (oldRJRow.getMap("list_item1").size() == 1) {

				if (oldRJRow.getMap("list_item2").size() == 0) {
					// remove this key from left join agg, if exits
					if (!leftJoinAggTable.equals("false")) {

						CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, leftJoinAggTable, json));
						stream.setLeftOrRightJoinAggDeleteRow(crow);
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), leftJoinAggTable, joinKeyName, oldJoinKeyValue);
					}

				} else {
					// remove this key left and inner join aggs, if they exist

					if (!leftJoinAggTable.equals("false")) {
						CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, leftJoinAggTable, json));
						stream.setLeftOrRightJoinAggDeleteRow(crow);
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), leftJoinAggTable, joinKeyName, oldJoinKeyValue);
					}
					if (!innerJoinAggTable.equals("false")) {
						CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, innerJoinAggTable, json));
						stream.setInnerJoinAggDeleteRow(crow);
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJoinAggTable, joinKeyName, oldJoinKeyValue);
					}
				}

			} else {

				if (!leftJoinAggTable.equals("false")) {
					while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",stream.getDeltaUpdatedRow(), json, leftJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK));
					//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",stream.getDeltaUpdatedRow(), json, leftJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK);
				}

				if (!innerJoinAggTable.equals("false") && !oldRJRow.getMap("list_item2").isEmpty()){
					while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",stream.getDeltaUpdatedRow(), json, innerJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK));
					//	JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",stream.getDeltaUpdatedRow(), json, innerJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK);
				}
			}

			// b - Update new agg key

			if (newRJRow.getMap("list_item1").size() == 1) {

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

					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));

					stream.setLeftOrRightJoinAggNewRow(crow);
					JoinAggregationHelper.insertStatement(Float.valueOf(sum),count, Float.valueOf(avg), Float.valueOf(min),Float.valueOf(max), joinKeyName, joinKeyValue, leftJoinAggTable, json);
				}

				// add this key to inner table if exist [one item only]
				if (!newRJRow.getMap("list_item2").isEmpty() && !innerJoinAggTable.equals("false") ) {
					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));	
					stream.setInnerJoinAggNewRow(crow);
					JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, innerJoinAggTable, json);
				}

			} else {

				if (!leftJoinAggTable.equals("false")) {
					while(!	JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, leftJoinAggTable, aggColValue));
					//JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, leftJoinAggTable, aggColValue);
				}

				if (!newRJRow.getMap("list_item2")
						.isEmpty() && !innerJoinAggTable.equals("false")) {
					while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, innerJoinAggTable, aggColValue));
					//	JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, innerJoinAggTable, aggColValue);
				}

			}

		} else {
			// Case No change in join Key

			// there is change in agg col values otherwise ignore
			if (!oldAggColValue.equals(aggColValue)) {

				// updates take place in left_join_agg only
				if (newRJRow.getMap("list_item2").isEmpty()) {

					if (!leftJoinAggTable.equals("false")) {

						// only one item
						if (newRJRow.getMap("list_item1").size() == 1) {

							String sum = aggColValue;
							int count = 1;
							String avg = aggColValue;
							String min = aggColValue;
							String max = aggColValue;

							JoinAggregationHelper.insertStatement(Float.valueOf(sum), count,Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, leftJoinAggTable, json);
							CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));

							stream.setLeftOrRightJoinAggNewRow(crow);
						}
						// more than one item --> update
						else {

							if (!leftJoinAggTable.equals("false")) {
								while(!JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1", newRJRow, json, joinKeyName, joinKeyValue, leftJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow));
								//JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1", newRJRow, json, joinKeyName, joinKeyValue, leftJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
							}
						}
					}

				}else {// update takes place in inner and left join aggs
					// insert row with aggkey/joinkey as pk to left and inner
					// join agg, if exists and values for sum,count,..
					// are calculated for this item only

					if (!leftJoinAggTable.equals("false")|| !innerJoinAggTable.equals("false")) {

						// only one item
						if (newRJRow.getMap("list_item1").size() == 1) {

							String sum = aggColValue;
							int count = 1;
							String avg = aggColValue;
							String min = aggColValue;
							String max = aggColValue;

							if (!leftJoinAggTable.equals("false")) {
								JoinAggregationHelper.insertStatement(Float.valueOf(sum), count,Float.valueOf(avg),Float.valueOf(min),Float.valueOf(max), joinKeyName, joinKeyValue, leftJoinAggTable, json);
								CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));
								stream.setLeftOrRightJoinAggNewRow(crow);
							}

							if (!innerJoinAggTable.equals("false")) {
								JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg),Float.valueOf(min),Float.valueOf(max), joinKeyName, joinKeyValue, innerJoinAggTable, json);
								CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
								stream.setInnerJoinAggNewRow(crow);
							}

						}
						// more than one item --> update
						else {

							if (!leftJoinAggTable.equals("false")) {
								while(!JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1", newRJRow, json, joinKeyName, joinKeyValue, leftJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow));
								//JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1", newRJRow, json, joinKeyName, joinKeyValue, leftJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
							}

							if (!innerJoinAggTable.equals("false")) {
								while(!JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1", newRJRow, json, joinKeyName, joinKeyValue, innerJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow));
								//JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item1", newRJRow, json, joinKeyName, joinKeyValue, innerJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
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


		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_old");
		String aggColValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), aggColName, aggColType, "_new");
		String oldAggColValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), aggColName, aggColType, "_old");


		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();
		CustomizedRow changeAK = stream.getReverseJoinUpdatedOldRow_changeJoinKey();

		// change in join key value
		if (!oldJoinKeyValue.equals("null")&& !oldJoinKeyValue.equals("'null'")&& !joinKeyValue.equals(oldJoinKeyValue)) {

			// a - First update old agg key
			if (oldRJRow.getMap("list_item2").size() == 1) {

				if (oldRJRow.getMap("list_item1").size() == 0) {

					// remove this key from right join agg, if exits
					if (!rightJoinAggTable.equals("false")) {
						CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, rightJoinAggTable, json));
						stream.setLeftOrRightJoinAggDeleteRow(crow);

						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), rightJoinAggTable, joinKeyName, oldJoinKeyValue);
					}
				} else {
					// remove this key right and inner join aggs, if they exist
					if (!rightJoinAggTable.equals("false")) {
						CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, rightJoinAggTable, json));

						stream.setLeftOrRightJoinAggDeleteRow(crow);
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), rightJoinAggTable, joinKeyName, oldJoinKeyValue);
					}

					if (!innerJoinAggTable.equals("false")) {
						CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, innerJoinAggTable, json));
						stream.setInnerJoinAggDeleteRow(crow);
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJoinAggTable, joinKeyName, oldJoinKeyValue);
					}
				}

			} else {

				if (!rightJoinAggTable.equals("false")) {
					while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaUpdatedRow(), json, rightJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK));
					//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaUpdatedRow(), json, rightJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK);
				}

				if (!oldRJRow.getMap("list_item1").isEmpty() && !innerJoinAggTable.equals("false")) {
					while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaUpdatedRow(), json, innerJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK));
					//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaUpdatedRow(), json, innerJoinAggTable, joinKeyName, oldJoinKeyValue, aggColName, oldAggColValue, changeAK);
				}
			}

			// b - Update new agg key

			if (newRJRow.getMap("list_item2").size() == 1) {

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
					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));
					stream.setLeftOrRightJoinAggNewRow(crow);
				}

				// add this key to inner table if exist [one item only]
				if (!newRJRow.getMap("list_item1").isEmpty() && !innerJoinAggTable.equals("false")) {
					JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, innerJoinAggTable, json);
					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
					stream.setInnerJoinAggNewRow(crow);
				}

			} else {


				if (!rightJoinAggTable.equals("false") && aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {
					while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, rightJoinAggTable, aggColValue));
					//JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, rightJoinAggTable, aggColValue);
				}

				// add this key to inner table if exist [one item only]
				if (!newRJRow.getMap("list_item1").isEmpty() && !innerJoinAggTable.equals("false")) {
					while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, innerJoinAggTable, aggColValue));
					//JoinAggregationHelper.updateNewRowByAddingNewElement(stream,joinKeyName, joinKeyValue, json, innerJoinAggTable, aggColValue);
				}
			}

		} else {
			// Case No change in join Key

			// there is change in agg col values otherwise ignore
			if (!oldAggColValue.equals(aggColValue)) {

				// updates take place in right_join_agg only
				if (newRJRow.getMap("list_item1").isEmpty()) {

					if (!rightJoinAggTable.equals("false")) {

						// only one item
						if (newRJRow.getMap("list_item2").size() == 1) {

							String sum = aggColValue;
							int count = 1;
							String avg = aggColValue;
							String min = aggColValue;
							String max = aggColValue;

							JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, rightJoinAggTable, json);

							CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));
							stream.setLeftOrRightJoinAggNewRow(crow);
						}
						// more than one item --> update
						else {
							while(!JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2", newRJRow, json, joinKeyName, joinKeyValue, rightJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow));
							//JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2", newRJRow, json, joinKeyName, joinKeyValue, rightJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
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
						if (newRJRow.getMap("list_item2").size() == 1) {

							String sum = aggColValue;
							int count = 1;
							String avg = aggColValue;
							String min = aggColValue;
							String max = aggColValue;

							if(!rightJoinAggTable.equals("false")) {
								JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, rightJoinAggTable, json);
								CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));

								stream.setLeftOrRightJoinAggNewRow(crow);
							}

							if(!innerJoinAggTable.equals("false")) {
								JoinAggregationHelper.insertStatement(Float.valueOf(sum), count, Float.valueOf(avg), Float.valueOf(min), Float.valueOf(max), joinKeyName, joinKeyValue, innerJoinAggTable, json);
								CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
								stream.setInnerJoinAggNewRow(crow);
							}

						}else { 
							if (!rightJoinAggTable.equals("false")) {
								while(!JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2", newRJRow, json, joinKeyName, joinKeyValue, rightJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow));
								//JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2", newRJRow, json, joinKeyName, joinKeyValue, rightJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
							}

							if (!innerJoinAggTable.equals("false")) {
								while(!JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2", newRJRow, json, joinKeyName, joinKeyValue, innerJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow));
								//JoinAggregationHelper.updateAggColValueOfNewRow(stream,"list_item2", newRJRow, json, joinKeyName, joinKeyValue, innerJoinAggTable, aggColName, aggColValue, oldAggColValue,oldRJRow);
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


		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_old");

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();
		CustomizedRow changeAK = stream.getReverseJoinUpdatedOldRow_changeJoinKey();


		//  change in join/agg Key

		if (!oldJoinKeyValue.equals("null")&& !oldJoinKeyValue.equals("'null'")
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			// if(new.list_tem1 == 1 && new.list_tem2 > 0)
			// add this new key to inner table
			// // u can get from the right join agg table if it exists
			// //otherwise u must loop on new.list_item2

			if (oldRJRow.getMap("list_item1").size() == 1 && !oldRJRow.getMap("list_item2").isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, innerJoinAggTable, json));
				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJoinAggTable, joinKeyName, oldJoinKeyValue);
			}

			// if(new.list_tem1 == 1 && new.list_tem2 > 0)
			// add this new key to inner table
			// u can get from the right join agg table if it exists
			// otherwise u must loop on new.list_item2
			if (newRJRow.getMap("list_item1").size() == 1
					&& !newRJRow.getMap("list_item2").isEmpty() && !innerJoinAggTable.equals("false") && !rightJoinAggTable.equals("false")) {

				JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,rightJoinAggTable, innerJoinAggTable, joinKeyName, joinKeyValue, json);

			} else if (newRJRow.getMap("list_item1").size() == 1
					&& !newRJRow.getMap("list_item2").isEmpty() && !innerJoinAggTable.equals("false") && rightJoinAggTable.equals("false")) {
				JoinAggregationHelper.addRowsToInnerJoinAgg(stream,"list_item2", newRJRow, aggColIndexInList, innerJoinAggTable, json, joinKeyName, joinKeyValue);
			}

		} else {

			//no change in join key

			if (newRJRow.getMap("list_item1").size() == 1
					&& !newRJRow.getMap("list_item2").isEmpty()) {
				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2
				if (!innerJoinAggTable.equals("false") && !rightJoinAggTable.equals("false") ) {
					JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,rightJoinAggTable, innerJoinAggTable, joinKeyName, oldJoinKeyValue, json);

				}else {
					JoinAggregationHelper.addRowsToInnerJoinAgg(stream,"list_item1", newRJRow, aggColIndexInList, innerJoinAggTable, json, joinKeyName, oldJoinKeyValue);
				}
			}
		}

		return true;

	}

	public boolean updateJoinAgg_UpdateRight_AggColLeftSide(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType, int aggColIndexInList) {


		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_old");
		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_new");

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();
		CustomizedRow changeAK = stream.getReverseJoinUpdatedOldRow_changeJoinKey();

		//  change in join/agg Key

		if (!oldJoinKeyValue.equals("null") && !oldJoinKeyValue.equals("'null'") && !joinKeyValue.equals(oldJoinKeyValue)) {

			if (oldRJRow.getMap("list_item2").size() == 1
					&& !oldRJRow.getMap("list_item1").isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, oldJoinKeyValue, innerJoinAggTable, json));


				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK((String) json.get("keyspace"), innerJoinAggTable, joinKeyName, oldJoinKeyValue);

			}

			// if(new.list_tem2 == 1 && new.list_tem1 > 0)
			// add this new key to inner table
			// u can get from the left join agg table if it exists
			// otherwise u must loop on new.list_item2
			if (newRJRow.getMap("list_item2").size() == 1
					&& !newRJRow.getMap("list_item1").isEmpty()
					&& !innerJoinAggTable.equals("false") && !leftJoinAggTable.equals("false")) {

				JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,leftJoinAggTable, innerJoinAggTable, joinKeyName, joinKeyValue, json);

			} else if(newRJRow.getMap("list_item2").size() == 1
					&& !newRJRow.getMap("list_item1").isEmpty()
					&& !innerJoinAggTable.equals("false") && leftJoinAggTable.equals("false")) {

				JoinAggregationHelper.addRowsToInnerJoinAgg(stream,"list_item1", newRJRow, aggColIndexInList, innerJoinAggTable, json, joinKeyName, joinKeyValue);
			}

		} else {

			// no change in join key

			if (newRJRow.getMap("list_item2").size() == 1
					&& !newRJRow.getMap("list_item1").isEmpty()) {
				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2
				if (!innerJoinAggTable.equals("false") && !leftJoinAggTable.equals("false")) {
					JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,leftJoinAggTable, innerJoinAggTable, joinKeyName, joinKeyValue, json);
				}else{
					JoinAggregationHelper.addRowsToInnerJoinAgg(stream,"list_item1", newRJRow, aggColIndexInList, innerJoinAggTable, json, joinKeyName, joinKeyValue);
				}
			}
		}

		return true;

	}

	public String getColumnValueFromDeltaStream(CustomizedRow stream, String name,
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

		String joinKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				joinKeyName, joinKeyType, "_old");

		String aggKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggKey, aggKeyType, "_new");
		String oldAggKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggKey, aggKeyType, "_old");

		String aggColValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggColName, aggColType, "_new");
		String oldAggColValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggColName, aggColType, "_old");

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();

		
		// change in join key value or agg key value
		if (!(oldJoinKeyValue.equals("'null'"))
				&& (!joinKeyValue.equals(oldJoinKeyValue) || !aggKeyValue
						.equals(oldAggKeyValue))) {

			// Case 1
			if ((!joinKeyValue.equals(oldJoinKeyValue))
					&& aggKeyValue.equals(oldAggKeyValue)) {
				if (oldRJRow.getMap("list_item1")
						.size() == 1) {
					if (!innerJoinAggTable.equals("false")) {
						while(!JoinAggGroupByHelper
								.searchAndDeleteRowFromJoinAggGroupBy(stream,json,
										innerJoinAggTable, aggKey,
										oldAggKeyValue, oldAggColValue));

						if (!newRJRow.getMap("list_item2").isEmpty()) {
							while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
									json, innerJoinAggTable, aggKey,
									aggKeyValue, aggColValue, oldAggColValue,
									oldAggKeyValue));
						}
					}
				}
			}

			// Case 2
			if ((!joinKeyValue.equals(oldJoinKeyValue))
					&& !aggKeyValue.equals(oldAggKeyValue)) {

				if (!leftJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, leftJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue));
					while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
							leftJoinAggTable, aggKey, aggKeyValue, aggColValue,
							oldAggColValue, oldAggKeyValue));
				}
				if (!innerJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue));

					if (!newRJRow.getMap("list_item2").isEmpty()) {
						while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue));
					}
				}
			}

			// Case 3
			if ((joinKeyValue.equals(oldJoinKeyValue))
					&& !aggKeyValue.equals(oldAggKeyValue)) {

				if (!leftJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, leftJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue));
					while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
							leftJoinAggTable, aggKey, aggKeyValue, aggColValue,
							oldAggColValue, oldAggKeyValue));
				}
				if (!innerJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue));

					if (!newRJRow.getMap("list_item2").isEmpty()) {
						while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue));
					}
				}
			}

		} else {
			// Case No change in join Key or Case of first insertion

			// Case 4 if there is no change in join key values ignore
			if (!aggColValue.equals(oldAggColValue)) {

				// updates take place in left_join_agg only
				if (newRJRow.getMap("list_item2")
						.isEmpty()) {
					if (!leftJoinAggTable.equals("false")) {
						while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								leftJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue));
					}

				}
				if (!newRJRow.getMap("list_item2")
						.isEmpty()) {
					if (!innerJoinAggTable.equals("false")) {
						while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue));
					}
					if (!leftJoinAggTable.equals("false")) {
						while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								leftJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue));
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

		String joinKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				joinKey, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				joinKey, joinKeyType, "_old");

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();
		CustomizedRow changeAK = stream.getReverseJoinUpdatedOldRow_changeJoinKey();

		// change in join/agg Key

		if (!(oldJoinKeyValue.equals("'null'"))
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			if (oldRJRow.getMap("list_item2")
					.size() == 1
					&& !oldRJRow.getMap("list_item1").isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				JoinAggGroupByHelper.deleteListItem1FromGroupBy(stream,oldRJRow,
						index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}

			// if(new.list_tem2 == 1 && new.list_tem1 > 0)
			// add this new key to inner table
			// u can get from the left join agg table if it exists
			// otherwise u must loop on new.list_item2
			if (newRJRow.getMap("list_item2")
					.size() == 1
					&& !newRJRow.getMap("list_item1").isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				JoinAggGroupByHelper.addListItem1toInnerJoinGroupBy(stream,
						stream.getDeltaUpdatedRow(), aggColName, leftJoinAggTable,
						newRJRow, index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}
		} else {

			// no change in join key or first insertion

			if (newRJRow.getMap("list_item2")
					.size() == 1
					&& !newRJRow.getMap("list_item1").isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2

				JoinAggGroupByHelper.addListItem1toInnerJoinGroupBy(stream,
						stream.getDeltaUpdatedRow(), aggColName, leftJoinAggTable,
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

		String joinKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				joinKeyName, joinKeyType, "_old");

		String aggKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggKey, aggKeyType, "_new");
		String oldAggKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggKey, aggKeyType, "_old");

		String aggColValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggColName, aggColType, "_new");
		String oldAggColValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggColName, aggColType, "_old");

		CustomizedRow newRJRow = stream.getReverseJoinDeleteNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();
		CustomizedRow changeAK = stream.getReverseJoinUpdatedOldRow_changeJoinKey();

		// change in join key value or agg key value
		if (!(oldJoinKeyValue.equals("'null'"))
				&& (!joinKeyValue.equals(oldJoinKeyValue) || !aggKeyValue
						.equals(oldAggKeyValue))) {

			// Case 1
			if ((!joinKeyValue.equals(oldJoinKeyValue))
					&& aggKeyValue.equals(oldAggKeyValue)) {
				if (oldRJRow.getMap("list_item2")
						.size() == 1) {
					if (!innerJoinAggTable.equals("false")) {
						while(!JoinAggGroupByHelper
								.searchAndDeleteRowFromJoinAggGroupBy(stream,json,
										innerJoinAggTable, aggKey,
										oldAggKeyValue, oldAggColValue));

						if (!newRJRow.getMap("list_item2").isEmpty()) {
							while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
									json, innerJoinAggTable, aggKey,
									aggKeyValue, aggColValue, oldAggColValue,
									oldAggKeyValue));
						}
					}
				}
			}

			// Case 2
			if ((!joinKeyValue.equals(oldJoinKeyValue))
					&& !aggKeyValue.equals(oldAggKeyValue)) {

				if (!rightJoinAggTable.equals("false")) {

					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, rightJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue));
					while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
							rightJoinAggTable, aggKey, aggKeyValue,
							aggColValue, oldAggColValue, oldAggKeyValue));
				}
				if (!innerJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue));

					if (!newRJRow.getMap("list_item1").isEmpty()) {
						while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue));
					}
				}
			}

			// Case 3
			if ((joinKeyValue.equals(oldJoinKeyValue))
					&& !aggKeyValue.equals(oldAggKeyValue)) {

				if (!rightJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, rightJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue));
					while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
							rightJoinAggTable, aggKey, aggKeyValue,
							aggColValue, oldAggColValue, oldAggKeyValue));
				}
				if (!innerJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, oldAggKeyValue,
							oldAggColValue));

					if (!newRJRow.getMap("list_item1").isEmpty()) {
						while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue));
					}
				}
			}

		} else {
			// Case No change in join Key or Case of first insertion

			// Case 4 if there is no change in join key values ignore
			if (!aggColValue.equals(oldAggColValue)) {

				// updates take place in left_join_agg only
				if (newRJRow.getMap("list_item1")
						.isEmpty()) {
					if (!rightJoinAggTable.equals("false")) {
						while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,json,
								rightJoinAggTable, aggKey, aggKeyValue,
								aggColValue, oldAggColValue, oldAggKeyValue));
					}

					if (!newRJRow.getMap("list_item2").isEmpty()) {
						if (!rightJoinAggTable.equals("false")) {
							while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
									json, rightJoinAggTable, aggKey,
									aggKeyValue, aggColValue, oldAggColValue,
									oldAggKeyValue));
						}
						if (!innerJoinAggTable.equals("false")) {
							while(!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
									json, innerJoinAggTable, aggKey,
									aggKeyValue, aggColValue, oldAggColValue,
									oldAggKeyValue));
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

		String joinKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				joinKeyName, joinKeyType, "_old");
		String aggColValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggColName, aggColType, "_new");
		String oldAggColValue = getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(),
				aggColName, aggColType, "_old");

		CustomizedRow newRJRow = stream.getReverseJoinDeleteNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();
		CustomizedRow changeAK = stream.getReverseJoinUpdatedOldRow_changeJoinKey();

		if (!(oldJoinKeyValue.equals("'null'"))
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			if (oldRJRow.getMap("list_item1")
					.size() == 1
					&& !oldRJRow.getMap("list_item2").isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				JoinAggGroupByHelper.deleteListItem2FromGroupBy(stream,oldRJRow,
						index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}

			// if(new.list_tem2 == 1 && new.list_tem1 > 0)
			// add this new key to inner table
			// u can get from the left join agg table if it exists
			// otherwise u must loop on new.list_item2
			if (newRJRow.getMap("list_item1")
					.size() == 1
					&& !newRJRow.getMap("list_item2").isEmpty()
					&& !innerJoinAggTable.equals("false")) {

				JoinAggGroupByHelper.addListItem2toInnerJoinGroupBy(stream,
						stream.getDeltaUpdatedRow(), aggColName, rightJoinAggTable,
						newRJRow, index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex);
			}
		} else {

			// no change in join key or first insertion
			if (newRJRow.getMap("list_item1")
					.size() == 1
					&& !newRJRow.getMap("list_item2").isEmpty()
					&& !innerJoinAggTable.equals("false")) {
				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2
				JoinAggGroupByHelper.addListItem2toInnerJoinGroupBy(stream,
						stream.getDeltaUpdatedRow(), aggColName, rightJoinAggTable,
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

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");
		String aggColValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), aggColName, aggColType, "_new");

		CustomizedRow newRJRow = null;
		if(stream.getReverseJoinDeleteNewRow()==null){
			if (!leftJoinAggTable.equals("false")){

				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));

				stream.setLeftOrRightJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), leftJoinAggTable,joinKeyName, joinKeyValue);
			}
			if (!innerJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));

				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), innerJoinAggTable,joinKeyName, joinKeyValue);
			}
			return true;

		}else{
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item2").isEmpty()) {

			if (newRJRow.getMap("list_item1").isEmpty()) {
				// remove from left
				if (!leftJoinAggTable.equals("false")){
					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));
					stream.setLeftOrRightJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), leftJoinAggTable,joinKeyName, joinKeyValue);
				}
			} else {
				// update left by subtracting
				if (!leftJoinAggTable.equals("false") && aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {
					while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", stream.getDeltaDeletedRow(), json, leftJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow));
					//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", stream.getDeltaDeletedRow(), json, leftJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
				}
			}

		} else {

			if (newRJRow.getMap("list_item1").isEmpty()) {
				// remove from left and inner
				if (!leftJoinAggTable.equals("false")){

					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));

					stream.setLeftOrRightJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), leftJoinAggTable,joinKeyName, joinKeyValue);
				}

				if (!innerJoinAggTable.equals("false")){

					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));

					stream.setInnerJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), innerJoinAggTable,joinKeyName, joinKeyValue);
				}
			} else {
				// update left and inner
				if ((!leftJoinAggTable.equals("false") || !innerJoinAggTable.equals("false"))
						&& aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {

					if (!leftJoinAggTable.equals("false")) {
						while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", stream.getDeltaDeletedRow(), json, leftJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow));
						//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", stream.getDeltaDeletedRow(), json, leftJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
					}

					if (!innerJoinAggTable.equals("false")) {
						while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", stream.getDeltaDeletedRow(), json, innerJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow));
						//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1", stream.getDeltaDeletedRow(), json, innerJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
					}

				}
			}
		}
		return true;
	}

	public boolean deleteJoinAgg_DeleteRight_AggColRightSide(Stream stream, String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,String aggColName, String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");
		String aggColValue =  Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), aggColName, aggColType, "_new");

		CustomizedRow newRJRow = null;
		if(stream.getReverseJoinDeleteNewRow()==null){
			if (!rightJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));

				stream.setLeftOrRightJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), rightJoinAggTable,joinKeyName, joinKeyValue);
			}
			if (!innerJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));

				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), innerJoinAggTable,joinKeyName, joinKeyValue);
			}
			return true;

		}else{
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item1").isEmpty()) {

			if (newRJRow.getMap("list_item2").isEmpty()) {
				// remove from rightJoinAggTable
				if (!rightJoinAggTable.equals("false")){

					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));

					stream.setLeftOrRightJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), rightJoinAggTable,joinKeyName, joinKeyValue);
				}
			} else {
				// update right by subtracting
				if (!rightJoinAggTable.equals("false") && aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")) {
					while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaDeletedRow(), json, rightJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow));
					//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaDeletedRow(), json, rightJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
				}
			}

		} else {

			if (newRJRow.getMap("list_item2").isEmpty()) {
				// remove from left and inner
				if (!rightJoinAggTable.equals("false")){
					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));
					stream.setLeftOrRightJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), rightJoinAggTable,joinKeyName, joinKeyValue);
				}

				if (!innerJoinAggTable.equals("false")){
					CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
					stream.setInnerJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), innerJoinAggTable, joinKeyName, joinKeyValue);
				}

			} else {
				// update rightJoinAggTable and inner<

				if ((!rightJoinAggTable.equals("false") || !innerJoinAggTable.equals("false"))
						&& aggColValue != null
						&& !aggColValue.equals("null")
						&& !aggColValue.equals("'null'")) {

					if (!rightJoinAggTable.equals("false")) {
						while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaDeletedRow(), json, rightJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow));
						//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaDeletedRow(), json, rightJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
					}

					if (!innerJoinAggTable.equals("false")) {
						while(!JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaDeletedRow(), json, innerJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow));
						//JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2", stream.getDeltaDeletedRow(), json, innerJoinAggTable, joinKeyName, joinKeyValue, aggColName, aggColValue, newRJRow);
					}
				}
			}

		}

		return true;
	}


	public boolean deleteJoinAgg_DeleteLeft_AggColRightSide(Stream stream, String rightJoinAggTable, String innerJoinAggTable, JSONObject json, String joinKeyType,
			String joinKeyName, String aggColName, String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");

		CustomizedRow newRJRow = null;
		if(stream.getReverseJoinDeleteNewRow()==null){
			if (!rightJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, rightJoinAggTable, json));

				stream.setLeftOrRightJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), rightJoinAggTable,joinKeyName, joinKeyValue);
			}
			return true;

		}else{
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item1").isEmpty() && !newRJRow.getMap("list_item2").isEmpty()) {

			// remove from inner
			if (!innerJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),innerJoinAggTable, joinKeyName, joinKeyValue);
			}
		}
		return true;

	}


	public boolean deleteJoinAgg_DeleteRight_AggColLeftSide(Stream stream,
			String leftJoinAggTable, String innerJoinAggTable, JSONObject json, String joinKeyType,
			String joinKeyName, String aggColName, String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");

		CustomizedRow newRJRow = null;
		if(stream.getReverseJoinDeleteNewRow()==null){
			if (!leftJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, leftJoinAggTable, json));
				stream.setLeftOrRightJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), leftJoinAggTable,joinKeyName, joinKeyValue);
			}
			return true;

		}else{
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item2").isEmpty() && !newRJRow.getMap("list_item1").isEmpty()) {
			// remove from inner
			if (!innerJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggregationHelper.selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));

				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),innerJoinAggTable, joinKeyName, joinKeyValue);
			}
		}
		return true;

	}


	public Boolean deleteJoinAgg_DeleteLeft_AggColLeftSide_GroupBy(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String aggKeyType, String aggkey, String aggColName,
			String aggColType, int index) {

		String aggColValue = getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(),
				aggColName, aggColType, "_new");
		String aggKeyValue = getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(),
				aggkey, aggKeyType, "_new");

		CustomizedRow newRJRow = null;

		if(stream.getReverseJoinDeleteNewRow()==null){
			if (!leftJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggGroupByHelper.selectStatement(leftJoinAggTable, aggkey, aggKeyValue, json));

				stream.setLeftOrRightJoinAggGroupByDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), leftJoinAggTable,aggkey, aggKeyValue);
			}

			if (!innerJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggGroupByHelper.selectStatement(innerJoinAggTable, aggkey, aggKeyValue, json));

				stream.setInnerJoinAggGroupByDeleteOldRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), innerJoinAggTable,aggkey, aggKeyValue);
			}
			return true;


		}else{
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item2").isEmpty()) {

			// remove from left: if count == 1, then delete entire row, else
			// substract & update row
			if (!innerJoinAggTable.equals("false")) { //
				while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,json,
						innerJoinAggTable, aggkey, aggKeyValue, aggColValue));
			}

		} else {

			if (newRJRow.getMap("list_item1")
					.isEmpty()) {
				// remove from left and inner
				if (!leftJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, leftJoinAggTable, aggkey, aggKeyValue,
							aggColValue));
				}
				if (!innerJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggkey, aggKeyValue,
							aggColValue));
				}
			}
		}

		if(!newRJRow.getMap("list_item1").isEmpty() && !newRJRow.getMap("list_item2")
				.isEmpty()){

			if (!leftJoinAggTable.equals("false")) {
				while(!JoinAggGroupByHelper.deleteElementFromRow(stream, json, leftJoinAggTable, aggkey, aggKeyValue, aggColValue));
			}
			if (!innerJoinAggTable.equals("false")) {
				while(!JoinAggGroupByHelper.deleteElementFromRow(stream, json, innerJoinAggTable, aggkey, aggKeyValue, aggColValue));
			}
		}

		return true;
	}


	public Boolean deleteJoinAgg_DeleteRight_AggColRightSide_GroupBy(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String aggKeyType, String aggKey,
			String aggColName, String aggColType, int index) {

		String aggColValue = getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(),
				aggColName, aggColType, "_new");
		String aggKeyValue = getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(),
				aggKey, aggKeyType, "_new");

		CustomizedRow newRJRow = null;

		if(stream.getReverseJoinDeleteNewRow()==null){
			if (!rightJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggGroupByHelper.selectStatement(rightJoinAggTable, aggKey, aggKeyValue, json));

				stream.setLeftOrRightJoinAggGroupByDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), rightJoinAggTable,aggKey, aggKeyValue);
			}

			if (!innerJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggGroupByHelper.selectStatement(innerJoinAggTable, aggKey, aggKeyValue, json));

				stream.setInnerJoinAggGroupByDeleteOldRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), innerJoinAggTable,aggKey, aggKeyValue);
			}
			return true;

		}else{
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item1").isEmpty()) {

			// remove from left: if count == 1, then delete entire row, else
			// substract & update row
			if (!innerJoinAggTable.equals("false")) {
				while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,json,
						innerJoinAggTable, aggKey, aggKeyValue, aggColValue));
			}

		} else {

			if (newRJRow.getMap("list_item2")
					.isEmpty()) {
				// remove from left and inner
				if (!rightJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, rightJoinAggTable, aggKey, aggKeyValue,
							aggColValue));
				}
				if (!innerJoinAggTable.equals("false")) {
					while(!JoinAggGroupByHelper.searchAndDeleteRowFromJoinAggGroupBy(stream,
							json, innerJoinAggTable, aggKey, aggKeyValue,
							aggColValue));
				}
			}
		}

		if(!newRJRow.getMap("list_item1").isEmpty() && !newRJRow.getMap("list_item2")
				.isEmpty()){

			if (!rightJoinAggTable.equals("false")) {
				while(!JoinAggGroupByHelper.deleteElementFromRow(stream, json, rightJoinAggTable, aggKey, aggKeyValue, aggColValue));
			}
			if (!innerJoinAggTable.equals("false")) {
				while(!JoinAggGroupByHelper.deleteElementFromRow(stream, json, innerJoinAggTable, aggKey, aggKeyValue, aggColValue));
			}
		}

		return true;
	}


	public Boolean deleteJoinAgg_DeleteLeft_AggColRightSide_GroupBy(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable, JSONObject json,
			String aggKeyType, String aggKey, String aggColName,
			String aggColType, int aggKeyIndex, int index) {

		CustomizedRow newRJRow = null;
		String aggKeyValue = getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(),
				aggKey, aggKeyType, "_new");

		if(stream.getReverseJoinDeleteNewRow()==null){
			if (!rightJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggGroupByHelper.selectStatement(rightJoinAggTable, aggKey, aggKeyValue, json));

				stream.setLeftOrRightJoinAggGroupByDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), rightJoinAggTable,aggKey, aggKeyValue);
			}
			return true;

		}else{
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}


		if (newRJRow.getMap("list_item1").isEmpty()
				&& !newRJRow.getMap("list_item2")
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

		CustomizedRow newRJRow = null;
		String aggKeyValue = getColumnValueFromDeltaStream(stream.getDeltaDeletedRow(),
				aggKey, aggKeyType, "_new");

		if(stream.getReverseJoinDeleteNewRow()==null){
			if (!leftJoinAggTable.equals("false")){
				CustomizedRow crow = new CustomizedRow(JoinAggGroupByHelper.selectStatement(leftJoinAggTable, aggKey, aggKeyValue, json));
				stream.setLeftOrRightJoinAggGroupByDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(), leftJoinAggTable,aggKey, aggKeyValue);
			}
			return true;

		}else{
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item2").isEmpty() && !newRJRow.getMap("list_item1")
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

	public void deleteElementFromHaving(Stream stream, JSONObject json,
			String havingTable, String aggKey, String aggKeyType, String pkVAlue,String aggCol, String aggColType) {

		String aggKeyValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), aggKey, aggKeyType, "_old");
		String aggColValue = Utils.getColumnValueFromDeltaStream(stream.getDeltaUpdatedRow(), aggCol, aggColType, "_old");

		ResultSet rs = PreaggregationHelper.selectStatement(json, havingTable, aggKey, aggKeyValue);
		Row row = rs.one();
		Map<String, String> temp = row.getMap("list_item", String.class, String.class);

		Map<String, String> map = new HashMap<String, String>();
		map.putAll(temp);

		if(map.size()==1){
			Utils.deleteEntireRowWithPK((String)json.get("keyspace"), havingTable, aggKey, aggKeyValue);
		}else{


			int aggColIndexInList = 0;

			for (int i = 0; i < stream.getDeltaDeletedRow().colDefSize; i++) {
				if (stream.getDeltaDeletedRow().getName(i).contentEquals(aggCol + "_new")) {
					break;
				}
				if (stream.getDeltaDeletedRow().getName(i).contains("_new"))
					aggColIndexInList++;
			}

			map.remove(pkVAlue);
			float sum = row.getFloat("sum") - Float.valueOf(aggColValue);
			int count =  row.getInt("count") - 1;
			float average = sum/count;

			float max = -Float.MAX_VALUE;
			float min = Float.MAX_VALUE;

			for (Map.Entry<String, String> entry : map.entrySet()) {
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

			PreaggregationHelper.insertStatement(json, havingTable, aggKey, aggKeyValue, map, sum, count, min, max, average);

		}
	}

	public String getReverseJoinTableName() {
		return reverseJoinTableName;
	}

	public void setReverseJoinTableName(String reverseJoinTableName) {
		this.reverseJoinTableName = reverseJoinTableName;
	}

}