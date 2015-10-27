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
import java.util.Queue;
import java.util.Set;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.cassandra.transport.SerDeserTest;
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

public class ViewManager {

	Cluster currentCluster = null;
	private String reverseJoinTableName;
	private String identifier;

	public ViewManager(Cluster currenCluster, String identifier) {
		this.currentCluster = currenCluster;
		this.identifier = identifier;
	}

	public boolean updateDelta(Stream stream, JSONObject json,
			int indexBaseTableName, String baseTablePrimaryKey) {

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

		ResultSet result = Utils.selectStatement(
				selectStatement_new.toString(), keyspace, table,
				baseTablePrimaryKey, data.get(baseTablePrimaryKey).toString());

		Row theRow = result.one();
		StringBuilder insertQueryAgg = new StringBuilder();

		// 2. If the row retrieved is empty, then this is a new insertion
		// 2.a Insert into delta, values from json in the appropriate _new
		// columns
		// 2.b the _old columns will have nulls as values

		if (theRow == null) {

			// 3. Execute insertion statement in delta
			org.apache.commons.lang.StringUtils.join(
					selectStatement_new_values, ", ");
			Utils.insertStatement(keyspace, "delta_" + table,
					selectStatement_new.toString(), selectStatement_new_values
					.toString().replace("[", "").replace("]", ""));

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
			Utils.insertStatement(
					keyspace,
					"delta_" + table,
					selectStatement_new.toString() + ", " + selectStatement_old,
					insertQueryAgg.toString());

		}

		System.out.println("Done Delta update");

		// 5. get the entire row from delta where update has happened
		// 5.a save the row and send bk to controller

		Row row = Utils.selectAllStatement(keyspace, "delta_" + table,
				baseTablePrimaryKey, data.get(baseTablePrimaryKey).toString());

		CustomizedRow crow = new CustomizedRow(row);

		// TO BE REMOVED
		// stream.setDeltaUpdatedRow(row);
		stream.setDeltaUpdatedRow(crow);
		stream.setDeltaJSON(json);

		return true;
	}

	public boolean deleteRowDelta(Stream stream, JSONObject json) {

		JSONObject condition = (JSONObject) json.get("condition");
		Object[] hm = condition.keySet().toArray();

		// 1. retrieve the row to be deleted from delta table
		Row row = Utils.selectAllStatement((String) json.get("keyspace"),
				"delta_" + json.get("table"), hm[0].toString(),
				condition.get(hm[0]).toString());

		// 2. set DeltaDeletedRow variable for streaming
		CustomizedRow crow = new CustomizedRow(row);
		stream.setDeltaDeletedRow(crow);

		// 3. delete row from delta
		Utils.deleteEntireRowWithPK((String) json.get("keyspace"), "delta_"
				+ json.get("table"), hm[0].toString(), condition.get(hm[0])
				.toString());

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
		float old_aggColValue = 0;

		if(stream.getDeltaDeletedRow()==null){
			stream.setDeltaDeletedRow(stream.getDeltaUpdatedRow());
		}

		// 1. retrieve agg key value from delta stream to retrieve the correct
		// row from preagg
		String aggKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), aggKey, aggKeyType, "_new");
		float aggColValue = 0;

		// 1.b Retrieve row key from delta stream
		String pk = Utils.getColumnValueFromDeltaStream(stream
				.getDeltaDeletedRow(), stream.getDeltaDeletedRow().getName(0)
				.toString(), stream.getDeltaDeletedRow().getType(0), "");

		Row theRow = PreaggregationHelper.selectStatement(json, preaggTable,
				aggKey, aggKeyValue).one();



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
				stream.setUpdatedPreaggRowDeleted(crow);
				stream.setDeleteOperation(true);
				String blob = Serialize.serializeStream2(stream);

				if(PreaggregationHelper.insertStatementToDelete(json, preaggTable,
						aggKey, aggKeyValue, blob, identifier,crow)){

					deleteEntireRowWithPK((String) json.get("keyspace"),
							preaggTable, aggKey, aggKeyValue);
				}

				stream.setDeleteOperation(false);

			} else {


				CustomizedRow crow = new CustomizedRow(theRow);
				stream.setUpdatedPreaggRowOldState(crow);

				// 5.a remove entry from map with that pk
				myMap.remove(pk);

				// 5.b retrieve aggCol value
				aggColValue = Float.valueOf(Utils
						.getColumnValueFromDeltaStream(
								stream.getDeltaDeletedRow(), aggCol,
								aggColType, "_new"));

				String oldAggColValue = Utils.getColumnValueFromDeltaStream(
						stream.getDeltaDeletedRow(), aggCol,
						aggColType, "_old");

				if(!oldAggColValue.equals("null"))
					old_aggColValue = Float.valueOf(Utils
							.getColumnValueFromDeltaStream(
									stream.getDeltaDeletedRow(), aggCol,
									aggColType, "_old"));
				else
					old_aggColValue = aggColValue;



				// 5.c adjust sum,count,average values
				count = myMap.size();
				sum = theRow.getFloat("sum") - old_aggColValue;
				average = sum / count;

				max = -Float.MAX_VALUE;
				min = Float.MAX_VALUE;

				int aggColIndexInList = 0;

				for (int i = 0; i < stream.getDeltaDeletedRow().colDefSize; i++) {
					if (stream.getDeltaDeletedRow().getName(i)
							.contentEquals(aggCol + "_new")) {
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

				CustomizedRow constructedRow = CustomizedRow
						.constructUpdatedPreaggRow(aggKey, aggKeyValue,aggKeyType, myMap,
								sum, (int) count, average, min, max,
								Serialize.serializeStream2(stream));
				stream.setUpdatedPreaggRow(constructedRow);
				String buffer_new = Serialize.serializeStream2(stream);

				while (!PreaggregationHelper.updateStatement(sum, (int) count,
						average, min, max, myMap, aggKey, aggKeyValue,
						preaggTable, json, blob_old, buffer_new, identifier)) {
					Row row = PreaggregationHelper.selectStatement(json,
							preaggTable, aggKey, aggKeyValue).one();
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

	public boolean updatePreaggregation(Stream stream, String aggKey,
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

			aggKeyValue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow,
					aggKey, aggKeyType, "_new");
			aggKeyValue_old = Utils.getColumnValueFromDeltaStream(
					deltaUpdatedRow, aggKey, aggKeyType, "_old");

			if (aggKeyValue.equals(aggKeyValue_old)
					|| deltaUpdatedRow.isNull(aggKey + "_old")) {
				sameKeyValue = true;
			}
		}

		// 1.b get all column values (_new) from delta table + including pk
		// & save them in myList

		ArrayList<String> myList = new ArrayList<String>();

		int aggColIndexInList = 0;

		for (int i = 0; i < deltaUpdatedRow.colDefSize; i++) {
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

			String temp = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow,
					aggCol, aggColType, "_new");
			if (temp.equals("null"))
				aggColValue = 0;
			else
				aggColValue = Float.valueOf(temp);

			temp = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, aggCol,
					aggColType, "_old");
			if (temp.equals("null"))
				aggColValue_old = 0;
			else
				aggColValue_old = Float.valueOf(temp);
		}



		// 2. if AggKey hasnt been updated or first insertion
		if (sameKeyValue || override) {

			// 2.a select from preagg table row with AggKey as PK

			boolean loop = false;

			String pk = myList.get(0);
			myList.remove(0);

			do {

				if (!override) {
					stream.setUpdatedPreaggRowDeleted(null);
				}
				if (!mapSize1) {
					stream.setUpdatedPreaggRowDeleted(null);
				}

				ResultSet rs = PreaggregationHelper.selectStatement(json,
						preaggTable, aggKey, aggKeyValue);
				Row theRow1 = rs.one();
				HashMap<String, String> myMap = new HashMap<>();

				CustomizedRow crowOld = new CustomizedRow(theRow1);
				stream.setUpdatedPreaggRowOldState(crowOld);

				// 2.c If row retrieved is null, then this is the first
				// insertion
				// for this given Agg key
				if (theRow1 == null) {
					// 2.c.1 create a map, add pk and list with delta _new
					// values
					// 2.c.2 set the agg col values

					if (PreaggregationHelper
							.firstInsertion(aggKeyType,stream, myList, aggColValue, json,
									preaggTable, aggKey, aggKeyValue, identifier,pk)) {
						loop = false;
					} else {
						loop = true;
					}

				} else {
					// 2.d If row is not null, then this is not the first
					// insertion
					// for this agg Key

					ByteBuffer blob_old = theRow1.getBytes("stream");

					System.out.println("sameKeyValue "+sameKeyValue);

					if (PreaggregationHelper.updateAggColValue(aggKeyType,stream, myList,
							aggColValue, aggColValue_old, theRow1,
							aggColIndexInList, json, preaggTable, aggKey,
							aggKeyValue, blob_old, identifier,pk)) {
						loop = false;
					} else {
						loop = true;
					}
				}

			} while (loop);

		} else if ((!sameKeyValue && !override)) {

			// 1. retrieve old agg key value from delta stream to retrieve the
			// correct row from preagg
			// was retrieved above in aggKeyValue_old variable

			// 2. select row with old aggkeyValue from delta stream
			ResultSet PreAggMap = PreaggregationHelper.selectStatement(json,
					preaggTable, aggKey, aggKeyValue_old);

			Row theRow = PreAggMap.one();

			CustomizedRow crowOld = new CustomizedRow(theRow);
			stream.setUpdatedPreaggRowOldState(crowOld);

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

					CustomizedRow crow = new CustomizedRow(theRow);
					stream.setUpdatedPreaggRowDeleted(crow);
					stream.setDeleteOperation(true);
					String blob = Serialize.serializeStream2(stream);
					if(PreaggregationHelper.insertStatementToDelete(json,
							preaggTable, aggKey, aggKeyValue_old, blob, identifier,crow)){

						// 4. delete the whole row
						Utils.deleteEntireRowWithPK((String) json.get("keyspace"),
								preaggTable, aggKey, aggKeyValue_old);

					}
					// Reseting the stream
					stream.setDeleteOperation(false);
					stream.setUpdatedPreaggRowDeleted(null);

					// 4.a perform a new insertion with new values
					updatePreaggregation(stream, aggKey, aggKeyType, json,
							preaggTable, baseTablePrimaryKey, aggCol,
							aggColType, true, true);

				} else {

					// 5. retrieve the pk value that has to be removed from the
					// map
					// 5.a remove entry from map with that pk
					// 5.c adjust sum,count,average values
					// 6. Execute insertion statement of the row with the
					// aggKeyValue_old to refelect changes

					ByteBuffer blob_old = theRow.getBytes("stream");

					while (!PreaggregationHelper.subtractOldAggColValue(aggKeyType,stream,
							myList, aggColValue_old, myMap, theRow,
							aggColIndexInList, json, preaggTable, aggKey,
							aggKeyValue_old, blob_old, identifier)) {
						PreAggMap = PreaggregationHelper.selectStatement(json,
								preaggTable, aggKey, aggKeyValue_old);
						theRow = PreAggMap.one();
						blob_old = theRow.getBytes("stream");
					}

					System.out.println("sameKeyVale "+sameKeyValue);
					//					// perform a new insertion for the new aggkey given in json
					updatePreaggregation(stream, aggKey, aggKeyType, json,
							preaggTable, baseTablePrimaryKey, aggCol,
							aggColType, true, false);
				}
			}

			//			// perform a new insertion for the new aggkey given in json
			updatePreaggregation(stream, aggKey, aggKeyType, json, preaggTable,
					baseTablePrimaryKey, aggCol, aggColType, true, false);
		}

		return true;
	}

	public void updateReverseJoin(Stream stream, JSONObject json, int cursor,
			int nrOfTables, String joinTable, List<String> baseTables,
			String joinKeyName, String tableName, String keyspace,
			String joinKeyType, int column) {

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

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(
				deltaUpdatedRow, joinKeyName, joinKeyType, "_new");
		String oldJoinKeyValue = Utils.getColumnValueFromDeltaStream(
				deltaUpdatedRow, joinKeyName, joinKeyType, "_old");

		Row theRow = Utils.selectAllStatement(keyspace, joinTable, joinKeyName,
				joinKeyValue);

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

		int counter;

		// already exists
		if (theRow != null) {

			Map<String, String> tempMapImmutable = theRow.getMap("list_item"
					+ column, String.class, String.class);

			System.out.println(tempMapImmutable);

			myMap.putAll(tempMapImmutable);
			myMap.put(pk, myList.toString());

			counter = theRow.getInt("counter");

		}
		else
			counter = -1;

		// change in join key value
		if (!oldJoinKeyValue.equals("null")
				&& !oldJoinKeyValue.equals("'null'")
				&& !joinKeyValue.equals(oldJoinKeyValue)) {

			int counter2 ;

			boolean loop = true;
			while(loop){

				// The row that contains the old join key value
				Row row_old_join_value = Utils.selectAllStatement(keyspace,
						joinTable, joinKeyName, oldJoinKeyValue);
				CustomizedRow crow = new CustomizedRow(row_old_join_value);
				if(row_old_join_value==null){
					crow = CustomizedRow.constructRJRow(joinKeyName, oldJoinKeyValue,joinKeyType,
							new HashMap<String, String>(), new HashMap<String, String>());
					counter2 = -1;
				}
				else
					counter2 = row_old_join_value.getInt("counter");

				stream.setReverseJoinUpadteOldRow(crow);

				HashMap<String, String> myMap2 = new HashMap<String, String>();

				if(row_old_join_value!=null){
					Map<String, String> tempMapImmutable2 = row_old_join_value.getMap(
							"list_item" + column, String.class, String.class);



					myMap2.putAll(tempMapImmutable2);
					// delete this from the other row
					myMap2.remove(pk);


				}

				// new updated row
				CustomizedRow newcr = null;

				if (column == 1){
					newcr = CustomizedRow.constructRJRow(joinKeyName, oldJoinKeyValue,joinKeyType,
							myMap2, crow.getMap("list_item2"));
					if(crow.getMap("list_item2").isEmpty())
						stream.setOppositeSizeZero(true);

				}
				else{
					newcr = CustomizedRow.constructRJRow(joinKeyName, oldJoinKeyValue,joinKeyType,
							crow.getMap("list_item1"), myMap2);
					if(crow.getMap("list_item1").isEmpty())
						stream.setOppositeSizeZero(true);
				}

				stream.setReverseJoinUpdateNewRow(newcr);

				stream.setChangeInJoinKey(true);


				boolean tempBool = ReverseJoinHelper.insertStatement(joinTable, keyspace, joinKeyName,
						oldJoinKeyValue, column, myMap2, stream, counter2);

				if(!tempBool)
					continue;
				else
					loop = false;


				// check if all maps are empty --> remove the row
				boolean allNull = true;

				if (myMap2.size() == 0 && row_old_join_value!=null) {

					for (int k = 0; k < baseTables.size() && allNull; k++) {
						if (column != k + 1
								&& row_old_join_value.getMap("list_item" + (k + 1),
										String.class, String.class).size() != 0) {
							allNull = false;

						}
					}
				} else{
					if(row_old_join_value!=null){
						allNull = false;
					}
				}

				// all entries are nulls
				if (allNull) {
					Utils.deleteEntireRowWithPK(keyspace, joinTable, joinKeyName,
							oldJoinKeyValue, counter2+1);
				}
			}

		}


		boolean loop = true;
		while(loop){
			CustomizedRow crow2;
			if (theRow != null) {
				crow2 = new CustomizedRow(theRow);

			} else {
				crow2 = CustomizedRow.constructRJRow(joinKeyName,joinKeyValue ,joinKeyType,
						new HashMap<String, String>(),
						new HashMap<String, String>());
			}
			stream.setReverseJoinUpadteOldRow(crow2);



			// Set the rj updated row for join updates
			// new updated row
			CustomizedRow newcr = null;

			if (theRow != null) {
				if (column == 1)
					newcr = CustomizedRow.constructRJRow(joinKeyName, joinKeyValue,joinKeyType,
							myMap, crow2.getMap("list_item2"));
				else
					newcr = CustomizedRow.constructRJRow(joinKeyName, joinKeyValue,joinKeyType,
							crow2.getMap("list_item1"), myMap);
			} else {
				if (column == 1)
					newcr = CustomizedRow.constructRJRow(joinKeyName, joinKeyValue,joinKeyType,
							myMap, new HashMap<String, String>());
				else
					newcr = CustomizedRow.constructRJRow(joinKeyName, joinKeyValue,joinKeyType,
							new HashMap<String, String>(), myMap);
			}

			stream.setReverseJoinUpdateNewRow(newcr);


			if(ReverseJoinHelper.insertStatement(joinTable, keyspace, joinKeyName,
					joinKeyValue, column, myMap, stream, counter)){
				loop = false;
			}
			else{

				theRow = Utils.selectAllStatement(keyspace, joinTable, joinKeyName,
						joinKeyValue);

				myList = new ArrayList<String>();

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

				myMap = new HashMap<String, String>();
				pk = myList.get(0);
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

					counter = theRow.getInt("counter");

				}
				else
					counter = -1;

			}

		}

	}

	public boolean updateJoinController(Stream stream, String innerJName,
			String leftJName, String rightJName, JSONObject json,
			Boolean updateLeft, Boolean updateRight, String joinKeyType,
			String joinKeyName, String pkName) {

		CustomizedRow oldRow = stream.getReverseJoinUpadteOldRow();
		CustomizedRow newRow = stream.getReverseJoinUpdateNewRow();

		if (updateLeft) {

			Map<String, String> tempMapImmutable1_old = oldRow
					.getMap("list_item1");
			Map<String, String> tempMapImmutable1_new = newRow
					.getMap("list_item1");

			Map<String, String> tempMapImmutable2_new = newRow
					.getMap("list_item2");

			// list_item2 size = 0
			if (tempMapImmutable2_new.isEmpty()) {

				// increase/same --> item added/updated
				if (tempMapImmutable1_new.size() >= tempMapImmutable1_old
						.size()) {
					if (!leftJName.equals("false"))
						JoinHelper.updateLeftJoinTable(stream, leftJName,
								newRow, json);
				}
				// decrease --> item removed
				else {

					if (!leftJName.equals("false") && !stream.isChangeInJoinKey()) {

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

						String joinTablePk = VmXmlHandler.getInstance()
								.getlJSchema()
								.getString(temp + ".primaryKey.name");

						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), leftJName, joinTablePk, pkValue);
					}
				}

			}
			// list_item2.size > 0
			else {

				// increase/same --> item added/updated
				if (tempMapImmutable1_new.size() >= tempMapImmutable1_old
						.size()) {
					leftCrossRight(stream, json, innerJName);

					if (tempMapImmutable1_new.size() == 1
							&& tempMapImmutable1_old.size() == 0
							&& !rightJName.equals("false")) {
						HashMap<String, String> myMap2 = new HashMap<String, String>();
						myMap2.putAll(tempMapImmutable2_new);
						DeleteJoinHelper.deleteFromRightJoinTable(stream,myMap2,
								rightJName, json, false);
					}


					if (!leftJName.equals("false") && stream.isChangeInJoinKey() && stream.isOppositeSizeZero()) {

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

						String joinTablePk = VmXmlHandler.getInstance()
								.getlJSchema()
								.getString(temp + ".primaryKey.name");

						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), leftJName, joinTablePk, pkValue);
					}


				}
				// dercrease
				else {
					JoinHelper.removeLeftCrossRight(stream, json, innerJName);

					if (tempMapImmutable1_new.isEmpty()
							&& !rightJName.equals("false")) {
						JoinHelper.addAllToRightJoinTable(rightJName,
								newRow.getMap("list_item2"), json);
					}

				}

			}
		}
		// update in right side
		else {

			Map<String, String> tempMapImmutable2_old = oldRow
					.getMap("list_item2");
			Map<String, String> tempMapImmutable2_new = newRow
					.getMap("list_item2");
			Map<String, String> tempMapImmutable1_new = newRow
					.getMap("list_item1");

			// list_item1 size = 0
			if (tempMapImmutable1_new.isEmpty()) {

				// increase/same --> item added/updated
				if (tempMapImmutable2_new.size() >= tempMapImmutable2_old
						.size()) {
					if (!rightJName.equals("false"))
						JoinHelper.updateRightJoinTable(stream, rightJName,
								newRow, json);
				}
				// decrease --> item removed
				else {

					if (!rightJName.equals("false") && !stream.isChangeInJoinKey()) {

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

						String joinTablePk = VmXmlHandler.getInstance()
								.getrJSchema()
								.getString(temp + ".primaryKey.name");

						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), rightJName, joinTablePk, pkValue);
					}
				}

			}
			// list_item1.size > 0
			else {

				// increase/same --> item added/updated
				if (tempMapImmutable2_new.size() >= tempMapImmutable2_old
						.size()) {
					rightCrossLeft(stream, json, innerJName);

					if (tempMapImmutable2_new.size() == 1
							&& tempMapImmutable2_old.size() == 0
							&& !leftJName.equals("false")) {
						HashMap<String, String> myMap1 = new HashMap<String, String>();
						myMap1.putAll(tempMapImmutable1_new);
						DeleteJoinHelper.deleteFromLeftJoinTable(myMap1,
								leftJName, json, false);
					}


					if (!rightJName.equals("false") && stream.isChangeInJoinKey() && stream.isOppositeSizeZero()) {

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

						String joinTablePk = VmXmlHandler.getInstance()
								.getrJSchema()
								.getString(temp + ".primaryKey.name");

						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), rightJName, joinTablePk, pkValue);
					}

				}
				// dercrease
				else {
					JoinHelper.removeRightCrossLeft(stream, json, innerJName);

					if (tempMapImmutable2_new.isEmpty()
							&& !leftJName.equals("false")) {
						JoinHelper.addAllToLeftJoinTable(leftJName,
								newRow.getMap("list_item1"), json);
					}

				}

			}

		}

		return true;
	}

	public boolean updateSelection(CustomizedRow row, String keyspace,
			String selecTable, String selColName) {

		// 1. get column names of delta table
		// 1.b save column values from delta stream too

		StringBuilder insertion = new StringBuilder();
		StringBuilder insertionValues = new StringBuilder();

		for (int i = 0; i < row.colDefSize; i++) {

			switch (row.getType(i)) {

			case "text":
				if (!row.getName(i).contains("_old")) {
					if (row.getName(i).contains("_new")) {
						String[] split = row.getName(i).split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getName(i) + ", ");
					}

					insertionValues.append("'" + row.getString(row.getName(i))
							+ "', ");
				}
				break;

			case "int":
				if (!row.getName(i).contains("_old")) {

					if (row.getName(i).contains("_new")) {
						String[] split = row.getName(i).split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getName(i) + ", ");
					}

					insertionValues.append(row.getInt(row.getName(i)) + ", ");
				}
				break;

			case "varint":
				if (!row.getName(i).contains("_old")) {

					if (row.getName(i).contains("_new")) {
						String[] split = row.getName(i).split("_");
						insertion.append(split[0] + ", ");
					} else {
						insertion.append(row.getName(i) + ", ");
					}
					insertionValues
					.append(row.getVarint(row.getName(i)) + ", ");
				}
				break;

			case "varchar":
				if (!row.getName(i).contains("_old")) {

					if (row.getName(i).contains("_new")) {
						String[] split = row.getName(i).split("_");
						insertion.append(split[0] + ", ");
					} else {

						insertion.append(row.getName(i) + ", ");
					}

					insertionValues.append("'" + row.getString(row.getName(i))
							+ "', ");
				}
				break;

			}
		}

		insertion.deleteCharAt(insertion.length() - 2);
		insertionValues.deleteCharAt(insertionValues.length() - 2);

		Utils.insertStatement(keyspace, selecTable, insertion.toString(),
				insertionValues.toString());

		return true;
	}

	public boolean deleteRowSelection(String keyspace, String selecTable,
			String baseTablePrimaryKey, JSONObject json) {

		if (json.containsKey("condition")) {

			JSONObject condition = (JSONObject) json.get("condition");
			Object[] hm = condition.keySet().toArray();
			Utils.deleteEntireRowWithPK(keyspace, selecTable,
					baseTablePrimaryKey, condition.get(hm[0]).toString());
		} else {
			JSONObject data = (JSONObject) json.get("data");
			Utils.deleteEntireRowWithPK(keyspace, selecTable,
					baseTablePrimaryKey, data.get(baseTablePrimaryKey)
					.toString());
		}

		System.out.println("Possible Deletes Successful from Selection View");

		return true;

	}

	private boolean rightCrossLeft(Stream stream, JSONObject json,
			String innerJTableName) {

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

			String rightPkValue = Utils.getColumnValueFromDeltaStream(
					stream.getDeltaUpdatedRow(), rightPkName, rightPkType, "");

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

	private boolean leftCrossRight(Stream stream, JSONObject json,
			String innerJTableName) {

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
			String leftPkValue = Utils.getColumnValueFromDeltaStream(
					stream.getDeltaUpdatedRow(), leftPkName, leftPkType, "");

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
	public void deleteReverseJoin(Stream stream, JSONObject json, int cursor,
			int nrOfTables, String joinTable, List<String> baseTables,
			String joinKeyName, String tableName, String keyspace,
			String joinKeyType, int column) {

		setReverseJoinTableName(joinTable);

		if(stream.getDeltaDeletedRow()==null){
			stream.setDeltaDeletedRow(stream.getDeltaUpdatedRow());
		}

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");

		boolean loop = true;

		while(loop){


			Row theRow = Utils.selectAllStatement(keyspace, joinTable, joinKeyName,
					joinKeyValue);


			CustomizedRow crow = new CustomizedRow(theRow);
			stream.setRevereJoinDeleteOldRow(crow);

			HashMap<String, String> myMap = null;

			String pkType = stream.getDeltaDeletedRow().getType(0);
			String pkName = stream.getDeltaDeletedRow().getName(0);
			String pk = Utils.getColumnValueFromDeltaStream(
					stream.getDeltaDeletedRow(), pkName, pkType, "");

			// already exists
			if (theRow != null) {

				Map<String, String> tempMapImmutable = theRow.getMap("list_item"
						+ column, String.class, String.class);

				System.out.println(tempMapImmutable);

				int counter = theRow.getInt("counter");

				if (tempMapImmutable.size() > 1) {
					myMap = new HashMap<String, String>();
					myMap.putAll(tempMapImmutable);
					myMap.remove(pk);
				}

				// new updated row
				CustomizedRow newcr = null;

				if (column == 1){
					if(myMap==null){
						newcr = CustomizedRow.constructRJRow(joinKeyName, joinKeyValue,joinKeyType,
								new HashMap<String, String>(), crow.getMap("list_item2"));
					}else{
						newcr = CustomizedRow.constructRJRow(joinKeyName, joinKeyValue,joinKeyType,
								myMap, crow.getMap("list_item2"));
					}
				}else{
					if(myMap==null){
						newcr = CustomizedRow.constructRJRow(joinKeyName, joinKeyValue,joinKeyType,
								crow.getMap("list_item1"), new HashMap<String, String>());
					}else{
						newcr = CustomizedRow.constructRJRow(joinKeyName, joinKeyValue,joinKeyType,
								crow.getMap("list_item1"), myMap);
					}
				}

				stream.setReverseJoinDeleteNewRow(newcr);

				stream.setDeleteOperation(true);

				boolean tempBool = ReverseJoinHelper.insertStatement(joinTable, keyspace, joinKeyName,
						joinKeyValue, column, myMap, stream, counter);

				stream.setDeleteOperation(false);

				if(!tempBool)
					continue;
				else
					loop = false;


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
					Utils.deleteEntireRowWithPK(keyspace, joinTable, joinKeyName,
							joinKeyValue, counter+1);
				}

			}

		}

	}

	public boolean deleteJoinController(Stream stream,
			CustomizedRow deltaDeletedRow, String innerJName, String leftJName,
			String rightJName, JSONObject json, Boolean updateLeft,
			Boolean updateRight) {

		// 1. get row updated by reverse join
		CustomizedRow theRow = stream.getRevereJoinDeleteOldRow();
		HashMap<String, String> myMap2  = new HashMap<String, String>();
		HashMap<String, String> myMap1  = new HashMap<String, String>();

		if(theRow!=null) {
			// 1.a get columns item_1, item_2
			Map<String, String> tempMapImmutable1 = theRow.getMap("list_item1");
			Map<String, String> tempMapImmutable2 = theRow.getMap("list_item2");

			// 2. retrieve list_item1, list_item2
			myMap1.putAll(tempMapImmutable1);
			myMap2.putAll(tempMapImmutable2);
		}

		// Case 1 : delete from left join table if item_list2 is empty
		// !leftJName.equals(false) meaning : no left join wanted, only right
		if (updateLeft && myMap2.size() == 0 && !leftJName.equals("false")) {
			DeleteJoinHelper.deleteElementFromLeftJoinTable(stream,myMap1, leftJName, json,
					true);
			return true;
		}

		// Case 2: delete from right join table if item_list1 is empty
		// !rightName.equals(false) meaning : no right join wanted, only left
		if (updateRight && myMap1.size() == 0 && !rightJName.equals("false")) {
			DeleteJoinHelper.deleteElementFromRightJoinTable(stream,myMap2, rightJName, json,
					true);
			return true;
		}

		CustomizedRow newDeletedRow = stream.getReverseJoinDeleteNewRow();

		// Case 3: delete happened from left and list_item1 is not empty
		// remove cross product from innerjoin
		if (updateLeft && myMap2.size() > 0) {

			DeleteJoinHelper.removeDeleteLeftCrossRight(stream, json,
					innerJName, myMap2);

			// delete happened in left and new list_item 1 is empty
			// add all list_item2 to right join
			if (newDeletedRow.getMap("list_item1").size() == 0) {
				JoinHelper.addAllToRightJoinTable(rightJName,
						newDeletedRow.getMap("list_item2"), json);
			}

		}

		// Case 4: delete happened from rigth and list_item2 is not empty
		// remove cross product from inner join
		if (updateRight && myMap1.size() > 0) {

			// removeRightCrossLeft(json, innerJName);
			DeleteJoinHelper.removeDeleteRightCrossLeft(stream, json,
					innerJName, myMap1);

			// delete happened in right and new list_item 2 is empty
			// add all list_item1 to left join
			if (newDeletedRow.getMap("list_item2").size() == 0) {
				JoinHelper.addAllToLeftJoinTable(leftJName,
						newDeletedRow.getMap("list_item1"), json);
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
		String pkVAlue = Utils.getColumnValueFromDeltaStream(preagRow, pkName,
				pkType, "");

		PreaggregationHelper.insertStatement(json, havingTable, pkName,
				pkVAlue, myMap, sum, count, min, max, average, identifier);

		return true;
	}


	public boolean deleteRowHaving(String keyspace, String havingTable,
			CustomizedRow preagRow) {

		String pkName = preagRow.getName(0);
		String pkType = preagRow.getType(0);
		String pkVAlue = Utils.getColumnValueFromDeltaStream(preagRow, pkName,
				pkType, "");

		Utils.deleteEntireRowWithPK(keyspace, havingTable, pkName, pkVAlue);

		return true;
	}



	public boolean updateJoinAgg_UpdateLeft_AggColLeftSide(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String joinKeyType, String joinKeyName, String aggColName,
			String aggColType) {
		// TODO Auto-generated method stub



		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();


		String joinKeyValue = newRJRow.getString(joinKeyName);

		String aggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggColName, aggColType, "_new");
		String oldAggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggColName, aggColType, "_old");



		// there is change in agg col values otherwise ignore
		//if (!oldAggColValue.equals(aggColValue)) {

		// increase
		if (newRJRow.getMap("list_item1").size() >= oldRJRow.getMap(
				"list_item1").size()) {



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

						JoinAggregationHelper.insertStatement(
								Float.valueOf(sum), count,
								Float.valueOf(avg), Float.valueOf(min),
								Float.valueOf(max), joinKeyName,
								joinKeyValue, leftJoinAggTable, json, identifier);
						CustomizedRow crow = new CustomizedRow(
								JoinAggregationHelper.selectStatement(
										joinKeyName, joinKeyValue,
										leftJoinAggTable, json));

						stream.setLeftOrRightJoinAggNewRow(crow);
					}
					// more than one item --> update
					else {

						if (!leftJoinAggTable.equals("false")) {
							if(newRJRow.getMap("list_item1").size() == oldRJRow.getMap(
									"list_item1").size())
								while (!JoinAggregationHelper
										.updateAggColValueOfNewRow(stream,
												"list_item1", newRJRow, json,
												joinKeyName, joinKeyValue,
												leftJoinAggTable, aggColName,
												aggColValue, oldAggColValue,
												oldRJRow, identifier))
									;
							else
								while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream, joinKeyName, joinKeyValue, json, leftJoinAggTable, aggColValue, identifier));

						}
					}
				}

			} else {// update takes place in inner and left join aggs
				// insert row with aggkey/joinkey as pk to left and
				// inner
				// join agg, if exists and values for sum,count,..
				// are calculated for this item only

				if (!leftJoinAggTable.equals("false")
						|| !innerJoinAggTable.equals("false")) {

					// only one item
					if (newRJRow.getMap("list_item1").size() == 1) {

						String sum = aggColValue;
						int count = 1;
						String avg = aggColValue;
						String min = aggColValue;
						String max = aggColValue;

						if (!leftJoinAggTable.equals("false")) {
							JoinAggregationHelper.insertStatement(
									Float.valueOf(sum), count,
									Float.valueOf(avg), Float.valueOf(min),
									Float.valueOf(max), joinKeyName,
									joinKeyValue, leftJoinAggTable, json, identifier);
							CustomizedRow crow = new CustomizedRow(
									JoinAggregationHelper.selectStatement(
											joinKeyName, joinKeyValue,
											leftJoinAggTable, json));
							stream.setLeftOrRightJoinAggNewRow(crow);
						}

						if (!innerJoinAggTable.equals("false")) {
							JoinAggregationHelper.insertStatement(
									Float.valueOf(sum), count,
									Float.valueOf(avg), Float.valueOf(min),
									Float.valueOf(max), joinKeyName,
									joinKeyValue, innerJoinAggTable, json, identifier);
							CustomizedRow crow = new CustomizedRow(
									JoinAggregationHelper.selectStatement(
											joinKeyName, joinKeyValue,
											innerJoinAggTable, json));
							stream.setInnerJoinAggNewRow(crow);
						}

					}
					// more than one item --> update
					else {

						if (!leftJoinAggTable.equals("false")) {
							if(newRJRow.getMap("list_item1").size() == oldRJRow.getMap(
									"list_item1").size())
								while (!JoinAggregationHelper
										.updateAggColValueOfNewRow(stream,
												"list_item1", newRJRow, json,
												joinKeyName, joinKeyValue,
												leftJoinAggTable, aggColName,
												aggColValue, oldAggColValue,
												oldRJRow, identifier))
									;
							else
								while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream, joinKeyName, joinKeyValue, json, leftJoinAggTable, aggColValue, identifier));


						}

						if (!innerJoinAggTable.equals("false")) {
							if(newRJRow.getMap("list_item1").size() == oldRJRow.getMap(
									"list_item1").size())
								while (!JoinAggregationHelper
										.updateAggColValueOfNewRow(stream,
												"list_item1", newRJRow, json,
												joinKeyName, joinKeyValue,
												innerJoinAggTable, aggColName,
												aggColValue, oldAggColValue,
												oldRJRow, identifier))
									;

							else
								while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream, joinKeyName, joinKeyValue, json, innerJoinAggTable, aggColValue, identifier));

						}

					}
				}
			}
		}
		// decrease
		else {
			// updates take place in left_join_agg only
			if (newRJRow.getMap("list_item2").isEmpty()) {



				if (newRJRow.getMap("list_item1").isEmpty()) {
					// delete it from left

					if(!leftJoinAggTable.equals("false")) {
						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), leftJoinAggTable, joinKeyName,
								joinKeyValue);
					}

				} else {
					if(!leftJoinAggTable.equals("false")) {
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item1",
										stream.getDeltaUpdatedRow(), json,
										leftJoinAggTable, joinKeyName,
										joinKeyValue, aggColName,
										oldAggColValue, newRJRow, identifier))
							;
					}
				}

			}

			else {

				if (newRJRow.getMap("list_item1").isEmpty()) {
					// delete it from left
					if(!leftJoinAggTable.equals("false")){
						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), leftJoinAggTable, joinKeyName,
								joinKeyValue);
					}
					if(!innerJoinAggTable.equals("false")){
						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), innerJoinAggTable, joinKeyName,
								joinKeyValue);
					}

				} else {
					if(leftJoinAggTable.equals("false")){
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item1",
										stream.getDeltaUpdatedRow(), json,
										leftJoinAggTable, joinKeyName,
										joinKeyValue, aggColName,
										oldAggColValue, newRJRow, identifier))
							;
					}

					if(innerJoinAggTable.equals("false")){
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item1",
										stream.getDeltaUpdatedRow(), json,
										innerJoinAggTable, joinKeyName,
										joinKeyValue, aggColName,
										oldAggColValue, newRJRow, identifier))
							;

					}
				}

			}
		}

		//	}

		return true;
	}

	public boolean updateJoinAgg_UpdateRight_AggColRightSide(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType) {


		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();


		String joinKeyValue = newRJRow.getString(joinKeyName);

		String aggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggColName, aggColType, "_new");
		String oldAggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggColName, aggColType, "_old");



		// there is change in agg col values otherwise ignore
		//if (!oldAggColValue.equals(aggColValue)) {

		// increase
		if (newRJRow.getMap("list_item2").size() >= oldRJRow.getMap(
				"list_item2").size()) {


			// updates take place in left_join_agg only
			if (newRJRow.getMap("list_item1").isEmpty()) {

				if (!rightJoinAggTable.equals("false")) {

					// only one item
					if (newRJRow.getMap("list_item2").size() == 1) {

						String sum = aggColValue;
						int count = 1;
						String avg = aggColValue;
						String min = aggColValue;
						String max = aggColValue;

						JoinAggregationHelper.insertStatement(
								Float.valueOf(sum), count,
								Float.valueOf(avg), Float.valueOf(min),
								Float.valueOf(max), joinKeyName,
								joinKeyValue, rightJoinAggTable, json, identifier);
						CustomizedRow crow = new CustomizedRow(
								JoinAggregationHelper.selectStatement(
										joinKeyName, joinKeyValue,
										rightJoinAggTable, json));

						stream.setLeftOrRightJoinAggNewRow(crow);
					}
					// more than one item --> update
					else {

						if (!rightJoinAggTable.equals("false")) {

							if(newRJRow.getMap("list_item2").size() == oldRJRow.getMap(
									"list_item2").size())
								while (!JoinAggregationHelper
										.updateAggColValueOfNewRow(stream,
												"list_item2", newRJRow, json,
												joinKeyName, joinKeyValue,
												rightJoinAggTable, aggColName,
												aggColValue, oldAggColValue,
												oldRJRow, identifier))
									;
							else
								while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream, joinKeyName, joinKeyValue, json, rightJoinAggTable, aggColValue, identifier));

						}
					}
				}

			} else {// update takes place in inner and left join aggs
				// insert row with aggkey/joinkey as pk to left and
				// inner
				// join agg, if exists and values for sum,count,..
				// are calculated for this item only

				if (!rightJoinAggTable.equals("false")
						|| !innerJoinAggTable.equals("false")) {

					// only one item
					if (newRJRow.getMap("list_item2").size() == 1) {

						String sum = aggColValue;
						int count = 1;
						String avg = aggColValue;
						String min = aggColValue;
						String max = aggColValue;

						if (!rightJoinAggTable.equals("false")) {
							JoinAggregationHelper.insertStatement(
									Float.valueOf(sum), count,
									Float.valueOf(avg), Float.valueOf(min),
									Float.valueOf(max), joinKeyName,
									joinKeyValue, rightJoinAggTable, json, identifier);
							CustomizedRow crow = new CustomizedRow(
									JoinAggregationHelper.selectStatement(
											joinKeyName, joinKeyValue,
											rightJoinAggTable, json));
							stream.setLeftOrRightJoinAggNewRow(crow);
						}

						if (!innerJoinAggTable.equals("false")) {
							JoinAggregationHelper.insertStatement(
									Float.valueOf(sum), count,
									Float.valueOf(avg), Float.valueOf(min),
									Float.valueOf(max), joinKeyName,
									joinKeyValue, innerJoinAggTable, json, identifier);
							CustomizedRow crow = new CustomizedRow(
									JoinAggregationHelper.selectStatement(
											joinKeyName, joinKeyValue,
											innerJoinAggTable, json));
							stream.setInnerJoinAggNewRow(crow);
						}

					}
					// more than one item --> update
					else {

						if (!rightJoinAggTable.equals("false")) {
							if(newRJRow.getMap("list_item2").size() == oldRJRow.getMap(
									"list_item2").size())
								while (!JoinAggregationHelper
										.updateAggColValueOfNewRow(stream,
												"list_item2", newRJRow, json,
												joinKeyName, joinKeyValue,
												rightJoinAggTable, aggColName,
												aggColValue, oldAggColValue,
												oldRJRow, identifier))
									;
							else
								while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream, joinKeyName, joinKeyValue, json, rightJoinAggTable, aggColValue, identifier));


						}

						if (!innerJoinAggTable.equals("false")) {
							if(newRJRow.getMap("list_item2").size() == oldRJRow.getMap(
									"list_item2").size())
								while (!JoinAggregationHelper
										.updateAggColValueOfNewRow(stream,
												"list_item2", newRJRow, json,
												joinKeyName, joinKeyValue,
												innerJoinAggTable, aggColName,
												aggColValue, oldAggColValue,
												oldRJRow, identifier))
									;

							else
								while(!JoinAggregationHelper.updateNewRowByAddingNewElement(stream, joinKeyName, joinKeyValue, json, innerJoinAggTable, aggColValue, identifier));

						}

					}
				}
			}
		}
		// decrease
		else {
			// updates take place in left_join_agg only
			if (newRJRow.getMap("list_item1").isEmpty()) {


				if (newRJRow.getMap("list_item2").isEmpty()) {
					// delete it from left
					if(!rightJoinAggTable.equals("false")){
						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), rightJoinAggTable, joinKeyName,
								joinKeyValue);
					}
				} else {
					if(!rightJoinAggTable.equals("false")){
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item2",
										stream.getDeltaUpdatedRow(), json,
										rightJoinAggTable, joinKeyName,
										joinKeyValue, aggColName,
										oldAggColValue, newRJRow, identifier))
							;
					}
				}

			}

			else {

				if (newRJRow.getMap("list_item2").isEmpty()) {
					// delete it from left
					if(!rightJoinAggTable.equals("false")){
						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), rightJoinAggTable, joinKeyName,
								joinKeyValue);
					}

					if(!innerJoinAggTable.equals("false")){
						Utils.deleteEntireRowWithPK(json.get("keyspace")
								.toString(), innerJoinAggTable, joinKeyName,
								joinKeyValue);
					}

				} else {
					if(!rightJoinAggTable.equals("false")){
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item2",
										stream.getDeltaUpdatedRow(), json,
										rightJoinAggTable, joinKeyName,
										joinKeyValue, aggColName,
										oldAggColValue, newRJRow, identifier))
							;
					}
					if(!innerJoinAggTable.equals("false")){
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item2",
										stream.getDeltaUpdatedRow(), json,
										innerJoinAggTable, joinKeyName,
										joinKeyValue, aggColName,
										oldAggColValue, newRJRow, identifier))
							;
					}
				}

			}
		}

		//	}

		return true;
	}

	public boolean updateJoinAgg_UpdateLeft_AggColRightSide(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType, int aggColIndexInList) {


		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();

		String joinKeyValue =newRJRow.getString(joinKeyName);


		// no change in join key

		// increase
		if (newRJRow.getMap("list_item1").size() >= oldRJRow.getMap(
				"list_item1").size()) {



			if (newRJRow.getMap("list_item1").size() == 1
					&& oldRJRow.getMap("list_item1").size() == 0
					&& !newRJRow.getMap("list_item2").isEmpty()) {
				// add this key to the inner table
				// u can get from the right join agg table if it exists
				// otherwise u must loop on new.list_item2
				if (!innerJoinAggTable.equals("false")
						&& !rightJoinAggTable.equals("false")) {
					JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,
							rightJoinAggTable, innerJoinAggTable, joinKeyName,
							joinKeyValue, json, identifier);

				} else {

					JoinAggregationHelper.addRowsToInnerJoinAgg(stream,
							"list_item2", newRJRow, aggColIndexInList,
							innerJoinAggTable, json, joinKeyName, joinKeyValue, identifier);
				}
			}
		}
		// decrease
		else {

			if (newRJRow.getMap("list_item1").size() == 0
					&& !newRJRow.getMap("list_item2").isEmpty()) {

				if(!innerJoinAggTable.equals("false")) {
					Utils.deleteEntireRowWithPK((String) json.get("keyspace"),
							innerJoinAggTable, joinKeyName, joinKeyValue);

				}
			}

		}

		return true;

	}

	public boolean updateJoinAgg_UpdateRight_AggColLeftSide(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String joinKeyType, String joinKeyName, String aggColName,
			String aggColType, int aggColIndexInList) {



		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();

		String joinKeyValue =newRJRow.getString(joinKeyName);

		// change in join/agg Key
		// increase
		if (newRJRow.getMap("list_item2").size() >= oldRJRow.getMap(
				"list_item2").size()) {

			if (newRJRow.getMap("list_item2").size() == 1
					&& oldRJRow.getMap("list_item2").size() == 0
					&& !newRJRow.getMap("list_item1").isEmpty()) {

				if (!innerJoinAggTable.equals("false")
						&& !leftJoinAggTable.equals("false")) {
					JoinAggregationHelper.moveRowsToInnerJoinAgg(stream,
							leftJoinAggTable, innerJoinAggTable, joinKeyName,
							joinKeyValue, json, identifier);

				} else {
					JoinAggregationHelper.addRowsToInnerJoinAgg(stream,
							"list_item1", newRJRow, aggColIndexInList,
							innerJoinAggTable, json, joinKeyName, joinKeyValue, identifier);
				}
			}
		}
		// decrease
		else {

			if (newRJRow.getMap("list_item2").size() == 0
					&& !newRJRow.getMap("list_item1").isEmpty()) {

				if(!innerJoinAggTable.equals("false")){
					Utils.deleteEntireRowWithPK((String) json.get("keyspace"),
							innerJoinAggTable, joinKeyName, joinKeyValue);
				}
			}

		}

		return true;

	}


	public Boolean updateJoinAgg_UpdateLeft_AggColLeftSide_GroupBy(
			Stream stream, String innerJoinAggTable, String leftJoinAggTable,
			JSONObject json, String aggKeyType, String aggKey,
			String aggColName, String aggColType, String joinKeyName,
			String joinKeyType) {

		String aggKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggKey, aggKeyType, "_new");
		String oldAggKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggKey, aggKeyType, "_old");

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_new");
		String oldjoinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_old");

		String aggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggColName, aggColType, "_new");
		String oldAggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggColName, aggColType, "_old");

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();

		boolean changeJK = !oldjoinKeyValue.equals(joinKeyValue) && !oldjoinKeyValue.equals("'null'");

		// ==================================================
		// increase
		if ((oldRJRow.getMap("list_item1").isEmpty() && newRJRow.getMap(
				"list_item1").size() == 1)
				|| oldRJRow.getMap("list_item1").size() < newRJRow.getMap(
						"list_item1").size()) {

			if (!leftJoinAggTable.equals("false")) {
				while (!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
						json, leftJoinAggTable, aggKey, aggKeyValue,
						aggColValue, oldAggColValue, oldAggKeyValue,changeJK, identifier))
					;
			}

			if (newRJRow.getMap("list_item2").size() > 0) {
				if (!innerJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(
							stream, json, innerJoinAggTable, aggKey,
							aggKeyValue, aggColValue, oldAggColValue,
							oldAggKeyValue,changeJK, identifier))
						;
				}
			}

			// decrease
		} else if ((oldRJRow.getMap("list_item1").size() == 1 && newRJRow
				.getMap("list_item1").isEmpty())
				|| oldRJRow.getMap("list_item1").size() > newRJRow.getMap(
						"list_item1").size()) {

			if (!leftJoinAggTable.equals("false")) {
				while (!JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
								leftJoinAggTable, aggKey, oldAggKeyValue,
								oldAggColValue, identifier))
					;
			}

			if (newRJRow.getMap("list_item2").size() > 0) {
				if (!innerJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper
							.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
									innerJoinAggTable, aggKey, oldAggKeyValue,
									oldAggColValue, identifier))
						;
				}
			}
			// value update
		} else if (oldRJRow.getMap("list_item1").size() == newRJRow.getMap(
				"list_item1").size()) {

			if (!oldAggKeyValue.equals(aggKeyValue)) {

				if (!leftJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper
							.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
									leftJoinAggTable, aggKey, oldAggKeyValue,
									oldAggColValue, identifier))
						;
				}

				if (newRJRow.getMap("list_item2").size() > 0) {
					if (!innerJoinAggTable.equals("false")) {
						while (!JoinAggGroupByHelper
								.searchAndDeleteRowFromJoinAggGroupBy(stream,
										json, innerJoinAggTable, aggKey,
										oldAggKeyValue, oldAggColValue, identifier))
							;
					}
				}

				if (!leftJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(
							stream, json, leftJoinAggTable, aggKey,
							aggKeyValue, aggColValue, oldAggColValue,
							oldAggKeyValue,changeJK, identifier))
						;
				}

				if (newRJRow.getMap("list_item2").size() > 0) {
					if (!innerJoinAggTable.equals("false")) {
						while (!JoinAggGroupByHelper
								.JoinAggGroupByChangeAddRow(stream, json,
										innerJoinAggTable, aggKey, aggKeyValue,
										aggColValue, oldAggColValue,
										oldAggKeyValue,changeJK, identifier))
							;
					}
				}
			}

			if(!oldAggColValue.equals(aggColValue) && oldAggKeyValue.equals(aggKeyValue)){

				if (!leftJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(
							stream, json, leftJoinAggTable, aggKey,
							aggKeyValue, aggColValue, oldAggColValue,
							oldAggKeyValue,changeJK, identifier))
						;
				}

				if (newRJRow.getMap("list_item2").size() > 0) {
					if (!innerJoinAggTable.equals("false")) {
						while (!JoinAggGroupByHelper
								.JoinAggGroupByChangeAddRow(stream, json,
										innerJoinAggTable, aggKey, aggKeyValue,
										aggColValue, oldAggColValue,
										oldAggKeyValue,changeJK, identifier))
							;
					}
				}
			}

		}

		return true;
	}

	public Boolean updateJoinAgg_UpdateRight_AggColLeftSide_GroupBy(
			Stream stream, String innerJoinAggTable, String leftJoinAggTable,
			JSONObject json, String joinKeyType, String joinKey,
			String aggColName, String aggColType, int index, String key,
			String keyType, int aggKeyIndex) {

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();

		if (newRJRow.getMap("list_item2").isEmpty()
				&& !newRJRow.getMap("list_item1").isEmpty()) {
			if (!innerJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.deleteListItem1FromGroupBy(stream,
						oldRJRow, index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex, identifier);
			}
		}

		if (newRJRow.getMap("list_item2").size() == 1 && oldRJRow.getMap("list_item2").size() == 0
				&& !newRJRow.getMap("list_item1").isEmpty()) {
			if (!innerJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.addListItem1toInnerJoinGroupBy(stream,
						stream.getDeltaUpdatedRow(), aggColName,
						leftJoinAggTable, newRJRow, index, keyType, key, json,
						innerJoinAggTable, aggKeyIndex, identifier);
			}
		}

		return true;

	}

	public Boolean updateJoinAgg_UpdateRight_AggColRightSide_GroupBy(
			Stream stream, String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String aggKeyType, String aggKey,
			String aggColName, String aggColType, String joinKeyName,
			String joinKeyType) {

		String aggKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggKey, aggKeyType, "_new");
		String oldAggKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggKey, aggKeyType, "_old");

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_new");
		String oldjoinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), joinKeyName, joinKeyType, "_old");

		String aggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggColName, aggColType, "_new");
		String oldAggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaUpdatedRow(), aggColName, aggColType, "_old");

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();

		boolean changeJK = !oldjoinKeyValue.equals(joinKeyValue) && oldjoinKeyValue.equals("'null'");

		// ==================================================
		// increase
		if ((oldRJRow.getMap("list_item2").isEmpty() && newRJRow.getMap(
				"list_item2").size() == 1)
				|| oldRJRow.getMap("list_item2").size() < newRJRow.getMap(
						"list_item2").size()) {

			if (!rightJoinAggTable.equals("false")) {
				while (!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(stream,
						json, rightJoinAggTable, aggKey, aggKeyValue,
						aggColValue, oldAggColValue, oldAggKeyValue,changeJK, identifier))
					;
			}

			if (newRJRow.getMap("list_item1").size() > 0) {
				if (!innerJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(
							stream, json, innerJoinAggTable, aggKey,
							aggKeyValue, aggColValue, oldAggColValue,
							oldAggKeyValue,changeJK, identifier))
						;
				}
			}

			// decrease
		} else if ((oldRJRow.getMap("list_item2").size() == 1 && newRJRow
				.getMap("list_item2").isEmpty())
				|| oldRJRow.getMap("list_item2").size() > newRJRow.getMap(
						"list_item2").size()) {

			if (!rightJoinAggTable.equals("false")) {
				while (!JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
								rightJoinAggTable, aggKey, oldAggKeyValue,
								oldAggColValue, identifier))
					;
			}

			if (newRJRow.getMap("list_item1").size() > 0) {
				if (!innerJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper
							.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
									innerJoinAggTable, aggKey, oldAggKeyValue,
									oldAggColValue, identifier))
						;
				}
			}
			// value update
		} else if (oldRJRow.getMap("list_item2").size() == newRJRow.getMap(
				"list_item2").size()) {

			if (!oldAggKeyValue.equals(aggKeyValue)) {

				if (!rightJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper
							.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
									rightJoinAggTable, aggKey, oldAggKeyValue,
									oldAggColValue, identifier))
						;
				}

				if (newRJRow.getMap("list_item1").size() > 0) {
					if (!innerJoinAggTable.equals("false")) {
						while (!JoinAggGroupByHelper
								.searchAndDeleteRowFromJoinAggGroupBy(stream,
										json, innerJoinAggTable, aggKey,
										oldAggKeyValue, oldAggColValue, identifier))
							;
					}
				}

				if (!rightJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(
							stream, json, rightJoinAggTable, aggKey,
							aggKeyValue, aggColValue, oldAggColValue,
							oldAggKeyValue,changeJK, identifier))
						;
				}

				if (newRJRow.getMap("list_item1").size() > 0) {
					if (!innerJoinAggTable.equals("false")) {
						while (!JoinAggGroupByHelper
								.JoinAggGroupByChangeAddRow(stream, json,
										innerJoinAggTable, aggKey, aggKeyValue,
										aggColValue, oldAggColValue,
										oldAggKeyValue,changeJK, identifier))
							;
					}
				}
			}

			if( !oldAggColValue.equals(aggColValue) && oldAggKeyValue.equals(aggKeyValue)){

				if (!rightJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.JoinAggGroupByChangeAddRow(
							stream, json, rightJoinAggTable, aggKey,
							aggKeyValue, aggColValue, oldAggColValue,
							oldAggKeyValue,changeJK, identifier))
						;
				}

				if (newRJRow.getMap("list_item1").size() > 0) {
					if (!innerJoinAggTable.equals("false")) {
						while (!JoinAggGroupByHelper
								.JoinAggGroupByChangeAddRow(stream, json,
										innerJoinAggTable, aggKey, aggKeyValue,
										aggColValue, oldAggColValue,
										oldAggKeyValue,changeJK, identifier))
							;
					}
				}
			}

		}

		return true;

	}

	public Boolean updateJoinAgg_UpdateLeft_AggColRightSide_GroupBy(
			Stream stream, String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType, int index, String key,
			String keyType, int aggKeyIndex) {

		CustomizedRow newRJRow = stream.getReverseJoinUpdateNewRow();
		CustomizedRow oldRJRow = stream.getReverseJoinUpadteOldRow();

		if (newRJRow.getMap("list_item1").isEmpty()
				&& !newRJRow.getMap("list_item2").isEmpty()) {
			if (!innerJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.deleteListItem2FromGroupBy(stream,
						oldRJRow, index, keyType, key, json, innerJoinAggTable,
						aggKeyIndex, identifier);
			}
		}

		if (newRJRow.getMap("list_item1").size() == 1 && oldRJRow.getMap("list_item1").size() == 1
				&& !newRJRow.getMap("list_item2").isEmpty()) {
			if (!innerJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.addListItem2toInnerJoinGroupBy(stream,
						stream.getDeltaUpdatedRow(), aggColName,
						rightJoinAggTable, newRJRow, index, keyType, key, json,
						innerJoinAggTable, aggKeyIndex, identifier);
			}
		}

		return true;

	}

	public boolean deleteJoinAgg_DeleteLeft_AggColLeftSide(Stream stream,
			String innerJoinAggTable, String leftJoinAggTable, JSONObject json,
			String joinKeyType, String joinKeyName, String aggColName,
			String aggColType) {


		String joinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");
		String aggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), aggColName, aggColType, "_new");

		CustomizedRow newRJRow = null;
		if (stream.getReverseJoinDeleteNewRow() == null) {
			if (!leftJoinAggTable.equals("false")) {

				CustomizedRow crow = new CustomizedRow(
						JoinAggregationHelper.selectStatement(joinKeyName,
								joinKeyValue, leftJoinAggTable, json));

				stream.setLeftOrRightJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						leftJoinAggTable, joinKeyName, joinKeyValue);
			}
			if (!innerJoinAggTable.equals("false")) {
				CustomizedRow crow = new CustomizedRow(
						JoinAggregationHelper.selectStatement(joinKeyName,
								joinKeyValue, innerJoinAggTable, json));

				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						innerJoinAggTable, joinKeyName, joinKeyValue);
			}
			return true;

		} else {
			newRJRow = stream.getReverseJoinDeleteNewRow();

		}

		if (newRJRow.getMap("list_item2").isEmpty()) {


			if (newRJRow.getMap("list_item1").isEmpty()) {
				// remove from left
				if (!leftJoinAggTable.equals("false")) {
					CustomizedRow crow = new CustomizedRow(
							JoinAggregationHelper.selectStatement(joinKeyName,
									joinKeyValue, leftJoinAggTable, json));
					stream.setLeftOrRightJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(
							json.get("keyspace").toString(), leftJoinAggTable,
							joinKeyName, joinKeyValue);
				}
			} else {
				// update left by subtracting
				if (!leftJoinAggTable.equals("false") && aggColValue != null
						&& !aggColValue.equals("null")
						&& !aggColValue.equals("'null'")) {
					while (!JoinAggregationHelper.UpdateOldRowBySubtracting(
							stream, "list_item1", stream.getDeltaDeletedRow(),
							json, leftJoinAggTable, joinKeyName, joinKeyValue,
							aggColName, aggColValue, newRJRow, identifier))
						;
					// JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",
					// stream.getDeltaDeletedRow(), json, leftJoinAggTable,
					// joinKeyName, joinKeyValue, aggColName, aggColValue,
					// newRJRow);
				}
			}

		} else {

			if (newRJRow.getMap("list_item1").isEmpty()) {
				// remove from left and inner
				if (!leftJoinAggTable.equals("false")) {

					CustomizedRow crow = new CustomizedRow(
							JoinAggregationHelper.selectStatement(joinKeyName,
									joinKeyValue, leftJoinAggTable, json));

					stream.setLeftOrRightJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(
							json.get("keyspace").toString(), leftJoinAggTable,
							joinKeyName, joinKeyValue);
				}

				if (!innerJoinAggTable.equals("false")) {

					CustomizedRow crow = new CustomizedRow(
							JoinAggregationHelper.selectStatement(joinKeyName,
									joinKeyValue, innerJoinAggTable, json));

					stream.setInnerJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(
							json.get("keyspace").toString(), innerJoinAggTable,
							joinKeyName, joinKeyValue);
				}
			} else {
				// update left and inner
				if ((!leftJoinAggTable.equals("false") || !innerJoinAggTable
						.equals("false"))
						&& aggColValue != null
						&& !aggColValue.equals("null")
						&& !aggColValue.equals("'null'")) {

					if (!leftJoinAggTable.equals("false")) {
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item1",
										stream.getDeltaDeletedRow(), json,
										leftJoinAggTable, joinKeyName,
										joinKeyValue, aggColName, aggColValue,
										newRJRow, identifier))
							;
						// JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",
						// stream.getDeltaDeletedRow(), json, leftJoinAggTable,
						// joinKeyName, joinKeyValue, aggColName, aggColValue,
						// newRJRow);
					}

					if (!innerJoinAggTable.equals("false")) {
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item1",
										stream.getDeltaDeletedRow(), json,
										innerJoinAggTable, joinKeyName,
										joinKeyValue, aggColName, aggColValue,
										newRJRow, identifier))
							;
						// JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item1",
						// stream.getDeltaDeletedRow(), json, innerJoinAggTable,
						// joinKeyName, joinKeyValue, aggColName, aggColValue,
						// newRJRow);
					}

				}
			}
		}
		return true;
	}

	public boolean deleteJoinAgg_DeleteRight_AggColRightSide(Stream stream,
			String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");
		String aggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), aggColName, aggColType, "_new");

		CustomizedRow newRJRow = null;
		if (stream.getReverseJoinDeleteNewRow() == null) {
			if (!rightJoinAggTable.equals("false")) {
				CustomizedRow crow = new CustomizedRow(
						JoinAggregationHelper.selectStatement(joinKeyName,
								joinKeyValue, rightJoinAggTable, json));

				stream.setLeftOrRightJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						rightJoinAggTable, joinKeyName, joinKeyValue);
			}
			if (!innerJoinAggTable.equals("false")) {
				CustomizedRow crow = new CustomizedRow(
						JoinAggregationHelper.selectStatement(joinKeyName,
								joinKeyValue, innerJoinAggTable, json));

				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						innerJoinAggTable, joinKeyName, joinKeyValue);
			}
			return true;

		} else {
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item1").isEmpty()) {

			if (newRJRow.getMap("list_item2").isEmpty()) {
				// remove from rightJoinAggTable
				if (!rightJoinAggTable.equals("false")) {

					CustomizedRow crow = new CustomizedRow(
							JoinAggregationHelper.selectStatement(joinKeyName,
									joinKeyValue, rightJoinAggTable, json));

					stream.setLeftOrRightJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(
							json.get("keyspace").toString(), rightJoinAggTable,
							joinKeyName, joinKeyValue);
				}
			} else {
				// update right by subtracting
				if (!rightJoinAggTable.equals("false") && aggColValue != null
						&& !aggColValue.equals("null")
						&& !aggColValue.equals("'null'")) {
					while (!JoinAggregationHelper.UpdateOldRowBySubtracting(
							stream, "list_item2", stream.getDeltaDeletedRow(),
							json, rightJoinAggTable, joinKeyName, joinKeyValue,
							aggColName, aggColValue, newRJRow, identifier))
						;
					// JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2",
					// stream.getDeltaDeletedRow(), json, rightJoinAggTable,
					// joinKeyName, joinKeyValue, aggColName, aggColValue,
					// newRJRow);
				}
			}

		} else {

			if (newRJRow.getMap("list_item2").isEmpty()) {
				// remove from left and inner
				if (!rightJoinAggTable.equals("false")) {
					CustomizedRow crow = new CustomizedRow(
							JoinAggregationHelper.selectStatement(joinKeyName,
									joinKeyValue, rightJoinAggTable, json));
					stream.setLeftOrRightJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(
							json.get("keyspace").toString(), rightJoinAggTable,
							joinKeyName, joinKeyValue);
				}

				if (!innerJoinAggTable.equals("false")) {
					CustomizedRow crow = new CustomizedRow(
							JoinAggregationHelper.selectStatement(joinKeyName,
									joinKeyValue, innerJoinAggTable, json));
					stream.setInnerJoinAggDeleteRow(crow);
					Utils.deleteEntireRowWithPK(
							json.get("keyspace").toString(), innerJoinAggTable,
							joinKeyName, joinKeyValue);
				}

			} else {
				// update rightJoinAggTable and inner<

				if ((!rightJoinAggTable.equals("false") || !innerJoinAggTable
						.equals("false"))
						&& aggColValue != null
						&& !aggColValue.equals("null")
						&& !aggColValue.equals("'null'")) {

					if (!rightJoinAggTable.equals("false")) {
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item2",
										stream.getDeltaDeletedRow(), json,
										rightJoinAggTable, joinKeyName,
										joinKeyValue, aggColName, aggColValue,
										newRJRow, identifier))
							;
						// JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2",
						// stream.getDeltaDeletedRow(), json, rightJoinAggTable,
						// joinKeyName, joinKeyValue, aggColName, aggColValue,
						// newRJRow);
					}

					if (!innerJoinAggTable.equals("false")) {
						while (!JoinAggregationHelper
								.UpdateOldRowBySubtracting(stream,
										"list_item2",
										stream.getDeltaDeletedRow(), json,
										innerJoinAggTable, joinKeyName,
										joinKeyValue, aggColName, aggColValue,
										newRJRow, identifier))
							;
						// JoinAggregationHelper.UpdateOldRowBySubtracting(stream,"list_item2",
						// stream.getDeltaDeletedRow(), json, innerJoinAggTable,
						// joinKeyName, joinKeyValue, aggColName, aggColValue,
						// newRJRow);
					}
				}
			}

		}

		return true;
	}

	public boolean deleteJoinAgg_DeleteLeft_AggColRightSide(Stream stream,
			String rightJoinAggTable, String innerJoinAggTable,
			JSONObject json, String joinKeyType, String joinKeyName,
			String aggColName, String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");

		CustomizedRow newRJRow = null;
		if (stream.getReverseJoinDeleteNewRow() == null) {
			if (!rightJoinAggTable.equals("false")) {
				CustomizedRow crow = new CustomizedRow(
						JoinAggregationHelper.selectStatement(joinKeyName,
								joinKeyValue, rightJoinAggTable, json));

				stream.setLeftOrRightJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						rightJoinAggTable, joinKeyName, joinKeyValue);
			}
			return true;

		} else {
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item1").isEmpty()
				&& !newRJRow.getMap("list_item2").isEmpty()) {

			// remove from inner
			if (!innerJoinAggTable.equals("false")) {
				CustomizedRow crow = new CustomizedRow(
						JoinAggregationHelper.selectStatement(joinKeyName,
								joinKeyValue, innerJoinAggTable, json));
				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						innerJoinAggTable, joinKeyName, joinKeyValue);
			}
		}
		return true;

	}

	public boolean deleteJoinAgg_DeleteRight_AggColLeftSide(Stream stream,
			String leftJoinAggTable, String innerJoinAggTable, JSONObject json,
			String joinKeyType, String joinKeyName, String aggColName,
			String aggColType) {

		String joinKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), joinKeyName, joinKeyType, "_new");

		CustomizedRow newRJRow = null;
		if (stream.getReverseJoinDeleteNewRow() == null) {
			if (!leftJoinAggTable.equals("false")) {
				CustomizedRow crow = new CustomizedRow(
						JoinAggregationHelper.selectStatement(joinKeyName,
								joinKeyValue, leftJoinAggTable, json));
				stream.setLeftOrRightJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						leftJoinAggTable, joinKeyName, joinKeyValue);
			}
			return true;

		} else {
			newRJRow = stream.getReverseJoinDeleteNewRow();
		}

		if (newRJRow.getMap("list_item2").isEmpty()
				&& !newRJRow.getMap("list_item1").isEmpty()) {
			// remove from inner
			if (!innerJoinAggTable.equals("false")) {
				CustomizedRow crow = new CustomizedRow(
						JoinAggregationHelper.selectStatement(joinKeyName,
								joinKeyValue, innerJoinAggTable, json));

				stream.setInnerJoinAggDeleteRow(crow);
				Utils.deleteEntireRowWithPK(json.get("keyspace").toString(),
						innerJoinAggTable, joinKeyName, joinKeyValue);
			}
		}
		return true;

	}

	public Boolean deleteJoinAgg_DeleteLeft_AggColLeftSide_GroupBy(
			Stream stream, String innerJoinAggTable, String leftJoinAggTable,
			JSONObject json, String aggKeyType, String aggkey,
			String aggColName, String aggColType, int index) {

		String aggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), aggColName, aggColType, "_new");
		String aggKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), aggkey, aggKeyType, "_new");

		CustomizedRow newRJRow = stream.getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item1").isEmpty()) {

			// remove from left and inner
			if (!leftJoinAggTable.equals("false")) {
				while (!JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
								leftJoinAggTable, aggkey, aggKeyValue,
								aggColValue, identifier))
					;
			}
			if (!innerJoinAggTable.equals("false")) {
				while (!JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
								innerJoinAggTable, aggkey, aggKeyValue,
								aggColValue, identifier))
					;
			}

		} else {

			if (newRJRow.getMap("list_item2").isEmpty()) {

				// remove from left: if count == 1, then delete entire row, else
				// substract & update row
				if (!innerJoinAggTable.equals("false")) { //
					while (!JoinAggGroupByHelper
							.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
									innerJoinAggTable, aggkey, aggKeyValue,
									aggColValue, identifier))
						;
				}

			}

			if (newRJRow.getMap("list_item1").size() == 1
					&& newRJRow.getMap("list_item2").isEmpty()) {
				while (!JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
								leftJoinAggTable, aggkey, aggKeyValue,
								aggColValue, identifier))
					;

			}

			if (!newRJRow.getMap("list_item1").isEmpty() && !(newRJRow.getMap("list_item1").size() == 1)
					&& newRJRow.getMap("list_item2").isEmpty()) {

				if (!leftJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.deleteElementFromRow(stream,
							json, leftJoinAggTable, aggkey, aggKeyValue,
							aggColValue, identifier))
						;
				}
			}

			if (!newRJRow.getMap("list_item1").isEmpty()
					&& !newRJRow.getMap("list_item2").isEmpty()) {

				if (!leftJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.deleteElementFromRow(stream,
							json, leftJoinAggTable, aggkey, aggKeyValue,
							aggColValue, identifier))
						;
				}
				if (!innerJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.deleteElementFromRow(stream,
							json, innerJoinAggTable, aggkey, aggKeyValue,
							aggColValue, identifier))
						;
				}
			}

		}

		return true;
	}

	public Boolean deleteJoinAgg_DeleteRight_AggColRightSide_GroupBy(
			Stream stream, String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String aggKeyType, String aggKey,
			String aggColName, String aggColType, int index) {

		String aggColValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), aggColName, aggColType, "_new");
		String aggKeyValue = Utils.getColumnValueFromDeltaStream(
				stream.getDeltaDeletedRow(), aggKey, aggKeyType, "_new");

		CustomizedRow newRJRow = stream.getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item2").isEmpty()) {

			// remove from left and inner
			if (!rightJoinAggTable.equals("false")) {
				while (!JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
								rightJoinAggTable, aggKey, aggKeyValue,
								aggColValue, identifier))
					;
			}
			if (!innerJoinAggTable.equals("false")) {
				while (!JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
								innerJoinAggTable, aggKey, aggKeyValue,
								aggColValue, identifier))
					;
			}

		} else {

			if (newRJRow.getMap("list_item1").isEmpty()) {

				// remove from left: if count == 1, then delete entire row, else
				// substract & update row
				if (!innerJoinAggTable.equals("false")) { //
					while (!JoinAggGroupByHelper
							.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
									innerJoinAggTable, aggKey, aggKeyValue,
									aggColValue, identifier))
						;
				}

			}


			if (newRJRow.getMap("list_item2").size() == 1
					&& newRJRow.getMap("list_item1").isEmpty()) {
				while (!JoinAggGroupByHelper
						.searchAndDeleteRowFromJoinAggGroupBy(stream, json,
								rightJoinAggTable, aggKey, aggKeyValue,
								aggColValue, identifier))
					;

			}


			if (!newRJRow.getMap("list_item2").isEmpty() &&  !(newRJRow.getMap("list_item2").size() == 1)
					&& newRJRow.getMap("list_item1").isEmpty()) {

				if (!rightJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.deleteElementFromRow(stream,
							json, rightJoinAggTable, aggKey, aggKeyValue,
							aggColValue, identifier))
						;
				}
			}


			if (!newRJRow.getMap("list_item2").isEmpty()
					&& !newRJRow.getMap("list_item1").isEmpty()) {

				if (!rightJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.deleteElementFromRow(stream,
							json, rightJoinAggTable, aggKey, aggKeyValue,
							aggColValue, identifier))
						;
				}
				if (!innerJoinAggTable.equals("false")) {
					while (!JoinAggGroupByHelper.deleteElementFromRow(stream,
							json, innerJoinAggTable, aggKey, aggKeyValue,
							aggColValue, identifier))
						;
				}
			}

		}

		return true;
	}

	public Boolean deleteJoinAgg_DeleteLeft_AggColRightSide_GroupBy(
			Stream stream, String innerJoinAggTable, String rightJoinAggTable,
			JSONObject json, String aggKeyType, String aggKey,
			String aggColName, String aggColType, int aggKeyIndex, int index) {

		CustomizedRow newRJRow = stream.getReverseJoinDeleteNewRow();
		if (newRJRow.getMap("list_item1").isEmpty()
				&& !newRJRow.getMap("list_item2").isEmpty()) {

			// remove from inner
			if (!innerJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.deleteListItem2FromGroupBy(stream,
						newRJRow, index, aggKeyType, aggKey, json,
						innerJoinAggTable, aggKeyIndex, identifier);
			}
		}

		return true;

	}

	public Boolean deleteJoinAgg_DeleteRight_AggColLeftSide_GroupBy(
			Stream stream, String innerJoinAggTable, String leftJoinAggTable,
			JSONObject json, String aggKeyType, String aggKey,
			String aggColName, String aggColType, int aggKeyIndex, int index) {

		CustomizedRow newRJRow = stream.getReverseJoinDeleteNewRow();

		if (newRJRow.getMap("list_item2").isEmpty()
				&& !newRJRow.getMap("list_item1").isEmpty()) {

			// remove from inner
			if (!innerJoinAggTable.equals("false")) {
				JoinAggGroupByHelper.deleteListItem1FromGroupBy(stream,
						newRJRow, index, aggKeyType, aggKey, json,
						innerJoinAggTable, aggKeyIndex, identifier);
			}
		}

		return true;
	}


	public String getReverseJoinTableName() {
		return reverseJoinTableName;
	}

	public void setReverseJoinTableName(String reverseJoinTableName) {
		this.reverseJoinTableName = reverseJoinTableName;
	}
	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

}