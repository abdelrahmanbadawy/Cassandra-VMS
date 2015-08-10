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
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.simple.JSONObject;

import client.client.Client;
import client.client.XmlHandler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
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
	
	
	List<String> rj_joinTables = VmXmlHandler.getInstance()
			.getDeltaReverseJoinMapping().getList("mapping.unit.Join.name");

	List<String> rj_joinKeys = VmXmlHandler.getInstance()
			.getDeltaReverseJoinMapping().getList("mapping.unit.Join.JoinKey");

	List<String> rj_joinKeyTypes = VmXmlHandler.getInstance()
			.getDeltaReverseJoinMapping().getList("mapping.unit.Join.type");

	List<String> rj_nrDelta = VmXmlHandler.getInstance()
			.getDeltaReverseJoinMapping().getList("mapping.unit.nrDelta");

	int rjoins = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
			.getInt("mapping.nrUnit");

	public ViewManager(Cluster currenCluster) {
		this.currentCluster = currenCluster;
	}

	public Row getDeltaUpdatedRow() {
		return deltaUpdatedRow;
	}

	private void setDeltaUpdatedRow(Row row) {
		deltaUpdatedRow = row;
	}

	public boolean updateDelta(JSONObject json, int indexBaseTableName,
			String baseTablePrimaryKey) {

		// retrieve values from json
		String table = (String) json.get("table");
		String keyspace = (String) json.get("keyspace");
		JSONObject data = (JSONObject) json.get("data");
		boolean firstInsertion = false;

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

		StringBuilder selectQuery = new StringBuilder("SELECT ")
		.append(selectStatement_new);
		selectQuery.append(" FROM ").append(keyspace).append(".")
		.append("delta_" + table).append(" WHERE ")
		.append(baseTablePrimaryKey + " = ")
		.append(data.get(baseTablePrimaryKey) + " ;");

		System.out.println(selectQuery);

		ResultSet previousCol_newValues;

		try {
			Session session = currentCluster.connect();
			previousCol_newValues = session.execute(selectQuery.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		Row theRow = previousCol_newValues.one();
		StringBuilder insertQueryAgg = new StringBuilder();

		// 2. If the row retrieved is empty, then this is a new insertion
		// 2.a Insert into delta, values from json in the appropriate _new
		// columns
		// 2.b the _old columns will have nulls as values

		if (theRow == null) {

			insertQueryAgg = new StringBuilder("INSERT INTO ");
			insertQueryAgg.append(keyspace).append(".")
			.append("delta_" + table).append(" ( ")
			.append(selectStatement_new).append(") VALUES (");

			for (String s : selectStatement_new_values) {
				insertQueryAgg.append(s).append(", ");
			}

			insertQueryAgg.deleteCharAt(insertQueryAgg.length() - 2);

			System.out.println(insertQueryAgg);

			firstInsertion = true;

		} else {

			// 3. If retrieved row is not empty, then this is an update
			// operation, values of row are retrieved
			// 3.a Insert into delta, values from json in the appropriate _new
			// columns
			// 3.b the _old columns will have the retrieved values from 3.

			insertQueryAgg = new StringBuilder("INSERT INTO ");
			insertQueryAgg.append(keyspace).append(".")
			.append("delta_" + table + " (")
			.append(selectStatement_new).append(", ")
			.append(selectStatement_old).append(") VALUES (");

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

				}
			}
		}

		// 4. Execute insertion statement in delta
		try {
			insertQueryAgg.append(");");
			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());
			System.out.println(insertQueryAgg.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		System.out.println("Done Delta update");

		// 5. get the entire row from delta where update has happened
		// 5.a save the row and send bk to controller

		StringBuilder selectQuery1 = new StringBuilder("SELECT * ")
		.append(" FROM ").append(keyspace).append(".")
		.append("delta_" + table).append(" WHERE ")
		.append(baseTablePrimaryKey + " = ")
		.append(data.get(baseTablePrimaryKey) + " ;");

		System.out.println(selectQuery1);

		try {
			Session session = currentCluster.connect();
			setDeltaUpdatedRow(session.execute(selectQuery1.toString()).one());
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		// decideSelection(keyspace,"delta_"+table,pkName.get(indexBaseTableName),data.get(pkName.get(indexBaseTableName)).toString(),json,theRow);
		// updatePreaggregation(firstInsertion,keyspace,table,pkName.get(indexBaseTableName),data.get(pkName.get(indexBaseTableName)).toString(),json);

		firstInsertion = false;

		return true;

	}

	public boolean deleteRowDelta(JSONObject json) {

		JSONObject condition = (JSONObject) json.get("condition");
		Object[] hm = condition.keySet().toArray();

		// 1. retrieve the row to be deleted from delta table

		StringBuilder selectQuery = new StringBuilder("SELECT *");
		selectQuery.append(" FROM ").append(json.get("keyspace")).append(".")
		.append("delta_" + json.get("table")).append(" WHERE ")
		.append(hm[0]).append(" = ")
		.append(condition.get(hm[0]) + " ;");

		System.out.println(selectQuery);

		ResultSet selectionResult;

		try {

			Session session = currentCluster.connect();
			selectionResult = session.execute(selectQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		// 2. set DeltaDeletedRow variable for streaming
		setDeltaDeletedRow(selectionResult.one());

		// 3. delete row from delta

		StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
		deleteQuery.append(json.get("keyspace")).append(".")
		.append("delta_" + json.get("table")).append(" WHERE ");

		Object[] hm1 = condition.keySet().toArray();

		for (int i = 0; i < hm1.length; i++) {
			deleteQuery.append(hm1[i]).append(" = ")
			.append(condition.get(hm1[i]));
			deleteQuery.append(";");
		}

		System.out.println(deleteQuery);

		ResultSet deleteResult;
		try {

			Session session = currentCluster.connect();
			deleteResult = session.execute(deleteQuery.toString());
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		System.out.println("Delete Successful from Delta Table");

		return true;

	}

	/*
	 * private void decideSelection(String keyspace, String table, String pk,
	 * String pkValue, JSONObject json, Row oldDeltaColumns) {
	 * 
	 * List<String> deltaTable =
	 * VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getList("mapping.unit.deltaTable");
	 * 
	 * JSONObject data = (JSONObject) json.get("data");
	 * 
	 * int position = deltaTable.indexOf(table);
	 * 
	 * if(position!=-1){
	 * 
	 * String temp= "mapping.unit("; temp+=Integer.toString(position);
	 * temp+=")";
	 * 
	 * int nrConditions = VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getInt(temp+".nrCond");
	 * 
	 * for(int i=0;i<nrConditions;i++){
	 * 
	 * String s = temp+".Cond("+Integer.toString(i)+")";
	 * 
	 * String selColName =
	 * VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getString(s+".selectionCol");
	 * 
	 * String selecTable =
	 * VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getString(s+".name");
	 * 
	 * String operation = VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getString(s+".operation");
	 * 
	 * String value = VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getString(s+".value");
	 * 
	 * String type = VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getString(s+".type");
	 * 
	 * 
	 * if(data.containsKey(selColName)){
	 * 
	 * switch (type) {
	 * 
	 * case "text":
	 * 
	 * break;
	 * 
	 * case "int":
	 * 
	 * String s1 = data.get(selColName).toString(); Integer valueInt = new
	 * Integer(s1); int compareValue = valueInt.compareTo(new Integer(value));
	 * 
	 * if((operation.equals(">") && (compareValue<0))){ return; }else
	 * if((operation.equals("<") && (compareValue>0))){ return; }else
	 * if((operation.equals("=") && (compareValue!=0))){ return; }
	 * 
	 * break;
	 * 
	 * case "varint":
	 * 
	 * break;
	 * 
	 * case "float":
	 * 
	 * break; }
	 * 
	 * 
	 * 
	 * }else{ return; }
	 * 
	 * updateSelection(keyspace,selecTable,selColName,pk,pkValue,table);
	 * 
	 * } }
	 * 
	 * }
	 */

	private void setDeltaDeletedRow(Row one) {
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
		.append("list_item, ").append("sum, ").append("count, ")
		.append("average ");
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
				sum = theRow.getInt("sum") - aggColValue;
				average = sum / count;

				// 6. Execute insertion statement of the row with the
				// aggKeyValue_old to refelect changes

				StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
				insertQueryAgg.append((String) json.get("keyspace"))
				.append(".").append(preaggTable).append(" ( ")
				.append(aggKey + ", ").append("list_item, ")
				.append("sum, count, average").append(") VALUES (")
				.append(aggKeyValue + ", ").append("?, ?, ?, ?);");

				Session session1 = currentCluster.connect();

				PreparedStatement statement1 = session1.prepare(insertQueryAgg
						.toString());
				BoundStatement boundStatement = new BoundStatement(statement1);
				session1.execute(boundStatement.bind(myMap, (int) sum,
						(int) count, average));
				System.out.println(boundStatement.toString());

			}

		}

		System.out.println("Done deleting from preagg and agg table");

		return true;
	}

	private boolean deleteEntireRowWithPK(String keyspace, String tableName,
			String pk, String pkValue) {

		StringBuilder deleteQuery = new StringBuilder("delete from ");
		deleteQuery.append(keyspace).append(".").append(tableName)
		.append(" WHERE ").append(pk + " = ").append(pkValue)
		.append(";");

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
			boolean override) {

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
			.append("average ");
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

				count = myMap.keySet().size();

				if (count > prev_count)
					sum = theRow1.getInt("sum") + aggColValue;
				else
					sum = theRow1.getInt("sum") - aggColValue_old + aggColValue;

				average = sum / count;

			}
			try {

				// 3. execute the insertion
				StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
				insertQueryAgg.append((String) json.get("keyspace"))
				.append(".").append(preaggTable).append(" ( ")
				.append(aggKey + ", ").append("list_item, ")
				.append("sum, count, average").append(") VALUES (")
				.append(aggKeyValue + ", ").append("?, ?, ?, ?);");

				Session session1 = currentCluster.connect();

				PreparedStatement statement1 = session1.prepare(insertQueryAgg
						.toString());
				BoundStatement boundStatement = new BoundStatement(statement1);
				session1.execute(boundStatement.bind(myMap, (int) sum,
						(int) count, average));
				System.out.println(boundStatement.toString());

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
			.append("average ");
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
					// 4. delete the whole row
					deleteEntireRowWithPK((String) json.get("keyspace"),
							preaggTable, aggKey, aggKeyValue_old);

					// 4.a perform a new insertion with new values
					updatePreaggregation(deltaUpdatedRow, aggKey, aggKeyType,
							json, preaggTable, baseTablePrimaryKey, aggCol,
							aggColType, true);

				} else {

					// 5. retrieve the pk value that has to be removed from the
					// map
					String pk = myList.get(0);
					myList.remove(0);

					// 5.a remove entry from map with that pk
					myMap.remove(pk);

					// 5.c adjust sum,count,average values
					count = myMap.size();
					sum = theRow.getInt("sum") - aggColValue_old;
					average = sum / count;

					// 6. Execute insertion statement of the row with the
					// aggKeyValue_old to refelect changes

					StringBuilder insertQueryAgg = new StringBuilder(
							"INSERT INTO ");
					insertQueryAgg.append((String) json.get("keyspace"))
					.append(".").append(preaggTable).append(" ( ")
					.append(aggKey + ", ").append("list_item, ")
					.append("sum, count, average").append(") VALUES (")
					.append(aggKeyValue_old + ", ")
					.append("?, ?, ?, ?);");

					Session session1 = currentCluster.connect();

					PreparedStatement statement1 = session1
							.prepare(insertQueryAgg.toString());
					BoundStatement boundStatement = new BoundStatement(
							statement1);
					session1.execute(boundStatement.bind(myMap, (int) sum,
							(int) count, average));
					System.out.println(boundStatement.toString());

					// perform a new insertion for the new aggkey given in json
					updatePreaggregation(deltaUpdatedRow, aggKey, aggKeyType,
							json, preaggTable, baseTablePrimaryKey, aggCol,
							aggColType, true);
				}

			}

		}

		return true;
	}

	/*
	 * private boolean updateAggregation(String keyspace, String preaggTable,
	 * String aggKey, String aggKeyValue, Map myMap) {
	 * 
	 * List<String> PreTable = VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getList("mapping.unit.Preagg");
	 * 
	 * 
	 * int position = PreTable.indexOf(preaggTable);
	 * 
	 * if(position!=-1){
	 * 
	 * String temp= "mapping.unit("; temp+=Integer.toString(position);
	 * temp+=").Agg";
	 * 
	 * String aggTable = VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getString(temp+".name");
	 * 
	 * String aggKeyType = VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getString(temp+".AggKeyType");
	 * 
	 * insertAggregation(keyspace,preaggTable,aggKey,aggKeyValue,myMap,aggTable,
	 * aggKeyType);
	 * 
	 * }
	 * 
	 * return true; }
	 */

	/*
	 * private boolean insertAggregation(String keyspace, String preaggTable,
	 * String aggKey, String aggKeyValue, Map myMap, String aggTable,String
	 * aggKeyType) {
	 * 
	 * float count = myMap.size(); float sum = 0;
	 * 
	 * 
	 * for (Object value : myMap.values()) {
	 * 
	 * sum+= Float.valueOf(value.toString()); }
	 * 
	 * float avg = sum/count;
	 * 
	 * 
	 * StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
	 * insertQueryAgg.append(keyspace).append(".")
	 * .append(aggTable).append(" ( ") .append(aggKey)
	 * .append(", sum, count, average) VALUES (");
	 * 
	 * insertQueryAgg.append(aggKeyValue+", ").append((int)sum).append(", ").append
	 * ((int)count).append(", ").append(avg) .append(");");
	 * 
	 * System.out.println(insertQueryAgg);
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * session.execute(insertQueryAgg.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * 
	 * return true;
	 * 
	 * }
	 */

	/*
	 * private boolean InsertPreaggregation(String keyspace, String preaggTable,
	 * String pk, String pkValue, String aggKey, String aggKeyType, String
	 * aggCol, String aggColType, JSONObject json, String deltaTable) {
	 * 
	 * JSONObject data = (JSONObject) json.get("data");
	 * 
	 * 
	 * StringBuilder selectPreaggQuery = new
	 * StringBuilder("SELECT ").append(aggKey+"_old, ")
	 * .append(aggKey+"_new, ").append(aggCol+"_old, ").append(aggCol+"_new ")
	 * .append(" FROM ").append(keyspace).append(".")
	 * .append(deltaTable).append(" where ")
	 * .append(pk).append(" = ").append(pkValue);
	 * 
	 * System.out.println(selectPreaggQuery);
	 * 
	 * 
	 * ResultSet deltaRow; try {
	 * 
	 * Session session = currentCluster.connect(); deltaRow =
	 * session.execute(selectPreaggQuery.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * 
	 * Row theRow = deltaRow.one();
	 * 
	 * if(theRow==null){ // TO DO // row got deleted men el delta table, what to
	 * do now ? }else{
	 * 
	 * boolean sameKeyValue = false; boolean sameColValue = false;
	 * 
	 * switch (aggKeyType) {
	 * 
	 * case "text": sameKeyValue =
	 * theRow.getString(aggKey+"_old").equals(theRow.getString(aggKey+"_new"));
	 * break;
	 * 
	 * case "int":
	 * if(theRow.getInt(aggKey+"_old")==theRow.getInt(aggKey+"new")){
	 * sameKeyValue = true; } break;
	 * 
	 * case "varint":
	 * if(theRow.getVarint(aggKey+"_old")==theRow.getVarint(aggKey+"new")){
	 * sameKeyValue = true; } break;
	 * 
	 * case "varchar": sameKeyValue =
	 * theRow.getString(aggKey+"_old").equals(theRow.getString(aggKey+"_new"));
	 * break; }
	 * 
	 * String stringRepresenation = "";
	 * 
	 * 
	 * switch (aggColType) {
	 * 
	 * case "text": stringRepresenation =data.get(aggCol).toString();
	 * sameColValue =
	 * theRow.getString(aggCol+"_old").equals(theRow.getString(aggCol+"_new"));
	 * break;
	 * 
	 * case "int":
	 * if(theRow.getInt(aggCol+"_old")==theRow.getInt(aggCol+"_new")){
	 * sameColValue = true; } Integer i = new
	 * Integer(data.get(aggCol).toString()); stringRepresenation = i.toString();
	 * break;
	 * 
	 * case "varint":
	 * if(theRow.getVarint(aggCol+"_old")==theRow.getVarint(aggCol+"_new")){
	 * sameColValue = true; } Integer v = new
	 * Integer(data.get(aggCol).toString()); stringRepresenation = v.toString();
	 * break;
	 * 
	 * case "varchar": stringRepresenation =data.get(aggCol).toString();
	 * sameColValue =
	 * theRow.getString(aggCol+"_old").equals(theRow.getString(aggCol+"_new"));
	 * 
	 * break;
	 * 
	 * case "float":
	 * if(theRow.getFloat(aggCol+"_old")==theRow.getFloat(aggCol+"_new")){
	 * sameColValue = true; } Float f = new Float(data.get(aggCol).toString());
	 * stringRepresenation = f.toString();
	 * 
	 * break; }
	 * 
	 * 
	 * 
	 * if(sameKeyValue && !sameColValue){
	 * 
	 * StringBuilder selectPreaggQuery1 = new
	 * StringBuilder("SELECT ").append("list_item");
	 * selectPreaggQuery1.append(" FROM ").append(keyspace).append(".")
	 * .append(preaggTable).append(" where ") .append(aggKey+
	 * " = ").append(data.get(aggKey).toString());
	 * 
	 * System.out.println(selectPreaggQuery1);
	 * 
	 * ResultSet PreAggMap; try {
	 * 
	 * Session session = currentCluster.connect(); PreAggMap =
	 * session.execute(selectPreaggQuery1.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * Row theRow1 = PreAggMap.one();
	 * 
	 * if (theRow1 == null) {
	 * 
	 * // TO DO // row has been deleted men el preaggregation table
	 * 
	 * } else { Map<String, String> tempMapImmutable=
	 * theRow1.getMap("list_item",String.class, String.class);
	 * 
	 * HashMap<String, String> myMap = new HashMap<String,String>();
	 * myMap.putAll(tempMapImmutable); myMap.remove(pkValue);
	 * 
	 * myMap.put(pkValue, stringRepresenation); StringBuilder insertQueryAgg =
	 * new StringBuilder("INSERT INTO ");
	 * insertQueryAgg.append(keyspace).append(".")
	 * .append(preaggTable).append(" ( ") .append(aggKey+", ")
	 * .append("list_item").append(") VALUES (")
	 * .append(data.get(aggKey)+", ").append("?);");
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * 
	 * PreparedStatement statement = session.prepare(insertQueryAgg.toString());
	 * BoundStatement boundStatement = statement.bind(myMap);
	 * System.out.println(boundStatement.toString());
	 * session.execute(boundStatement);
	 * 
	 * updateAggregation(keyspace,preaggTable,aggKey,data.get(aggKey).toString(),
	 * myMap);
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; } } }else
	 * if((!sameKeyValue && sameColValue)|| (!sameKeyValue && !sameColValue)){
	 * 
	 * //get row key of aggKey_old StringBuilder selectPreaggQuery1 = new
	 * StringBuilder("SELECT ").append("list_item");
	 * selectPreaggQuery1.append(" FROM ").append(keyspace).append(".")
	 * .append(preaggTable).append(" where ") .append(aggKey+ " = ");
	 * 
	 * 
	 * switch (aggKeyType) {
	 * 
	 * case "text":
	 * selectPreaggQuery1.append("'"+theRow.getString(aggKey+"_old")+"';");
	 * break;
	 * 
	 * case "int": selectPreaggQuery1.append(theRow.getInt(aggKey+"_old")+";");
	 * break;
	 * 
	 * case "varint":
	 * selectPreaggQuery1.append(theRow.getVarint(aggKey+"_old")+";"); break;
	 * 
	 * case "varchar":
	 * selectPreaggQuery1.append("'"+theRow.getString(aggKey+"_old")+"';");
	 * break;
	 * 
	 * case "float":
	 * selectPreaggQuery1.append(theRow.getFloat(aggKey+"_old")+";"); break;
	 * 
	 * }
	 * 
	 * System.out.println(selectPreaggQuery1);
	 * 
	 * ResultSet PreAggMap; try {
	 * 
	 * Session session = currentCluster.connect(); PreAggMap =
	 * session.execute(selectPreaggQuery1.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * Row theRow2 = PreAggMap.one();
	 * 
	 * if(theRow2==null){ // TO DO // Row has been deleted }else{ Map<String,
	 * String> tempMapImmutable= theRow2.getMap("list_item",String.class,
	 * String.class);
	 * 
	 * HashMap<String, String> myMap = new HashMap<String,String>();
	 * myMap.putAll(tempMapImmutable); myMap.remove(pkValue);
	 * 
	 * if(myMap.isEmpty()){ DeletePreaggregation();
	 * 
	 * }else{
	 * 
	 * StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
	 * insertQueryAgg.append(keyspace).append(".")
	 * .append(preaggTable).append(" ( ") .append(aggKey+", ")
	 * .append("list_item").append(") VALUES (");
	 * 
	 * 
	 * StringBuilder aggKeyOld = new StringBuilder() ;
	 * 
	 * switch (aggKeyType) {
	 * 
	 * case "text":
	 * insertQueryAgg.append("'"+theRow.getString(aggKey+"_old")+"', ");
	 * aggKeyOld.append("'"+theRow.getString(aggKey+"_old")+"'"); break;
	 * 
	 * case "int": insertQueryAgg.append(theRow.getInt(aggKey+"_old")+", ");
	 * aggKeyOld.append(theRow.getInt(aggKey+"_old")); break;
	 * 
	 * case "varint":
	 * insertQueryAgg.append(theRow.getVarint(aggKey+"_old")+", ");
	 * aggKeyOld.append(theRow.getVarint(aggKey+"_old")); break;
	 * 
	 * case "varchar":
	 * insertQueryAgg.append("'"+theRow.getString(aggKey+"_old")+"', ");
	 * aggKeyOld.append("'"+theRow.getString(aggKey+"_old")+"'"); break;
	 * 
	 * case "float": insertQueryAgg.append(theRow.getFloat(aggKey+"_old")+", ");
	 * aggKeyOld.append(theRow.getFloat(aggKey+"_old")); break;
	 * 
	 * } insertQueryAgg.append("?);");
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * 
	 * PreparedStatement statement = session.prepare(insertQueryAgg.toString());
	 * BoundStatement boundStatement = statement.bind(myMap);
	 * System.out.println(boundStatement.toString());
	 * session.execute(boundStatement);
	 * 
	 * updateAggregation(keyspace,preaggTable,aggKey,aggKeyOld.toString(),myMap);
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * newInsertPreaggregation(keyspace, preaggTable, pk, pkValue, aggKey,
	 * aggKeyType, aggCol, aggColType, json);
	 * 
	 * } }
	 * 
	 * }else{ return true; }
	 * 
	 * }
	 * 
	 * return true;
	 * 
	 * }
	 */

	/*
	 * private boolean DeletePreaggregation() { // TODO
	 * 
	 * return true; }
	 */

	/*
	 * private boolean newInsertPreaggregation( String keyspace, String
	 * preaggTable, String pk, String pkValue, String aggKey, String aggKeyType,
	 * String aggCol, String aggColType, JSONObject json){
	 * 
	 * 
	 * JSONObject data = (JSONObject) json.get("data");
	 * 
	 * //Select from PreAggtable to prevent overwrites
	 * 
	 * StringBuilder selectPreaggQuery = new
	 * StringBuilder("SELECT ").append("list_item");
	 * selectPreaggQuery.append(" FROM ").append(keyspace).append(".")
	 * .append(preaggTable).append(" where ") .append(aggKey+
	 * " = ").append(data.get(aggKey).toString());
	 * 
	 * System.out.println(selectPreaggQuery);
	 * 
	 * 
	 * ResultSet PreAggMap; try {
	 * 
	 * Session session = currentCluster.connect(); PreAggMap =
	 * session.execute(selectPreaggQuery.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * 
	 * Row theRow = PreAggMap.one();
	 * 
	 * StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
	 * insertQueryAgg.append(keyspace).append(".")
	 * .append(preaggTable).append(" ( ") .append(aggKey+", ")
	 * .append("list_item").append(") VALUES (")
	 * .append(data.get(aggKey)+", ").append("?);");
	 * 
	 * String stringRepresenation = "";
	 * 
	 * switch (aggColType) {
	 * 
	 * case "text": stringRepresenation =data.get(aggCol).toString(); break;
	 * 
	 * case "int": Integer i = new Integer(data.get(aggCol).toString());
	 * stringRepresenation = i.toString();
	 * 
	 * break;
	 * 
	 * case "varint": Integer v = new Integer(data.get(aggCol).toString());
	 * stringRepresenation = v.toString();
	 * 
	 * break;
	 * 
	 * case "float":
	 * 
	 * Float f = new Float(data.get(aggCol).toString()); stringRepresenation =
	 * f.toString();
	 * 
	 * break; }
	 * 
	 * 
	 * Map myMap;
	 * 
	 * if (theRow == null) {
	 * 
	 * myMap = new HashMap<String,String>(); myMap.put(pkValue,
	 * stringRepresenation);
	 * 
	 * System.out.println(insertQueryAgg);
	 * 
	 * } else {
	 * 
	 * 
	 * System.out.println(theRow);
	 * 
	 * Map<String, String> tempMapImmutable=
	 * theRow.getMap("list_item",String.class, String.class);
	 * //tempMap.put(currentRow.getInt(deltaPkName.get(i)),
	 * currentRow.getInt(deltaAggColName.get(i)));
	 * 
	 * myMap = new HashMap<String,String>(); myMap.putAll(tempMapImmutable);
	 * myMap.put(pkValue, stringRepresenation);
	 * //insertQueryAgg.append(tempMap.toString()+");");
	 * 
	 * System.out.println(insertQueryAgg); }
	 * 
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * 
	 * PreparedStatement statement = session.prepare(insertQueryAgg.toString());
	 * BoundStatement boundStatement = statement.bind(myMap);
	 * System.out.println(boundStatement.toString());
	 * session.execute(boundStatement);
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * 
	 * 
	 * //update Aggregation
	 * updateAggregation(keyspace,preaggTable,aggKey,data.get
	 * (aggKey).toString(),myMap);
	 * 
	 * return true; }
	 */
	/*
	 * public boolean updateSelection(String keyspace, String selecTable, String
	 * selColName, String pk, String pkValue,String deltaTable){
	 * 
	 * 
	 * StringBuilder selectQuery = new StringBuilder();
	 * 
	 * selectQuery.append("SELECT * FROM ").append(keyspace)
	 * .append(".").append(deltaTable)
	 * .append(" WHERE ").append(pk).append(" = ").append(pkValue).append(";");
	 * 
	 * System.out.println(selectQuery);
	 * 
	 * Session session = null; ResultSet queryResults;
	 * 
	 * try {
	 * 
	 * session = currentCluster.connect(); queryResults = session.execute(
	 * selectQuery.toString());
	 * 
	 * }catch(Exception e){ e.printStackTrace(); return false; }
	 * 
	 * 
	 * Row theRow = queryResults.one(); StringBuilder insertionSelection = new
	 * StringBuilder() ; StringBuilder insertionSelectionValues = new
	 * StringBuilder() ;
	 * 
	 * if (theRow == null) { //record got deleted from delta table }else{
	 * 
	 * List<Definition> colDef = theRow.getColumnDefinitions().asList();
	 * 
	 * for(int i=0;i<colDef.size();i++){
	 * 
	 * switch (theRow.getColumnDefinitions().getType(i).toString()) {
	 * 
	 * case "text": if(!
	 * theRow.getColumnDefinitions().getName(i).contains("_old")){ if(
	 * theRow.getColumnDefinitions().getName(i).contains("_new")){ String[]
	 * split = theRow.getColumnDefinitions().getName(i).split("_");
	 * insertionSelection.append(split[0]+", ");
	 * 
	 * }else{
	 * insertionSelection.append(theRow.getColumnDefinitions().getName(i)+", ");
	 * }
	 * 
	 * insertionSelectionValues.append("'"+theRow.getString(theRow.
	 * getColumnDefinitions().getName(i))+"', "); } break;
	 * 
	 * case "int": if(!
	 * theRow.getColumnDefinitions().getName(i).contains("_old")){
	 * 
	 * if( theRow.getColumnDefinitions().getName(i).contains("_new")){ String[]
	 * split = theRow.getColumnDefinitions().getName(i).split("_");
	 * insertionSelection.append(split[0]+", "); }else{
	 * insertionSelection.append(theRow.getColumnDefinitions().getName(i)+", ");
	 * }
	 * 
	 * insertionSelectionValues.append(theRow.getInt(theRow.getColumnDefinitions(
	 * ).getName(i))+", "); } break;
	 * 
	 * case "varint": if(!
	 * theRow.getColumnDefinitions().getName(i).contains("_old")){
	 * 
	 * if( theRow.getColumnDefinitions().getName(i).contains("_new")){ String[]
	 * split = theRow.getColumnDefinitions().getName(i).split("_");
	 * insertionSelection.append(split[0]+", "); }else{
	 * insertionSelection.append(theRow.getColumnDefinitions().getName(i)+", ");
	 * }
	 * insertionSelectionValues.append(theRow.getVarint(theRow.getColumnDefinitions
	 * ().getName(i))+", "); } break;
	 * 
	 * case "varchar": if(!
	 * theRow.getColumnDefinitions().getName(i).contains("_old")){
	 * 
	 * if( theRow.getColumnDefinitions().getName(i).contains("_new")){ String[]
	 * split = theRow.getColumnDefinitions().getName(i).split("_");
	 * insertionSelection.append(split[0]+", "); }else{
	 * 
	 * insertionSelection.append(theRow.getColumnDefinitions().getName(i)+", ");
	 * } insertionSelectionValues.append("'"+theRow.getString(theRow.
	 * getColumnDefinitions().getName(i))+"', "); } break;
	 * 
	 * } }
	 * 
	 * insertionSelection.deleteCharAt(insertionSelection.length()-2);
	 * insertionSelectionValues
	 * .deleteCharAt(insertionSelectionValues.length()-2);
	 * 
	 * StringBuilder insertQuery = new StringBuilder( "INSERT INTO ");
	 * insertQuery.append(keyspace).append(".") .append(selecTable).append(" (")
	 * .append(insertionSelection).append(") VALUES (")
	 * .append(insertionSelectionValues).append(");");
	 * 
	 * System.out.println(insertQuery);
	 * 
	 * session.execute(insertQuery.toString());
	 * 
	 * }
	 * 
	 * return true; }
	 */

	/*
	 * public boolean cascadeDeleteRow(JSONObject json) {
	 * 
	 * markDeltaTableRow(json); deleteRowSelection(json);
	 * deleteRowPreaggregation(json); deleteRowAggregation(json);
	 * deleteRowDelta(json);
	 * 
	 * System.out.println("Done Cascade Row Delete");
	 * 
	 * return true; }
	 */

	/*
	 * private boolean markDeltaTableRow(JSONObject json) {
	 * 
	 * 
	 * JSONObject condition = (JSONObject) json.get("condition"); Object[] hm =
	 * condition.keySet().toArray();
	 * 
	 * StringBuilder selectQuery = new StringBuilder("SELECT *");
	 * selectQuery.append(" FROM ").append(json.get("keyspace")).append(".")
	 * .append("delta_"+json.get("table")).append(" WHERE ")
	 * .append(hm[0]).append(" = ").append(condition.get(hm[0])+" ;");
	 * 
	 * System.out.println(selectQuery);
	 * 
	 * ResultSet selectionResult;
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect(); selectionResult =
	 * session.execute(selectQuery.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * Row theRow = selectionResult.one(); List<Definition> myColDef =
	 * theRow.getColumnDefinitions().asList();
	 * 
	 * StringBuilder colNames = new StringBuilder(); StringBuilder colValues =
	 * new StringBuilder();
	 * 
	 * 
	 * for(int i=0;i<myColDef.size();i++){
	 * colNames.append(myColDef.get(i).getName()+", "); }
	 * 
	 * colNames.deleteCharAt(colNames.length()-2);
	 * 
	 * StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
	 * insertQueryAgg
	 * .append(json.get("keyspace")).append(".").append("delta_"+json
	 * .get("table")+" (") .append(colNames).append(") VALUES (");
	 * 
	 * 
	 * for(int i=0;i<theRow.getColumnDefinitions().size();i++) {
	 * 
	 * switch (theRow.getColumnDefinitions().getType(i).toString()) {
	 * 
	 * case "text": if(i==0){ colValues.append("'"+theRow.getString(i)+"', ");
	 * }else if(i%2!=0){ colValues.append("null, ");
	 * colValues.append("'"+theRow.getString(i)+"', "); } break;
	 * 
	 * case "int": if(i==0){ colValues.append(theRow.getInt(i)+", "); }else
	 * if(i%2!=0){ colValues.append("null, ");
	 * colValues.append(theRow.getInt(i)+", "); } break;
	 * 
	 * case "varint": if(i==0){ colValues.append(theRow.getVarint(i)+", ");
	 * }else if(i%2!=0){ colValues.append("null, ");
	 * colValues.append(theRow.getVarint(i)+", "); } break;
	 * 
	 * case "varchar": if(i==0){
	 * colValues.append("'"+theRow.getString(i)+"', "); }else if(i%2!=0){
	 * colValues.append("null, ");
	 * colValues.append("'"+theRow.getString(i)+"', "); } break;
	 * 
	 * }
	 * 
	 * 
	 * }
	 * 
	 * colValues.deleteCharAt(colValues.length()-2);
	 * 
	 * insertQueryAgg.append(colValues).append(");");
	 * System.out.println(insertQueryAgg.toString());
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * session.execute(insertQueryAgg.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * System.out.println("Done marking Delta Table rows"); return true; }
	 * 
	 * 
	 * private boolean deleteRowAggregation(JSONObject json) {
	 * 
	 * List<String> deltaTable =
	 * VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getList("mapping.unit.deltaTable");
	 * 
	 * 
	 * int position = deltaTable.indexOf("delta_"+json.get("table"));
	 * 
	 * JSONObject condition = (JSONObject) json.get("condition"); Object[] hm =
	 * condition.keySet().toArray();
	 * 
	 * if(position!=-1){
	 * 
	 * String temp= "mapping.unit("; temp+=Integer.toString(position);
	 * temp+=").Agg";
	 * 
	 * String aggTableName = VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getString(temp+".name");
	 * 
	 * String aggKey = VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getString(temp+".AggKey");
	 * 
	 * String aggKeyType = VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getString(temp+".AggKeyType");
	 * 
	 * String aggCol = VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getString(temp+".AggCol");
	 * 
	 * String aggColType = VmXmlHandler.getInstance().getPreaggAggMapping().
	 * getString(temp+".AggColType");
	 * 
	 * StringBuilder selectQuery = new
	 * StringBuilder("SELECT ").append(aggKey+"_old ,").append(aggCol+"_old");
	 * selectQuery.append(" FROM ").append(json.get("keyspace")).append(".")
	 * .append("delta_"+json.get("table")).append(" WHERE ")
	 * .append(hm[0]).append(" = ").append(condition.get(hm[0])+" ;");
	 * 
	 * System.out.println(selectQuery);
	 * 
	 * ResultSet selectionResult;
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect(); selectionResult =
	 * session.execute(selectQuery.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * Row deltaResult = selectionResult.one();
	 * //===============================
	 * ===========================================
	 * 
	 * 
	 * StringBuilder selectQuery1 = new
	 * StringBuilder("Select sum, count, average from ");
	 * selectQuery1.append(json.get("keyspace")).append(".")
	 * .append(aggTableName).append(" where ") .append(aggKey).append(" = ");
	 * 
	 * String aggKeyValue = null;
	 * 
	 * switch (aggKeyType) {
	 * 
	 * case "text":
	 * selectQuery1.append("'"+deltaResult.getString(aggKey+"_old")+"';");
	 * aggKeyValue = "'"+deltaResult.getString(aggKey+"_old")+"'"; break;
	 * 
	 * case "int": selectQuery1.append(deltaResult.getInt(aggKey+"_old")+";");
	 * aggKeyValue = Integer.toString(deltaResult.getInt(aggKey+"_old")); break;
	 * 
	 * case "varint":
	 * selectQuery1.append(deltaResult.getVarint(aggKey+"_old")+";");
	 * aggKeyValue =deltaResult.getVarint(aggKey+"_old").toString();
	 * 
	 * break;
	 * 
	 * case "float":
	 * selectQuery1.append(deltaResult.getFloat(aggKey+"_old")+";"); aggKeyValue
	 * =Float.toString(deltaResult.getFloat(aggKey+"_old"));
	 * 
	 * break;
	 * 
	 * case "varchar":
	 * selectQuery1.append("'"+deltaResult.getString(aggKey+"_old")+"';");
	 * aggKeyValue = "'"+deltaResult.getString(aggKey+"_old")+"'"; break; }
	 * 
	 * System.out.println(selectQuery1);
	 * 
	 * ResultSet selectResult; try {
	 * 
	 * Session session = currentCluster.connect(); selectResult =
	 * session.execute(selectQuery1.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * Row theRow = selectResult.one();
	 * 
	 * if(theRow.getInt("count")==1){
	 * deleteEntireRowAggregation(json,aggTableName
	 * ,aggKey,aggKeyType,aggKeyValue); }else{
	 * 
	 * StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
	 * insertQueryAgg.append(json.get("keyspace")).append(".")
	 * .append(aggTableName).append(" ( ") .append(aggKey)
	 * .append(", sum, count, average) VALUES (");
	 * 
	 * 
	 * switch (aggKeyType) {
	 * 
	 * case "text":
	 * insertQueryAgg.append("'"+deltaResult.getString(aggKey+"_old")+"', ");
	 * break;
	 * 
	 * case "int":
	 * insertQueryAgg.append(deltaResult.getInt(aggKey+"_old")+", "); break;
	 * 
	 * case "varint":
	 * insertQueryAgg.append(deltaResult.getVarint(aggKey+"_old")+", "); break;
	 * 
	 * case "float":
	 * insertQueryAgg.append(deltaResult.getFloat(aggKey+"_old")+", "); break;
	 * 
	 * case "varchar":
	 * insertQueryAgg.append("'"+deltaResult.getString(aggKey+"_old")+"', ");
	 * break; }
	 * 
	 * float Sum = 0;
	 * 
	 * switch(aggColType){
	 * 
	 * case "int": Sum = theRow.getInt("sum")-deltaResult.getInt(aggCol+"_old");
	 * break;
	 * 
	 * case "varint": Sum =
	 * theRow.getInt("sum")-deltaResult.getVarint(aggCol+"_old").intValue();
	 * break;
	 * 
	 * case "float": Sum =
	 * theRow.getInt("sum")-deltaResult.getFloat(aggCol+"_old"); break;
	 * 
	 * }
	 * 
	 * 
	 * float Count = theRow.getInt("count")-1; float Average = Sum/Count;
	 * 
	 * insertQueryAgg.append((int)Sum+", ").append((int)Count+", ").append(Average
	 * ).append(");");
	 * 
	 * System.out.println(insertQueryAgg);
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * session.execute(insertQueryAgg.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * }
	 * 
	 * }
	 * 
	 * System.out.println("Done deleting row in Agg Table"); return true;
	 * 
	 * }
	 * 
	 * 
	 * private boolean deleteEntireRowAggregation(JSONObject json, String
	 * aggTableName, String aggKey, String aggKeyType, String aggKeyValue) {
	 * 
	 * StringBuilder deleteQuery = new StringBuilder("delete from ");
	 * deleteQuery.append(json.get("keyspace")).append(".")
	 * .append(aggTableName)
	 * .append(" WHERE ").append(aggKey+" = ").append(aggKeyValue).append(";");
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * session.execute(deleteQuery.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * 
	 * return true; }
	 * 
	 * 
	 * public boolean deleteRowDelta(JSONObject json) {
	 * 
	 * JSONObject condition = (JSONObject) json.get("condition");
	 * 
	 * StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
	 * deleteQuery.append(json.get("keyspace")).append(".")
	 * .append("delta_"+json.get("table")).append(" WHERE ");
	 * 
	 * 
	 * 
	 * Object[] hm = condition.keySet().toArray();
	 * 
	 * for(int i=0;i<hm.length;i++){
	 * deleteQuery.append(hm[i]).append(" = ").append(condition.get(hm[i]));
	 * deleteQuery.append(";"); }
	 * 
	 * System.out.println(deleteQuery);
	 * 
	 * ResultSet deleteResult; try{
	 * 
	 * Session session = currentCluster.connect(); deleteResult =
	 * session.execute(deleteQuery.toString()); }catch(Exception e){
	 * e.printStackTrace(); return false; }
	 * 
	 * System.out.println("Delete Successful from Delta Table");
	 * 
	 * return true;
	 * 
	 * }
	 * 
	 * private boolean deleteRowPreaggregation(JSONObject json) {
	 * 
	 * List<String> deltaTable =
	 * VmXmlHandler.getInstance().getDeltaPreaggMapping().
	 * getList("mapping.unit.deltaTable");
	 * 
	 * 
	 * int position = deltaTable.indexOf("delta_"+json.get("table"));
	 * 
	 * 
	 * JSONObject condition = (JSONObject) json.get("condition"); Object[] hm =
	 * condition.keySet().toArray();
	 * 
	 * if(position!=-1){
	 * 
	 * String temp= "mapping.unit("; temp+=Integer.toString(position);
	 * temp+=")";
	 * 
	 * int nrPreagg = VmXmlHandler.getInstance().getDeltaPreaggMapping().
	 * getInt(temp+".nrPreagg");
	 * 
	 * for(int i=0;i<nrPreagg;i++){
	 * 
	 * String s = temp+".Preagg("+Integer.toString(i)+")";
	 * 
	 * String aggTable = VmXmlHandler.getInstance().getDeltaPreaggMapping().
	 * getString(s+".name");
	 * 
	 * String aggKey = VmXmlHandler.getInstance().getDeltaPreaggMapping().
	 * getString(s+".AggKey");
	 * 
	 * String aggKeyType = VmXmlHandler.getInstance().getDeltaPreaggMapping().
	 * getString(s+".AggKeyType");
	 * 
	 * StringBuilder selectQuery = new
	 * StringBuilder("SELECT ").append(aggKey+"_old");
	 * selectQuery.append(" FROM ").append(json.get("keyspace")).append(".")
	 * .append("delta_"+json.get("table")).append(" WHERE ")
	 * .append(hm[0]).append(" = ").append(condition.get(hm[0])+" ;");
	 * 
	 * System.out.println(selectQuery);
	 * 
	 * ResultSet selectionResult;
	 * 
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect(); selectionResult =
	 * session.execute(selectQuery.toString());
	 * 
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * 
	 * //=========================================================================
	 * ======== //Select from PreAggtable to prevent overwrites
	 * 
	 * StringBuilder selectPreaggQuery = new
	 * StringBuilder("SELECT ").append("list_item");
	 * selectPreaggQuery.append(" FROM "
	 * ).append(json.get("keyspace")).append(".")
	 * .append(aggTable).append(" WHERE ") .append(aggKey+ " = ");
	 * 
	 * 
	 * String aggKeyValue = new String();
	 * 
	 * switch(aggKeyType){ case "text": String s1 =
	 * selectionResult.one().getString(aggKey+"_old");
	 * selectPreaggQuery.append("'"+s1+"';"); aggKeyValue = "'"+s1+"'"; break;
	 * 
	 * case "int": String s2 =
	 * Integer.toString(selectionResult.one().getInt(aggKey+"_old"));
	 * selectPreaggQuery.append(s2+";"); aggKeyValue = s2;
	 * 
	 * break;
	 * 
	 * case "varint": String s3 =
	 * selectionResult.one().getVarint(aggKey+"_old").toString();
	 * selectPreaggQuery.append(s3+";"); aggKeyValue = s3;
	 * 
	 * break;
	 * 
	 * case "varchar": String s4 =
	 * selectionResult.one().getString(aggKey+"_old");
	 * selectPreaggQuery.append("'"+s4+"';"); aggKeyValue = "'"+s4+"'"; break;
	 * 
	 * }
	 * 
	 * System.out.println(selectPreaggQuery);
	 * 
	 * ResultSet PreAggMap; try {
	 * 
	 * Session session = currentCluster.connect(); PreAggMap =
	 * session.execute(selectPreaggQuery.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * 
	 * Row theRow = PreAggMap.one();
	 * 
	 * Map myMap;
	 * 
	 * if (theRow == null) { //should not happen } else {
	 * 
	 * 
	 * System.out.println(theRow);
	 * 
	 * Map<String, String> tempMapImmutable=
	 * theRow.getMap("list_item",String.class, String.class); myMap = new
	 * HashMap<String,String>(); myMap.putAll(tempMapImmutable);
	 * 
	 * if(myMap.size()==1){
	 * deleteEntireRowPreagg(json,aggTable,aggKey,aggKeyValue); }else{
	 * 
	 * Iterator<Map.Entry<String, String>> mapIter =
	 * myMap.entrySet().iterator();
	 * 
	 * while(mapIter.hasNext()){ Map.Entry<String, String> entry =
	 * mapIter.next();
	 * if(entry.getKey().equals(condition.get(hm[0]).toString())){
	 * mapIter.remove(); } }
	 * 
	 * 
	 * StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
	 * insertQueryAgg.append(json.get("keyspace")).append(".")
	 * .append(aggTable).append(" ( ") .append(aggKey+", ")
	 * .append("list_item").append(") VALUES (").append(aggKeyValue+", ");
	 * insertQueryAgg.append("?);"); System.out.println(insertQueryAgg);
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * 
	 * PreparedStatement statement = session.prepare(insertQueryAgg.toString());
	 * BoundStatement boundStatement = statement.bind(myMap);
	 * System.out.println(boundStatement.toString());
	 * session.execute(boundStatement);
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * }
	 * 
	 * 
	 * } } }
	 * 
	 * System.out.println("Done deleting row in PreAgg Table");
	 * 
	 * return true;
	 * 
	 * }
	 * 
	 * 
	 * private boolean deleteEntireRowPreagg(JSONObject json, String aggTable,
	 * String aggKey, String aggKeyValue) {
	 * 
	 * StringBuilder deleteQuery = new StringBuilder("delete from ");
	 * deleteQuery.append(json.get("keyspace")).append(".")
	 * .append(aggTable).append
	 * (" WHERE ").append(aggKey+" = ").append(aggKeyValue+";");
	 * 
	 * try {
	 * 
	 * Session session = currentCluster.connect();
	 * session.execute(deleteQuery.toString());
	 * 
	 * } catch (Exception e) { e.printStackTrace(); return false; }
	 * 
	 * return true; }
	 */

	/*
	 * public boolean deleteRowSelection(JSONObject json) {
	 * 
	 * List<String> deltaTable =
	 * VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getList("mapping.unit.deltaTable");
	 * 
	 * int position = deltaTable.indexOf("delta_"+json.get("table"));
	 * 
	 * if(position!=-1){
	 * 
	 * String temp= "mapping.unit("; temp+=Integer.toString(position);
	 * temp+=")";
	 * 
	 * 
	 * int nrConditions = VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getInt(temp+".nrCond");
	 * 
	 * for(int i=0;i<nrConditions;i++){
	 * 
	 * String s = temp+".Cond("+Integer.toString(i)+")"; String selecTable =
	 * VmXmlHandler.getInstance().getDeltaSelectionMapping().
	 * getString(s+".name");
	 * 
	 * 
	 * 
	 * StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
	 * deleteQuery.append(json.get("keyspace")).append(".")
	 * .append(selecTable).append(" WHERE ");
	 * 
	 * 
	 * JSONObject condition = (JSONObject) json.get("condition"); Object[] hm =
	 * condition.keySet().toArray();
	 * 
	 * for(int i1=0;i1<hm.length;i1++){
	 * deleteQuery.append(hm[i1]).append(" = ").append(condition.get(hm[i1]));
	 * deleteQuery.append(";"); }
	 * 
	 * System.out.println(deleteQuery);
	 * 
	 * try{
	 * 
	 * Session session = currentCluster.connect();
	 * session.execute(deleteQuery.toString()); }catch(Exception e){
	 * e.printStackTrace(); return false; }
	 * 
	 * 
	 * } }
	 * 
	 * System.out.println("Possible Deletes Successful from Selection View");
	 * 
	 * return true;
	 * 
	 * }
	 */

	public void updateReverseJoin(JSONObject json) {

		// check for rj mappings after updating delta
		List<String> rj_joinTables = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping().getList("mapping.unit.Join.name");

		List<String> rj_joinKeys = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping()
				.getList("mapping.unit.Join.JoinKey");

		List<String> rj_joinKeyTypes = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping().getList("mapping.unit.Join.type");

		List<String> rj_nrDelta = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping().getList("mapping.unit.nrDelta");

		int rjoins = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
				.getInt("mapping.nrUnit");

		int cursor = 0;

		// for each join
		for (int j = 0; j < rjoins; j++) {

			// basetables
			int nrOfTables = Integer.parseInt(rj_nrDelta.get(j));

			String joinTable = rj_joinTables.get(j);

			// include only indices from 1 to nrOfTables
			// get basetables from name of rj table
			List<String> baseTables = Arrays.asList(
					rj_joinTables.get(j).split("_")).subList(1, nrOfTables + 1);

			String tableName = (String) json.get("table");
			String keyspace = (String) json.get("keyspace");

			int column = baseTables.indexOf(tableName) + 1;

			StringBuilder selectQuery = new StringBuilder();

			String joinKeyName = rj_joinKeys.get(cursor + column - 1);

			JSONObject data;
			// insert
			if (json.get("data") != null)
				data = (JSONObject) json.get("data");
			// update
			else
				data = (JSONObject) json.get("set_data");

			String joinKeyValue = (String) data.get(joinKeyName);

			selectQuery.append("SELECT * FROM ").append(keyspace).append(".")
			.append(joinTable).append(" WHERE ").append(joinKeyName)
			.append(" = ").append(data.get(joinKeyName)).append(";");

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
			.append(keyspace).append(".").append(joinTable)
			.append(" (").append(rj_joinKeys.get(cursor)).append(", ")
			.append("list_item" + column).append(") VALUES (")
			.append(joinKeyValue).append(", ?);");

			ArrayList<String> myList = new ArrayList<String>();

			List<Definition> def = deltaUpdatedRow.getColumnDefinitions()
					.asList();

			for (int i = 0; i < def.size(); i++) {

				switch (def.get(i).getType().toString()) {

				case "text":
					if (!def.get(i).getName().contains("_old")) {
						myList.add(deltaUpdatedRow.getString(i));
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
						myList.add("" + deltaUpdatedRow.getString(i));
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
			myMap.put(pk, myList.toString());

			// already exists
			if (theRow != null) {

				Map<String, String> tempMapImmutable = theRow.getMap(
						"list_item" + column, String.class, String.class);

				System.out.println(tempMapImmutable);

				myMap.putAll(tempMapImmutable);

				myMap.put(pk, myList.toString());

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

			cursor += nrOfTables;

		}

	}

	public boolean updateSelection(Row row, String keyspace, String selecTable,
			String selColName) {


		//1. get column names of delta table
		//1.b save column values from delta stream too

		StringBuilder insertion = new StringBuilder();
		StringBuilder insertionValues = new StringBuilder();

		List<Definition> colDef = row.getColumnDefinitions().asList();

		for(int i=0;i<colDef.size();i++){

			switch (colDef.get(i).getType().toString()) {

			case "text": 
				if(! row.getColumnDefinitions().getName(i).contains("_old")){
					if(row.getColumnDefinitions().getName(i).contains("_new")){
						String[]split = row.getColumnDefinitions().getName(i).split("_");
						insertion.append(split[0]+", ");
					}else{
						insertion.append(row.getColumnDefinitions().getName(i)+", ");
					}

					insertionValues.append("'"+row.getString(row.getColumnDefinitions().getName(i))+"', "); } break;

			case "int": 
				if(! row.getColumnDefinitions().getName(i).contains("_old")){

					if( row.getColumnDefinitions().getName(i).contains("_new")){
						String[] split = row.getColumnDefinitions().getName(i).split("_");
						insertion.append(split[0]+", "); }
					else{
						insertion.append(row.getColumnDefinitions().getName(i)+", ");
					}

					insertionValues.append(row.getInt(row.getColumnDefinitions().getName(i))+", ");
				} 
				break;

			case "varint": 
				if(!row.getColumnDefinitions().getName(i).contains("_old")){

					if( row.getColumnDefinitions().getName(i).contains("_new")){
						String[]split = row.getColumnDefinitions().getName(i).split("_");
						insertion.append(split[0]+", "); }else{
							insertion.append(row.getColumnDefinitions().getName(i)+", ");
						}
					insertionValues.append(row.getVarint(row.getColumnDefinitions().getName(i))+", "); 
				} 
				break;

			case "varchar": 
				if(!row.getColumnDefinitions().getName(i).contains("_old")){

					if( row.getColumnDefinitions().getName(i).contains("_new")){
						String[]split = row.getColumnDefinitions().getName(i).split("_");
						insertion.append(split[0]+", "); 
					}else{

						insertion.append(row.getColumnDefinitions().getName(i)+", ");
					} 

					insertionValues.append("'"+row.getString(row.
							getColumnDefinitions().getName(i))+"', "); 
				} break;

			} 
		}

		insertion.deleteCharAt(insertion.length()-2);
		insertionValues
		.deleteCharAt(insertionValues.length()-2);

		try {

			// 1. execute the insertion
			StringBuilder insertQuery = new StringBuilder( "INSERT INTO ");
			insertQuery.append(keyspace).append(".") .append(selecTable).append(" (")
			.append(insertion).append(") VALUES (")
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
		deleteQuery.append(keyspace).append(".")
		.append(selecTable).append(" WHERE ").append(baseTablePrimaryKey).append(" = ");

		if(json.containsKey("condition")){

			JSONObject condition = (JSONObject) json.get("condition"); 
			Object[] hm = condition.keySet().toArray();
			deleteQuery.append(condition.get(hm[0]));
			deleteQuery.append(";");

		}else{
			JSONObject data = (JSONObject) json.get("data");
			deleteQuery.append(data.get(baseTablePrimaryKey)).append(" ;");
		}

		System.out.println(deleteQuery);

		try{

			Session session = currentCluster.connect();
			session.execute(deleteQuery.toString()); 
		}catch(Exception e){
			e.printStackTrace();
			return false; 
		}

		System.out.println("Possible Deletes Successful from Selection View");

		return true;

	}

	public boolean updateLeftJoin(Row deltaUpdatedRow, String joinTableName) {
		
		return true;
	}
	
	public void deleteReverseJoin(JSONObject json) {

		// check for rj mappings after updating delta
		
		int cursor = 0;

		// for each join
		for (int j = 0; j < rjoins; j++) {

			// basetables
			int nrOfTables = Integer.parseInt(rj_nrDelta.get(j));

			String joinTable = rj_joinTables.get(j);

			// include only indices from 1 to nrOfTables
			// get basetables from name of rj table
			List<String> baseTables = Arrays.asList(
					rj_joinTables.get(j).split("_")).subList(1, nrOfTables + 1);

			String tableName = (String) json.get("table");
			
			String keyspace = (String) json.get("keyspace");

			int column = baseTables.indexOf(tableName) + 1;

			StringBuilder selectQuery = new StringBuilder();

			String joinKeyName = rj_joinKeys.get(cursor + column - 1);

			String aggKeyType = rj_joinKeyTypes.get(j);

			String joinKeyValue = null;
			
			switch(aggKeyType){
			case "text":
				
					joinKeyValue = ("'"+deltaDeletedRow.getString(joinKeyName+"_new")+"'");
				

				break;

			case "int":
				
				joinKeyValue =( ""+deltaDeletedRow.getInt(joinKeyName+"_new"));
				
				break;

			case "varint":
				
					joinKeyValue =("" + deltaDeletedRow.getVarint(joinKeyName+"_new"));
				
				break;

			case "varchar":
				
					joinKeyValue =("" + deltaDeletedRow.getString(joinKeyName+"_new"));
				
				break;

			case "float":
				
					joinKeyValue =("" + deltaDeletedRow.getFloat(joinKeyName+"_new"));
				
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

			StringBuilder insertQuery = new StringBuilder("INSERT INTO ")
			.append(keyspace).append(".").append(joinTable)
			.append(" (").append(rj_joinKeys.get(cursor)).append(", ")
			.append("list_item" + column).append(") VALUES (")
			.append(joinKeyValue).append(", ?);");

			
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

				Map<String, String> tempMapImmutable = theRow.getMap(
						"list_item" + column, String.class, String.class);

				System.out.println(tempMapImmutable);

				if(tempMapImmutable.size()>1){
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
				
				//checking if all list items are null --> delete the whole row
				boolean allNull = true;
				if(myMap == null){
					
				for(int k = 0; k< baseTables.size() && allNull;k++ ){
					if(column != k+1 && theRow.getMap(
							"list_item" + (k+1), String.class, String.class).size()!=0){
						allNull = false;
					
						
						
					}
				}
			}
				
				
				
				//all entries are nulls
				if(allNull){
					StringBuilder deleteQuery = new StringBuilder("delete from ");
					deleteQuery.append(keyspace).append(".").append(joinTable)
							.append(" WHERE ").append(joinKeyName + " = ").append(joinKeyValue)
							.append(";");	
					
					System.out.println(deleteQuery);
					
					session = currentCluster.connect();
					session.execute(deleteQuery.toString());
				}

			}

			

			cursor += nrOfTables;

		}

	}


}
