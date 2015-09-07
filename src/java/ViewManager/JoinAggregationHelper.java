package ViewManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class JoinAggregationHelper {


	static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
					.getString("config.host.localhost"))
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();

	public static boolean insertStatement(Float sum, int count, Float avg, Float min, Float max, String key, String keyValue,
			String joinAggTable, JSONObject json){

		StringBuilder insertQueryAgg = new StringBuilder(
				"INSERT INTO ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable).append(" ( ")
		.append(key + ", ")
		.append("sum, count, average, min, max")
		.append(") VALUES (").append(keyValue + ", ")
		.append(sum).append(", ").append(count)
		.append(", ").append(avg).append(", ").append(min)
		.append(", ").append(max).append(");");

		try {

			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;

	}
	
	
	public static boolean insertStatement(JSONObject json, String joinAggTable,Row row){

		
		String aggKeyName = row.getColumnDefinitions().getName(0);
		String aggKeyType = row.getColumnDefinitions().getType(0).toString();
		String aggKeyValue = Utils.getColumnValueFromDeltaStream(row, aggKeyName, aggKeyType, "");
		
		float sum = row.getFloat("sum");
		float avg = row.getFloat("average");
		float min = row.getFloat("min");
		float max = row.getFloat("max");
		int count = row.getInt("count");
		
		
		StringBuilder insertQueryAgg = new StringBuilder(
				"INSERT INTO ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable).append(" ( ")
		.append(aggKeyName + ", ")
		.append("sum, count, average, min, max")
		.append(") VALUES (").append(aggKeyValue + ", ")
		.append(sum).append(", ").append(count)
		.append(", ").append(avg).append(", ").append(min)
		.append(", ").append(max).append(");");

		try {

			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;

	}
	

	public static Row selectStatement(String key, String keyValue, String joinAggTable, JSONObject json){
		StringBuilder selectQuery1 = new StringBuilder(
				"SELECT ").append(key)
				.append(", sum, ").append("count, ")
				.append("average, min, max ");
		selectQuery1.append(" FROM ")
		.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable)
		.append(" where ")
		.append(key + " = ")
		.append(keyValue).append(";");


		Row theRow = null;
		try {
			Session session = currentCluster.connect();
			theRow = session.execute(
					selectQuery1.toString()).one();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return theRow;

	}

	public static void UpdateOldRowBySubtracting(Stream stream,String listItem,Row deltaUpdatedRow, JSONObject json,String joinAggTable, String joinKey,String joinKeyValue, String aggColName, String aggColValue, Row changedKeyReverseRow){


		Row theRow = selectStatement(joinKey, joinKeyValue, joinAggTable, json);

		if(theRow!=null){

			Float sum = theRow.getFloat("sum");
			sum -= Float.parseFloat(aggColValue);

			int count = theRow.getInt("count");
			count--;

			Float avg = sum / (float) count;

			Float min = theRow.getFloat("min");

			if (min == Float.parseFloat(aggColValue)) {
				// loop on list_item1 to get the new minimum

				Map<String, String> map1 = changedKeyReverseRow.getMap(listItem,String.class, String.class);

				min = Float.MAX_VALUE;

				List<Definition> def = deltaUpdatedRow.getColumnDefinitions().asList();

				int aggColIndexInList = 0;

				for (int i = 0; i < def.size(); i++) {
					if (def.get(i).getName()
							.contentEquals(aggColName + "_new")) {
						break;
					}
					if (def.get(i).getName().contains("_new"))
						aggColIndexInList++;
				}

				for (Map.Entry<String, String> entry : map1.entrySet()) {

					String list = entry.getValue().replaceAll("\\[", "")
							.replaceAll("\\]", "");
					String[] listArray = list.split(",");
					Float x = Float
							.parseFloat(listArray[aggColIndexInList]);
					if (x < min)
						min = x;

				}
			}

			Float max = theRow.getFloat("max");

			Map<String, String> map1 = changedKeyReverseRow.getMap(listItem,String.class, String.class);

			max = Float.MIN_VALUE;

			List<Definition> def = deltaUpdatedRow.getColumnDefinitions().asList();

			int aggColIndexInList = 0;

			for (int i = 0; i < def.size(); i++) {
				if (def.get(i).getName().contentEquals(aggColName + "_new")) {
					break;
				}
				if (def.get(i).getName().contains("_new"))
					aggColIndexInList++;
			}

			for (Map.Entry<String, String> entry : map1.entrySet()) {

				String list = entry.getValue().replaceAll("\\[", "")
						.replaceAll("\\]", "");
				String[] listArray = list.split(",");
				Float x = Float
						.parseFloat(listArray[aggColIndexInList]);
				if (x > max)
					max = x;

			}

			StringBuilder insertQueryAgg = new StringBuilder(
					"INSERT INTO ");
			insertQueryAgg.append((String) json.get("keyspace"))
			.append(".").append(joinAggTable).append(" ( ")
			.append(joinKey + ", ")
			.append("sum, count, average, min, max")
			.append(") VALUES (")
			.append(joinKeyValue + ", ").append(sum)
			.append(", ").append(count).append(", ")
			.append(avg).append(", ").append(min).append(", ")
			.append(max).append(");");

			System.out.println(insertQueryAgg);

			try {
				Session session = currentCluster.connect();
				session.execute(insertQueryAgg.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		
		if(joinAggTable.contains("inner"))
			stream.setInnerJoinAggUpdatedOldRow(selectStatement(joinKey, joinKeyValue, joinAggTable, json));
		else
			stream.setLeftOrRightJoinAggUpdatedOldRow(selectStatement(joinKey, joinKeyValue, joinAggTable, json));

	}

	public static void updateNewRowByAddingNewElement(Stream stream,String joinKeyName,String joinKeyValue, JSONObject json,String joinAggTable,String aggColValue){

		Row theRow = selectStatement(joinKeyName, joinKeyValue, joinAggTable, json);


		Float sum = theRow.getFloat("sum");
		sum += Float.parseFloat(aggColValue);

		int count = theRow.getInt("count");
		count++;

		Float avg = sum / (float) count;

		Float min = theRow.getFloat("min");

		if (aggColValue != null && !aggColValue.equals("null")
				&& !aggColValue.equals("'null'")
				&& Float.parseFloat(aggColValue) < min) {
			min = Float.parseFloat(aggColValue);
		}

		Float max = theRow.getFloat("max");

		if (aggColValue != null && !aggColValue.equals("null")
				&& !aggColValue.equals("'null'")
				&& Float.parseFloat(aggColValue) > max) {
			max = Float.parseFloat(aggColValue);
		}


		StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable).append(" ( ")
		.append(joinKeyName + ", ")
		.append("sum, count, average, min, max")
		.append(") VALUES (").append(joinKeyValue + ", ")
		.append(sum).append(", ").append(count)
		.append(", ").append(avg).append(", ").append(min)
		.append(", ").append(max).append(");");

		System.out.println(insertQueryAgg);

		try {
			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(joinAggTable.contains("inner"))
			stream.setInnerJoinAggNewRow(selectStatement(joinKeyName, joinKeyValue, joinAggTable, json));
		else
			stream.setLeftOrRightJoinAggNewRow(selectStatement(joinKeyName, joinKeyValue, joinAggTable, json));
	}

	public static void updateAggColValueOfNewRow(Stream stream, String listItem,Row deltaUpdatedRow, Row newRJRow, JSONObject json, String joinKeyName, String joinKeyValue,String joinAggTable, String aggColName, String aggColValue, String oldAggColValue, Row oldRJRow){

		
		Row theRow = selectStatement(joinKeyName, joinKeyValue, joinAggTable, json);

		Map<String, String> mapNew = newRJRow.getMap(listItem, String.class,String.class);
		Map<String, String> mapOld = oldRJRow.getMap(listItem, String.class,String.class);
		
		Float sum = theRow.getFloat("sum");
		int count = theRow.getInt("count");
		
		//havent taken into account the case where aggcolnew is null, shouldnt reach this case aslan
		if(mapNew.size()>mapOld.size()){
			sum += Float.parseFloat(aggColValue);
			count++;
		}else if(mapNew.size()==mapOld.size()){
			sum += Float.parseFloat(aggColValue)-Float.parseFloat(oldAggColValue);
		}

		/*Float sum = theRow.getFloat("sum");
		if (aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'"))
			sum += Float.parseFloat(aggColValue);

		if (oldAggColValue != null && !oldAggColValue.equals("null") && !oldAggColValue.equals("'null'"))
			sum -= Float.parseFloat(oldAggColValue);

		int count = theRow.getInt("count");

		// old = null and new != null
		if ((oldAggColValue == null || oldAggColValue.equals("null") || oldAggColValue.equals("'null'"))
				&& (aggColValue != null || !aggColValue.equals("null") || !aggColValue.equals("'null'")))
			count++;

		else // new = null and old != null
			if ((oldAggColValue != null || !oldAggColValue.equals("null") || !oldAggColValue.equals("'null'"))
					&& (aggColValue == null || aggColValue.equals("null") || aggColValue.equals("'null'")))
				count--;*/

		Float avg = sum / (float) count;

		Float min = theRow.getFloat("min");

		// if newAggCol != null and newAggCol < min
		if (aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'") && Float.parseFloat(aggColValue) < min)
			min = Float.parseFloat(aggColValue);

		// if(oldAggCol == min)
		else if (oldAggColValue != null	&& !oldAggColValue.equals("null") && !oldAggColValue.equals("'null'") && Float.parseFloat(oldAggColValue) == min) {
			// loop on list_item1 to get the new minimum

			Map<String, String> map1 = newRJRow.getMap(listItem, String.class,String.class);

			min = Float.MAX_VALUE;

			List<Definition> def = deltaUpdatedRow.getColumnDefinitions().asList();

			int aggColIndexInList = 0;

			for (int i = 0; i < def.size(); i++) {
				if (def.get(i).getName().contentEquals(aggColName + "_new")) {
					break;
				}
				if (def.get(i).getName().contains("_new"))
					aggColIndexInList++;
			}

			for (Map.Entry<String, String> entry : map1.entrySet()) {

				String list = entry.getValue()
						.replaceAll("\\[", "")
						.replaceAll("\\]", "");
				String[] listArray = list.split(",");
				Float x = Float
						.parseFloat(listArray[aggColIndexInList]);
				if (x < min)
					min = x;

			}
		}

		Float max = theRow.getFloat("max");

		// if newAggCol != null and newAggCol < min
		if (aggColValue != null && !aggColValue.equals("null") && !aggColValue.equals("'null'")
				&& Float.parseFloat(aggColValue) > max)

			max = Float.parseFloat(aggColValue);

		// if(oldAggCol == min)
		else if (oldAggColValue != null && !oldAggColValue.equals("null")&& !oldAggColValue.equals("'null'")
				&& Float.parseFloat(oldAggColValue) == max) {
			// loop on list_item1 to get the new minimum

			Map<String, String> map1 = newRJRow.getMap(listItem, String.class,String.class);

			max = Float.MIN_VALUE;

			List<Definition> def = deltaUpdatedRow.getColumnDefinitions().asList();

			int aggColIndexInList = 0;

			for (int i = 0; i < def.size(); i++) {
				if (def.get(i).getName()
						.contentEquals(aggColName + "_new")) {
					break;
				}
				if (def.get(i).getName().contains("_new"))
					aggColIndexInList++;
			}

			for (Map.Entry<String, String> entry : map1.entrySet()) {

				String list = entry.getValue()
						.replaceAll("\\[", "")
						.replaceAll("\\]", "");
				String[] listArray = list.split(",");
				Float x = Float
						.parseFloat(listArray[aggColIndexInList]);
				if (x > max)
					max = x;
			}

		}

		insertStatement(sum, count, avg, min, max, joinKeyName, joinKeyValue, joinAggTable, json);

		if(joinAggTable.contains("inner"))
			stream.setInnerJoinAggUpdatedOldRow(selectStatement(joinKeyName, joinKeyValue, joinAggTable, json));
		else
			stream.setLeftOrRightJoinAggUpdatedOldRow(selectStatement(joinKeyName, joinKeyValue, joinAggTable, json));
		
	}

	public static void moveRowsToInnerJoinAgg(Stream stream,String joinAggTable,String innerJoinAggTable,String joinKeyName,String joinKeyValue,JSONObject json){
		
		if (!joinAggTable.equals("false")) {
			
			Row theRow = selectStatement(joinKeyName, joinKeyValue, joinAggTable, json);
			
			Float sum = theRow.getFloat("sum");
			int count = theRow.getInt("count");
			Float avg = sum / (float) count;
			Float min = theRow.getFloat("min");
			Float max = theRow.getFloat("max");

			insertStatement(sum, count, avg, min, max, joinKeyName, joinKeyValue, innerJoinAggTable, json);
			stream.setInnerJoinAggNewRow(selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
			
		}
	}
		
	public static void addRowsToInnerJoinAgg(Stream stream,String listItem,Row deltaUpdatedRow,Row newRJRow,int aggColIndexInList,String innerJoinAggTable,JSONObject json,String joinKey,String joinKeyValue){
		
		Float sum = 0.0f;
		Float avg = 0.0f;
		int count = 0;
		Float min = Float.MAX_VALUE;
		Float max = Float.MIN_VALUE;

		List<Definition> def = deltaUpdatedRow.getColumnDefinitions().asList();

		Map<String, String> map1 = newRJRow.getMap(listItem,String.class, String.class);

		for (Map.Entry<String, String> entry : map1.entrySet()) {

			String list = entry.getValue().replaceAll("\\[", "")
					.replaceAll("\\]", "");
			String[] listArray = list.split(",");
			Float x = Float.parseFloat(listArray[aggColIndexInList]); // if
			// x
			// is
			// not
			// null
			if (x > max)
				max = x;

			if (x < min)
				min = x;

			count++;

			sum += x;

		}
		
		avg = sum/count;
		
		insertStatement(sum, count, avg, min, max, joinKey, joinKeyValue,innerJoinAggTable,json);
		stream.setInnerJoinAggNewRow(selectStatement(joinKey, joinKeyValue, innerJoinAggTable, json));

	}



}
