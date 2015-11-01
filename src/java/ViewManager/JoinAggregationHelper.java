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
			String joinAggTable, JSONObject json, String identifier){

		if(json.get("recovery_mode").equals("on") || json.get("recovery_mode").equals("last_recovery_line")){
			Row rs = selectStatement(key, keyValue, joinAggTable, json);

			if(rs!= null && Long.parseLong(rs.getMap("signature", String.class, String.class).get(identifier).split(":")[1])
					>= Long.parseLong(json.get("readPtr").toString().split(":")[1]))
				return true;

		}

		StringBuilder insertQueryAgg = new StringBuilder("UPDATE ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable).append(" SET sum = ").append(sum)
		.append(", count = ").append(count).append(", average = ").append(avg).append(", min = ")
		.append(min).append(", max = ").append(max).append(", signature['").append(identifier).append("']= '").append(json.get("readPtr").toString()).append("' WHERE ").append(key).append(" = ").append(keyValue)
		.append(";");

		System.out.println(insertQueryAgg);

		try {

			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());
			session.close();

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}


		return true;

	}


	public static boolean insertStatement(JSONObject json, String joinAggTable,CustomizedRow row, String identifier){

		String aggKeyName = row.getName(0);
		String aggKeyType = row.getType(0);
		String aggKeyValue = Utils.getColumnValueFromDeltaStream(row, aggKeyName, aggKeyType, "");

		if(json.get("recovery_mode").equals("on") || json.get("recovery_mode").equals("last_recovery_line")){
			Row rs = selectStatement(aggKeyName, aggKeyValue, joinAggTable, json);

			if(rs!= null && Long.parseLong(rs.getMap("signature", String.class, String.class).get(identifier).split(":")[1])
					>= Long.parseLong(json.get("readPtr").toString().split(":")[1]))
				return true;

		}

		float sum = row.getFloat("sum");
		float avg = row.getFloat("average");
		float min = row.getFloat("min");
		float max = row.getFloat("max");
		int count = row.getInt("count");

		StringBuilder insertQueryAgg = new StringBuilder("UPDATE ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable).append(" SET sum = ").append(sum)
		.append(", count = ").append(count).append(", average = ").append(avg).append(", min = ")
		.append(min).append(", max = ").append(max).append(", signature['").append(identifier).append("']= '")
		.append(json.get("readPtr").toString()).append("' WHERE ").append(aggKeyName).append(" = ").append(aggKeyValue)
		.append(";");

		System.out.println(insertQueryAgg);

		return true;

	}

	public static boolean updateStatement(Float sum, int count, Float avg, Float min, Float max, String key, String keyValue,
			String joinAggTable, JSONObject json, Float oldSum, String identifier){

		if(json.get("recovery_mode").equals("on") || json.get("recovery_mode").equals("last_recovery_line")){
			Row rs = selectStatement(key, keyValue, joinAggTable, json);

			if(rs!= null && Long.parseLong(rs.getMap("signature", String.class, String.class).get(identifier).split(":")[1])
					>= Long.parseLong(json.get("readPtr").toString().split(":")[1]))
				return true;

		}


		StringBuilder updateQuery = new StringBuilder("UPDATE ");
		updateQuery.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable).append(" SET sum = ").append(sum)
		.append(", count = ").append(count).append(", average = ").append(avg).append(", min = ")
		.append(min).append(", max = ").append(max).append(", signature['").append(identifier).append("']= '").append(json.get("readPtr").toString()).append("' WHERE ").append(key).append(" = ").append(keyValue)
		.append(" IF sum = ").append(oldSum).append(";");


		System.out.println(updateQuery);

		Row updated ;
		try {

			Session session = currentCluster.connect();
			updated = session.execute(updateQuery.toString()).one();
			session.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		if(updated.getBool("[applied]"))
			return true;
		else
			return false;


	}

	public static Row selectStatement(String key, String keyValue, String joinAggTable, JSONObject json){
		StringBuilder selectQuery1 = new StringBuilder(
				"SELECT ").append(key)
				.append(", sum, ").append("count, ")
				.append("average, min, max, signature ");
		selectQuery1.append(" FROM ")
		.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable)
		.append(" where ")
		.append(key + " = ")
		.append(keyValue).append(";");

		System.out.println(selectQuery1);
		Row theRow = null;
		try {
			Session session = currentCluster.connect();
			theRow = session.execute(
					selectQuery1.toString()).one();
			session.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return theRow;

	}

	public static boolean UpdateOldRowBySubtracting(Stream stream,String listItem,CustomizedRow deltaRow, JSONObject json,String joinAggTable, String joinKey,String joinKeyValue, String aggColName, String aggColValue, CustomizedRow newRow, String identifier){


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

				Map<String, String> map1 = newRow.getMap(listItem);

				min = Float.MAX_VALUE;


				int aggColIndexInList = 0;

				for (int i = 0; i < deltaRow.colDefSize; i++) {
					if (deltaRow.getName(i)
							.contentEquals(aggColName + "_new")) {
						break;
					}
					if (deltaRow.getName(i).contains("_new"))
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

			Map<String, String> map1 = newRow.getMap(listItem);

			max = Float.MIN_VALUE;


			int aggColIndexInList = 0;

			for (int i = 0; i < deltaRow.colDefSize; i++) {
				if (deltaRow.getName(i).contentEquals(aggColName + "_new")) {
					break;
				}
				if (deltaRow.getName(i).contains("_new"))
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

			if(!updateStatement(sum, count, avg, min, max, joinKey , joinKeyValue, joinAggTable, json, theRow.getFloat("sum"), identifier))
				return false;
		}

		if(joinAggTable.contains("inner")){
			CustomizedRow crow = new CustomizedRow(selectStatement(joinKey, joinKeyValue, joinAggTable, json));
			stream.setInnerJoinAggUpdatedOldRow(crow);
		}else{
			CustomizedRow crow = new CustomizedRow(selectStatement(joinKey, joinKeyValue, joinAggTable, json));
			stream.setLeftOrRightJoinAggUpdatedOldRow(crow);		
		}

		return true;

	}

	public static boolean updateNewRowByAddingNewElement(Stream stream,String joinKeyName,String joinKeyValue, JSONObject json,String joinAggTable,String aggColValue, String identifier){

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

		
		if(!updateStatement(sum, count, avg, min, max, joinKeyName , joinKeyValue, joinAggTable, json, theRow.getFloat("sum"), identifier))
			return false;

		if(joinAggTable.contains("inner")){
			CustomizedRow crow = new CustomizedRow(selectStatement(joinKeyName, joinKeyValue, joinAggTable, json));
			stream.setInnerJoinAggNewRow(crow);
		}else{
			CustomizedRow crow = new CustomizedRow(selectStatement(joinKeyName, joinKeyValue, joinAggTable, json));
			stream.setLeftOrRightJoinAggNewRow(crow);
		}
		return true;
	}

	public static boolean updateAggColValueOfNewRow(Stream stream, String listItem, CustomizedRow newRJRow, JSONObject json, String joinKeyName, String joinKeyValue,String joinAggTable, String aggColName, String aggColValue, String oldAggColValue, CustomizedRow oldRJRow, String identifier){


		Row theRow = selectStatement(joinKeyName, joinKeyValue, joinAggTable, json);

		Float sum = theRow.getFloat("sum");

		if (!aggColValue.equals("null"))
			sum += Float.parseFloat(aggColValue);

		if (!oldAggColValue.equals("null"))
			sum -= Float.parseFloat(oldAggColValue);

		int count = theRow.getInt("count");

		// old = null and new != null
		if (oldAggColValue.equals("null")
				&& !aggColValue.equals("null"))
			count++;

		else // new = null and old != null
			if (!oldAggColValue.equals("null") 
					&& aggColValue.equals("null"))
				count--;

		Float avg = sum / (float) count;

		Float min = theRow.getFloat("min");

		// if newAggCol != null and newAggCol < min
		if (!aggColValue.equals("null") && Float.parseFloat(aggColValue) < min)
			min = Float.parseFloat(aggColValue);

		// if(oldAggCol == min)
		else if (!oldAggColValue.equals("null") && Float.parseFloat(oldAggColValue) == min) {
			// loop on list_item1 to get the new minimum

			Map<String, String> map1 = newRJRow.getMap(listItem);

			min = Float.MAX_VALUE;

			int aggColIndexInList = 0;

			for (int i = 0; i < stream.getDeltaUpdatedRow().colDefSize; i++) {
				if (stream.getDeltaUpdatedRow().getName(i).contentEquals(aggColName + "_new")) {
					break;
				}
				if (stream.getDeltaUpdatedRow().getName(i).contains("_new"))
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
		if (!aggColValue.equals("null")
				&& Float.parseFloat(aggColValue) > max)

			max = Float.parseFloat(aggColValue);

		// if(oldAggCol == min)
		else if (!oldAggColValue.equals("null")
				&& Float.parseFloat(oldAggColValue) == max) {
			// loop on list_item1 to get the new minimum

			Map<String, String> map1 = newRJRow.getMap(listItem);

			max = Float.MIN_VALUE;


			int aggColIndexInList = 0;

			for (int i = 0; i < stream.getDeltaUpdatedRow().colDefSize; i++) {
				if (stream.getDeltaUpdatedRow().getName(i)
						.contentEquals(aggColName + "_new")) {
					break;
				}
				if (stream.getDeltaUpdatedRow().getName(i).contains("_new"))
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

		if(!updateStatement(sum, count, avg, min, max, joinKeyName, joinKeyValue, joinAggTable, json, theRow.getFloat("sum"), identifier))
			return false;

		if(joinAggTable.contains("inner")) {
			CustomizedRow crow = new CustomizedRow(selectStatement(joinKeyName, joinKeyValue, joinAggTable, json));
			stream.setInnerJoinAggUpdatedOldRow(crow);
		}else {
			CustomizedRow crow = new CustomizedRow(selectStatement(joinKeyName, joinKeyValue, joinAggTable, json));
			stream.setLeftOrRightJoinAggUpdatedOldRow(crow);
		}
		return true;

	}

	public static void moveRowsToInnerJoinAgg(Stream stream,String joinAggTable,String innerJoinAggTable,String joinKeyName,String joinKeyValue,JSONObject json, String identifier){

		if (!joinAggTable.equals("false")) {

			Row theRow = selectStatement(joinKeyName, joinKeyValue, joinAggTable, json);

			Float sum = theRow.getFloat("sum");
			int count = theRow.getInt("count");
			Float avg = sum / (float) count;
			Float min = theRow.getFloat("min");
			Float max = theRow.getFloat("max");

			insertStatement(sum, count, avg, min, max, joinKeyName, joinKeyValue, innerJoinAggTable, json,  identifier);
			CustomizedRow crow = new CustomizedRow(selectStatement(joinKeyName, joinKeyValue, innerJoinAggTable, json));
			stream.setInnerJoinAggNewRow(crow);
		}
	}

	public static void addRowsToInnerJoinAgg(Stream stream,String listItem,CustomizedRow newRJRow,int aggColIndexInList,String innerJoinAggTable,JSONObject json,String joinKey,String joinKeyValue, String identifier){

		Float sum = 0.0f;
		Float avg = 0.0f;
		int count = 0;
		Float min = Float.MAX_VALUE;
		Float max = Float.MIN_VALUE;


		Map<String, String> map1 = newRJRow.getMap(listItem);

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

		insertStatement(sum, count, avg, min, max, joinKey, joinKeyValue,innerJoinAggTable,json, identifier);
		CustomizedRow crow = new CustomizedRow(selectStatement(joinKey, joinKeyValue, innerJoinAggTable, json));
		stream.setInnerJoinAggNewRow(crow);
	}



}
