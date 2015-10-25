package ViewManager;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class PreaggregationHelper {

	static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
					.getString("config.host.localhost"))
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();

	public static ResultSet selectStatement(JSONObject json,String preaggTable,String aggKey,String aggKeyValue){

		StringBuilder selectPreaggQuery1 = new StringBuilder("SELECT ")
		.append(aggKey + ", ").append("list_item, ").append("sum, ").append("count, ")
		.append("average, min, max, stream, signature ");
		selectPreaggQuery1.append(" FROM ")
		.append((String) json.get("keyspace")).append(".")
		.append(preaggTable).append(" where ")
		.append(aggKey + " = ").append(aggKeyValue).append(";");

		System.out.println(selectPreaggQuery1);

		// 2.b execute select statement
		ResultSet PreAggMap = null;
		try {

			Session session = currentCluster.connect();
			PreAggMap = session.execute(selectPreaggQuery1.toString());

		} catch (Exception e) {
			e.printStackTrace();
		}

		return PreAggMap;
	}

	public static ResultSet selectStatementSignature(JSONObject json,String preaggTable,String aggKey,String aggKeyValue){

		StringBuilder selectPreaggQuery1 = new StringBuilder("SELECT ")
		.append("signature");
		selectPreaggQuery1.append(" FROM ")
		.append((String) json.get("keyspace")).append(".")
		.append(preaggTable).append(" where ")
		.append(aggKey + " = ").append(aggKeyValue).append(";");

		System.out.println(selectPreaggQuery1);

		// 2.b execute select statement
		ResultSet PreAggMap = null;
		try {

			Session session = currentCluster.connect();
			PreAggMap = session.execute(selectPreaggQuery1.toString());

		} catch (Exception e) {
			e.printStackTrace();
		}

		return PreAggMap;
	}

	public static boolean firstInsertion(String aggKeyType,Stream stream,ArrayList<String> colValues, float aggColValue, JSONObject json, String preaggTable, String aggKey, String aggKeyValue, String identifier,String pk){

		// 2.c.1 create a map, add pk and list with delta _new values
		// 2.c.2 set the agg col values

		HashMap<String, String> myMap = new HashMap<>();
		
		myMap.put(pk, colValues.toString());

		float sum = aggColValue;
		int count = 1;
		float average = sum / count;
		float min = aggColValue;
		float max = aggColValue;

		ResultSet rs = null;

		CustomizedRow constructedRow = CustomizedRow.constructUpdatedPreaggRow(aggKey,aggKeyValue,aggKeyType,myMap,sum,count,average,min,max, Serialize.serializeStream2(stream));
		stream.setUpdatedPreaggRow(constructedRow);
		String blob = Serialize.serializeStream2(stream);


		// 3. execute the insertion
		StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(preaggTable).append(" ( ")
		.append(aggKey + ", ").append("list_item, ")
		.append("sum, count, average, min, stream, max, signature")
		.append(") VALUES (").append(aggKeyValue + ", ")
		.append("?, ").append(sum+", ").append((int) count+", ").append(average+", ").append(min+", ")
		.append(blob+", ").append(max).append(", {'").append(identifier).append("': '").append(json.get("readPtr").toString())
		.append("'} ) IF NOT EXISTS ;");

		System.out.println(insertQueryAgg.toString());

		try{
			Session session1 = currentCluster.connect();

			PreparedStatement statement1 = session1.prepare(insertQueryAgg
					.toString());
			BoundStatement boundStatement = new BoundStatement(statement1);
			rs = session1.execute(boundStatement.bind(myMap));

			System.out.println(boundStatement.toString());

		}catch (Exception e) {
			e.printStackTrace();
		}

		if(rs.one().getBool("[applied]"))
			return true;
		else
			return false;


	}

	public static boolean insertStatementToDelete(JSONObject json,String preaggTable,String aggKey,String aggKeyValue,String blob, String identifier,CustomizedRow crow){


		if(json.get("recovery_mode").equals("on") || json.get("recovery_mode").equals("last_recovery_line")){
			Row rs = selectStatementSignature(json, preaggTable, aggKey, aggKeyValue).one();

			if(rs!= null && Long.parseLong(rs.getMap("signature", String.class, String.class).get(identifier).split(":")[1])
					>= Long.parseLong(json.get("readPtr").toString().split(":")[1]))
				return true;

		}

		// 3. execute the insertion
		StringBuilder insertQueryAgg = new StringBuilder("UPDATE ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(preaggTable).append(" SET stream = ").append(blob).append(", signature['").append(identifier).append("']= '")
		.append(json.get("readPtr")).append("' WHERE ").append(aggKey).append(" = ").append(aggKeyValue)
		.append("IF sum =").append(crow.getFloat("sum")).append("and count = ").append(crow.getInt("count"))
		.append(";");

		System.out.println(insertQueryAgg);

		Row updated;
		
		try{
			Session session = currentCluster.connect();
			updated = session.execute(insertQueryAgg.toString()).one();
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		if(updated.getBool("[applied]"))
			return true;
		else
			return false;
	}

	public static void insertStatement(JSONObject json,String preaggTable,String aggKey,String aggKeyValue,Map<String, String> myMap,float sum,float count,float min,float max,float average, String identifier){

		if(json.get("recovery_mode").equals("on") || json.get("recovery_mode").equals("last_recovery_line")){
			Row rs = selectStatementSignature(json, preaggTable, aggKey, aggKeyValue).one();

			if(rs!= null && Long.parseLong(rs.getMap("signature", String.class, String.class).get(identifier).split(":")[1])
					>= Long.parseLong(json.get("readPtr").toString().split(":")[1]))
				return ;

		}

		StringBuilder updateQuery = new StringBuilder("UPDATE ");
		updateQuery.append((String) json.get("keyspace"))
		.append(".").append(preaggTable).append(" SET list_item = ?, sum = ").append(sum)
		.append(", count = ").append((int)count).append(", average = ").append(average).append(", min = ")
		.append(min).append(", max = ").append(max).append(", signature['").append(identifier).append("']= '")
		.append(json.get("readPtr")).append("' WHERE ").append(aggKey).append(" = ").append(aggKeyValue)
		.append(";");


		System.out.println(updateQuery);

		try {
			Session session = currentCluster.connect();
			PreparedStatement statement1 = session.prepare(updateQuery
					.toString());
			BoundStatement boundStatement = new BoundStatement(statement1);
			System.out.println(boundStatement.toString());
			session.execute(boundStatement.bind(myMap));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean updateStatement(Float sum, int count, Float avg, Float min, Float max, Map<String, String> myMap, String key, String keyValue,
			String preaggTable, JSONObject json, ByteBuffer blob_old,String blob_new, String identifier){

		if(json.get("recovery_mode").equals("on") || json.get("recovery_mode").equals("last_recovery_line")){
			Row rs = selectStatementSignature(json, preaggTable, key, keyValue).one();

			if(rs!= null && Long.parseLong(rs.getMap("signature", String.class, String.class).get(identifier).split(":")[1])
					>= Long.parseLong(json.get("readPtr").toString().split(":")[1]))
				return true;

		}

		StringBuilder updateQuery = new StringBuilder("UPDATE ");
		updateQuery.append((String) json.get("keyspace"))
		.append(".").append(preaggTable).append(" SET list_item = ?, sum = ").append(sum)
		.append(", count = ").append(count).append(", average = ").append(avg).append(", min = ")
		.append(min).append(", max = ").append(max).append(", stream = ").append(blob_new).append(", signature['").append(identifier).append("']= '")
		.append(json.get("readPtr")).append("' WHERE ").append(key).append(" = ").append(keyValue)
		.append(" IF stream = ?").append(";");


		System.out.println(updateQuery);

		Row updated ;
		try {

			Session session = currentCluster.connect();
			PreparedStatement statement1 = session.prepare(updateQuery
					.toString());
			BoundStatement boundStatement = new BoundStatement(statement1);
			System.out.println(boundStatement.toString());
			updated = session.execute(boundStatement.bind(myMap,blob_old)).one();

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		if(updated.getBool("[applied]"))
			return true;
		else
			return false;


	}

	public static boolean updateAggColValue(String aggKeyType,Stream stream, ArrayList<String> myList,float aggColValue,float aggColValue_old,Row theRow, int aggColIndexInList,JSONObject json, String preaggTable,String aggKey,String aggKeyValue, ByteBuffer buffer_old, String identifier,String pk){

		float sum = 0;
		int count = 0;
		float average = 0;
		float min = 0;
		float max = 0;
		Map<String, String> tempMapImmutable = theRow.getMap("list_item", String.class, String.class);

		HashMap<String, String> myMap = new HashMap<String,String>();
		myMap.putAll(tempMapImmutable);

		int prev_count = myMap.keySet().size();
		myMap.put(pk, myList.toString());

		// 2.e set agg col values

		if (myMap.size() != 1) {
			count = myMap.keySet().size();

			if (count > prev_count)
				sum = theRow.getFloat("sum") + aggColValue;
			else
				sum = theRow.getFloat("sum") - aggColValue_old
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


		CustomizedRow constructedRow = CustomizedRow.constructUpdatedPreaggRow(aggKey,aggKeyValue,aggKeyType,myMap,sum,count,average,min,max, Serialize.serializeStream2(stream));
		stream.setUpdatedPreaggRow(constructedRow);
		String buffer_new = Serialize.serializeStream2(stream);

		//insertStatement(json, preaggTable, aggKey, aggKeyValue, myMap, sum, count, min, max, average);
		if(updateStatement(sum, count, average, min, max, myMap, aggKey, aggKeyValue, preaggTable, json, buffer_old,buffer_new,identifier))
			return true;
		else
			return false;


	}

	public static boolean subtractOldAggColValue(String aggKeyType, Stream stream,ArrayList<String> myList, float aggColValue_old,Map<String, String> myMap,Row theRow, int aggColIndexInList,JSONObject json, String preaggTable,String aggKey,String aggKeyValue,ByteBuffer blob_old, String identifier){

		String pk = myList.get(0);
		myList.remove(0);

		// 5.a remove entry from map with that pk
		myMap.remove(pk);

		// 5.c adjust sum,count,average values
		int count = myMap.size();
		float sum = theRow.getFloat("sum") - aggColValue_old;
		float average = sum / count;

		float max = -Float.MAX_VALUE;
		float min = Float.MAX_VALUE;

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


		CustomizedRow crow = CustomizedRow.constructUpdatedPreaggRow(aggKey, aggKeyValue, aggKeyType,myMap, sum, count, average, min, max, Serialize.serializeStream2(stream));
		stream.setUpdatedPreaggRow(crow);
		String blob_new = Serialize.serializeStream2(stream);

		//insertStatement(json, preaggTable, aggKey, aggKeyValue, myMap, sum, count, min, max, average);
		if(updateStatement(sum, count, average, min, max, myMap, aggKey, aggKeyValue, preaggTable, json, blob_old,blob_new, identifier))
			return true;
		else
			return false;


	}

}