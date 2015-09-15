package ViewManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class JoinAggGroupByHelper {

	static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
					.getString("config.host.localhost"))
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();


	public static void insertStatement(JSONObject json, String joinAggTable,Row row){

		String aggKeyName = row.getColumnDefinitions().getName(0);
		String aggKeyType = row.getColumnDefinitions().getType(0).toString();
		String aggKeyValue = Utils.getColumnValueFromDeltaStream(row, aggKeyName, aggKeyType, "");

		List<Float> myList = new ArrayList<Float>();
		myList.addAll(row.getList("list_item", Float.class));

		float sum = row.getFloat("sum");
		float avg = row.getFloat("average");
		float min = row.getFloat("min");
		float max = row.getFloat("max");
		int count = row.getInt("count");

		StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ").append((String) json.get("keyspace"))
				.append(".").append(joinAggTable).append(" ( ")
				.append(aggKeyName + ", ").append("list_item, sum, count, average, min, max")
				.append(") VALUES (")
				.append(aggKeyValue + ", ").append(myList+", ").append(sum).append(", ").append(count).append(", ")
				.append(avg).append(", ").append(min).append(", ").append(max).append(");");

		System.out.println(insertQueryAgg);

		try {
			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public static boolean updateStatement(Float sum, int count, Float avg, Float min, Float max, List<Float> myList, String key, String keyValue,
			String preaggTable, JSONObject json, Float oldSum){
		
		StringBuilder updateQuery = new StringBuilder("UPDATE ");
		updateQuery.append((String) json.get("keyspace"))
		.append(".").append(preaggTable).append(" SET list_item = ").append(myList)
		.append(", sum = ").append(sum).append(", count = ").append(count).append(", average = ").append(avg).append(", min = ")
		.append(min).append(", max = ").append(max).append(" WHERE ").append(key).append(" = ").append(keyValue)
		.append(" IF sum = ").append(oldSum).append(";");
		
		
		System.out.println(updateQuery);
		
		Row updated ;
		try {

			Session session = currentCluster.connect();
			
			updated = session.execute(updateQuery.toString()).one();

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		if(updated.getBool("[applied]"))
		return true;
		else
		return false;
		
		
	}


	public static Row selectStatement(String joinAggTable,String aggKeyName,String aggKeyValue,JSONObject json){

		StringBuilder selectQuery1 = new StringBuilder("SELECT ").append(aggKeyName+", ").append("list_item")
				.append(", sum, count,average, min, max ").append(" FROM ").append((String) json.get("keyspace")).append(".")
				.append(joinAggTable).append(" where ").append(aggKeyName + " = ").append(aggKeyValue).append(";");

		Row theRow = null;
		try {
			Session session = currentCluster.connect();
			theRow = session.execute(selectQuery1.toString()).one();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return theRow;
	}


	public static void deleteListItem1FromGroupBy(Stream stream,Row row, int index, String aggKeyType, String aggKeyName, JSONObject json, String innerJoinAggTable, int aggKeyIndex){

		Map<String,String> temp= row.getMap("list_item1", String.class, String.class);

		for (Map.Entry<String, String> entry : temp.entrySet()) {

			String list = entry.getValue().replaceAll("\\[", "").replaceAll("\\]", "");
			String[] listArray = list.split(",");
			String aggColValue = listArray[index];
			String aggKeyValue = listArray[aggKeyIndex];
			while(!searchAndDeleteRowFromJoinAggGroupBy(stream,json, innerJoinAggTable, aggKeyName, aggKeyValue, aggColValue));

		}
	}

	public static void deleteListItem2FromGroupBy(Stream stream,Row row, int index, String aggKeyType, String aggKeyName, JSONObject json, String innerJoinAggTable, int aggKeyIndex){

		Map<String,String> temp= row.getMap("list_item2", String.class, String.class);

		for (Map.Entry<String, String> entry : temp.entrySet()) {

			String list = entry.getValue().replaceAll("\\[", "").replaceAll("\\]", "");
			String[] listArray = list.split(",");
			String aggColValue = listArray[index];
			String aggKeyValue = listArray[aggKeyIndex];
			while(!searchAndDeleteRowFromJoinAggGroupBy(stream,json, innerJoinAggTable, aggKeyName, aggKeyValue, aggColValue));

		}
	}

	public static boolean searchAndDeleteRowFromJoinAggGroupBy(Stream stream, JSONObject json, String joinAggTable, String aggKeyName, String aggKeyValue, String aggColValue) {

		List<Float> myList = new ArrayList<Float>();

		StringBuilder selectQuery1 = new StringBuilder("SELECT *").append(" FROM ").append((String) json.get("keyspace")).append(".")
				.append(joinAggTable).append(" where ").append(aggKeyName + " = ").append(aggKeyValue).append(";");

		Row theRow = null;
		try {
			Session session = currentCluster.connect();
			theRow = session.execute(selectQuery1.toString()).one();
		} catch (Exception e) {
			e.printStackTrace();
		}

		if(theRow.getInt("count")==1){

			if(joinAggTable.contains("inner"))
				stream.setInnerJoinAggGroupByDeleteOldRow(theRow);
			else
				stream.setLeftOrRightJoinAggGroupByDeleteRow(theRow);

			Utils.deleteEntireRowWithPK((String) json.get("keyspace"), joinAggTable, aggKeyName, aggKeyValue);
		}else{


			if(joinAggTable.contains("inner"))
				stream.setInnerJoinAggGroupByOldRow(theRow);
			else
				stream.setLeftOrRightJoinAggGroupByOldRow(theRow);


			Float sum = theRow.getFloat("sum");
			sum -= Float.parseFloat(aggColValue);

			int count = theRow.getInt("count");
			count--;

			Float avg = sum / (float) count;

			float min = Float.MAX_VALUE;
			float max = -Float.MAX_VALUE;

			myList.addAll(theRow.getList("list_item", Float.class));
			myList.remove(Float.parseFloat(aggColValue));

			for(int i=0;i<myList.size();i++){
				if(myList.get(i)<min){
					min = myList.get(i);
				}

				if(myList.get(i)>max){
					max =myList.get(i);
				}
			}


			if (!joinAggTable.equals("false")) {

//				StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ").append((String) json.get("keyspace"))
//						.append(".").append(joinAggTable).append(" ( ")
//						.append(aggKeyName + ", ").append("list_item, sum, count, average, min, max")
//						.append(") VALUES (")
//						.append(aggKeyValue + ", ").append(myList+", ").append(sum).append(", ").append(count).append(", ")
//						.append(avg).append(", ").append(min).append(", ").append(max).append(");");
//
//				System.out.println(insertQueryAgg);
//
//				try {
//					Session session = currentCluster.connect();
//					session.execute(insertQueryAgg.toString());
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
				
				
				
				if(!updateStatement(sum, count, avg, min, max, myList, aggKeyName, aggKeyValue, joinAggTable, json, theRow.getFloat("sum")))
					return false;

				if(joinAggTable.contains("inner"))
					stream.setInnerJoinAggGroupByUpdatedOldRow(selectStatement(joinAggTable, aggKeyName, aggKeyValue, json));
				else
					stream.setLeftOrRightJoinAggGroupByUpdatedOldRow(selectStatement(joinAggTable, aggKeyName, aggKeyValue, json));


			}
		}
		
		return true;

	}

	public static void addKeytoInnerAggJoinGroupBy(Stream stream,Row deltaUpdatedRow, String leftJoinAggTable,JSONObject json, String aggColValue, String aggColName,int index,Row newRJRow, String innerJoinAggTable,String aggKey,String aggKeyValue){

		float sum = 0 ;
		int count = 0 ;
		float avg = 0 ;
		float min = 0 ;
		float max = 0 ;
		List<Float> myList = new ArrayList<Float>();

		if (!leftJoinAggTable.equals("false")) {

			StringBuilder selectQuery1 = new StringBuilder("SELECT ")
			.append("list_item").append(", sum, ")
			.append("count, ").append("average, min, max ");
			selectQuery1.append(" FROM ")
			.append((String) json.get("keyspace")).append(".")
			.append(leftJoinAggTable).append(" where ")
			.append(aggKey + " = ").append(aggKeyValue)
			.append(";");

			Row theRow = null;
			try {
				Session session = currentCluster.connect();
				theRow = session.execute(selectQuery1.toString()).one();
			} catch (Exception e) {
				e.printStackTrace();

			}

			myList.addAll(theRow.getList("list_item", Float.class));
			sum = theRow.getFloat("sum");
			count = theRow.getInt("count");
			avg = sum / (float) count;
			min = theRow.getFloat("min");
			max = theRow.getFloat("max");

		} else {

			sum = 0;
			count = 0;
			min = Float.MAX_VALUE;
			max = Float.MIN_VALUE;

			List<Definition> def = deltaUpdatedRow.getColumnDefinitions().asList();

			Map<String, String> map1 = newRJRow.getMap("list_item1",String.class, String.class);

			for (Map.Entry<String, String> entry : map1.entrySet()) {

				String list = entry.getValue().replaceAll("\\[", "").replaceAll("\\]", "");
				String[] listArray = list.split(",");
				Float x = Float.parseFloat(listArray[index]); // if
				myList.add(x);
				if (x > max)
					max = x;
				if (x < min)
					min = x;
				count++;
				sum += x;
			}
		}

		StringBuilder insertQueryAgg = new StringBuilder(
				"INSERT INTO ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(innerJoinAggTable)
		.append(" ( ").append(aggKey + ", ").append("list_item, ")
		.append("sum, count, average, min, max")
		.append(") VALUES (").append(aggKeyValue + ", ").append(myList+", ")
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

		stream.setInnerJoinAggGroupByNewRow(selectStatement(innerJoinAggTable, aggKey, aggKeyValue, json));


	}

	public static void addListItem1toInnerJoinGroupBy(Stream stream,Row deltaUpdatedRow,String aggColName, String leftJoinAggTable, Row newRJRow, int index,
			String aggKeyType, String aggKeyName, JSONObject json,
			String innerJoinAggTable, int aggKeyIndex) {


		Map<String,String> temp= newRJRow.getMap("list_item1", String.class, String.class);

		for (Map.Entry<String, String> entry : temp.entrySet()) {

			String list = entry.getValue().replaceAll("\\[", "").replaceAll("\\]", "");
			String[] listArray = list.split(",");
			String aggColValue = listArray[index];
			String aggKeyValue = listArray[aggKeyIndex];
			addKeytoInnerAggJoinGroupBy(stream,deltaUpdatedRow,leftJoinAggTable, json, aggColValue, aggColName, aggKeyIndex, newRJRow, innerJoinAggTable, aggKeyName, aggKeyValue);

		}

	}

	public static void JoinAggGroupByChangeinAggKUpdateOldRow(JSONObject json, String leftJoinAggTable, String aggKey, String aggKeyValue, String aggColValue, String oldAggColValue, String oldAggKeyValue, String innerJoinAggTable) {


		List<Float> myList = new ArrayList<Float>();

		StringBuilder selectQuery1 = new StringBuilder("SELECT ").append("list_item")
				.append(", sum, count,average, min, max ").append(" FROM ").append((String) json.get("keyspace")).append(".")
				.append(leftJoinAggTable).append(" where ").append(aggKey + " = ").append(oldAggKeyValue).append(";");

		Row theRow = null;
		try {
			Session session = currentCluster.connect();
			theRow = session.execute(selectQuery1.toString()).one();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Float sum = theRow.getFloat("sum");
		sum -= Float.parseFloat(oldAggColValue);

		int count = theRow.getInt("count");
		count--;

		Float avg = sum / (float) count;

		float min = Float.MAX_VALUE;
		float max = -Float.MAX_VALUE;

		for(int i=0;i<myList.size();i++){
			if(myList.get(i)<min){
				min =myList.get(i);
			}

			if(myList.get(i)>max){
				max = myList.get(i);
			}
		}

		myList.addAll(theRow.getList("list_item", Float.class));
		myList.remove(oldAggColValue);

		// update thw row with this join/aggkey in left join agg, if
		// exits
		if (!leftJoinAggTable.equals("false")) {

			StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ").append((String) json.get("keyspace"))
					.append(".").append(leftJoinAggTable).append(" ( ")
					.append(aggKey + ", ").append("list_item, sum, count, average, min, max")
					.append(") VALUES (")
					.append(oldAggKeyValue + ", ").append(myList+", ").append(sum).append(", ").append(count).append(", ")
					.append(avg).append(", ").append(min).append(", ").append(max).append(");");

			System.out.println(insertQueryAgg);

			try {
				Session session = currentCluster.connect();
				session.execute(insertQueryAgg.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		// remove this key inner join aggs, if they exist
		if (!innerJoinAggTable.equals("false")) {

			StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
			insertQueryAgg.append((String) json.get("keyspace")).append(".").append(innerJoinAggTable)
			.append(" ( ").append(aggKey + ", list_item, ").append("sum, count, average, min, max").append(") VALUES (")
			.append(oldAggKeyValue + ", ").append(myList+", ").append(sum).append(", ").append(count).append(", ")
			.append(avg).append(", ").append(min).append(", ").append(max).append(");");

			System.out.println(insertQueryAgg);

			try {
				Session session = currentCluster.connect();
				session.execute(insertQueryAgg.toString());
			} catch (Exception e) {
				e.printStackTrace();

			}
		}

	}

	public static boolean JoinAggGroupByChangeAddRow(Stream stream, JSONObject json, String joinTable, String aggKey, String aggKeyValue, String aggColValue, String oldAggColValue,String oldAggKeyValue){

		float sum = 0;
		float min = 0;
		float max = 0;
		int count = 0;
		float average = 0;
		List<Float> myList = new ArrayList<Float>();


		StringBuilder selectQuery1 = new StringBuilder("SELECT ")
		.append("list_item, sum, count, average, min, max FROM ")
		.append((String) json.get("keyspace")).append(".").append(joinTable)
		.append(" WHERE ").append(aggKey + " = ").append(aggKeyValue).append(";");

		Row theRow = null;
		try {
			Session session = currentCluster.connect();
			theRow = session.execute(
					selectQuery1.toString()).one();
		} catch (Exception e) {
			e.printStackTrace();

		}

		
		//First Insertion
		if(theRow==null){
			if(!aggColValue.equals("'null'") && !aggColValue.equals("null") ){
				sum = Float.valueOf(aggColValue);
				min = Float.valueOf(aggColValue);
				max = Float.valueOf(aggColValue);
				count = 1;
				average = Float.valueOf(aggColValue);
				myList.add(Float.valueOf(aggColValue));
			}
			
			
			StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
			insertQueryAgg
			.append((String) json.get("keyspace"))
			.append(".").append(joinTable).append(" ( ").append(aggKey + ", ").append("list_item, sum, count, average, min, max").append(") VALUES (")
			.append(aggKeyValue + ", ").append(myList+", ").append(sum).append(", ").append(count).append(", ")
			.append(average).append(", ").append(min).append(", ").append(max).append(");");
	
			System.out.println(insertQueryAgg);
	
			try {
				Session session = currentCluster.connect();
				session.execute(insertQueryAgg.toString());
			} catch (Exception e) {
				e.printStackTrace();
	
			}
		}else{
			//Update
			myList.addAll(theRow.getList("list_item", Float.class));
			sum = theRow.getFloat("sum");
			count = theRow.getInt("count");

			if((oldAggColValue.equals("'null'")|| oldAggColValue.equals("null"))|| (!aggKeyValue.equals(oldAggKeyValue)) ){
				if(!aggColValue.equals("'null'") && !aggColValue.equals("null")){
					count++;								
					sum += Float.parseFloat(aggColValue);
					average = sum/count;
					myList.add(Float.parseFloat(aggColValue));
				}
			}

			if(!oldAggColValue.equals("'null'") && !oldAggColValue.equals("null") && aggKeyValue.equals(oldAggKeyValue)){
				if(!aggColValue.equals("'null'") && !aggColValue.equals("null")){
					sum+=Float.parseFloat(aggColValue);
					sum-=Float.parseFloat(oldAggColValue);
					myList.remove(Float.parseFloat(oldAggColValue));
					myList.add(Float.parseFloat(aggColValue));
					average = sum/count;
				}
			}


			min = Float.MAX_VALUE;
			max = -Float.MAX_VALUE;

			for(int i=0;i<myList.size();i++){
				if(myList.get(i)<min){
					min = myList.get(i);
				}

				if(myList.get(i)>max){
					max = myList.get(i);
				}
			}
			
			
			if(!updateStatement(sum, count, average, min, max, myList, aggKey, aggKeyValue, joinTable, json, theRow.getFloat("sum")))
				return false;
			
		}

//		StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
//		insertQueryAgg
//		.append((String) json.get("keyspace"))
//		.append(".").append(joinTable).append(" ( ").append(aggKey + ", ").append("list_item, sum, count, average, min, max").append(") VALUES (")
//		.append(aggKeyValue + ", ").append(myList+", ").append(sum).append(", ").append(count).append(", ")
//		.append(average).append(", ").append(min).append(", ").append(max).append(");");
//
//		System.out.println(insertQueryAgg);
//
//		try {
//			Session session = currentCluster.connect();
//			session.execute(insertQueryAgg.toString());
//		} catch (Exception e) {
//			e.printStackTrace();
//
//		}

	
		if(joinTable.contains("inner"))
			stream.setInnerJoinAggGroupByNewRow(selectStatement(joinTable, aggKey, aggKeyValue, json));
		else
			stream.setLeftOrRightJoinAggGroupByNewRow(selectStatement(joinTable, aggKey, aggKeyValue, json));
		
		return true;

	}

	public static void addListItem2toInnerJoinGroupBy(Stream stream,Row deltaUpdatedRow, String aggColName,
			String rightJoinAggTable, Row newRJRow, int index, String keyType,
			String aggKeyName, JSONObject json, String innerJoinAggTable,
			int aggKeyIndex) {


		Map<String,String> temp= newRJRow.getMap("list_item2", String.class, String.class);

		for (Map.Entry<String, String> entry : temp.entrySet()) {

			String list = entry.getValue().replaceAll("\\[", "").replaceAll("\\]", "");
			String[] listArray = list.split(",");
			String aggColValue = listArray[index];
			String aggKeyValue = listArray[aggKeyIndex];
			addKeytoInnerAggJoinGroupBy(stream,deltaUpdatedRow,rightJoinAggTable, json, aggColValue, aggColName, aggKeyIndex, newRJRow, innerJoinAggTable, aggKeyName, aggKeyValue);

		}


	}

	public static boolean deleteElementFromRow(Stream stream, JSONObject json, String joinTable, String aggKey, String aggKeyValue, String aggColValue){

		float sum = 0;
		float min = 0;
		float max = 0;
		int count = 0;
		float average = 0;
		List<Float> myList = new ArrayList<Float>();


		StringBuilder selectQuery1 = new StringBuilder("SELECT ")
		.append("list_item, sum, count, average, min, max FROM ")
		.append((String) json.get("keyspace")).append(".").append(joinTable)
		.append(" WHERE ").append(aggKey + " = ").append(aggKeyValue).append(";");

		Row theRow = null;
		try {
			Session session = currentCluster.connect();
			theRow = session.execute(
					selectQuery1.toString()).one();
		} catch (Exception e) {
			e.printStackTrace();

		}


		//Update
		myList.addAll(theRow.getList("list_item", Float.class));
		sum = theRow.getFloat("sum");
		count = theRow.getInt("count");

		count --;
		sum-=Float.parseFloat(aggColValue);
		myList.remove(Float.parseFloat(aggColValue));

		average = sum/count;


		min = Float.MAX_VALUE;
		max = -Float.MAX_VALUE;

		for(int i=0;i<myList.size();i++){
			if(myList.get(i)<min){
				min = myList.get(i);
			}

			if(myList.get(i)>max){
				max = myList.get(i);
			}
		}

//		StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
//		insertQueryAgg
//		.append((String) json.get("keyspace"))
//		.append(".").append(joinTable).append(" ( ").append(aggKey + ", ").append("list_item, sum, count, average, min, max").append(") VALUES (")
//		.append(aggKeyValue + ", ").append(myList+", ").append(sum).append(", ").append(count).append(", ")
//		.append(average).append(", ").append(min).append(", ").append(max).append(");");
//
//		System.out.println(insertQueryAgg);
//
//		try {
//			Session session = currentCluster.connect();
//			session.execute(insertQueryAgg.toString());
//		} catch (Exception e) {
//			e.printStackTrace();
//
//		}
		
		if(!updateStatement(sum, count, average, min, max, myList, aggKey, aggKeyValue, joinTable, json, theRow.getFloat("sum")))
			return false;

		if(joinTable.contains("inner"))
			stream.setInnerJoinAggGroupByNewRow(selectStatement(joinTable, aggKey, aggKeyValue, json));
		else
			stream.setLeftOrRightJoinAggGroupByNewRow(selectStatement(joinTable, aggKey, aggKeyValue, json));
		
		return true;

	}


}
