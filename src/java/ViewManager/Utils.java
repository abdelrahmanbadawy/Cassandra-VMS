package ViewManager;

import java.io.File;
import java.io.FilenameFilter;
import java.math.BigInteger;
import java.util.HashMap;

import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class Utils {

	/*static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
					.getString("config.host.localhost"))
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();*/

	
	public static boolean deleteEntireRowWithPK(Session session,String keyspace, String tableName,
			String pk, String pkValue,int count,float sum) {
	
		Row updated;
		
		StringBuilder deleteQuery = new StringBuilder("delete from ");
		deleteQuery.append(keyspace).append(".").append(tableName)
		.append(" WHERE ").append(pk + " = ").append(pkValue)
		.append(" IF count = ").append(count)
		.append(" and sum = ").append(sum)
		.append(";");

		System.out.println(deleteQuery.toString());
		try {

			//Session session = currentCluster.connect();
			updated = session.execute(deleteQuery.toString()).one();
			//session.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		if(updated.getBool("[applied]"))
			return true;
		else
			return false;
	}
	
	
	
	//delete row from a table with primary key
	public static boolean deleteEntireRowWithPK(Session session,String keyspace, String tableName,
			String pk, String pkValue) {

		StringBuilder deleteQuery = new StringBuilder("delete from ");
		deleteQuery.append(keyspace).append(".").append(tableName)
		.append(" WHERE ").append(pk + " = ").append(pkValue)
		.append(";");

		System.out.println(deleteQuery.toString());
		try {

			//Session session = currentCluster.connect();
			session.execute(deleteQuery.toString());
			//session.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}
	
	public static boolean deleteEntireRowWithPK(Session session,String keyspace, String tableName,
			String pk, String pkValue, int counter) {

		StringBuilder deleteQuery = new StringBuilder("delete from ");
		deleteQuery.append(keyspace).append(".").append(tableName)
		.append(" WHERE ").append(pk + " = ").append(pkValue)
		.append(" IF counter = ").append(counter).append(";");

		System.out.println(deleteQuery.toString());
		try {

		//	Session session = currentCluster.connect();
			session.execute(deleteQuery.toString());
		//	session.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static Row selectAllStatement(Session session, String keyspace,String table,String pk,String pkValue ){

		StringBuilder selectQuery1 = new StringBuilder("SELECT * ")
		.append(" FROM ").append(keyspace).append(".")
		.append(table).append(" WHERE ")
		.append(pk + " = ")
		.append(pkValue + " ;");

		System.out.println(selectQuery1);

		ResultSet rs = null;
		try {
			//Session session = currentCluster.connect();
			rs = session.execute(selectQuery1.toString());
			//session.close();
		} catch (Exception e) {
			e.printStackTrace();

		}

		return rs.one();
	}


	public static ResultSet selectStatement(Session session,String selectColNames,String keyspace,String table, String pk, String pkValue){

		StringBuilder selectQuery = new StringBuilder("SELECT ")
		.append(selectColNames);
		selectQuery.append(" FROM ").append(keyspace).append(".")
		.append("delta_" + table).append(" WHERE ")
		.append(pk + " = ")
		.append(pkValue + " ;");

		System.out.println(selectQuery);

		ResultSet result = null;

		try {
		//	Session session = currentCluster.connect();
			result = session.execute(selectQuery.toString());
		//	session.close();
		} catch (Exception e) {
			e.printStackTrace();

		}

		return result;
	}



	public static void insertStatement(Session s,String keyspace,String table, String colNames, String colValues){

		StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
		insertQueryAgg.append(keyspace).append(".")
		.append(table).append(" ( ").append(colNames).append(") VALUES (").append(colValues).append(" );");

		System.out.println(insertQueryAgg);



		try {
			//Session session = currentCluster.connect();
			s.execute(insertQueryAgg.toString());
			//session.close();
		} catch (Exception e) {
			e.printStackTrace();
		}


	}


	//evaluates a condition
	public static boolean evaluateCondition(CustomizedRow row, String operation, String value,
			String type, String colName) {

		boolean eval = true;

		/*if (row.isNull(colName)) {
			return false;
		}*/

		switch (type) {

		case "text":

			if (operation.equals("=")) {

				if(row.isNull(colName)){
					eval = false;
				}else if (row.getString(colName).equals(value)) {
					eval &= true;
				} else {
					eval &= false;
				}

			} else if (operation.equals("!=")) {

				if(row.isNull(colName)){
					eval = false;
				}else if (!row.getString(colName).equals(value)) {
					eval = true;
				} else {
					eval = false;
				}

			}

			break;

		case "varchar":

			if (operation.equals("=")) {

				if(row.isNull(colName)){
					eval = false;
				}else if (row.getString(colName).equals(value)) {
					eval &= true;
				} else {
					eval &= false;
				}

			} else if (operation.equals("!=")) {
				if (!row.getString(colName).equals(value)) {
					eval &= true;
				} else {
					eval &= false;
				}

			}

			break;

		case "int":

			// for _new col

			if(row.isNull(colName)){
				eval = false;
				return eval;
			}

			String s1 = Integer.toString(row.getInt(colName));
			Integer valueInt = new Integer(s1);
			int compareValue = valueInt.compareTo(new Integer(value));

			if ((operation.equals(">") && (compareValue > 0))) {
				eval &= true;
			} else if ((operation.equals("<") && (compareValue < 0))) {
				eval &= true;
			} else if ((operation.equals("=") && (compareValue == 0))) {
				eval &= true;
			} else {
				eval &= false;
			}

			break;

		case "varint":

			if(row.isNull(colName)){
				eval = false;
				return eval;
			}

			// for _new col
			s1 = row.getVarint(colName).toString();
			valueInt = new Integer(new BigInteger(s1).intValue());
			compareValue = valueInt.compareTo(new Integer(value));

			if ((operation.equals(">") && (compareValue > 0))) {
				eval &= true;
			} else if ((operation.equals("<") && (compareValue < 0))) {
				eval &= true;
			} else if ((operation.equals("=") && (compareValue == 0))) {
				eval &= true;
			} else {
				eval &= false;
			}

			break;

		case "float":

			if(row.isNull(colName)){
				eval = false;
				return eval;
			}

			compareValue = Float.compare(row.getFloat(colName),
					Float.valueOf(value));

			if ((operation.equals(">") && (compareValue > 0))) {
				eval &= true;
			} else if ((operation.equals("<") && (compareValue < 0))) {
				eval &= true;
			} else if ((operation.equals("=") && (compareValue == 0))) {
				eval &= true;
			} else {
				eval &= false;
			}

			break;
		}

		return eval;

	}

	public static String getColumnValueFromDeltaStream(CustomizedRow stream, String name,String type, String suffix){

		String value = "";

		switch (type) {

		case "text":
			if(stream.isNull(name + suffix)){
				value = "null";
			}else{
				if(stream.getString(name + suffix).trim().startsWith("'") && stream.getString(name + suffix).trim().endsWith("'"))
					value = stream.getString(name + suffix);
				else
					value = ("'"+ stream.getString(name + suffix) + "'");
			}	
			break;

		case "int":
			if(stream.isNull(name + suffix)){
				value = "null";
			}else {
				value = ("" + stream.getInt(name + suffix));
			}
			break;

		case "varint":
			if(stream.isNull(name + suffix)){
				value = "null";
			}else{
				value = ("" + stream.getVarint(name + suffix));
			}
			break;

		case "varchar":
			if(stream.isNull(name + suffix)){
				value = "null";
			}else{
				if(stream.getString(name + suffix).trim().startsWith("'") && stream.getString(name + suffix).trim().endsWith("'"))
					value = stream.getString(name + suffix);
				else
					value = ("'"+ stream.getString(name + suffix) + "'");
			}
			break;

		case "float":
			if(stream.isNull(name + suffix)){
				value = "null";
			}else{
				value = ("" + stream.getFloat(name + suffix));
			}	
			break;

		}

		return value;

	}

	public static boolean evalueJoinAggConditions(CustomizedRow row, String aggFct, String operation, String value){

		boolean eval = true;
		float min1 = 0;
		float max1 = 0;
		float average1 = 0;
		float sum1 = 0;
		int count1 = 0;

		if (row != null) {
			min1 = row.getFloat("min");
			max1 = row.getFloat("max");
			average1 = row.getFloat("average");
			sum1 = row.getFloat("sum");
			count1 = row.getInt("count");
		}

		if (aggFct.equals("sum")) {

			if (row != null) {

				int compareValue = new Float(sum1)
				.compareTo(new Float(value));

				if ((operation.equals(">") && (compareValue > 0))) {
					eval &= true;
				} else if ((operation.equals("<") && (compareValue < 0))) {
					eval &= true;
				} else if ((operation.equals("=") && (compareValue == 0))) {
					eval &= true;
				} else {
					eval &= false;
				}
			}

		} else if (aggFct.equals("average")) {

			if (row != null) {

				int compareValue = Float.compare(average1,
						Float.valueOf(value));

				if ((operation.equals(">") && (compareValue > 0))) {
					eval &= true;
				} else if ((operation.equals("<") && (compareValue < 0))) {
					eval &= true;
				} else if ((operation.equals("=") && (compareValue == 0))) {
					eval &= true;
				} else {
					eval &= false;
				}

			}

		} else if (aggFct.equals("count")) {

			if (row != null) {

				int compareValue = new Integer(count1)
				.compareTo(new Integer(value));

				if ((operation.equals(">") && (compareValue > 0))) {
					eval &= true;
				} else if ((operation.equals("<") && (compareValue < 0))) {
					eval &= true;
				} else if ((operation.equals("=") && (compareValue == 0))) {
					eval &= true;
				} else {
					eval &= false;
				}

			}

		} else if (aggFct.equals("min")) {

			if (row != null) {

				int compareValue = Float.compare(min1,
						Float.valueOf(value));

				if ((operation.equals(">") && (compareValue > 0))) {
					eval &= true;
				} else if ((operation.equals("<") && (compareValue < 0))) {
					eval &= true;
				} else if ((operation.equals("=") && (compareValue == 0))) {
					eval &= true;
				} else {
					eval &= false;
				}

			}

		} else if (aggFct.equals("max")) {

			if (row != null) {
				int compareValue = Float.compare(max1,
						Float.valueOf(value));

				if ((operation.equals(">") && (compareValue > 0))) {
					eval &= true;
				} else if ((operation.equals("<") && (compareValue < 0))) {
					eval &= true;
				} else if ((operation.equals("=") && (compareValue == 0))) {
					eval &= true;
				} else {
					eval &= false;
				}
			}
		}

		return eval;
	}
	
	public static boolean updateSignature(Session session,String rowKey, String keyValue,
			String table, JSONObject json, String mapKey, String value){

		StringBuilder updateQuery = new StringBuilder("UPDATE ");
		updateQuery.append((String) json.get("keyspace"))
		.append(".").append(table).append(" SET signature['").append(mapKey).append("']= '").append(value)
		.append("' WHERE ").append(rowKey).append(" = ").append(keyValue)
		.append(";");


		System.out.println(updateQuery);

		
		try {

			//Session session = currentCluster.connect();
			session.execute(updateQuery.toString()).one();
			//session.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;

	}
	
	public static File[] getFilesInDirectory(){
		File folder = new File("logs/");
		
		
		File [] listOfFiles = folder.listFiles(new FilenameFilter() {
		    @Override
		    public boolean accept(File dir, String name) {
		        return name.endsWith(".log") && name.startsWith("output");
		    }
		});  
		
		return listOfFiles;
	}
}
