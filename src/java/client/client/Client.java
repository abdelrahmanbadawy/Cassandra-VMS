package client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.sun.org.apache.xpath.internal.SourceTree;

import java.util.List;


public class Client {
	
	
	
	Client(){
		
	}

	public static Cluster connectToCluster(String ipAddress) {

		Cluster cluster = null;

		try{
			cluster = Cluster.builder()
					.addContactPoint(ipAddress)
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
					.build();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return cluster;	
	}
	
	public static void disconnectFromCluster(Cluster cluster) {
		try {
			cluster.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

	public static void createKeySpace(String ipAddress,String keyspace){
		
	}
	
		
	/*public static boolean createKeySpace(Cluster cluster, String keyspace) {

		ResultSet selectQueryResults = null;
		Session session = null;

		try {
			session = cluster.connect();
			Select.Where select = QueryBuilder.select()
					.all()
					.from("system", "schema_keyspaces")
					.where(QueryBuilder.eq("keyspace_name", keyspace));

			selectQueryResults = session.execute(select);

			String selectQueryResultString = selectQueryResults.all().toString();

			System.out.println("Results of the Keyspace Select Query = " + selectQueryResultString);
			
			if (selectQueryResultString.length() == 2 && selectQueryResultString.equals("[]")) {
				selectQueryResultString = "";
			}

			System.out.println("ResultString = " + selectQueryResultString);
			
			if (selectQueryResultString == null || "".equalsIgnoreCase(selectQueryResultString)) {
				
				////logger.debug("Creating keyspace {}", keyspace);
				
				String query = "CREATE SCHEMA " +
						keyspace + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
				selectQueryResults = session.execute(query);
				
			} else {
				////logger.debug("Keyspace {} already present", keyspace);
			}

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			session.close();
		}

		return true;
	}

	public static Cluster getConnection(String ip) {

		Cluster cluster = null;
		
		try {
			
			cluster = Cluster.builder()
	                .addContactPoint(ip)
	                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
	                .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
	                .build();
			
		} catch (Exception e) {
			////logger.error("Error occurred in Client| getConnection | " + e);
			e.printStackTrace();
		}
		
		return cluster;
	}

	public static boolean closeConnection(Cluster cluster) {
		try {
			cluster.close();
			////logger.info("Connection is successfully closed!!");
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static boolean createTable(Cluster cluster, Table table) {

		ResultSet results = null;
		Session session = null;

		try {
			session = cluster.connect();
			
			StringBuilder query = new StringBuilder();
			query.append("create table " + table.getKeySpace() + "." + table.getName() + " (");
			
			List<Column> columns = table.getColumns();
			for (Column col : columns) {
				String primaryKey = col.isPrimaryKey() ? " PRIMARY KEY" : "";
				query.append(col.getName() + " " + col.getDataType() + primaryKey + ",");
			}
			
			String finalQuery = query.substring(0, query.length() - 1) + ");";
			System.out.println("Final query = " + finalQuery);
			results = session.execute(finalQuery);

			////logger.debug("Successfully created table {}.{}", table.getKeySpace(), table.getName());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			session.close();
		}

		return true;
	}

	public static boolean deleteTable(Cluster cluster, Table table) {
		
		ResultSet results = null;
		Session session = null;
		
		try {
			session = cluster.connect();
			StringBuilder query = new StringBuilder();
			query.append("delete table " + table.getKeySpace() + "." + table.getName() + ";");

			System.out.println("Final query = " + query);
			results = session.execute(query.toString());

			////logger.debug("Successfully delete table {}.{}", table.getKeySpace(), table.getName());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			session.close();
		}

		return true;
	}

	public static boolean searchTable(Cluster cluster, Table table) {
		
		ResultSet results = null;
		Session session = null;
		
		try {
			session = cluster.connect();
			StringBuilder query = new StringBuilder();
			query.append("Select columnfamily_name from system.schema_columnfamilies where columnfamily_name = '"+ table.getName() +"' ALLOW FILTERING ;");

			System.out.println("Final query = " + query);
			
			results = session.execute(query.toString());
			String resultString = results.all().toString();
			
			////logger.debug("Resultset {}", resultString);
			
			if (resultString.contains(table.getName())) {
				return true;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			session.close();
		}

		return false;
	}

	public static boolean commandExecution(Cluster cluster, String query){
		ResultSet results = null;
		Session session = null;
		
		try {
			session = cluster.connect();
			System.out.println("Final query = " + query);
			
			results = session.execute(query);
			String resultString = results.all().toString();
			
			////logger.debug("Resultset {}", resultString);

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			session.close();
		}

		return true;
	}*/

}
