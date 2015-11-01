package client.testExperiment1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class ClientExp1 {

	
	static Cluster currentCluster = null;
	
	static int clients_number;
	
	List<Cluster> clusters;
	static List<Session> sessions;
	
	String [] ips;
	
	public ClientExp1(int n,  String [] ips) {
		// TODO Auto-generated constructor stub
		
		this.ips = ips;
		
		
		
		clients_number = n;
		
		clusters = new ArrayList<Cluster>();
		sessions = new ArrayList<Session>();
		
		for(int i=0; i<n ; i++){
			Cluster cluster = Cluster
					.builder()
					.addContactPoint(ips[i])
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();
			
			Session session = cluster.connect();
			
			
			
			clusters.add(cluster);
			sessions.add(session);
		}
		
		
		
	}
	
	public static void connectToCluster(String ipAddress) {

		Cluster cluster = null;

		try {
			cluster = Cluster
					.builder()
					.addContactPoint(ipAddress)
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();
		} catch (Exception e) {
			e.printStackTrace();
		}

		currentCluster = cluster;

	}
	
	public static void insertBaseTable(String table, String filePath ) {

		BufferedReader br;
		String line;
		

		

		List<String> keyspace = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.keyspace");
		List<String> schemaList = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.name");
		int indexFileName = schemaList.indexOf(table);
		List<String> nrColumns = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.columnNumber");
		int cursorColName = 0;
		for (int i = 0; i < indexFileName; i++) {
			cursorColName += Integer.parseInt(nrColumns.get(i));
		}
		List<String> colName = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.column.name");
		List<String> primarykeyName = XmlHandler.getInstance()
				.getDatabaseConfig()
				.getList("dbSchema.tableDefinition.primaryKey.name");

		String tempInsertQuery = " (";
		tempInsertQuery += primarykeyName.get(indexFileName) + ", ";

		for (int i = 1; i <= Integer.parseInt(nrColumns.get(indexFileName)); i++) {
			tempInsertQuery += colName.get(cursorColName + (i - 1)) + ", ";
		}
		tempInsertQuery = tempInsertQuery.substring(0,
				tempInsertQuery.length() - 2);
		tempInsertQuery += ")";

		try {

			br = new BufferedReader(new FileReader(filePath));
			int counter = 0;

			while ((line = br.readLine()) != null) {

				String[] columns = line.split(",");

				String insertQuery = "insert into "
						+ keyspace.get(indexFileName) + "." + table
						+ tempInsertQuery + " VALUES (";

				for (int i = 0; i < columns.length; i++) {
					insertQuery += columns[i] + ", ";
				}

				insertQuery = insertQuery
						.substring(0, insertQuery.length() - 2);
				insertQuery += ");";

				System.out.println(insertQuery);

				try {

					sessions.get(counter).execute(insertQuery
							.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}

				counter ++;
				
				if(counter == clients_number)
					counter =0;
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	public static void main(String [] args){
		
		String [] ips ={"192.168.56.101"};
		
		ClientExp1 c = new ClientExp1(1,  ips);
		c.insertBaseTable("courses", "src/java/client/data/ex1-courses.csv" );
		c.insertBaseTable("student", "src/java/client/data/ex1-student.csv" );
	
		
	}
	
}
