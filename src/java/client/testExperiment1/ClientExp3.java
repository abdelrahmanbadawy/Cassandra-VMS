package client.testExperiment1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class ClientExp3 {

	
	static Cluster currentCluster = null;
	
	static int clients_number;
	
	List<Cluster> clusters;
	static List<Session> sessions;
	
	String [] ips;
	static HashMap<String, String> map;
	
	
	public ClientExp3(int n,  String [] ips) {
		// TODO Auto-generated constructor stub
		
		 map = new HashMap<String, String>();
		
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
			int h = 1;

			while ((line = br.readLine()) != null && h <= 5000) {

				String[] columns = line.split(",");
				
				//map.put(columns[3], "");

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

				h++;
				counter ++;
				
				if(counter == clients_number)
					counter =0;
				
			}
			
			while ((line = br.readLine()) != null && h < 8000) {
				
				System.out.println(line);

				String[] columns = line.split(",");
				
				//map.put(columns[3], "");
				
				String insertQuery =  " UPDATE  "
						+ keyspace.get(indexFileName) + "." + table
						+ " SET " + "name = " + columns[1] +", faculty = "+columns[3]+", ects = "
						+ columns[2]+" WHERE id = "+columns[0]+";";

				System.out.println(insertQuery);

				try {

					sessions.get(counter).execute(insertQuery
							.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}

				h++;
				counter ++;
				
				if(counter == clients_number)
					counter =0;
				
			}
			while ((line = br.readLine()) != null && h < 10000) {

				String[] columns = line.split(",");
				
				//map.put(columns[3], "");

				String insertQuery = "delete from "+ keyspace.get(indexFileName) + "." + table+
						" where id = "+ columns[0]+";";

				System.out.println(insertQuery);

				try {

					sessions.get(counter).execute(insertQuery
							.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}

				h++;
				counter ++;
				
				if(counter == clients_number)
					counter =0;
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(map.size());
	}

	
	public static void main(String [] args){
		
		//String [] ips ={"192.168.56.101"};
		//String [] ips ={"141.40.254.16"};
		//String [] ips ={"10.155.208.27","10.155.208.67","10.155.208.91"};
		String [] ips ={"10.155.208.67",
				"10.155.208.91", "10.155.208.83","10.155.208.95"};
		
		ClientExp3 c = new ClientExp3(1,  ips);
		c.insertBaseTable("courses", "src/java/client/data/ex3-data/ex3-courses-agg-"+args[0]+".csv" );
		//c.insertBaseTable("student", "src/java/client/data/ex1-student.csv" );
	
		
	}
	
}
