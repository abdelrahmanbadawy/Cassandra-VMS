package client.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.io.PrintWriter;
import java.util.List;
import java.util.Iterator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class Client {

	static Cluster currentCluster = null;

	private static XMLConfiguration empData;
	private static XMLConfiguration studentData;
	public String currentDataFile;

	public Client() {

		empData = new XMLConfiguration();
		empData.setDelimiterParsingDisabled(true);
		try {
			empData.load("client/data/emp.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

		studentData = new XMLConfiguration();
		studentData.setDelimiterParsingDisabled(true);
		try {
			studentData.load("client/data/emp.xml"); // change
		} catch (ConfigurationException e) {
			e.printStackTrace();
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

	public static void disconnectFromCluster(Cluster cluster) {

		try {
			cluster.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void disconnectFromCluster() {

		try {
			currentCluster.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean clusterIsConnected(Cluster cluster) {

		if (cluster.isClosed()) {
			return false;
		}

		return true;
	}

	public static Cluster getClusterInstance() {
		return currentCluster;
	}

	public static boolean createKeySpace(String keyspace) {

		Session session = currentCluster.connect();

		StringBuilder queryString = new StringBuilder();
		queryString
				.append("CREATE KEYSPACE IF NOT EXISTS ")
				.append(keyspace)
				.append(" WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");

		System.out.println(queryString.toString());
		ResultSet queryResults = session.execute(queryString.toString());

		return true;
	}

	public static boolean createTable() {

		List<String> keyspace = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.keyspace");
		List<String> tableName = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.name");
		Integer nrTables = XmlHandler.getInstance().getDatabaseConfig()
				.getInt("dbSchema.tableNumber");
		List<String> primarykeyType =XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.primaryKey.type");
		List<String> primarykeyName =XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.primaryKey.name");
		List<String> nrColumns = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.columnNumber");
		List<String> colFamily = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.column.family");
		List<String> colName = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.column.name");
		List<String> colType = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.column.type");

		int cursor = 0;
		
		for (int i = 0; i < nrTables; i++) {
			StringBuilder createQuery = new StringBuilder();
			createQuery.append("CREATE TABLE IF NOT EXISTS  ")
					.append(keyspace.get(i)).append(".")
					.append(tableName.get(i) + "(")
					.append(primarykeyName.get(i) + " ")
					.append(primarykeyType.get(i)).append(" PRIMARY KEY,");

			for (int j = 0; j < Integer.parseInt(nrColumns.get(i)); j++) {
				createQuery.append(colName.get(cursor+j) + " ").append(
						colType.get(j) + ",");
			}

			cursor+=Integer.parseInt(nrColumns.get(i));  
			
			createQuery.deleteCharAt(createQuery.length() - 1);
			createQuery.append(");");

			Session session = null;

			System.out.println(createQuery.toString());

			try {
				session = currentCluster.connect();
				ResultSet queryResults = session
						.execute(createQuery.toString());
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

		return true;
	}

	public void insertBaseTable(int fileCursor, int pk) {

		XMLConfiguration dataHandel = new XMLConfiguration();

		if (currentDataFile == "emp") {
			dataHandel = empData;
		} else if (currentDataFile == "student") {
			dataHandel = studentData;
		}

		currentDataFile = "emp";
		Session session = null;

		String insertQuery = "insert into test." + currentDataFile
				+ " (id, name, phone,salary)" + " VALUES (" + pk
				+ ", 'sara', 012425262, 4000);";
		System.out.println(insertQuery);

		try {
			session = currentCluster.connect();
			ResultSet queryResults = session.execute(insertQuery.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * This method creates the select view tables and inserts the data
	 * accordingly
	 */
	public static boolean createViewTable() {

		List<String> keyspace = XmlHandler.getInstance().getViewConfig()
				.getList("dbSchema.tableDefinition.keyspace");
		List<String> tableName = XmlHandler.getInstance().getViewConfig()
				.getList("dbSchema.tableDefinition.name");
		Integer nrTables = XmlHandler.getInstance().getViewConfig()
				.getInt("dbSchema.tableNumber");
		List<String> primarykeyType = XmlHandler.getInstance().getViewConfig()
				.getList("dbSchema.tableDefinition.primaryKey.type");
		List<String> primarykeyName = XmlHandler.getInstance().getViewConfig()
				.getList("dbSchema.tableDefinition.primaryKey.name");
		Integer nrColumns = XmlHandler.getInstance().getViewConfig()
				.getInt("dbSchema.tableDefinition.columnNumber");
		List<String> colFamily = XmlHandler.getInstance().getViewConfig()
				.getList("dbSchema.tableDefinition.column.family");
		List<String> colName = XmlHandler.getInstance().getViewConfig()
				.getList("dbSchema.tableDefinition.column.name");
		List<String> colType = XmlHandler.getInstance().getViewConfig()
				.getList("dbSchema.tableDefinition.column.type");

		for (int i = 0; i < nrTables -1; i++) {
			
			StringBuilder createQuery = new StringBuilder();
			createQuery.append("CREATE TABLE IF NOT EXISTS  ")
					.append(keyspace.get(i)).append(".")
					.append(tableName.get(i) + "(")
					.append(primarykeyName.get(i) + " ")
					.append(primarykeyType.get(i)).append(" PRIMARY KEY,");

			for (int j = 0; j < nrColumns; j++) {
				createQuery.append(colName.get(j) + " ").append(
						colType.get(j) + ",");
			}

			createQuery.deleteCharAt(createQuery.length() - 1);
			createQuery.append(");");

			Session session = null;

			System.out.println(createQuery.toString());

			try {
				session = currentCluster.connect();
				ResultSet queryResults = session
						.execute(createQuery.toString());
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

			StringBuilder selectQuery = new StringBuilder();

			selectQuery.append("SELECT * FROM ").append(keyspace.get(i))
					.append(".emp").append(";");

			try {
				Iterator<Row> queryResults = session.execute(
						selectQuery.toString()).iterator();

				while (queryResults.hasNext()) {

					Row currentRow = queryResults.next();

					//compare if salary = 2000
					if(currentRow.getVarint("salary").intValue()>=2000){
					
					StringBuilder columns = new StringBuilder();
					StringBuilder values = new StringBuilder();

					columns.append(primarykeyName.get(i));
					values.append(currentRow.getInt(primarykeyName.get(i)));

					for (int j = 0; j < nrColumns; j++) {
						columns.append(", ").append(colName.get(j));

						switch (colType.get(j)) {
						case "text":
							values.append(", ").append(
									"'" + currentRow.getString(colName.get(j))
											+ "'");
							break;
						case "int":
							values.append(", ").append(
									currentRow.getInt(colName.get(j)));
							break;
						case "varint":
							values.append(", ").append(
									currentRow.getVarint(colName.get(j)));
							break;

						}

						
					}

					StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
					insertQuery.append(keyspace.get(i)).append(".").append(tableName.get(i)).append(" (").append(columns)
					.append(") VALUES (").append(values).append(");"); 
					
					System.out.println(insertQuery);
					
					session
					.execute(insertQuery.toString());
					
					//clear log file
					PrintWriter writer = new PrintWriter("logs/output.log");
					writer.print("");
					writer.close();
				}
				}

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

		}

		return true;
	}

}
