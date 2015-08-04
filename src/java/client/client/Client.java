package client.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.apache.commons.configuration.ConfigurationFactory;

public class Client {

	static Cluster currentCluster = null;
	static BufferedReader br = null;
	static String csvFile = "src/java/client/data/emp.csv";
	static String csvFile1 = "src/java/client/data/student.csv";
	static String csvFile2 = "src/java/client/data/courses.csv";
	static Iterator<Row> deltaViewQueryResults;

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
		session.execute(queryString.toString());

		return true;
	}

	public static boolean createTable() {

		List<String> keyspace = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.keyspace");
		List<String> tableName = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.name");
		Integer nrTables = XmlHandler.getInstance().getDatabaseConfig()
				.getInt("dbSchema.tableNumber");
		List<String> primarykeyType = XmlHandler.getInstance()
				.getDatabaseConfig()
				.getList("dbSchema.tableDefinition.primaryKey.type");
		List<String> primarykeyName = XmlHandler.getInstance()
				.getDatabaseConfig()
				.getList("dbSchema.tableDefinition.primaryKey.name");
		List<String> clusteringKeyName = XmlHandler.getInstance()
				.getDatabaseConfig()
				.getList("dbSchema.tableDefinition.clusteringKey.name");
		List<String> nrClusteringKeys = XmlHandler
				.getInstance()
				.getDatabaseConfig()
				.getList(
						"dbSchema.tableDefinition.clusteringKey.componentNumber");
		List<String> nrColumns = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.columnNumber");
		List<String> colFamily = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.column.family");
		List<String> colName = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.column.name");
		List<String> colType = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.column.type");

		int cursor = 0;
		int cluseringCursor = 0;

		for (int i = 0; i < nrTables; i++) {
			StringBuilder createQuery = new StringBuilder();
			createQuery.append("CREATE TABLE IF NOT EXISTS  ")
			.append(keyspace.get(i)).append(".")
			.append(tableName.get(i) + "(")
			.append(primarykeyName.get(i) + " ")
			.append(primarykeyType.get(i) + ",");

			for (int j = 0; j < Integer.parseInt(nrColumns.get(i)); j++) {
				createQuery.append(colName.get(cursor + j) + " ").append(
						colType.get(cursor + j) + ",");
			}

			createQuery.append(" PRIMARY KEY (").append(
					primarykeyName.get(i) + ",");

			for (int j = 0; j < Integer.parseInt(nrClusteringKeys.get(i)); j++) {
				createQuery.append(clusteringKeyName.get(cluseringCursor + j)
						+ ",");
			}

			createQuery.deleteCharAt(createQuery.length() - 1);
			createQuery.append("));");

			cursor += Integer.parseInt(nrColumns.get(i));
			cluseringCursor += Integer.parseInt(nrClusteringKeys.get(i));

			Session session = null;

			System.out.println(createQuery.toString());

			try {
				session = currentCluster.connect();
				session.execute(createQuery.toString());
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

		return true;
	}


	public static void insertBaseTable(String fileName) {

		String file = null;
		String line;
		Session session = currentCluster.connect();

		if (fileName.equals("employee")) {
			file = csvFile;
		} else if (fileName.equals("student")) {
			file = csvFile1;
		} else
			file = csvFile2;

		List<String> keyspace = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.keyspace");
		List<String> schemaList = XmlHandler.getInstance().getDatabaseConfig()
				.getList("dbSchema.tableDefinition.name");
		int indexFileName = schemaList.indexOf(fileName);
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

			br = new BufferedReader(new FileReader(file));

			while ((line = br.readLine()) != null) {

				String[] columns = line.split(",");

				String insertQuery = "insert into "
						+ keyspace.get(indexFileName) + "." + fileName
						+ tempInsertQuery + " VALUES (";

				for (int i = 0; i < columns.length; i++) {
					insertQuery += columns[i] + ", ";
				}

				insertQuery = insertQuery
						.substring(0, insertQuery.length() - 2);
				insertQuery += ");";

				System.out.println(insertQuery);

				try {

					session.execute(insertQuery
							.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * This method creates the view tables and inserts the data accordingly
	 */
	public static boolean createViewTable() {

		return  createSelectViewTable() && createDeltaViewTable() && createPreAggregationViewTable() ;
	}


	/**
	 * This method creates the select view tables and inserts the data
	 * accordingly
	 */
	public static boolean createSelectViewTable() {

		List<String> keyspace = XmlHandler.getInstance().getSelectViewConfig()
				.getList("dbSchema.tableDefinition.keyspace");
		List<String> tableName = XmlHandler.getInstance().getSelectViewConfig()
				.getList("dbSchema.tableDefinition.name");
		Integer nrTables = XmlHandler.getInstance().getSelectViewConfig()
				.getInt("dbSchema.tableNumber");
		List<String> primarykeyType = XmlHandler.getInstance()
				.getSelectViewConfig()
				.getList("dbSchema.tableDefinition.primaryKey.type");
		List<String> primarykeyName = XmlHandler.getInstance()
				.getSelectViewConfig()
				.getList("dbSchema.tableDefinition.primaryKey.name");
		Integer nrColumns = XmlHandler.getInstance().getSelectViewConfig()
				.getInt("dbSchema.tableDefinition.columnNumber");

		List<String> colName = XmlHandler.getInstance().getSelectViewConfig()
				.getList("dbSchema.tableDefinition.column.name");
		List<String> colType = XmlHandler.getInstance().getSelectViewConfig()
				.getList("dbSchema.tableDefinition.column.type");
		List<String> baseTable = XmlHandler.getInstance().getSelectViewConfig()
				.getList("dbSchema.tableDefinition.baseTable");
		List<String> conditions = XmlHandler.getInstance()
				.getSelectViewConfig()
				.getList("dbSchema.tableDefinition.condition");

		for (int i = 0; i < nrTables; i++) {

			StringBuilder createQuery = new StringBuilder();
			createQuery.append("CREATE TABLE IF NOT EXISTS  ")
			.append(keyspace.get(i)).append(".")
			.append(tableName.get(i)+ "(")
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

		}

		return true;
	}


	/**
	 * This method creates and fills Delta view 
	 */
	public static boolean createDeltaViewTable() {

		List<String> keyspace = XmlHandler.getInstance()
				.getDeltaViewConfig()
				.getList("dbSchema.tableDefinition.keyspace");
		List<String> tableName = XmlHandler.getInstance()
				.getDeltaViewConfig()
				.getList("dbSchema.tableDefinition.name");
		Integer nrTables = XmlHandler.getInstance().getDeltaViewConfig()
				.getInt("dbSchema.tableNumber");
		List<String> primarykeyType = XmlHandler.getInstance()
				.getDeltaViewConfig()
				.getList("dbSchema.tableDefinition.primaryKey.type");
		List<String> primarykeyName = XmlHandler.getInstance()
				.getDeltaViewConfig()
				.getList("dbSchema.tableDefinition.primaryKey.name");
		List<String> nrColumns = XmlHandler.getInstance().getDeltaViewConfig()
				.getList("dbSchema.tableDefinition.columnNumber");
		List<String> colFamily = XmlHandler.getInstance()
				.getDeltaViewConfig()
				.getList("dbSchema.tableDefinition.column.family");
		List<String> colName = XmlHandler.getInstance().getDeltaViewConfig()
				.getList("dbSchema.tableDefinition.column.name");
		List<String> colType = XmlHandler.getInstance().getDeltaViewConfig()
				.getList("dbSchema.tableDefinition.column.type");


		int cursor = 0;

		// delta view
		for (int i = 0; i < nrTables; i++) {

			StringBuilder createQuery = new StringBuilder();
			createQuery.append("CREATE TABLE IF NOT EXISTS  ")
			.append(keyspace.get(i)).append(".")
			.append(tableName.get(i) + "(")
			.append(primarykeyName.get(i) + " ")
			.append(primarykeyType.get(i)).append(",");

			for (int j = 0; j < Integer.parseInt(nrColumns.get(i)); j++) {
				String colNameString = colName.get(cursor + j);
				String colName_old = colNameString+"_old";
				String colName_new = colNameString+"_new";

				createQuery.append(colName_old + " ").append(
						colType.get(cursor + j) + ",");

				createQuery.append(colName_new + " ").append(
						colType.get(cursor + j) + ",");
			}

			createQuery.append(" PRIMARY KEY (").append(
					primarykeyName.get(i) + "));");


			cursor += Integer.parseInt(nrColumns.get(i));


			Session session = null;

			System.out.println(createQuery.toString());

			try {
				session = currentCluster.connect();
				session.execute(createQuery.toString());

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

		return true;
	}






	public static boolean createPreAggregationViewTable() {

		List<String> keyspace = XmlHandler.getInstance()
				.getPreAggViewConfig()
				.getList("dbSchema.tableDefinition.keyspace");
		List<String> tableName = XmlHandler.getInstance()
				.getPreAggViewConfig()
				.getList("dbSchema.tableDefinition.name");
		Integer nrTables = XmlHandler.getInstance().getPreAggViewConfig()
				.getInt("dbSchema.tableNumber");
		List<String> primarykeyType = XmlHandler.getInstance()
				.getPreAggViewConfig()
				.getList("dbSchema.tableDefinition.primaryKey.type");
		List<String> primarykeyName = XmlHandler.getInstance()
				.getPreAggViewConfig()
				.getList("dbSchema.tableDefinition.primaryKey.name");
		List<String> nrColumns = XmlHandler.getInstance().getPreAggViewConfig()
				.getList("dbSchema.tableDefinition.columnNumber");
		List<String> colName = XmlHandler.getInstance().getPreAggViewConfig()
				.getList("dbSchema.tableDefinition.column.name");
		List<String> colType = XmlHandler.getInstance().getPreAggViewConfig()
				.getList("dbSchema.tableDefinition.column.type");


		int cursor = 0;

		// preAgg view
		for (int i = 0; i < nrTables; i++) {

			StringBuilder createQuery = new StringBuilder();
			createQuery.append("CREATE TABLE IF NOT EXISTS  ")
			.append(keyspace.get(i)).append(".")
			.append(tableName.get(i) + "(")
			.append(primarykeyName.get(i) + " ")
			.append(primarykeyType.get(i)).append(",");

			for (int j = 0; j < Integer.parseInt(nrColumns.get(i)); j++) {
				createQuery.append(colName.get(cursor + j) + " ").append(
						colType.get(cursor + j) + ",");
			}

			createQuery.append(" PRIMARY KEY (").append(
					primarykeyName.get(i) + "));");


			cursor += Integer.parseInt(nrColumns.get(i));


			Session session = null;

			System.out.println(createQuery.toString());

			try {
				session = currentCluster.connect();
				session.execute(createQuery.toString());
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

		}

		return true;
	}



}
