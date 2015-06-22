package client.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import java.util.List;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;


public class Client {

	static Cluster currentCluster = null;
	private static XMLConfiguration databaseConfig;

	Client() {

		databaseConfig = new XMLConfiguration();
		databaseConfig.setDelimiterParsingDisabled(true);
		try {
			databaseConfig.load("client/resources/DatabaseConfig.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
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

		currentCluster = cluster; 
		return cluster;	
	}


	public static void disconnectFromCluster(Cluster cluster) {

		try {
			cluster.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

	public static boolean clusterIsConnected(Cluster cluster){

		if(cluster.isClosed()){
			return false;
		}

		return true;
	}

	public static boolean createKeySpace(String keyspace){

		Session session =  currentCluster.connect();

		StringBuilder queryString = new StringBuilder();
		queryString.append("CREATE KEYSPACE IF NOT EXISTS ").append(keyspace).append(" WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");	

		System.out.println(queryString.toString());
		ResultSet queryResults = session.execute(queryString.toString());

		return true;
	}

	public static boolean createTable(){

		List<String> keyspace = databaseConfig.getList("dbSchema.tableDefinition.keyspace");
		List<String> tableName  = databaseConfig.getList("dbSchema.tableDefinition.name");
		Integer nrTables =  databaseConfig.getInt("dbSchema.tableNumber");
		List<String> primarykeyType =  databaseConfig.getList("dbSchema.tableDefinition.primaryKey.type");
		List<String> primarykeyName =  databaseConfig.getList("dbSchema.tableDefinition.primaryKey.name");
		Integer nrColumns = databaseConfig.getInt("dbSchema.tableDefinition.columnNumber");
		List<String> colFamily  = databaseConfig.getList("dbSchema.tableDefinition.column.family");
		List<String> colName  = databaseConfig.getList("dbSchema.tableDefinition.column.name");
		List<String> colType  = databaseConfig.getList("dbSchema.tableDefinition.column.type");

		for(int i=0;i<nrTables;i++){
			StringBuilder createQuery = new StringBuilder();
			createQuery.append("CREATE TABLE ").append(keyspace.get(i)).append(".").append(tableName.get(i)+"(")
			.append(primarykeyName.get(i)+" ").append(primarykeyType.get(i)).append(" PRIMARY KEY,");

			for(int j=0;j<nrColumns;j++){
				createQuery.append(colName.get(j)+" ").append(colType.get(j)+",");
			}

			createQuery.deleteCharAt(createQuery.length()-1);
			createQuery.append(");");

			Session session = null;
			
			System.out.println(createQuery.toString());

			try{
				session = currentCluster.connect();
				ResultSet queryResults = session.execute(createQuery.toString());
			}catch(Exception e) {
				e.printStackTrace();
				return false;
			}
		}		
		
		return true;
	}






}
