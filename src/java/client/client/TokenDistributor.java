package client.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class TokenDistributor {

	private static XMLConfiguration clusterConfig;
	private static Integer nrNodes;
	private static String localhost;
	private static Client client;
	private static List<String> ipAddress;
	private static List<List<String>> tokenRanges;

	TokenDistributor(){

		client = new Client();
		clusterConfig = new XMLConfiguration();
		tokenRanges = new ArrayList<List<String>>(); 
		clusterConfig.setDelimiterParsingDisabled(true);
		try {
			clusterConfig.load("client/resources/ClusterConfig.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}

	public static void tokenDistributorProcess() {

		parseConfigFile();
		retrieveTokens();

        



	}

	private static void retrieveTokens(){


		Session session = null;
		String tokenSelectQuery = null;
		ResultSet result = null;

		try{

			session = client.connectToCluster(ipAddress.get(0)).connect();
			tokenSelectQuery = "select tokens from system.peers where peer='"+getLocalhost()+"';";			

			result = session.execute(tokenSelectQuery);
			List<String> temp = new ArrayList<String>();
			temp = Arrays.asList(result.all().toString().replaceAll("[^0-9-.,]+", "").split(","));

			tokenRanges.add(0, temp);
			
			client.disconnectFromCluster();

			session = client.connectToCluster(getLocalhost()).connect();

			for(int i=0;i<ipAddress.size();i++){ 
				tokenSelectQuery = "select tokens from system.peers where peer='"+ipAddress.get(i)+"';";			
				result = session.execute(tokenSelectQuery);

				temp = new ArrayList<String>();
				temp = Arrays.asList(result.all().toString().replaceAll("[^0-9-.,]+", "").split(","));

				tokenRanges.add(i+1, temp);
			}

			client.disconnectFromCluster();
			
			System.out.println("Tokens have been retrieved");

		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static void parseConfigFile(){

		setNrNodes(clusterConfig.getInt("config.numberNodes.value"));
		setLocalhost(clusterConfig.getString("config.host.localhost"));
		ipAddress = clusterConfig.getList("config.host.ip");	
	}

	public Integer getNrNodes() {
		return nrNodes;
	}

	public static void setNrNodes(int nrNodes) {
		TokenDistributor.nrNodes = nrNodes;
	}

	public static String getLocalhost() {
		return localhost;
	}

	public static void setLocalhost(String localhost) {
		TokenDistributor.localhost = localhost;
	}




}
