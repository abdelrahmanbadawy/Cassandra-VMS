package client.client;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
//import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.collect.Multiset.Entry;

public class TokenDistributor {

	private static XMLConfiguration clusterConfig;
	private static Integer nrNodes;
	private static String localhost;
	private static Client client;
	private static List<String> ipAddress;
	private static List<List<String>> tokenRanges;
	//private static Set<Set<TokenRange>> tokenRanges1;
	private static NavigableMap<Long,String> testmap;
	private static XMLConfiguration empConfig;

	TokenDistributor(){

		client = new Client();
		clusterConfig = new XMLConfiguration();
		tokenRanges = new ArrayList<List<String>>(); 
		//tokenRanges1 = new HashSet<Set<TokenRange>>();
		testmap = new TreeMap();

		clusterConfig.setDelimiterParsingDisabled(true);
		try {
			clusterConfig.load("client/resources/ClusterConfig.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

		empConfig = new XMLConfiguration();
		empConfig.setDelimiterParsingDisabled(true);
		try {
			empConfig.load("client/data/emp.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

	}

	public static void tokenDistributorProcess() {

		parseConfigFile();
		retrieveTokens();

		//retrieveTokens1();
        client.disconnectFromCluster();
		client.connectToCluster(getLocalhost());
		distributeData("emp");



	}

	
	static LongToken generateRandomTokenMurmurPartition(int randKeyGenerated) {
		BigInteger bigInt = BigInteger.valueOf(randKeyGenerated);
		Murmur3Partitioner murmur3PartitionerObj = new Murmur3Partitioner();
		Token.TokenFactory tokenFac = murmur3PartitionerObj.getTokenFactory();
		org.apache.cassandra.dht.LongToken generatedToken = murmur3PartitionerObj.getToken(ByteBufferUtil.bytes(randKeyGenerated));
		return generatedToken;
	}

	private static void distributeData(String tableFile){

		int nrPrimaryKeys = empConfig.getInt("NumberOfEntries");
		int nrNodes = ipAddress.size()+1;
		int nrEntriesPerNode = (nrPrimaryKeys%nrNodes)*nrNodes;
		Map count = new HashMap<String,Integer>();
		int fileCursor = 0;

		// init count map
		for(int i=0;i<ipAddress.size();i++){
			count.put(ipAddress.get(i), nrEntriesPerNode);
		}
		count.put(getLocalhost(),nrPrimaryKeys-(nrEntriesPerNode*(nrNodes-1)));


		/*for(int i=0;i<nrPrimaryKeys;i++){

		     int pk = randInt(1,1000000);			
			 LongToken hashedPK = generateRandomTokenMurmurPartition(pk);

			 System.out.println(pk);
			 //System.out.println(hashedPK);
		     System.out.println(testmap.ceilingEntry(hashedPK.getTokenValue()));
		}*/

		while(!count.isEmpty()){

			int pk = randInt(1,1000000);
			LongToken hashedPK = generateRandomTokenMurmurPartition(pk);

			String suggestedNode = testmap.ceilingEntry(hashedPK.getTokenValue()).getValue();
			
			if(suggestedNode==null || !count.containsKey(suggestedNode)){
				continue;
			}
			
			int remainingCount = (int) count.get(suggestedNode);

			if(remainingCount>0){
				
				remainingCount--;
				count.put(suggestedNode,remainingCount);
				
				client.insertBaseTable(fileCursor,pk);

				fileCursor++;
				if(remainingCount==0){
					count.remove(suggestedNode);
				}
				
				System.out.println("The pk is "+pk);
				System.out.println("The  hashed pk is "+hashedPK);
				System.out.println("The suggested node is "+suggestedNode);
			}

		}

	}

	public static int randInt(int min, int max) {

		// NOTE: Usually this should be a field rather than a method
		// variable so that it is not re-seeded every call.
		Random rand = new Random();

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;

		return randomNum;
	}


	/*private static void retrieveTokens1(){

		Metadata localhostMetadata = client.connectToCluster(getLocalhost()).getMetadata();
		Set<TokenRange> localhostTokenRange = localhostMetadata.getTokenRanges();
		client.disconnectFromCluster();
		System.out.println(localhostTokenRange);
		tokenRanges1.add(localhostTokenRange);

		for(int i=0;i<ipAddress.size();i++){ 

			Metadata metadata = client.connectToCluster(ipAddress.get(i)).getMetadata();
			Set<TokenRange> tokenRange = metadata.getTokenRanges();
			System.out.println(tokenRange);
			tokenRanges1.add(tokenRange);

		}

	}*/

	private static void retrieveTokens(){


		Session session = null;
		String tokenSelectQuery = null;
		ResultSet result = null;

		try{

			//session = client.connectToCluster(ipAddress.get(0)).connect();
			tokenSelectQuery = "select tokens from system.peers where peer='"+getLocalhost()+"';";			

			result = session.execute(tokenSelectQuery);
			List<String> temp = new ArrayList<String>();
			temp = Arrays.asList(result.all().toString().replaceAll("[^0-9-.,]+", "").split(","));

			tokenRanges.add(0, temp);
			for (String i : temp) testmap.put(Long.parseLong(i),getLocalhost());

			client.disconnectFromCluster();

			//session = client.connectToCluster(getLocalhost()).connect();

			for(int i=0;i<ipAddress.size();i++){ 
				tokenSelectQuery = "select tokens from system.peers where peer='"+ipAddress.get(i)+"';";			
				result = session.execute(tokenSelectQuery);

				temp = new ArrayList<String>();
				temp = Arrays.asList(result.all().toString().replaceAll("[^0-9-.,]+", "").split(","));

				tokenRanges.add(i+1, temp);
				for (String s : temp) testmap.put(Long.parseLong(s),ipAddress.get(i));
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
