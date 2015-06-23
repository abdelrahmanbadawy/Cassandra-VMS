package client.client;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import com.datastax.driver.core.Cluster;


public class ClientProcess {


	private static Cluster cluster = null;
	private static XMLConfiguration databaseConfig;
	static Client client;
	static TokenDistributor tokenDist;


	public static void main(String[] args){

		client = new Client();
		cluster =  client.connectToCluster("localhost");
		tokenDist = new TokenDistributor();

		databaseConfig = new XMLConfiguration();
		databaseConfig.setDelimiterParsingDisabled(true);
		try {
			databaseConfig.load("client/resources/DatabaseConfig.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

		while(args.length == 0 || !args[0].equals("exit")){

			if(args == null || args.length == 0){
				System.out.println("Possible commands are:");
				System.out.println("-----exit");
				System.out.println("-----help");
				System.out.println("-----create keyspace ");
				System.out.println("-----create table ");
				System.out.println("-----insert basetable data ");
				System.out.println("-----drop table ");
				System.out.println("-----drop keyspace ");
			}else{

				if(args[0].equals("create") && args[1].equals("keyspace") ){

					List<String> keyspaceEntries  = databaseConfig.getList("dbSchema.tableDefinition.keyspace");
					HashSet<String> uniqueKeyspaceEntries = new HashSet<String>();
					uniqueKeyspaceEntries.addAll(keyspaceEntries);

					for(String keyspace:uniqueKeyspaceEntries){
						if(client.createKeySpace(keyspace)){
							System.out.println("Keyspace has been added");
						}
					}			
				}


				if(args[0].equals("create") && args[1].equals("table") ){
					if(client.createTable()){
						System.out.println("Base tables have been inserted");
					}
				}
				
				if(args[0].equals("insert") && args[1].equals("basetable") && args[2].equals("data") ){
					client.disconnectFromCluster(Client.currentCluster);
					tokenDist.tokenDistributorProcess();
					
				}
			}


			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			try {
				args = br.readLine().split("\\s+");

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}




}
