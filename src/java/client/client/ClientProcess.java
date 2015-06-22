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


	public static void main(String[] args){
		
		cluster =  Client.connectToCluster("localhost");
		
		databaseConfig = new XMLConfiguration();
		databaseConfig.setDelimiterParsingDisabled(true);
		try {
			databaseConfig.load("resources/DatabaseConfig.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

		while(args.length == 0 || !args[0].equals("exit")){

			if(args == null || args.length == 0){
				System.out.println("Possible commands are:");
				System.out.println("-----exit");
				System.out.println("-----help");
				System.out.println("-----create keyspace -name-");
				System.out.println("-----create table -name-");
			}else{

				if(args[0].equals("create") && args[1].equals("keyspace") ){	
					List<String> keyspaceEntries  = databaseConfig.getList("dbSchema.tableDefinition.keyspace");
					HashSet<String> uniqueKeyspaceEntries = new HashSet<String>();
					uniqueKeyspaceEntries.addAll(keyspaceEntries);
					
					for(String keyspace:uniqueKeyspaceEntries){
						System.out.println(keyspace);
						System.out.println(cluster);
						Client.createKeySpace(cluster,keyspace);	
					}			
				}

				if(args[0].equals("create") && args[1].equals("table") ){
					List<String> tableSchemaEntries  = databaseConfig.getList("dbSchema.tableDefinition");

					for(int i=0;i<tableSchemaEntries.size();i++){
						
					}	
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
