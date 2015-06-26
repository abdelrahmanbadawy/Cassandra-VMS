package client.client;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;

import com.datastax.driver.core.Cluster;


public class ClientProcess {


	private static Cluster cluster = null;
	static Client client;

	public static void main(String[] args){

		client = new Client();
		cluster =  client.connectToCluster(XmlHandler.getInstance().getClusterConfig().getString("config.host.localhost"));
		

		while(args.length == 0 || !args[0].equals("exit")){

			if(args == null || args.length == 0){
				System.out.println("Possible commands are:");
				System.out.println("-----exit");
				System.out.println("-----help");
				System.out.println("-----create keyspace ");
				System.out.println("-----create table ");
				System.out.println("-----insert basetable data ");
				System.out.println("-----insert viewtable ");
				
			}else{

				if(args[0].equals("create") && args[1].equals("keyspace") ){

					List<String> keyspaceEntries  = XmlHandler.getInstance().getDatabaseConfig().
							getList("dbSchema.tableDefinition.keyspace");
					HashSet<String> uniqueKeyspaceEntries = new HashSet<String>();
					uniqueKeyspaceEntries.addAll(keyspaceEntries);

					for(String keyspace:uniqueKeyspaceEntries){
						if(client.createKeySpace(keyspace)){
							System.out.println("Keyspace has been added");
						}
					}	
					
				}else if(args[0].equals("create") && args[1].equals("table") ){
					if(client.createTable()){
						System.out.println("Base table schemas have been inserted");
					}
					
				}else if(args[0].equals("insert") && args[1].equals("basetable") && args[2].equals("data") ){
					client.disconnectFromCluster(Client.currentCluster);
				//	tokenDist.tokenDistributorProcess();

				}else if(args[0].equals("insert") && args[1].equals("viewtable")){
					if(client.createViewTable()){
						System.out.println("View tables have been inserted");
					}
				}
					
					
					
				else	if(args[0].equals("help")){

					System.out.println("Possible commands are:");
					System.out.println("-----exit");
					System.out.println("-----help");
					System.out.println("-----create keyspace ");
					System.out.println("-----create table ");
					System.out.println("-----insert basetable data ");
					System.out.println("-----insert viewtable ");
					
				}else{

					System.out.println("Possible commands are:");
					System.out.println("-----exit");
					System.out.println("-----help");
					System.out.println("-----create keyspace ");
					System.out.println("-----create table ");
					System.out.println("-----insert basetable data ");
					System.out.println("-----insert viewtable ");
					
				}
			}


			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			try {
				args = br.readLine().split("\\s+");

			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		
		client.currentCluster.close();

	}

}
