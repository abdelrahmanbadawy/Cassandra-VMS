package ViewManager;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.datastax.driver.core.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import client.client.Client;

public class CommitLogReader {

	BufferedReader br;

	public CommitLogReader() {

		try {
			br = new BufferedReader(new FileReader("logs//output.log"));

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void read() {

		
		Client.connectToCluster("192.168.56.101");
		
		String raw;
		try {
			raw = br.readLine();

			while (raw != null) {
				String[] splitRaw = raw.split(" - ");
				String jsonString = splitRaw[1];

				//System.out.println("timestamp=" + splitRaw[0]);

				JSONObject json = (JSONObject) new JSONParser()
						.parse(jsonString);

				String type = json.get("type").toString();

//				System.out.println("type=" + type);
//				System.out.println("keyspace=" + json.get("keyspace"));
//				System.out.println("table=" + json.get("table"));
//				System.out.println("tid=" + json.get("tid"));

				if (type.equals("update") || type.equals("delete")) {
					JSONObject condition = (JSONObject) json.get("condition");

					if (condition != null) {
						Object[] hm = condition.entrySet().toArray();

						for (int i = 0; i < hm.length; i++) {
							//System.out.println(hm[i]);
						}
					}

				}
				
				if (type.equals("insert")) {
					JSONObject data = (JSONObject) json.get("data");
					int salary = Integer.parseInt(data.get("salary").toString());

					if (data != null && salary>=2000) {
						Object[] hm = data.entrySet().toArray();
						
						StringBuilder columns = new StringBuilder();
						StringBuilder values = new StringBuilder();

						for (int i = 0; i < hm.length; i++) {
							//System.out.println(hm[i]);
							String [] split = hm[i].toString().split("=");
							
							columns.append(split[0]);
							values.append(split[1]);
							
							
							if(i<hm.length-1){
								columns.append(", ");
								values.append(", ");
							}
							
						}
						
						StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
						insertQuery.append(json.get("table")).append("SelectView (").append(columns)
						.append(") VALUES (").append(values).append(");"); 	
						
						System.out.println(insertQuery);
						
						Session session = Client.getClusterInstance().connect();
						
						session
						.execute(insertQuery.toString());
						
					}

				}
				
				if (type.equals("update")) {
					JSONObject set_data = (JSONObject) json.get("set_data");

					if (set_data != null) {
						Object[] hm = set_data.entrySet().toArray();

						for (int i = 0; i < hm.length; i++) {
							//System.out.println(hm[i]);
						}
					}

				}

				raw = br.readLine();
			}
			
			Client.getClusterInstance().close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {

		CommitLogReader cmr = new CommitLogReader();
		cmr.read();

	}
}
