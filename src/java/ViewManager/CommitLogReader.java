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
	ViewManager vm;

	public CommitLogReader() {

		try {
			br = new BufferedReader(new FileReader("logs//output.log"));
			vm = new ViewManager();

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

				// System.out.println("timestamp=" + splitRaw[0]);

				JSONObject json = (JSONObject) new JSONParser()
				.parse(jsonString);

				String type = json.get("type").toString();

				// System.out.println("type=" + type);
				// System.out.println("keyspace=" + json.get("keyspace"));
				// System.out.println("table=" + json.get("table"));
				// System.out.println("tid=" + json.get("tid"));

				if (type.equals("insert")) {

					//vm.insertCourses_Faculty_AggView(json);
					if(json.get("table").equals("courses")){
						vm.updateDelta(json);
					}	
				}

				if (type.equals("update")) {
					if(json.get("table").equals("courses")){
						vm.updateDelta(json);
					}
				}

				if (type.equals("delete-row")) {
					if(json.get("table").equals("courses")){
						vm.cascadeDeleteRow(json);
					}
				}

				if (type.equals("delete-col")) {


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
