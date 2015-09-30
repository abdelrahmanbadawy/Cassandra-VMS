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
	ViewManagerController vmc;

	public CommitLogReader() {

		try {
			br = new BufferedReader(new FileReader("logs//output.log"));
			vmc = new ViewManagerController();

		} catch (FileNotFoundException e) {

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

				JSONObject json = (JSONObject) new JSONParser()
						.parse(jsonString);

				String type = json.get("type").toString();
				String table = json.get("table").toString();

				if (table.contains("preagg_agg")) {

					if (type.equalsIgnoreCase("insert"))
						vmc.propagatePreaggUpdate(json);

				} else {

					if (table.contains("RJ_")) {
						if (type.equalsIgnoreCase("insert"))
							vmc.propagateRJ(json);
					} else {

						if (type.equals("insert")) {
							vmc.update(json);
						}

						if (type.equals("update")) {
							vmc.update(json);
						}

						if (type.equals("delete-row")) {
							vmc.cascadeDelete(json, true);
						}
					}
				}

				raw = br.readLine();
			}

			Client.getClusterInstance().close();

		} catch (IOException e) {
			e.printStackTrace();

		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {

		CommitLogReader cmr = new CommitLogReader();
		cmr.read();

	}
}
