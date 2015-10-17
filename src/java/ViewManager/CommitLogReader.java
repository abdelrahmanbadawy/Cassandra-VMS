package ViewManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import client.client.Client;

public class CommitLogReader {

	BufferedReader br;
	static TaskDistributor td;
	static RandomAccessFile raf;
	static ViewManagerController vmc;
	static String fileName = "logs//output.log";
	static long readPtr;

	public CommitLogReader() {

		try {
			raf = new RandomAccessFile(fileName, "rw");
			br = new BufferedReader(new FileReader("logs//output.log"));
			readPtr = -1;
			td = new TaskDistributor();

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}

	}


	public static void main(String[] args) {

		CommitLogReader cmr = new CommitLogReader();
	
		// monitor a single file
		TimerTask task = new FileWatcher(new File(fileName)) {
			protected void onChange(File file) {
				readCL();
			}
		};

		Timer timer = new Timer();
		timer.schedule(task,new Date(),1000);

	}

	private static void readCL() {

		System.out.println("on change");
		
		String raw;
		int counter = 0;

		try {

			if(readPtr==-1){
				raw = raf.readLine();
				readPtr = raf.getFilePointer();
				counter++;
			}else{
				raf.seek(readPtr);
				raw = raf.readLine();
				readPtr = raf.getFilePointer();
				counter++;
			}

			while (raw != null) {
				String[] splitRaw = raw.split(" - ");
				String jsonString = splitRaw[1];

				JSONObject json = (JSONObject) new JSONParser().parse(jsonString);

				String type = json.get("type").toString();
				String table = json.get("table").toString();

				System.out.println(table);
				td.processRequest(json,type,table);
				System.out.println("back");

				if(counter<10){
					raw = raf.readLine();
					readPtr = raf.getFilePointer();
				}else{
					raw = null;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();

		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public void closeCLREader(){
		try {
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Client.getClusterInstance().close();
	}

}
