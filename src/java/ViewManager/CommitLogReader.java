package ViewManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import client.client.Client;

public class CommitLogReader {

	BufferedReader br;
	 TaskDistributor td;
	static RandomAccessFile raf;
	static ViewManagerController vmc;
	static String fileName = "logs//output.log";
	static long readPtr;
	ConsistentHash<String> consistentHashing;
	ArrayList<String> vm_identifiers;

	public CommitLogReader(ConsistentHash<String> ch, ArrayList<String> vm_identifiers) {

		try {
			raf = new RandomAccessFile(fileName, "rw");
			br = new BufferedReader(new FileReader("logs//output.log"));
			readPtr = -1;
			td = new TaskDistributor(vm_identifiers);
			this.consistentHashing = ch;
			this.vm_identifiers = vm_identifiers;

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}

	}
	
//	public  boolean recoveryMode(){
//		System.out.println("Recovery Mode: on");
//		
//		List<String> pointers = VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtr1");
//		List<String> pointersRJ =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrRJ");
//		List<String> pointersPreagg =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrPreagg");
//		List<String> pointersGB =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrGB");
//		
//		long min = Long.parseLong(pointers.get(0));
//		long max = min;
//		
//		for(int i =1; i < pointers.size(); i++){
//			Long c =  Long.parseLong(pointers.get(i));
//			if(c < min)
//				min = c;
//			
//			if(max < c)
//				max = c;
//		}
//		
//		for(int i =0; i < pointersRJ.size(); i++){
//			Long c =  Long.parseLong(pointersRJ.get(i));
//			if(c < min)
//				min = c;
//			
//			if(max < c)
//				max = c;
//		}
//		
//		for(int i =0; i < pointersGB.size(); i++){
//			Long c =  Long.parseLong(pointersGB.get(i));
//			if(c < min)
//				min = c;
//			
//			if(max < c)
//				max = c;
//		}
//		
//		for(int i =0; i < pointersPreagg.size(); i++){
//			Long c =  Long.parseLong(pointersPreagg.get(i));
//			if(c < min)
//				min = c;
//			
//			if(max < c)
//				max = c;
//		}
//		
//		System.out.println("recovery mode on");
//		System.out.println("minimum pointer "+min);
//		System.out.println("maximum pointer "+max);
//		
//		try {
//			raf.seek(min);
//			
//			while(min!=max){
//			String raw = raf.readLine();
//			min = raf.getFilePointer();
//			
//			
//			String[] splitRaw = raw.split(" - ");
//			String jsonString = splitRaw[1];
//		
//
//			JSONObject json = (JSONObject) new JSONParser().parse(jsonString);
//
//			String type = json.get("type").toString();
//			String table = json.get("table").toString();
//
//			json.put("recovery_mode", "on");
//			
//			td.processRequest(json,type,table, min);
//			
//			}
//			
//			
//			// one more line
//			String raw = raf.readLine();
//			
//			if(raw!=null){
//			min = raf.getFilePointer();
//			
//			
//			String[] splitRaw = raw.split(" - ");
//			String jsonString = splitRaw[1];
//
//			JSONObject json = (JSONObject) new JSONParser().parse(jsonString);
//
//			String type = json.get("type").toString();
//			String table = json.get("table").toString();
//			
//			json.put("recovery_mode", "on");
//
//			td.processRequest(json,type,table, min);
//			}
//			
//			
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		
//		
//		return true;
//	}


//	public static void main(String[] args) {
//
//		CommitLogReader cmr = new CommitLogReader();
//		
//		//recovery mode
//	
//		//recoveryMode();
//		
//		// monitor a single file
//		TimerTask task = new FileWatcher(new File(fileName)) {
//			protected void onChange(File file) {
//				readCL();
//			}
//		};
//
//		Timer timer = new Timer();
//		timer.schedule(task,new Date(),1000);
//
//	}

	public void readCL() {

		System.out.println("CL file has changed");
		
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
				
				json.put("recovery_mode", "off");

				String responsibleVM = consistentHashing.get(json.get("pk").toString());
				
				System.out.println("responsible vm is "+ responsibleVM +" for key "+json.get("pk").toString());
				
				int vm_index = vm_identifiers.indexOf(responsibleVM);
				
				td.processRequest(json,type,table, readPtr, vm_index);
				
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
