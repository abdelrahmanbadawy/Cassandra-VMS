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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import client.client.Client;

public class CommitLogReader{

	
	TaskDistributor td;
	RandomAccessFile raf;
	ViewManagerController vmc;
	String fileName;
	long readPtr;
	ConsistentHash<String> consistentHashing;
	ArrayList<String> vm_identifiers;

	public CommitLogReader(ConsistentHash<String> ch, ArrayList<String> vm_identifiers, boolean recovery) {

		try {
			
			if(!recovery){
			String [] ep = get_Execution_Pointer_At_StartUp();
			
			fileName = ep[0];
			
			raf = new RandomAccessFile("logs//"+fileName, "rw");
			readPtr = Long.parseLong(ep[1]);
			}
		
			
			td = new TaskDistributor(vm_identifiers);
			this.consistentHashing = ch;
			this.vm_identifiers = vm_identifiers;

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}

	}
	
	public  boolean recoveryMode(){
		System.out.println("Recovery Mode: on");
		
		List<String> pointers = VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtr1");
		List<String> pointersRJ =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrRJ");
		List<String> pointersPreagg =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrPreagg");
		List<String> pointersGB =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrGB");
		
		String []  max = {"","-1"};
		String []  min = {"z","-1"};
		
		
		for(int i = 0; i < pointers.size(); i++){
			String [] c =  pointers.get(i).split(":");
			
			if(c.length==1)
				continue;
		
			long p = Long.parseLong(c[1]);
			
			if(c[0].compareTo(max[0])>0 || (c[0].compareTo(max[0])==0 && p >  Long.parseLong(max[1])))
				max = c;
			
			if(c[0].compareTo(min[0])<0 || (c[0].compareTo(min[0])==0 && p <  Long.parseLong(min[1])))
				min = c;
			
		}
		
		for(int i =0; i < pointersRJ.size(); i++){
			String [] c =  pointersRJ.get(i).split(":");
			
			if(c.length==1)
				continue;
			
			long p = Long.parseLong(c[1]);
			
			if(c[0].compareTo(max[0])>0 || (c[0].compareTo(max[0])==0 && p >  Long.parseLong(max[1])))
				max = c;
			
			if(c[0].compareTo(min[0])<0 || (c[0].compareTo(min[0])==0 && p <  Long.parseLong(min[1])))
				min = c;
		}
		
		for(int i =0; i < pointersGB.size(); i++){
			String [] c =  pointersGB.get(i).split(":");
			
			if(c.length==1)
				continue;
			
			
			long p = Long.parseLong(c[1]);
			
			
			if(c[0].compareTo(max[0])>0 || (c[0].compareTo(max[0])==0 && p >  Long.parseLong(max[1])))
				max = c;
			

			if(c[0].compareTo(min[0])<0 || (c[0].compareTo(min[0])==0 && p <  Long.parseLong(min[1])))
				min = c;
		}
		
		for(int i =0; i < pointersPreagg.size(); i++){
			String [] c =  pointersPreagg.get(i).split(":");
			
			if(c.length==1)
				continue;
			long p = Long.parseLong(c[1]);
			
			
			
			if(c[0].compareTo(max[0])>0 || (c[0].compareTo(max[0])==0 && p >  Long.parseLong(max[1])))
				max = c;
			

			if(c[0].compareTo(min[0])<0 || (c[0].compareTo(min[0])==0 && p <  Long.parseLong(min[1])))
				min = c;
		}
		
		
		System.out.println("recovery mode on");
		System.out.println("minimum pointer "+min[0] + ":"+min[1]);
		System.out.println("maximum pointer "+max[0] + ":"+max[1]);
		
		try {
			
			raf = new RandomAccessFile("logs//"+min[0], "rw");
			
			raf.seek(Long.parseLong(min[1]));
			
			while(min[0].equals(max[0]) && min[1].equals(max[1])){
				
			String raw = raf.readLine();
			min[1] = raf.getFilePointer()+"";

			if(raw==null){
				File [] files = Utils.getFilesInDirectory();
				
				int filePosition = Arrays.asList(files).indexOf(new File("logs//"+min[0]));
				
				if(filePosition < files.length-1){
					
					min[0] = files[filePosition+1].getName();
					min[1] = "-1";
					raf = new RandomAccessFile("logs//"+min[0], "rw");
					
				}
			}else{
			
			String[] splitRaw = raw.split(" - ");
			String jsonString = splitRaw[1];
		

			JSONObject json = (JSONObject) new JSONParser().parse(jsonString);

			String type = json.get("type").toString();
			String table = json.get("table").toString();

			json.put("recovery_mode", "on");
			
			String responsibleVM = consistentHashing.get(json.get("pk").toString());
			
			System.out.println("responsible vm is "+ responsibleVM +" for key "+json.get("pk").toString());
			
			int vm_index = vm_identifiers.indexOf(responsibleVM);
			
			td.processRequest(json,type,table, Long.parseLong(min[1]), vm_index, min[0]);
			}
			}
			
			// one more line
			String raw = raf.readLine();
			
			if(raw!=null){
			min[1] = raf.getFilePointer()+"";
			
			
			String[] splitRaw = raw.split(" - ");
			String jsonString = splitRaw[1];
		

			JSONObject json = (JSONObject) new JSONParser().parse(jsonString);

			String type = json.get("type").toString();
			String table = json.get("table").toString();

			json.put("recovery_mode", "last_recovery_line");
			
			String responsibleVM = consistentHashing.get(json.get("pk").toString());
			
			System.out.println("responsible vm is "+ responsibleVM +" for key "+json.get("pk").toString());
			
			int vm_index = vm_identifiers.indexOf(responsibleVM);
			
			td.processRequest(json,type,table, Long.parseLong(min[1]), vm_index, min[0]);
			
			}else{
				//one more line --> at end of file
				// go to next file if exist and read one more line
				
				File [] files = Utils.getFilesInDirectory();
				
				int filePosition = Arrays.asList(files).indexOf(new File("logs//"+min[0]));
				
				
				if(filePosition < files.length-1){
					
					
					min[0] = files[filePosition+1].getName();
					min[1] = "-1";
					raf = new RandomAccessFile("logs//"+min[0], "rw");
					
					 raw = raf.readLine();
					 
					 if(raw!=null){
							min[1] = raf.getFilePointer()+"";
							
							String[] splitRaw = raw.split(" - ");
							String jsonString = splitRaw[1];
							
							JSONObject json = (JSONObject) new JSONParser().parse(jsonString);

							String type = json.get("type").toString();
							String table = json.get("table").toString();

							json.put("recovery_mode", "last_recovery_line");
							
							String responsibleVM = consistentHashing.get(json.get("pk").toString());
							
							System.out.println("responsible vm is "+ responsibleVM +" for key "+json.get("pk").toString());
							
							int vm_index = vm_identifiers.indexOf(responsibleVM);
							
							td.processRequest(json,type,table, Long.parseLong(min[1]), vm_index, min[0]);
							
							}
				}
				
			}
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return true;
	}




	private String getFirstFileName() {
		// TODO Auto-generated method stub
		
		File[] files = Utils.getFilesInDirectory();
		//fileName = files[0].getName();
		//System.out.println("The first file is "+files[0].getName());
		return files[0].getName();
		
	}

	public boolean readCL() {

	//	System.out.println("CL file has changed");
		
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
				
				td.processRequest(json,type,table, readPtr, vm_index, fileName);
				
				if(counter<10){
					raw = raf.readLine();
					readPtr = raf.getFilePointer();
				}else{
					raw = null;
				}
			}
			
			//check eof because no more updates or move to another file
			File [] files = Utils.getFilesInDirectory();
			
			int filePosition = Arrays.asList(files).indexOf(new File("logs//"+fileName));
			
			
			if(filePosition < files.length-1){
				
				fileName = files[filePosition+1].getName();
				readPtr = -1;
				raf = new RandomAccessFile("logs//"+fileName, "rw");
				
				return true;
			}
			
			
			

		} catch (IOException e) {
			e.printStackTrace();

		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return false;
	}

	public void closeCLREader(){
		try {
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Client.getClusterInstance().close();
	}


	
	public String[] get_Execution_Pointer_At_StartUp(){
		
		List<String> pointers = VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtr1");
		List<String> pointersRJ =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrRJ");
		List<String> pointersPreagg =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrPreagg");
		List<String> pointersGB =  VmXmlHandler.getInstance().getVMProperties().getList("vm.execPtrGB");
		
		String []  max = {"","-1"};
		
		
		for(int i = 0; i < pointers.size(); i++){
			String [] c =  pointers.get(i).split(":");
			
			if(c.length==1)
				continue;
		
			long p = Long.parseLong(c[1]);
			
			
			
			if(c[0].compareTo(max[0])>0 || (c[0].compareTo(max[0])==0 && p >  Long.parseLong(max[1])))
				max = c;
		}
		
		for(int i =0; i < pointersRJ.size(); i++){
			String [] c =  pointersRJ.get(i).split(":");
			
			if(c.length==1)
				continue;
			
			long p = Long.parseLong(c[1]);
			
			if(c[0].compareTo(max[0])>0 || (c[0].compareTo(max[0])==0 && p >  Long.parseLong(max[1])))
				max = c;
		}
		
		for(int i =0; i < pointersGB.size(); i++){
			String [] c =  pointersGB.get(i).split(":");
			
			if(c.length==1)
				continue;
			
			
			long p = Long.parseLong(c[1]);
			
			
			if(c[0].compareTo(max[0])>0 || (c[0].compareTo(max[0])==0 && p >  Long.parseLong(max[1])))
				max = c;
		}
		
		for(int i =0; i < pointersPreagg.size(); i++){
			String [] c =  pointersPreagg.get(i).split(":");
			
			if(c.length==1)
				continue;
			long p = Long.parseLong(c[1]);
			
			
			
			if(c[0].compareTo(max[0])>0 || (c[0].compareTo(max[0])==0 && p >  Long.parseLong(max[1])))
				max = c;
		}
		
		if(max[0]==""){
			
			 String [] x = {getFirstFileName(),"-1"};
			 System.out.println("Execution pointer "+x[0]+ ":"+x[1]);
			return  x;
		}
		else{
			System.out.println("Execution pointer "+max[0] + ":"+max[1]);
			return max;
		}
		

		
	}

}
