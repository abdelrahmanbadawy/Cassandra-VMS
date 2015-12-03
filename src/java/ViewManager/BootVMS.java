package ViewManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class BootVMS {
	
    final static Logger logger = Logger.getLogger("BootVMS");  
    
	public static void initLogger() {  

	    FileHandler fh;  
	    
	    try {  
	        // This block configure the logger with handler and formatter  
	        fh = new FileHandler("experiment_logs/%g.log", 200 * 1024 * 1024, 50);
	        MyFormatter formatter = new MyFormatter();
	        fh.setFormatter(formatter); 
	        logger.setUseParentHandlers(false);
	        logger.addHandler(fh);
	        // the following statement is used to log any messages  

	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  

	}
	
	
	public static void main(String [] args){
		
		//Utils.getFiles();

		initLogger();
		System.out.println("Booting VMS ...");
		
		// vm names
		ArrayList<String> vm_identifiers = (ArrayList<String>) VmXmlHandler.getInstance().getVMProperties().getList("vm.identifier");
		
		
		HashFunction hf = new MD5();
		
		// virtual nodes replicas 50
		ConsistentHash<String> consistentHashing = new ConsistentHash<String>(hf, 50, vm_identifiers);
		
		  final CommitLogReader cmr = new CommitLogReader(consistentHashing,  vm_identifiers, false, args[0]);
		
		//recovery mode
	
		//recoveryMode();
		  
		  
		
		  while(true){
			  
			  cmr.readCL();
			  try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		
	}
	
}
