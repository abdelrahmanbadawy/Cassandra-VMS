package ViewManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class BootVMS {

	public static void main(String [] args){
		
		System.out.println("Booting VMS ...");
		
		// vm names
		ArrayList<String> vm_identifiers = (ArrayList<String>) VmXmlHandler.getInstance().getVMProperties().getList("vm.identifier");
		
		
		HashFunction hf = new MD5();
		
		// virtual nodes replicas 50
		ConsistentHash<String> consistentHashing = new ConsistentHash<String>(hf, 50, vm_identifiers);
		
		 final CommitLogReader cmr = new CommitLogReader(consistentHashing,  vm_identifiers);
		
		//recovery mode
	
		//recoveryMode();
		
		// monitor a single file
		TimerTask task = new FileWatcher(new File(cmr.fileName)) {
			protected void onChange(File file) {
				cmr.readCL();
			}
		};

		Timer timer = new Timer();
		timer.schedule(task,new Date(),1000);
		
	}
	
}
