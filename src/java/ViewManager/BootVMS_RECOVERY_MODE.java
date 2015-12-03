package ViewManager;

import java.util.ArrayList;


public class BootVMS_RECOVERY_MODE {
	
	public static void main(String [] args){
		
		//Utils.getFiles();
		System.out.println("Booting VMS in RECOVERY_MODE...");
		
		// vm names
		ArrayList<String> vm_identifiers = (ArrayList<String>) VmXmlHandler.getInstance().getVMProperties().getList("vm.identifier");
		
		
		HashFunction hf = new MD5();
		
		// virtual nodes replicas 50
		ConsistentHash<String> consistentHashing = new ConsistentHash<String>(hf, 50, vm_identifiers);
		
		  final CommitLogReader cmr = new CommitLogReader(consistentHashing,  vm_identifiers, true, args[0]);
		
		//recovery mode
	
		cmr.recoveryMode();
		  
		
	}
	
}
