package ViewManager;

import java.util.ArrayList;


public class BootVMS {
	
	public static void main(String [] args){
		
		//Utils.getFiles();
		System.out.println("Booting VMS ...");
		
		// vm names
		ArrayList<String> vm_identifiers = (ArrayList<String>) VmXmlHandler.getInstance().getVMProperties().getList("vm.identifier");
		
		
		HashFunction hf = new MD5();
		
		// virtual nodes replicas 50
		ConsistentHash<String> consistentHashing = new ConsistentHash<String>(hf, 50, vm_identifiers);
		
		  final CommitLogReader cmr = new CommitLogReader(consistentHashing,  vm_identifiers);
		
		//recovery mode
	
		//recoveryMode();
		
		  while(true){
			  
			  cmr.run();
			  try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		
	}
	
}
