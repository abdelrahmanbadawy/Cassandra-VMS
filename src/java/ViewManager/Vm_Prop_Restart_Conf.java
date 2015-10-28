package ViewManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.commons.configuration.HierarchicalConfiguration.*;


public class Vm_Prop_Restart_Conf {

	
	public static void main(String [] args){
		
		System.out.println("Enter number of view managers:");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		int vms= 0;
		try {
			 vms = Integer.parseInt(br.readLine());
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Node parent = VmXmlHandler.getInstance().getVMProperties().getRoot();
		parent.removeChildren();
		
		parent.addChild(new Node("number",vms));
		
		
		for(int i=0; i<vms; i++){
			
			Node oneVM = new Node("vm");
			
			Node identifier = new Node("identifier", "vm"+(i+1));
			Node e1 = new Node("execPtr1",0);
			Node e2 = new Node("execPtrRJ",0);
			Node e3 = new Node("execPtrGB",0);
			Node e4 = new Node("execPtrPreagg",0);
			
			oneVM.addChild(identifier);
			oneVM.addChild(e1);
			oneVM.addChild(e2);
			oneVM.addChild(e3);
			oneVM.addChild(e4);
			
			parent.addChild(oneVM);

		}
	
		try {
			VmXmlHandler.getInstance().getVMProperties().save(VmXmlHandler.getInstance().getVMProperties().getFile());
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
