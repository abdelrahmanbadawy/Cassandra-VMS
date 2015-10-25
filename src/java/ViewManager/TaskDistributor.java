package ViewManager;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import client.client.XmlHandler;

import org.json.simple.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class TaskDistributor {



	List<ViewManager> viewManagers;

	List<ViewManagerController> viewManagerControllers;
	List<ViewManagerGroupByController> viewManagerGroupByControllers;
	List<ViewManagerPreaggController> viewManagerPreaggControllers;
	List<ViewManagerRJController> viewManagerRJControllers;


	private Cluster currentCluster;


	List<Thread> rjThreads;
	List<Thread> preaggThreads;
	List<Thread> groupbyThreads;
	List<Thread> deltaThreads;

	List<Queue<JSONObject>> rjQueues;
	List<Queue<JSONObject>> preaggQueues;
	List<Queue<JSONObject>> groupbyQueues;
	List<Queue<JSONObject>> deltaQueues;



	public TaskDistributor(ArrayList<String> vm_identifiers){

		connectToCluster();


		//initialize the lists
		viewManagers = new ArrayList<ViewManager>();
		viewManagerControllers = new ArrayList<ViewManagerController>();
		viewManagerGroupByControllers = new ArrayList<ViewManagerGroupByController>();
		viewManagerPreaggControllers = new ArrayList<ViewManagerPreaggController>();
		viewManagerRJControllers = new ArrayList<ViewManagerRJController>();

		rjQueues = new ArrayList<Queue<JSONObject>>();
		preaggQueues = new ArrayList<Queue<JSONObject>>();
		groupbyQueues = new ArrayList<Queue<JSONObject>>();
		deltaQueues = new ArrayList<Queue<JSONObject>>();

		preaggThreads = new ArrayList<Thread>();
		groupbyThreads = new ArrayList<Thread>();
		deltaThreads = new ArrayList<Thread>();
		rjThreads = new ArrayList<Thread>();

		for(int i = 0;i < vm_identifiers.size();i++){

			//view manager
			ViewManager vm = new ViewManager(getCurrentCluster(), vm_identifiers.get(i));
			viewManagers.add(vm);

			//Queues
			rjQueues.add(new LinkedList<JSONObject>());
			preaggQueues.add(new LinkedList<JSONObject>());
			deltaQueues.add(new LinkedList<JSONObject>());
			groupbyQueues.add(new LinkedList<JSONObject>());

			//Controllers and Threads
			ViewManagerGroupByController vmgb= new ViewManagerGroupByController(vm, currentCluster,this, i);
			Thread groupByThread = new Thread(vmgb);
			groupByThread.setName("groupByThread");
			groupByThread.start();
			groupbyThreads.add(groupByThread);

			ViewManagerPreaggController vmp = new ViewManagerPreaggController(vm, currentCluster,this, i);
			Thread preaggThread = new Thread(vmp);
			preaggThread.setName("preaggThread");
			preaggThread.start();
			preaggThreads.add(preaggThread);

			ViewManagerController vmc = new ViewManagerController(vm, currentCluster,this, i);
			Thread deltaThread = new Thread(vmc);
			deltaThread.setName("deltaThread");
			deltaThread.start();
			deltaThreads.add(deltaThread);

			ViewManagerRJController vmr = new ViewManagerRJController(vm, currentCluster,this, i);
			Thread rjThread = new Thread(vmr);
			rjThread.setName("rjThread");
			rjThread.start();
			rjThreads.add(rjThread);



		}


	}

	public void fillQueue(JSONObject json,int n, int vmIndex){

		if(n==1){
			rjQueues.get(vmIndex).add(json);
		}else if(n==2){
			preaggQueues.get(vmIndex).add(json);
		}else if(n==3){
			groupbyQueues.get(vmIndex).add(json);
		}else if(n==4){
			deltaQueues.get(vmIndex).add(json);
		}
	}

	public void processRequest(JSONObject json,String type,String table, long readPtr, int vmIndex, String fileName){

		json.put("readPtr", fileName+":"+readPtr);
		


		if (table.toLowerCase().contains("groupby")) {
			if (type.equalsIgnoreCase("insert")||type.equalsIgnoreCase("update")){
				fillQueue(json, 3, vmIndex);
			}
		}else if (table.toLowerCase().contains("preagg_agg")) {
			if (type.equalsIgnoreCase("insert")||type.equalsIgnoreCase("update")){
				fillQueue(json, 2, vmIndex);
			}
		} else if (table.toLowerCase().contains("rj_")) {
			if (type.equalsIgnoreCase("insert")||type.equalsIgnoreCase("update")){
				fillQueue(json, 1, vmIndex);
			}
		} else {
			if (type.equals("insert")) {
				fillQueue(json, 4, vmIndex);
			}
			if (type.equals("update")) {
				fillQueue(json, 4, vmIndex);
			}
			if (type.equals("delete-row")) {
				fillQueue(json, 4, vmIndex);
			}
		}
	}

	private void connectToCluster() {

		setCurrentCluster(Cluster
				.builder()
				.addContactPoint(
						XmlHandler.getInstance().getClusterConfig()
						.getString("config.host.localhost"))
						.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
						.withLoadBalancingPolicy(
								new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
								.build());

	}

	public Cluster getCurrentCluster() {
		return currentCluster;
	}

	public void setCurrentCluster(Cluster currentCluster) {
		this.currentCluster = currentCluster;
	}


}
