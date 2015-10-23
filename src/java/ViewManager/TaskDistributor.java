package ViewManager;

import java.util.LinkedList;
import java.util.Queue;

import client.client.XmlHandler;

import org.json.simple.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class TaskDistributor {

	ViewManagerController vmc;
	ViewManagerGroupByController vmgb;
	ViewManagerPreaggController vmp;
	ViewManagerRJController vmr;
	ViewManager vm;
	private Cluster currentCluster;
	Queue<JSONObject> rJ;
	Queue<JSONObject> preAgg;
	Queue<JSONObject> groupBy;
	Queue<JSONObject> delta;
	Thread rjThread;
	Thread preaggThread;
	Thread groupByThread;
	Thread deltaThread;


	public TaskDistributor(){

		connectToCluster();
		vm = new ViewManager(getCurrentCluster(), "vm1");

		rJ = new LinkedList<JSONObject>();
		preAgg = new LinkedList<JSONObject>();
		groupBy = new LinkedList<JSONObject>();
		delta = new LinkedList<JSONObject>();

	/*	vmgb= new ViewManagerGroupByController(vm, currentCluster,this);
		groupByThread = new Thread(vmgb);
		groupByThread.setName("groupByThread");
		groupByThread.start();

		vmp = new ViewManagerPreaggController(vm, currentCluster,this);
		preaggThread = new Thread(vmp);
		preaggThread.setName("preaggThread");
		preaggThread.start(); */

		vmc = new ViewManagerController(vm, currentCluster,this);
		deltaThread = new Thread(vmc);
		deltaThread.setName("deltaThread");
		deltaThread.start();

	/*	vmr = new ViewManagerRJController(vm, currentCluster,this);
		rjThread = new Thread(vmr);
		rjThread.setName("rjThread");
		rjThread.start();*/

	}

	public void fillQueue(JSONObject json,int n){



		if(n==1){
			rJ.add(json);
		}else if(n==2){
			preAgg.add(json);
		}else if(n==3){
			groupBy.add(json);
		}else if(n==4){
			delta.add(json);
		}
	}

	public void processRequest(JSONObject json,String type,String table, long readPtr){

		json.put("readPtr", readPtr);
		

		if (table.toLowerCase().contains("groupby")) {
			if (type.equalsIgnoreCase("insert")||type.equalsIgnoreCase("update")){
				fillQueue(json, 3);
			}
		}else if (table.toLowerCase().contains("preagg_agg")) {
			if (type.equalsIgnoreCase("insert")||type.equalsIgnoreCase("update")){
				fillQueue(json, 2);
			}
		} else if (table.toLowerCase().contains("rj_")) {
			if (type.equalsIgnoreCase("insert")||type.equalsIgnoreCase("update")){
				fillQueue(json, 1);
			}
		} else {
			if (type.equals("insert")) {
				fillQueue(json, 4);
			}
			if (type.equals("update")) {
				fillQueue(json, 4);
			}
			if (type.equals("delete-row")) {
				fillQueue(json, 4);
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
