package ViewManager;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.json.simple.JSONObject;

import com.datastax.driver.core.Cluster;

public class ViewManagerGroupByController implements Runnable {

	private Stream stream = null;
	ViewManager vm;
	Cluster cluster;
	List<String> havingJoinGroupBy;
	TaskDistributor td;
	List<String> vm_identifiers;
	int identifier_index;

	public ViewManagerGroupByController(ViewManager vm,Cluster cluster,TaskDistributor td) {	
		System.out.println("up group by");
		this.vm = vm;
		this.cluster = cluster;
		this.td = td;
		
		parseXML();	
	}


	private void parseXML() {

		havingJoinGroupBy =  VmXmlHandler.getInstance().getRJAggJoinGroupByHavingMapping()
				.getList("mapping.unit.name");
		vm_identifiers = VmXmlHandler.getInstance().getVMProperties().getList("vm.identifier");
		identifier_index = vm_identifiers.indexOf(vm.getIdentifier());
	}

	public void decideGroupBy(JSONObject json) {

		String table = json.get("table").toString();
		
		JSONObject data = (JSONObject) json.get("data");
		if(data==null)
			data = (JSONObject) json.get("set_data");

		String bufferString = null;
		Object buffer = data.get("stream");
		if(buffer==null)
			bufferString = data.get("stream ").toString();
		else
			bufferString = buffer.toString();

		String ptr = json.get("readPtr").toString();

		stream = Serialize.deserializeStream(bufferString);
		JSONObject deltaJSON = stream.getDeltaJSON();
		
		deltaJSON.put("readPtr", ptr);

		if(!stream.isDeleteOperation()){
			propagateGroupByUpdate(deltaJSON,table, ptr);
		}else{
			propagateGroupByDelete(deltaJSON,table, ptr);
		}
	}

	private void propagateGroupByUpdate(JSONObject json, String table, String ptr) {

		String groupByTable = table;

		int position = havingJoinGroupBy.indexOf(groupByTable);

		if(position!=-1){

			String temp = "mapping.unit("+position+").";
			Integer nrHaving =  VmXmlHandler.getInstance()
					.getRJAggJoinGroupByHavingMapping().getInt(temp + "nrHaving");

			if(nrHaving!=0){

				List<String> havingTableName =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp +"Having.name");

				List<String> aggFct =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp + "Having.aggFct");

				List<String> type =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp +"Having.type");
				List<String> operation =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp + "Having.operation");
				List<String> value =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp + "Having.value");

				for(int j=0;j<nrHaving;j++){

					if(stream.getUpdatedJoinAggGroupByRowDeleted()!=null){
						//boolean result = Utils.evalueJoinAggConditions(stream.getInnerJoinAggGroupByDeleteOldRow(), aggFct.get(j), operation.get(j), value.get(j));
						//if(result){
						String pkName = stream.getUpdatedJoinAggGroupByRowDeleted().getName(0);
						String pkType = stream.getUpdatedJoinAggGroupByRowDeleted().getType(0);
						String pkValue = Utils.getColumnValueFromDeltaStream(stream.getUpdatedJoinAggGroupByRowDeleted(), pkName, pkType, "");
						Utils.deleteEntireRowWithPK((String)json.get("keyspace"), havingTableName.get(j), pkName,pkValue);
						//}
					}

					if(stream.getUpdatedJoinAggGroupByRow()!=null){
						boolean result = Utils.evalueJoinAggConditions(stream.getUpdatedJoinAggGroupByRow(), aggFct.get(j), operation.get(j), value.get(j));
						if(result){
							JoinAggGroupByHelper.insertStatement(json, havingTableName.get(j), stream.getUpdatedJoinAggGroupByRow(), vm.getIdentifier());
						}else{
							String pkName = stream.getUpdatedJoinAggGroupByRow().getName(0);
							String pkType = stream.getUpdatedJoinAggGroupByRow().getType(0);
							String pkValue = Utils.getColumnValueFromDeltaStream(stream.getUpdatedJoinAggGroupByRow(), pkName, pkType, "");
							Utils.deleteEntireRowWithPK((String)json.get("keyspace"), havingTableName.get(j), pkName,pkValue);
						}
					}
				}
			}
		}
		System.out.println("saving execPtrGB "+ ptr);
		
		
		VmXmlHandler.getInstance().getVMProperties().setProperty("vm("+identifier_index+").execPtrGB", ptr);
		try {
			
			VmXmlHandler.getInstance().getVMProperties().save(VmXmlHandler.getInstance().getVMProperties().getFile());
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void propagateGroupByDelete(JSONObject json, String table, String ptr) {

		String groupByTable = table;

		int position = havingJoinGroupBy.indexOf(groupByTable);

		if(position!=-1){

			String temp = "mapping.unit("+position+").";
			Integer nrHaving =  VmXmlHandler.getInstance()
					.getRJAggJoinGroupByHavingMapping().getInt(temp + "nrHaving");

			if(nrHaving!=0){

				List<String> havingTableName =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp +"Having.name");

				List<String> aggFct =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp + "Having.aggFct");

				List<String> type =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp +"Having.type");
				List<String> operation =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp + "Having.operation");
				List<String> value =  VmXmlHandler.getInstance()
						.getRJAggJoinGroupByHavingMapping().getList(temp + "Having.value");

				for(int j=0;j<nrHaving;j++){

					if(stream.getUpdatedJoinAggGroupByRowDeleted()!=null){
						//boolean result = Utils.evalueJoinAggConditions(stream.getInnerJoinAggGroupByDeleteOldRow(), aggFct.get(j), operation.get(j), value.get(j));
						//if(result){
						String pkName = stream.getUpdatedJoinAggGroupByRowDeleted().getName(0);
						String pkType = stream.getUpdatedJoinAggGroupByRowDeleted().getType(0);
						String pkValue = Utils.getColumnValueFromDeltaStream(stream.getUpdatedJoinAggGroupByRowDeleted(), pkName, pkType, "");
						Utils.deleteEntireRowWithPK((String)json.get("keyspace"), havingTableName.get(j), pkName,pkValue);
						//}
					}

					if(stream.getUpdatedJoinAggGroupByRow()!=null){
						boolean result = Utils.evalueJoinAggConditions(stream.getUpdatedJoinAggGroupByRow(), aggFct.get(j), operation.get(j), value.get(j));
						if(result){
							JoinAggGroupByHelper.insertStatement(json, havingTableName.get(j), stream.getUpdatedJoinAggGroupByRow(), vm.getIdentifier());
						}else{
							String pkName = stream.getUpdatedJoinAggGroupByRow().getName(0);
							String pkType = stream.getUpdatedJoinAggGroupByRow().getType(0);
							String pkValue = Utils.getColumnValueFromDeltaStream(stream.getUpdatedJoinAggGroupByRow(), pkName, pkType, "");
							Utils.deleteEntireRowWithPK((String)json.get("keyspace"), havingTableName.get(j), pkName,pkValue);
						}
					}
				}
			}
		}
		System.out.println("saving execPtrGB "+ ptr);
		
		
		VmXmlHandler.getInstance().getVMProperties().setProperty("vm("+identifier_index+").execPtrGB", ptr);
		try {
			
			VmXmlHandler.getInstance().getVMProperties().save(VmXmlHandler.getInstance().getVMProperties().getFile());
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}


	
	@Override
	public void run() {
		
		while(true){
			
			while(!td.groupBy.isEmpty()){
				JSONObject head = td.groupBy.remove();
				decideGroupBy(head);
			}
			
			try {
		        Thread.sleep(3000);
		    } catch (InterruptedException e) {
		        // We've been interrupted: no more messages.
		        return;
		    }
		}	
	}


}
