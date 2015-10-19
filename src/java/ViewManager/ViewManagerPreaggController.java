package ViewManager;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.json.simple.JSONObject;

import com.datastax.driver.core.Cluster;

public class ViewManagerPreaggController implements Runnable{

	private Stream stream = null;
	ViewManager vm;
	Cluster cluster;
	List<String> havingJoinGroupBy;
	List<String> preaggTableNames;
	TaskDistributor td;
	List<String> vm_identifiers;
	int identifier_index;
	
	public ViewManagerPreaggController(ViewManager vm,Cluster cluster,TaskDistributor td) {	
		System.out.println("up preag");
		this.vm = vm;
		this.cluster = cluster;
		this.td = td;

		parseXML();	
	}


	private void parseXML() {
		preaggTableNames = VmXmlHandler.getInstance().getHavingPreAggMapping()
				.getList("mapping.unit.preaggTable");
		vm_identifiers = VmXmlHandler.getInstance().getVMProperties().getList("vm.identifier");
		identifier_index = vm_identifiers.indexOf(vm.getIdentifier());
	}


	public void decidePreagg(JSONObject json) {

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


		stream = Serialize.deserializeStream(bufferString);
		JSONObject deltaJSON = stream.getDeltaJSON();

		if(!stream.isDeleteOperation()){
			propagatePreaggUpdate(deltaJSON,table);
		}else{
			propagatePreaggDelete(deltaJSON,table);
		}
	}

	public void propagatePreaggUpdate(JSONObject json, String table) {

		String preaggTable = table;

		// 2.1 update preaggregations with having clause
		// check if preagg has some having clauses or not
		int position1 = preaggTableNames.indexOf(preaggTable);

		if (position1 != -1) {

			String temp4 = "mapping.unit(";
			temp4 += Integer.toString(position1);
			temp4 += ")";

			int nrConditions = VmXmlHandler.getInstance()
					.getHavingPreAggMapping().getInt(temp4 + ".nrCond");

			for (int m = 0; m < nrConditions; m++) {

				String s1 = temp4 + ".Cond(" + Integer.toString(m)
						+ ")";
				String havingTable = VmXmlHandler.getInstance()
						.getHavingPreAggMapping()
						.getString(s1 + ".name");

				String nrAnd = VmXmlHandler.getInstance()
						.getHavingPreAggMapping()
						.getString(s1 + ".nrAnd");

				boolean eval1 = true;
				CustomizedRow PreagRow = stream.getUpdatedPreaggRow();

				if(PreagRow!=null){

					for (int n = 0; n < Integer.parseInt(nrAnd); n++) {

						String s11 = s1 + ".And(";
						s11 += Integer.toString(n);
						s11 += ")";

						String aggFct = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s11 + ".aggFct");
						String operation = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s11 + ".operation");
						String value = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s11 + ".value");



						eval1&= Utils.evalueJoinAggConditions(PreagRow, aggFct, operation, value);
					}

					// if matching now & not matching before
					// if condition matching now & matched before
					if (eval1) {
						vm.updateHaving(stream.getDeltaUpdatedRow(),
								json,havingTable, PreagRow);
						// if not matching now
					} else if (!eval1) {
						vm.deleteRowHaving((String) json.get("keyspace"),
								havingTable, PreagRow);
						// if not matching now & not before, ignore
					}
				}

				CustomizedRow deletedRow = stream.getUpdatedPreaggRowDeleted();
				if (deletedRow != null) {
					vm.deleteRowHaving((String) json.get("keyspace"),
							havingTable, deletedRow);
				}
			}
		} else {
			System.out
			.println("No Having table for this joinpreaggregation Table "
					+ preaggTable + " available");
		}
		
		System.out.println("saving execPtrPreagg "+ json.get("readPtr").toString());
		
		
		VmXmlHandler.getInstance().getVMProperties().setProperty("vm("+identifier_index+").execPtrPreagg", json.get("readPtr").toString());
		try {
			
			VmXmlHandler.getInstance().getVMProperties().save(VmXmlHandler.getInstance().getVMProperties().getFile());
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void propagatePreaggDelete(JSONObject json, String table) {

		// update the corresponding preagg wih having clause

		String preaggTable = table;
		int position = preaggTableNames.indexOf(preaggTable);

		if (position != -1) {

			String temp4 = "mapping.unit(";
			temp4 += Integer.toString(position);
			temp4 += ")";

			int nrConditions = VmXmlHandler.getInstance()
					.getHavingPreAggMapping().getInt(temp4 + ".nrCond");

			for (int r = 0; r < nrConditions; r++) {

				String s1 = temp4 + ".Cond(" + Integer.toString(r)
						+ ")";
				String havingTable = VmXmlHandler.getInstance()
						.getHavingPreAggMapping()
						.getString(s1 + ".name");

				String nrAnd = VmXmlHandler.getInstance()
						.getHavingPreAggMapping()
						.getString(s1 + ".nrAnd");

				boolean eval1 = true;

				if(stream.getUpdatedPreaggRow()!=null){

					for (int j = 0; j < Integer.parseInt(nrAnd); j++) {

						String s11 = s1 + ".And(";
						s11 += Integer.toString(j);
						s11 += ")";

						String aggFct = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s11 + ".aggFct");
						String operation = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s11 + ".operation");
						String value = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s11 + ".value");


						eval1&= Utils.evalueJoinAggConditions(stream.getUpdatedPreaggRow(), aggFct, operation, value);

					}

					if (eval1) {
						vm.updateHaving(stream.getDeltaDeletedRow(),
								json,havingTable, stream.getUpdatedPreaggRow());
					} else {
						vm.deleteRowHaving((String) json.get("keyspace"),
								havingTable, stream.getUpdatedPreaggRow());
					}
				}

				CustomizedRow DeletedPreagRow = stream.getUpdatedPreaggRowDeleted();

				if (DeletedPreagRow != null) {
					vm.deleteRowHaving((String) json.get("keyspace"),
							havingTable, DeletedPreagRow);
				}
			}
		}
		System.out.println("saving execPtrPreagg "+ json.get("readPtr").toString());
		
		
		VmXmlHandler.getInstance().getVMProperties().setProperty("vm("+identifier_index+").execPtrPreagg", json.get("readPtr").toString());
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

			while(!td.preAgg.isEmpty()){
				JSONObject head = td.preAgg.remove();
				decidePreagg(head);
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
