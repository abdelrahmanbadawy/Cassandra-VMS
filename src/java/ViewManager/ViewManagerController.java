package ViewManager;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class ViewManagerController {


	Cluster currentCluster = null;
	ViewManager vm = null;
	private static XMLConfiguration baseTableKeysConfig;
	List<String> baseTableName;
	List<String> pkName;
	List<String> deltaTableName;


	public ViewManagerController(){

		connectToCluster();
		retrieveLoadXmlHandlers();
		parseXmlMapping();

		vm = new ViewManager(currentCluster);

	} 

	private void retrieveLoadXmlHandlers() {
		baseTableKeysConfig = new XMLConfiguration();
		baseTableKeysConfig.setDelimiterParsingDisabled(true);

		try {
			baseTableKeysConfig.load("ViewManager/properties/baseTableKeys.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} 

	}

	public void parseXmlMapping(){
		baseTableName = baseTableKeysConfig.getList("tableSchema.table.name");
		pkName = baseTableKeysConfig.getList("tableSchema.table.pkName");
		deltaTableName  = VmXmlHandler.getInstance().getDeltaPreaggMapping().
				getList("mapping.unit.deltaTable");
	}


	private void connectToCluster(){

		currentCluster = Cluster
				.builder()
				.addContactPoint(
						XmlHandler.getInstance().getClusterConfig()
						.getString("config.host.localhost"))
						.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
						.withLoadBalancingPolicy(
								new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
								.build();

	}

	public void update(JSONObject json){

		//===================================================================================

		//get position of basetable from xml list
		//retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json.get("table"));
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		Row deltaUpdatedRow = null;

		// 1. update Delta Table
		// 1.a If successful, retrieve entire updated Row from Delta to pass on as streams

		if(vm.updateDelta(json,indexBaseTableName,baseTablePrimaryKey)){
			deltaUpdatedRow = vm.getDeltaUpdatedRow();	
		}

		//===================================================================================
		//2. for the delta table updated, get the depending preaggregation/agg tables
		//preagg tables hold all column values, hence they have to be updated

		int position = deltaTableName.indexOf("delta_"+(String) json.get("table"));

		if(position!=-1){

			String temp= "mapping.unit(";
			temp+=Integer.toString(position);
			temp+=")";

			int nrPreagg = VmXmlHandler.getInstance().getDeltaPreaggMapping().
					getInt(temp+".nrPreagg");

			for(int i=0;i<nrPreagg;i++){

				String s = temp+".Preagg("+Integer.toString(i)+")";
				String AggKey = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".AggKey");
				String AggKeyType = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".AggKeyType");
				String preaggTable = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".name");
				String AggCol = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".AggCol");
				String AggColType = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".AggColType");


				//2.a after getting the preagg table name & neccessary parameters,
				//check if aggKey in delta (_old & _new ) is null
				//if null then dont update, else update

				boolean isNull = checkIfAggIsNull(AggKey,deltaUpdatedRow);

				if(! isNull){

					// by passing the whole delta Row, we have agg key value even if it is not in json
					vm.updatePreaggregation(deltaUpdatedRow,AggKey,AggKeyType,json,preaggTable,baseTablePrimaryKey,AggCol,AggColType,false);
				}

			}
		}else{
			System.out.println("No Preaggregation table for this delta table "+" delta_"+(String) json.get("table")+" available");
		}

		//===================================================================================================================















	}

	private boolean checkIfAggIsNull(String aggKey, Row deltaUpdatedRow) {

		if(deltaUpdatedRow!=null){	
			ColumnDefinitions colDef = deltaUpdatedRow.getColumnDefinitions();
			int indexNew = colDef.getIndexOf(aggKey+"_new");
			int indexOld = colDef.getIndexOf(aggKey+"_old");

			if(deltaUpdatedRow.isNull(indexNew) && deltaUpdatedRow.isNull(indexOld)){
				return true;
			}
		}

		return false;
	}

	public void cascadeDelete(JSONObject json){

		//===================================================================================

		//get position of basetable from xml list
		//retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json.get("table"));
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		Row deltaDeletedRow = null;

		// 1. delete from Delta Table
		// 1.a If successful, retrieve entire delta Row from Delta to pass on as streams

		if(vm.deleteRowDelta(json)){
			deltaDeletedRow = vm.getDeltaDeletedRow();	
		}

		//=================================================================================

		//===================================================================================
		//2. for the delta table updated, get the depending preaggregation/agg tables
		//preagg tables hold all column values, hence they have to be updated

		int position = deltaTableName.indexOf("delta_"+(String) json.get("table"));

		if(position!=-1){

			String temp= "mapping.unit(";
			temp+=Integer.toString(position);
			temp+=")";

			int nrPreagg = VmXmlHandler.getInstance().getDeltaPreaggMapping().
					getInt(temp+".nrPreagg");

			for(int i=0;i<nrPreagg;i++){

				String s = temp+".Preagg("+Integer.toString(i)+")";
				String AggKey = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".AggKey");
				String AggKeyType = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".AggKeyType");
				String preaggTable = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".name");
				String AggCol = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".AggCol");
				String AggColType = VmXmlHandler.getInstance().getDeltaPreaggMapping().
						getString(s+".AggColType");

				// by passing the whole delta Row, we have agg key value even if it is not in json
				vm.deleteRowPreaggAgg(deltaDeletedRow,baseTablePrimaryKey,json,preaggTable,AggKey,AggKeyType,AggCol,AggColType);

			}
		}else{
			System.out.println("No Preaggregation table for this delta table "+" delta_"+(String) json.get("table")+" available");
		}




	}

}
