package ViewManager;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
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

		//get position of basetable from xml list
		//retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json.get("table"));
        String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		
        
        // 1. update Delta Table
        // 1.a If successful, retrieve entire updated Row from Delta
		if(vm.updateDelta(json,indexBaseTableName,baseTablePrimaryKey)){
			Row deltaUpdatedRow = vm.getDeltaUpdatedRow();
			
		}

	}

	public void cascadeDelete(JSONObject json){

	}

}
