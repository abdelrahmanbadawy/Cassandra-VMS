package client.client;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class XmlHandler {

	private static XMLConfiguration databaseConfig;
	private static XMLConfiguration selectViewConfig;
	private static XMLConfiguration clusterConfig;
	private static XMLConfiguration aggViewConfig;
	private static XMLConfiguration deltaViewConfig;
	private static XMLConfiguration preAggViewConfig;
	
	private static XmlHandler _instance = null;

	private XmlHandler() {
		databaseConfig = new XMLConfiguration();
		databaseConfig.setDelimiterParsingDisabled(true);

		selectViewConfig = new XMLConfiguration();
		selectViewConfig.setDelimiterParsingDisabled(true);

		clusterConfig = new XMLConfiguration();
		clusterConfig.setDelimiterParsingDisabled(true);
		
		aggViewConfig = new XMLConfiguration();
		aggViewConfig.setDelimiterParsingDisabled(true);
		
		deltaViewConfig = new XMLConfiguration();
		deltaViewConfig.setDelimiterParsingDisabled(true);
		
		preAggViewConfig = new XMLConfiguration();
		preAggViewConfig.setDelimiterParsingDisabled(true);
		
		
		try {
			databaseConfig.load("client/resources/DatabaseConfig.xml");
			selectViewConfig.load("client/resources/SelectViewConfig.xml");
			clusterConfig.load("client/resources/ClusterConfig.xml");
			aggViewConfig.load("client/resources/AggregationViewConfig.xml");
			deltaViewConfig.load("client/resources/DeltaViewConfig.xml");
			preAggViewConfig.load("client/resources/Preaggregation.xml"); 
			
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}


	public synchronized static XmlHandler getInstance() {
		if (_instance == null)
			_instance = new XmlHandler();
		return _instance;
	}


	public XMLConfiguration getDatabaseConfig(){
		return databaseConfig;
	}

	public XMLConfiguration getClusterConfig(){
		return clusterConfig;
	}

	public XMLConfiguration getSelectViewConfig(){
		return selectViewConfig;
	}
	
	public XMLConfiguration getAggViewConfig(){
		return aggViewConfig;
	}
	
	public XMLConfiguration getDeltaViewConfig(){
		return deltaViewConfig;
	}
	
	public XMLConfiguration getPreAggViewConfig(){
		return preAggViewConfig;
	}
	


}
