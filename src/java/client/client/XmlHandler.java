package client.client;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class XmlHandler {

	private static XMLConfiguration databaseConfig;
	private static XMLConfiguration selectViewConfig;
	private static XMLConfiguration clusterConfig;
	private static XMLConfiguration sumViewConfig;
	private static XmlHandler _instance = null;

	private XmlHandler() {
		databaseConfig = new XMLConfiguration();
		databaseConfig.setDelimiterParsingDisabled(true);

		selectViewConfig = new XMLConfiguration();
		selectViewConfig.setDelimiterParsingDisabled(true);

		clusterConfig = new XMLConfiguration();
		clusterConfig.setDelimiterParsingDisabled(true);
		
		sumViewConfig = new XMLConfiguration();
		sumViewConfig.setDelimiterParsingDisabled(true);

		try {
			databaseConfig.load("client/resources/DatabaseConfig.xml");
			selectViewConfig.load("client/resources/SelectViewConfig.xml");
			clusterConfig.load("client/resources/ClusterConfig.xml");
			sumViewConfig.load("client/resources/SumViewConfig.xml");
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
	
	public XMLConfiguration getSumViewConfig(){
		return sumViewConfig;
	}

}
