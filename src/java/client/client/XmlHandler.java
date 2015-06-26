package client.client;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class XmlHandler {

	private static XMLConfiguration databaseConfig;
	private static XMLConfiguration viewConfig;
	private static XMLConfiguration clusterConfig;
	private static XmlHandler _instance = null;

	private XmlHandler() {
		databaseConfig = new XMLConfiguration();
		databaseConfig.setDelimiterParsingDisabled(true);

		viewConfig = new XMLConfiguration();
		viewConfig.setDelimiterParsingDisabled(true);

		clusterConfig = new XMLConfiguration();
		clusterConfig.setDelimiterParsingDisabled(true);

		try {
			databaseConfig.load("client/resources/DatabaseConfig.xml");
			viewConfig.load("client/resources/ViewConfig.xml");
			clusterConfig.load("client/resources/ClusterConfig.xml");
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

	public XMLConfiguration getViewConfig(){
		return viewConfig;
	}

}
