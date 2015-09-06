package client.client;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class XmlHandler {

	private static XMLConfiguration databaseConfig;
	private static XMLConfiguration selectViewConfig;
	private static XMLConfiguration clusterConfig;
	private static XMLConfiguration deltaViewConfig;
	private static XMLConfiguration preAggViewConfig;
	private static XMLConfiguration RJViewConfig;
	private static XMLConfiguration LeftJViewConfig;
	private static XMLConfiguration RightJViewConfig;
	private static XMLConfiguration InnerJViewConfig;
	private static XMLConfiguration joinPreAggViewConfig;
	private static XMLConfiguration havingAggConfig;
	private static XMLConfiguration havingJoinAggConfig;
	private static XMLConfiguration havingJoinAggGroupBy;
	

	private static XmlHandler _instance = null;

	private XmlHandler() {
		databaseConfig = new XMLConfiguration();
		databaseConfig.setDelimiterParsingDisabled(true);

		selectViewConfig = new XMLConfiguration();
		selectViewConfig.setDelimiterParsingDisabled(true);

		clusterConfig = new XMLConfiguration();
		clusterConfig.setDelimiterParsingDisabled(true);
		
		deltaViewConfig = new XMLConfiguration();
		deltaViewConfig.setDelimiterParsingDisabled(true);
		
		preAggViewConfig = new XMLConfiguration();
		preAggViewConfig.setDelimiterParsingDisabled(true);
		
		RJViewConfig = new XMLConfiguration();
		RJViewConfig.setDelimiterParsingDisabled(true);
		
		LeftJViewConfig = new XMLConfiguration();
		LeftJViewConfig.setDelimiterParsingDisabled(true);
		
		RightJViewConfig = new XMLConfiguration();
		RightJViewConfig.setDelimiterParsingDisabled(true);
		
		InnerJViewConfig = new XMLConfiguration();
		InnerJViewConfig.setDelimiterParsingDisabled(true);
		
		joinPreAggViewConfig = new XMLConfiguration();
		joinPreAggViewConfig.setDelimiterParsingDisabled(true);
		
		havingAggConfig = new XMLConfiguration();
		havingAggConfig.setDelimiterParsingDisabled(true);
		
		havingJoinAggConfig = new XMLConfiguration();
		havingJoinAggConfig.setDelimiterParsingDisabled(true);
		
		havingJoinAggGroupBy = new XMLConfiguration();
		havingJoinAggGroupBy.setDelimiterParsingDisabled(true);
		
		try {
			databaseConfig.load("client/resources/DatabaseConfig.xml");
			selectViewConfig.load("client/resources/SelectViewConfig.xml");
			clusterConfig.load("client/resources/ClusterConfig.xml");
			deltaViewConfig.load("client/resources/DeltaViewConfig.xml");
			preAggViewConfig.load("client/resources/Preaggregation.xml"); 
			RJViewConfig.load("client/resources/ReverseJoinViewConfig.xml"); 
			LeftJViewConfig.load("client/resources/LeftJoin.xml"); 
			RightJViewConfig.load("client/resources/RightJoin.xml");
			InnerJViewConfig.load("client/resources/InnerJoin.xml");
			joinPreAggViewConfig.load("client/resources/JoinPreaggregation.xml");
			havingAggConfig.load("client/resources/HavingAgg.xml");
			havingJoinAggConfig.load("client/resources/HavingJoinAggregation.xml");
			havingJoinAggGroupBy.load("client/resources/HavingGroupByJoin.xml");
			
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
	
	public XMLConfiguration getDeltaViewConfig(){
		return deltaViewConfig;
	}
	
	public XMLConfiguration getPreAggViewConfig(){
		return preAggViewConfig;
	}

	public XMLConfiguration getRJViewConfig(){
		return RJViewConfig;
	}
	
	public XMLConfiguration getLeftJoinViewConfig(){
		return LeftJViewConfig;
	}

	public XMLConfiguration getRightJoinViewConfig(){
		return RightJViewConfig;
	}
	
	public XMLConfiguration getInnerJoinViewConfig(){
		return InnerJViewConfig;
	}
	
	public XMLConfiguration getJoinPreagg(){
		return joinPreAggViewConfig;
	}
	
	public XMLConfiguration getHavingAgg(){
		return havingAggConfig;
	}
	
	public static XMLConfiguration getHavingJoinAggConfig() {
		return havingJoinAggConfig;
	}
	
	public static XMLConfiguration getHavingJoinAggGroupBy() {
		return havingJoinAggGroupBy;
	}



}
