package ViewManager;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class VmXmlHandler {

	private static XMLConfiguration deltaPreaggMapping;
	private static XMLConfiguration deltaSelectionMapping;
	private static XMLConfiguration deltaReverseJoinMapping;
	private static XMLConfiguration rjJoinMapping;
	private static XMLConfiguration leftJoinSchema;
	private static XMLConfiguration rightJoinSchema;
	private static XMLConfiguration innerJoinSchema;
	private static XMLConfiguration joinAggMapping;
	
	
	private static VmXmlHandler _instance = null;

	private VmXmlHandler() {
		deltaPreaggMapping = new XMLConfiguration();
		deltaPreaggMapping.setDelimiterParsingDisabled(true);
		
		deltaSelectionMapping = new XMLConfiguration();
		deltaSelectionMapping.setDelimiterParsingDisabled(true);
		
		deltaReverseJoinMapping = new XMLConfiguration();
		deltaReverseJoinMapping.setDelimiterParsingDisabled(true);
		
		rjJoinMapping = new XMLConfiguration();
		rjJoinMapping.setDelimiterParsingDisabled(true);
		
		leftJoinSchema = new XMLConfiguration();
		leftJoinSchema.setDelimiterParsingDisabled(true);
		
		rightJoinSchema = new XMLConfiguration();
		rightJoinSchema.setDelimiterParsingDisabled(true);
		
		innerJoinSchema = new XMLConfiguration();
		innerJoinSchema.setDelimiterParsingDisabled(true);
		
		joinAggMapping = new XMLConfiguration();
		joinAggMapping.setDelimiterParsingDisabled(true);
		
		
		try {
			deltaPreaggMapping.load("ViewManager/properties/Delta_PreAgg_mapping.xml");
			deltaSelectionMapping.load("ViewManager/properties/Delta_Selection_mapping.xml");
			deltaReverseJoinMapping.load("ViewManager/properties/Delta_RJ_mapping.xml");
			rjJoinMapping.load("ViewManager/properties/RJ_Join.xml");
			leftJoinSchema.load("ViewManager/properties/leftJoinSchema.xml");
			rightJoinSchema.load("ViewManager/properties/rightJoinSchema.xml");
			innerJoinSchema.load("ViewManager/properties/innerJoinSchema.xml");
			joinAggMapping.load("ViewManager/properties/Join_Agg_Mapping.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}


	public synchronized static VmXmlHandler getInstance() {
		if (_instance == null)
			_instance = new VmXmlHandler();
		return _instance;
	}


	public XMLConfiguration getDeltaPreaggMapping(){
		return deltaPreaggMapping;
	}
	
	public XMLConfiguration getDeltaSelectionMapping(){
		return deltaSelectionMapping;
	}
	
	public XMLConfiguration getDeltaReverseJoinMapping(){
		return deltaReverseJoinMapping;
	}
	
	public XMLConfiguration getRjJoinMapping(){
		return rjJoinMapping;
	}
	
	public XMLConfiguration getlJSchema(){
		return leftJoinSchema;
	}
	
	public XMLConfiguration getrJSchema(){
		return rightJoinSchema;
	}
	
	public XMLConfiguration getiJSchema(){
		return innerJoinSchema;
	}
	
	public XMLConfiguration getJoinAggMapping(){
		return joinAggMapping;
	}

}
