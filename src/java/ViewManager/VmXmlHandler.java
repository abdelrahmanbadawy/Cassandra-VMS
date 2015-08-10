package ViewManager;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class VmXmlHandler {

	private static XMLConfiguration deltaPreaggMapping;
	private static XMLConfiguration deltaSelectionMapping;
	private static XMLConfiguration deltaReverseJoinMapping;
	private static XMLConfiguration rjLeftJoinMapping;
	
	
	private static VmXmlHandler _instance = null;

	private VmXmlHandler() {
		deltaPreaggMapping = new XMLConfiguration();
		deltaPreaggMapping.setDelimiterParsingDisabled(true);
		
		deltaSelectionMapping = new XMLConfiguration();
		deltaSelectionMapping.setDelimiterParsingDisabled(true);
		
		deltaReverseJoinMapping = new XMLConfiguration();
		deltaReverseJoinMapping.setDelimiterParsingDisabled(true);
		
		rjLeftJoinMapping = new XMLConfiguration();
		rjLeftJoinMapping.setDelimiterParsingDisabled(true);
		
		
		try {
			deltaPreaggMapping.load("ViewManager/properties/Delta_PreAgg_mapping.xml");
			deltaSelectionMapping.load("ViewManager/properties/Delta_Selection_mapping.xml");
			deltaReverseJoinMapping.load("ViewManager/properties/Delta_RJ_mapping.xml");
			rjLeftJoinMapping.load("ViewManager/properties/RJ_LeftJoin.xml");	
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
	
	public XMLConfiguration getRjLeftJoinMapping(){
		return rjLeftJoinMapping;
	}

}
