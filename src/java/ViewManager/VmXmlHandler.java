package ViewManager;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.*;


public class VmXmlHandler {

	private static XMLConfiguration deltaPreaggMapping;
	private static XMLConfiguration deltaSelectionMapping;
	private static XMLConfiguration deltaReverseJoinMapping;
	private static XMLConfiguration rjJoinMapping;
	private static XMLConfiguration leftJoinSchema;
	private static XMLConfiguration rightJoinSchema;
	private static XMLConfiguration innerJoinSchema;
	private static XMLConfiguration joinAggMapping;
	private static XMLConfiguration havingAggMapping;
	private static XMLConfiguration havingJoinAggMapping;
	private static XMLConfiguration innerJoinMaps;

	private static XMLConfiguration rjAggJoinMapping;
	private static XMLConfiguration rjAggJoinGroupByMapping;
	private static XMLConfiguration rjAggJoinGroupByHavingMapping;
	
	private static XMLConfiguration vmProperties;


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

		havingAggMapping = new XMLConfiguration();
		havingAggMapping.setDelimiterParsingDisabled(true);

		havingJoinAggMapping = new XMLConfiguration();
		havingJoinAggMapping.setDelimiterParsingDisabled(true);

		innerJoinMaps = new XMLConfiguration();
		innerJoinMaps.setDelimiterParsingDisabled(true);

		rjAggJoinMapping = new XMLConfiguration();
		rjAggJoinMapping.setDelimiterParsingDisabled(true);

		rjAggJoinGroupByMapping = new XMLConfiguration();
		rjAggJoinGroupByMapping.setDelimiterParsingDisabled(true);

		rjAggJoinGroupByHavingMapping = new XMLConfiguration();
		rjAggJoinGroupByHavingMapping.setDelimiterParsingDisabled(true);
		
		vmProperties = new XMLConfiguration();
		vmProperties.setDelimiterParsingDisabled(true);
		
			
		try {
			deltaPreaggMapping.load("ViewManager/properties/Delta_PreAgg_mapping.xml");
			deltaSelectionMapping.load("ViewManager/properties/Delta_Selection_mapping.xml");
			deltaReverseJoinMapping.load("ViewManager/properties/Delta_RJ_mapping.xml");
			rjJoinMapping.load("ViewManager/properties/RJ_Join.xml");
			leftJoinSchema.load("ViewManager/properties/leftJoinSchema.xml");
			rightJoinSchema.load("ViewManager/properties/rightJoinSchema.xml");
			innerJoinSchema.load("ViewManager/properties/innerJoinSchema.xml");
			joinAggMapping.load("ViewManager/properties/Join_Agg_Mapping.xml");
			havingAggMapping.load("ViewManager/properties/Having_Preagg_mapping.xml");
			havingJoinAggMapping.load("ViewManager/properties/Having_Join_Preagg_mapping.xml");
			innerJoinMaps.load("ViewManager/properties/Inner_JoinAgg_ColinMap.xml");
			rjAggJoinMapping.load("ViewManager/properties/RJ_AggJoin.xml");
			rjAggJoinGroupByMapping.load("ViewManager/properties/RJ_Join_GroupBy.xml");
			rjAggJoinGroupByHavingMapping.load("ViewManager/properties/Having_GroupBy.xml");
			vmProperties.load("vm_prop/vm_properties.xml");
			
			
			
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

	public XMLConfiguration getHavingPreAggMapping(){
		return havingAggMapping;
	}


	public  XMLConfiguration getHavingJoinAggMapping() {
		return havingJoinAggMapping;
	}

	public  XMLConfiguration getInnerJoinMap() {
		return innerJoinMaps;
	}

	public  XMLConfiguration getRJAggJoinMapping() {
		return rjAggJoinMapping;
	}

	public  XMLConfiguration getRJAggJoinGroupByMapping() {
		return rjAggJoinGroupByMapping;
	}

	public  XMLConfiguration getRJAggJoinGroupByHavingMapping() {
		return rjAggJoinGroupByHavingMapping;
	}
	
	public  XMLConfiguration getVMProperties() {
		return vmProperties;
	}

	
	public synchronized void save(File file){
		try {
			getVMProperties().save(file);
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}	
	}

}
