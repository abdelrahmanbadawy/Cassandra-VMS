package ViewManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.simple.JSONObject;

import client.client.Client;
import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class ViewManager {

	Cluster currentCluster = null;
	private static XMLConfiguration baseTableKeysConfig;

	public ViewManager() {
		// TODO Auto-generated constructor stub

		currentCluster = Cluster
				.builder()
				.addContactPoint(
						XmlHandler.getInstance().getClusterConfig()
						.getString("config.host.localhost"))
						.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
						.withLoadBalancingPolicy(
								new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
								.build();

		baseTableKeysConfig = new XMLConfiguration();
		baseTableKeysConfig.setDelimiterParsingDisabled(true);

		try {
			baseTableKeysConfig.load("ViewManager/properties/baseTableKeys.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} 

	}

	
	public boolean updateDelta(JSONObject json) {

		List<String> baseTableName = baseTableKeysConfig.getList("tableSchema.table.name");
		List<String> pkName = baseTableKeysConfig.getList("tableSchema.table.pkName");

		String table =  (String) json.get("table");
		String keyspace = (String) json.get("keyspace");
		JSONObject data = (JSONObject) json.get("data");

		int indexBaseTableName = baseTableName.indexOf(table);

		Set<?> keySet  = data.keySet();
		Iterator<?> keySetIterator = keySet.iterator();

		List<String> selectStatement_new_values = new ArrayList<>();
		StringBuilder selectStatement_new = new StringBuilder();
		StringBuilder selectStatement_old = new StringBuilder();


		while( keySetIterator.hasNext() ) {

			String key = (String)keySetIterator.next();

			if(key.contains(pkName.get(indexBaseTableName))){
				selectStatement_new.append(key);
				selectStatement_new.append(", ");
				selectStatement_new_values.add(data.get(key).toString());

			}else{
				selectStatement_new.append(key+"_new ,");
				selectStatement_old.append(key+"_old ,");
				selectStatement_new_values.add(data.get(key).toString());
			}


		}

		selectStatement_new.deleteCharAt(selectStatement_new.length()-1);
		selectStatement_old.deleteCharAt(selectStatement_old.length()-1);



		StringBuilder selectQuery = new StringBuilder("SELECT ").append(selectStatement_new);
		selectQuery.append(" FROM ").append(keyspace).append(".")
		.append("delta_"+table).append(" WHERE ")
		.append(pkName.get(indexBaseTableName)+" = ").append(data.get(pkName.get(indexBaseTableName))+" ;");

		System.out.println(selectQuery);

		ResultSet previousCol_newValues;

		try {

			Session session = currentCluster.connect();
			previousCol_newValues = session.execute(selectQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}


		Row theRow = previousCol_newValues.one();

		StringBuilder insertQueryAgg = new StringBuilder();

		if (theRow == null) {

			insertQueryAgg = new StringBuilder("INSERT INTO ");
			insertQueryAgg.append(keyspace).append(".")
			.append("delta_"+table).append(" ( ")
			.append(selectStatement_new)
			.append(") VALUES (");

			for(String s : selectStatement_new_values){
				insertQueryAgg.append(s).append(", ");
			}

			insertQueryAgg.deleteCharAt(insertQueryAgg.length()-2);
			

			System.out.println(insertQueryAgg);

		} else {

			insertQueryAgg = new StringBuilder("INSERT INTO ");
			insertQueryAgg.append(keyspace).append(".").append("delta_"+table+" (")
			.append(selectStatement_new)
			.append(", ")
			.append(selectStatement_old)
			.append(") VALUES (");

			Iterator<?> keySetIterator1 = keySet.iterator();

			while( keySetIterator1.hasNext() ) {
				String key = (String)keySetIterator1.next();
				insertQueryAgg.append(data.get(key).toString()).append(", ");
			}

			insertQueryAgg.deleteCharAt(insertQueryAgg.length()-2);




			int nrColumns = theRow.getColumnDefinitions().size();

			for(int i=0;i<nrColumns;i++) {

				switch (theRow.getColumnDefinitions().getType(i).toString()) {

				case "text":
					if(! theRow.getColumnDefinitions().getName(i).equals(pkName.get(indexBaseTableName))){
						insertQueryAgg.append(", '"+theRow.getString(i) + "'");
					}
					break;
				case "int":
					if(! theRow.getColumnDefinitions().getName(i).equals(pkName.get(indexBaseTableName))){
						insertQueryAgg.append(", "+theRow.getInt(i));
					}
					break;
				case "varint":
					if(! theRow.getColumnDefinitions().getName(i).equals(pkName.get(indexBaseTableName))){
						insertQueryAgg.append(", "+theRow.getVarint(i));
					}
					break;

				case "varchar":
					if(! theRow.getColumnDefinitions().getName(i).equals(pkName.get(indexBaseTableName))){
						insertQueryAgg.append(", '"+theRow.getString(i)+"'");
					}
					break;	

				}
			}
		}


		try {

			//insertQueryAgg.deleteCharAt(insertQueryAgg.length()-1);
			insertQueryAgg.append(");");
			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());
			System.out.println(insertQueryAgg.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		System.out.println("Done Delta update");

		decideSelection(keyspace,"delta_"+table,pkName.get(indexBaseTableName),data.get(pkName.get(indexBaseTableName)).toString(),json,theRow);


		return true;

	}

	private void decideSelection(String keyspace, String table, String pk, String pkValue, JSONObject json, Row oldDeltaColumns) {

		List<String> deltaTable  = VmXmlHandler.getInstance().getDeltaSelectionMapping().
				getList("mapping.unit.deltaTable");

		JSONObject data = (JSONObject) json.get("data");

		int position = deltaTable.indexOf(table);

		if(position!=-1){

			String temp= "mapping.unit(";
			temp+=Integer.toString(position);
			temp+=")";

			int nrConditions = VmXmlHandler.getInstance().getDeltaSelectionMapping().
					getInt(temp+".nrCond");

			for(int i=0;i<nrConditions;i++){

				String s = temp+".Cond("+Integer.toString(i)+")";

				String selColName = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".selectionCol");

				String selecTable = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".name");

				String operation = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".operation");

				String value = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".value");

				String type = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".type");


				if(data.containsKey(selColName)){

					switch (type) {

					case "text":

						break;

					case "int":

						String s1 = data.get(selColName).toString();
						Integer valueInt = new Integer(s1);
						int compareValue = valueInt.compareTo(new Integer(value));

						if((operation.equals(">") && (compareValue<0))){
							return;
						}else if((operation.equals("<") && (compareValue>0))){
							return;
						}else if((operation.equals("=") && (compareValue!=0))){
							return;
						}

						break;

					case "varint":

						break;

					case "float":

						break;
					}


					
				}else{
										
				}

				updateSelection(keyspace,selecTable,selColName,pk,pkValue,table);

			}
		}

	}

	public boolean updatePreaggregation(){

		return false;
	}


	public boolean updateSelection(String keyspace, String selecTable, String selColName, String pk, String pkValue,String deltaTable){


		StringBuilder selectQuery = new StringBuilder();

		selectQuery.append("SELECT * FROM ").append(keyspace)
		.append(".").append(deltaTable)
		.append(" WHERE ").append(pk).append(" = ").append(pkValue).append(";");

		System.out.println(selectQuery);

		Session session = null;
		ResultSet queryResults;

		try {

			session = currentCluster.connect();
			queryResults = session.execute(
					selectQuery.toString());

		}catch(Exception e){
			e.printStackTrace();
			return false;
		}


		Row theRow = queryResults.one();
		StringBuilder insertionSelection = new StringBuilder() ;
		StringBuilder insertionSelectionValues = new StringBuilder() ;

		if (theRow == null) {
			//record got deleted from delta table
		}else{

			List<Definition> colDef = theRow.getColumnDefinitions().asList();

			for(int i=0;i<colDef.size();i++){

				switch (theRow.getColumnDefinitions().getType(i).toString()) {

				case "text":
					if(! theRow.getColumnDefinitions().getName(i).contains("_old")){
						if( theRow.getColumnDefinitions().getName(i).contains("_new")){
							String[] split = theRow.getColumnDefinitions().getName(i).split("_");
							insertionSelection.append(split[0]+", ");

						}else{
							insertionSelection.append(theRow.getColumnDefinitions().getName(i)+", ");	
						}

						insertionSelectionValues.append("'"+theRow.getString(theRow.getColumnDefinitions().getName(i))+"', ");
					}
					break;

				case "int":
					if(! theRow.getColumnDefinitions().getName(i).contains("_old")){

						if( theRow.getColumnDefinitions().getName(i).contains("_new")){
							String[] split = theRow.getColumnDefinitions().getName(i).split("_");
							insertionSelection.append(split[0]+", ");
						}else{
							insertionSelection.append(theRow.getColumnDefinitions().getName(i)+", ");
						}

						insertionSelectionValues.append(theRow.getInt(theRow.getColumnDefinitions().getName(i))+", ");
					}
					break;

				case "varint":
					if(! theRow.getColumnDefinitions().getName(i).contains("_old")){

						if( theRow.getColumnDefinitions().getName(i).contains("_new")){
							String[] split = theRow.getColumnDefinitions().getName(i).split("_");
							insertionSelection.append(split[0]+", ");
						}else{
							insertionSelection.append(theRow.getColumnDefinitions().getName(i)+", ");
						}
						insertionSelectionValues.append(theRow.getVarint(theRow.getColumnDefinitions().getName(i))+", ");
					}
					break;

				case "varchar":
					if(! theRow.getColumnDefinitions().getName(i).contains("_old")){

						if( theRow.getColumnDefinitions().getName(i).contains("_new")){
							String[] split = theRow.getColumnDefinitions().getName(i).split("_");
							insertionSelection.append(split[0]+", ");
						}else{

							insertionSelection.append(theRow.getColumnDefinitions().getName(i)+", ");
						}
						insertionSelectionValues.append("'"+theRow.getString(theRow.getColumnDefinitions().getName(i))+"', ");
					}
					break;	

				}
			}

			insertionSelection.deleteCharAt(insertionSelection.length()-2);
			insertionSelectionValues.deleteCharAt(insertionSelectionValues.length()-2);

			StringBuilder insertQuery = new StringBuilder(
					"INSERT INTO ");
			insertQuery.append(keyspace).append(".")
			.append(selecTable).append(" (")
			.append(insertionSelection).append(") VALUES (")
			.append(insertionSelectionValues).append(");");

			System.out.println(insertQuery);

			session.execute(insertQuery.toString());

		}

		return true;
	}

}
