package ViewManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.simple.JSONObject;

import client.client.Client;
import client.client.XmlHandler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.PreparedStatement;
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
		boolean firstInsertion = false;

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

			firstInsertion = true;

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

		updatePreaggregation(firstInsertion,keyspace,table,pkName.get(indexBaseTableName),data.get(pkName.get(indexBaseTableName)).toString(),json);

		firstInsertion = false;

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

	public boolean updatePreaggregation(boolean firstInsertion, String keyspace, String table, String pk, String pkValue, JSONObject json){

		List<String> deltaTable  = VmXmlHandler.getInstance().getDeltaPreaggMapping().
				getList("mapping.unit.deltaTable");

		JSONObject data = (JSONObject) json.get("data");

		int position = deltaTable.indexOf("delta_"+table);

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


				if(firstInsertion && data.containsKey(AggKey) && data.containsKey(AggCol)){
					newInsertPreaggregation(keyspace,preaggTable,pk,pkValue,AggKey,AggKeyType,AggCol,AggColType,json);

				}else if(!firstInsertion && data.containsKey(AggKey) && data.containsKey(AggCol)){
					InsertPreaggregation(keyspace,preaggTable,pk,pkValue,AggKey,AggKeyType,AggCol,AggColType,json,"delta_"+table);	

				}else if(!firstInsertion && (!data.containsKey(AggKey)) && data.containsKey(AggCol)){
					// TO DO 
				}else if(!firstInsertion && data.containsKey(AggKey) && (!data.containsKey(AggCol))){
					// TO DO
				}


			}
		}else{
			System.out.println("No Preaggregatin table for this delta table available");
			return true;
		}


		return true;
	}

	private boolean updateAggregation(String keyspace, String preaggTable, String aggKey, String aggKeyValue, Map myMap) {

		List<String> PreTable  = VmXmlHandler.getInstance().getPreaggAggMapping().
				getList("mapping.unit.Preagg");
		
		
		int position = PreTable.indexOf(preaggTable);

		if(position!=-1){

			String temp= "mapping.unit(";
			temp+=Integer.toString(position);
			temp+=").Agg";

			String aggTable = VmXmlHandler.getInstance().getPreaggAggMapping().
					getString(temp+".name");

			String aggKeyType = VmXmlHandler.getInstance().getPreaggAggMapping().
					getString(temp+".AggKeyType");

			insertAggregation(keyspace,preaggTable,aggKey,aggKeyValue,myMap,aggTable,aggKeyType);		

		}

		return true;
	}




	private boolean insertAggregation(String keyspace, String preaggTable, String aggKey, String aggKeyValue, Map myMap, String aggTable,String aggKeyType) {

		float count = myMap.size();
		float sum = 0;


		for (Object value : myMap.values()) {

			sum+= Float.valueOf(value.toString());
		}

		float avg = sum/count;


		StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
		insertQueryAgg.append(keyspace).append(".")
		.append(aggTable).append(" ( ")
		.append(aggKey)
		.append(", sum, count, average) VALUES (");

		insertQueryAgg.append(aggKeyValue+", ").append((int)sum).append(", ").append((int)count).append(", ").append(avg)
		.append(");");

		System.out.println(insertQueryAgg);

		try {

			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}


		return true;

	}


	private boolean InsertPreaggregation(String keyspace, String preaggTable,
			String pk, String pkValue, String aggKey, String aggKeyType,
			String aggCol, String aggColType, JSONObject json, String deltaTable) {

		JSONObject data = (JSONObject) json.get("data");


		StringBuilder selectPreaggQuery = new StringBuilder("SELECT ").append(aggKey+"_old, ")
				.append(aggKey+"_new, ").append(aggCol+"_old, ").append(aggCol+"_new ")
				.append(" FROM ").append(keyspace).append(".")
				.append(deltaTable).append(" where ")
				.append(pk).append(" = ").append(pkValue);

		System.out.println(selectPreaggQuery);


		ResultSet deltaRow;
		try {

			Session session = currentCluster.connect();
			deltaRow = session.execute(selectPreaggQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}


		Row theRow = deltaRow.one();

		if(theRow==null){
			// TO DO
			// row got deleted men el delta table, what to do now ?
		}else{

			boolean sameKeyValue = false;
			boolean sameColValue = false;

			switch (aggKeyType) {

			case "text":
				sameKeyValue = theRow.getString(aggKey+"_old").equals(theRow.getString(aggKey+"_new"));
				break;

			case "int":
				if(theRow.getInt(aggKey+"_old")==theRow.getInt(aggKey+"new")){
					sameKeyValue = true;
				}
				break;

			case "varint":
				if(theRow.getVarint(aggKey+"_old")==theRow.getVarint(aggKey+"new")){
					sameKeyValue = true;
				}
				break;

			case "varchar":
				sameKeyValue = theRow.getString(aggKey+"_old").equals(theRow.getString(aggKey+"_new"));
				break;	
			}

			String stringRepresenation = "";


			switch (aggColType) {

			case "text":
				stringRepresenation =data.get(aggCol).toString();
				sameColValue = theRow.getString(aggCol+"_old").equals(theRow.getString(aggCol+"_new"));
				break;

			case "int":
				if(theRow.getInt(aggCol+"_old")==theRow.getInt(aggCol+"_new")){
					sameColValue = true;
				}
				Integer i = new Integer(data.get(aggCol).toString());
				stringRepresenation = i.toString();
				break;

			case "varint":
				if(theRow.getVarint(aggCol+"_old")==theRow.getVarint(aggCol+"_new")){
					sameColValue = true;
				}
				Integer v = new Integer(data.get(aggCol).toString());
				stringRepresenation = v.toString();
				break;

			case "varchar":
				stringRepresenation =data.get(aggCol).toString();
				sameColValue = theRow.getString(aggCol+"_old").equals(theRow.getString(aggCol+"_new"));				

				break;	

			case "float":
				if(theRow.getFloat(aggCol+"_old")==theRow.getFloat(aggCol+"_new")){
					sameColValue = true;
				}
				Float f = new Float(data.get(aggCol).toString());
				stringRepresenation = f.toString();

				break;
			}



			if(sameKeyValue && !sameColValue){

				StringBuilder selectPreaggQuery1 = new StringBuilder("SELECT ").append("list_item");
				selectPreaggQuery1.append(" FROM ").append(keyspace).append(".")
				.append(preaggTable).append(" where ")
				.append(aggKey+ " = ").append(data.get(aggKey).toString());

				System.out.println(selectPreaggQuery1);

				ResultSet PreAggMap;
				try {

					Session session = currentCluster.connect();
					PreAggMap = session.execute(selectPreaggQuery1.toString());

				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}

				Row theRow1 = PreAggMap.one();

				if (theRow1 == null) {

					// TO DO
					// row has been deleted men el preaggregation table

				} else {
					Map<String, String> tempMapImmutable= theRow1.getMap("list_item",String.class, String.class);

					HashMap<String, String> myMap = new HashMap<String,String>();
					myMap.putAll(tempMapImmutable);
					myMap.remove(pkValue);

					myMap.put(pkValue, stringRepresenation);
					StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
					insertQueryAgg.append(keyspace).append(".")
					.append(preaggTable).append(" ( ")
					.append(aggKey+", ")
					.append("list_item").append(") VALUES (")
					.append(data.get(aggKey)+", ").append("?);");

					try {

						Session session = currentCluster.connect();

						PreparedStatement statement = session.prepare(insertQueryAgg.toString());
						BoundStatement boundStatement = statement.bind(myMap);
						System.out.println(boundStatement.toString());
						session.execute(boundStatement);

						updateAggregation(keyspace,preaggTable,aggKey,data.get(aggKey).toString(),myMap);

					} catch (Exception e) {
						e.printStackTrace();
						return false;
					}
				}
			}else if((!sameKeyValue && sameColValue)|| (!sameKeyValue && !sameColValue)){

				//get row key of aggKey_old
				StringBuilder selectPreaggQuery1 = new StringBuilder("SELECT ").append("list_item");
				selectPreaggQuery1.append(" FROM ").append(keyspace).append(".")
				.append(preaggTable).append(" where ")
				.append(aggKey+ " = ");


				switch (aggKeyType) {

				case "text":
					selectPreaggQuery1.append("'"+theRow.getString(aggKey+"_old")+"';");
					break;

				case "int":
					selectPreaggQuery1.append(theRow.getInt(aggKey+"_old")+";");
					break;

				case "varint":
					selectPreaggQuery1.append(theRow.getVarint(aggKey+"_old")+";");
					break;

				case "varchar":
					selectPreaggQuery1.append("'"+theRow.getString(aggKey+"_old")+"';");
					break;	

				case "float":
					selectPreaggQuery1.append(theRow.getFloat(aggKey+"_old")+";");
					break;

				}

				System.out.println(selectPreaggQuery1);

				ResultSet PreAggMap;
				try {

					Session session = currentCluster.connect();
					PreAggMap = session.execute(selectPreaggQuery1.toString());

				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}

				Row theRow2 = PreAggMap.one();

				if(theRow2==null){
					// TO DO
					// Row has been deleted
				}else{
					Map<String, String> tempMapImmutable= theRow2.getMap("list_item",String.class, String.class);

					HashMap<String, String> myMap = new HashMap<String,String>();
					myMap.putAll(tempMapImmutable);
					myMap.remove(pkValue);

					if(myMap.isEmpty()){
						DeletePreaggregation();

					}else{

						StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
						insertQueryAgg.append(keyspace).append(".")
						.append(preaggTable).append(" ( ")
						.append(aggKey+", ")
						.append("list_item").append(") VALUES (");


						StringBuilder aggKeyOld = new StringBuilder() ;

						switch (aggKeyType) {

						case "text":
							insertQueryAgg.append("'"+theRow.getString(aggKey+"_old")+"', ");
							aggKeyOld.append("'"+theRow.getString(aggKey+"_old")+"'"); 
							break;

						case "int":
							insertQueryAgg.append(theRow.getInt(aggKey+"_old")+", ");
							aggKeyOld.append(theRow.getInt(aggKey+"_old")); 
							break;

						case "varint":
							insertQueryAgg.append(theRow.getVarint(aggKey+"_old")+", ");
							aggKeyOld.append(theRow.getVarint(aggKey+"_old"));
							break;

						case "varchar":
							insertQueryAgg.append("'"+theRow.getString(aggKey+"_old")+"', ");
							aggKeyOld.append("'"+theRow.getString(aggKey+"_old")+"'");
							break;	

						case "float":
							insertQueryAgg.append(theRow.getFloat(aggKey+"_old")+", ");
							aggKeyOld.append(theRow.getFloat(aggKey+"_old"));
							break;

						}
						insertQueryAgg.append("?);");

						try {

							Session session = currentCluster.connect();

							PreparedStatement statement = session.prepare(insertQueryAgg.toString());
							BoundStatement boundStatement = statement.bind(myMap);
							System.out.println(boundStatement.toString());
							session.execute(boundStatement);

							updateAggregation(keyspace,preaggTable,aggKey,aggKeyOld.toString(),myMap);

						} catch (Exception e) {
							e.printStackTrace();
							return false;
						}

						newInsertPreaggregation(keyspace, preaggTable, pk, pkValue, aggKey, aggKeyType, aggCol, aggColType, json);

					}
				}

			}else{
				return true;
			}

		}

		return true;

	}


	private boolean  DeletePreaggregation() {
		// TODO 

		return true;
	}


	private boolean newInsertPreaggregation( String keyspace, String preaggTable, String pk, String pkValue, String aggKey, String aggKeyType, String aggCol, String aggColType, JSONObject json){


		JSONObject data = (JSONObject) json.get("data");

		//Select from PreAggtable to prevent overwrites

		StringBuilder selectPreaggQuery = new StringBuilder("SELECT ").append("list_item");
		selectPreaggQuery.append(" FROM ").append(keyspace).append(".")
		.append(preaggTable).append(" where ")
		.append(aggKey+ " = ").append(data.get(aggKey).toString());

		System.out.println(selectPreaggQuery);


		ResultSet PreAggMap;
		try {

			Session session = currentCluster.connect();
			PreAggMap = session.execute(selectPreaggQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}


		Row theRow = PreAggMap.one();

		StringBuilder insertQueryAgg = new StringBuilder("INSERT INTO ");
		insertQueryAgg.append(keyspace).append(".")
		.append(preaggTable).append(" ( ")
		.append(aggKey+", ")
		.append("list_item").append(") VALUES (")
		.append(data.get(aggKey)+", ").append("?);");

		String stringRepresenation = "";

		switch (aggColType) {

		case "text":
			stringRepresenation =data.get(aggCol).toString();
			break;

		case "int":
			Integer i = new Integer(data.get(aggCol).toString());
			stringRepresenation = i.toString();

			break;

		case "varint":
			Integer v = new Integer(data.get(aggCol).toString());
			stringRepresenation = v.toString();

			break;

		case "float":

			Float f = new Float(data.get(aggCol).toString());
			stringRepresenation = f.toString();

			break;
		}


		Map myMap;

		if (theRow == null) {

			myMap = new HashMap<String,String>();
			myMap.put(pkValue, stringRepresenation);

			System.out.println(insertQueryAgg);

		} else {


			System.out.println(theRow);

			Map<String, String> tempMapImmutable= theRow.getMap("list_item",String.class, String.class);
			//tempMap.put(currentRow.getInt(deltaPkName.get(i)), currentRow.getInt(deltaAggColName.get(i)));

			myMap = new HashMap<String,String>();
			myMap.putAll(tempMapImmutable);
			myMap.put(pkValue, stringRepresenation);
			//insertQueryAgg.append(tempMap.toString()+");");

			System.out.println(insertQueryAgg);
		}


		try {

			Session session = currentCluster.connect();

			PreparedStatement statement = session.prepare(insertQueryAgg.toString());
			BoundStatement boundStatement = statement.bind(myMap);
			System.out.println(boundStatement.toString());
			session.execute(boundStatement);

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}



		//update Aggregation
		updateAggregation(keyspace,preaggTable,aggKey,data.get(aggKey).toString(),myMap); 

		return true;
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
