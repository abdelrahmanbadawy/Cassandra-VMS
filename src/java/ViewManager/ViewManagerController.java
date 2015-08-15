package ViewManager;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
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
	List<String> deltaTableName;
	List<String> reverseTableName;

	
	List<String> rj_joinTables = VmXmlHandler.getInstance()
			.getDeltaReverseJoinMapping().getList("mapping.unit.Join.name");

	List<String> rj_joinKeys = VmXmlHandler.getInstance()
			.getDeltaReverseJoinMapping().getList("mapping.unit.Join.JoinKey");

	List<String> rj_joinKeyTypes = VmXmlHandler.getInstance()
			.getDeltaReverseJoinMapping().getList("mapping.unit.Join.type");

	List<String> rj_nrDelta = VmXmlHandler.getInstance()
			.getDeltaReverseJoinMapping().getList("mapping.unit.nrDelta");

	int rjoins = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
			.getInt("mapping.nrUnit");

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
		deltaTableName  = VmXmlHandler.getInstance().getDeltaPreaggMapping().
				getList("mapping.unit.deltaTable");
		reverseTableName = VmXmlHandler.getInstance().getRjJoinMapping().
				getList("mapping.unit.reverseJoin");
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

		//===================================================================================

		//get position of basetable from xml list
		//retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json.get("table"));
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		Row deltaUpdatedRow = null;

		// 1. update Delta Table
		// 1.a If successful, retrieve entire updated Row from Delta to pass on as streams

		if(vm.updateDelta(json,indexBaseTableName,baseTablePrimaryKey)){
			deltaUpdatedRow = vm.getDeltaUpdatedRow();	
		}

		//===================================================================================
		//update selection view
		// for each delta, loop on all selection views possible
		//check if selection condition is met based on selection key
		// if yes then update selection, if not ignore
		//also compare old values of selection condition, if they have changed then delete row from table

		int position1 = deltaTableName.indexOf("delta_"+(String) json.get("table"));

		if(position1!=-1){

			String temp4= "mapping.unit(";
			temp4+=Integer.toString(position1);
			temp4+=")";

			int nrConditions = VmXmlHandler.getInstance().getDeltaSelectionMapping().
					getInt(temp4+".nrCond");

			for(int i=0;i<nrConditions;i++){

				String s = temp4+".Cond("+Integer.toString(i)+")";
				String selecTable = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".name");

				String nrAnd = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".nrAnd");

				boolean eval = true;
				boolean eval_old = true;

				for(int j=0;j<Integer.parseInt(nrAnd);j++) {

					String s11 = s+".And(";
					s11+=Integer.toString(j);
					s11+=")";

					String operation = VmXmlHandler.getInstance().getDeltaSelectionMapping().
							getString(s11+".operation");
					String value = VmXmlHandler.getInstance().getDeltaSelectionMapping().
							getString(s11+".value");
					String type = VmXmlHandler.getInstance().getDeltaSelectionMapping().
							getString(s11+".type");

					String selColName = VmXmlHandler.getInstance().getDeltaSelectionMapping().
							getString(s11+".selectionCol");


					switch (type) {

					case "text":

						if(operation.equals("=")){
							if(deltaUpdatedRow.getString(selColName+"_new").equals(value)){
								eval &= true;
							}else{
								eval &= false;
							}

							if(deltaUpdatedRow.getString(selColName+"_old")==null){
								eval_old = false;
							}else if(deltaUpdatedRow.getString(selColName+"_old").equals(value)){
								eval_old &= true;
							}else{
								eval_old &= false;
							}
						}else if(operation.equals("!=")){
							if(!deltaUpdatedRow.getString(selColName+"_new").equals(value)){
								eval = true;
							}else{
								eval = false;
							}

							if(deltaUpdatedRow.getString(selColName+"_old")==null){
								eval_old = false;
							}else if(!deltaUpdatedRow.getString(selColName+"_old").equals(value)){
								eval_old &= true;
							}else{
								eval_old &= false;
							}
						}

						break;

					case "varchar":

						if(operation.equals("=")){
							if(deltaUpdatedRow.getString(selColName+"_new").equals(value)){
								eval &= true;
							}else{
								eval &= false;
							}

							if(deltaUpdatedRow.getString(selColName+"_old")==null){
								eval_old = false;
							}else if(deltaUpdatedRow.getString(selColName+"_old").equals(value)){
								eval_old &= true;
							}else{
								eval_old &= false;
							}
						}else if(operation.equals("!=")){
							if(!deltaUpdatedRow.getString(selColName+"_new").equals(value)){
								eval &= true;
							}else{
								eval &= false;
							}

							if(deltaUpdatedRow.getString(selColName+"_old")==null){
								eval_old = false;
							}else if(!deltaUpdatedRow.getString(selColName+"_old").equals(value)){
								eval_old &= true;
							}else{
								eval_old &= false;
							}
						}

						break;	

					case "int":

						// for _new col
						String s1 = Integer.toString(deltaUpdatedRow.getInt(selColName+"_new"));
						Integer valueInt = new Integer(s1);
						int compareValue = valueInt.compareTo(new Integer(value));

						if((operation.equals(">") && (compareValue>0))){
							eval &= true;
						}else if((operation.equals("<") && (compareValue<0))){
							eval &= true;
						}else if((operation.equals("=") && (compareValue==0))){
							eval &= true;
						}else{
							eval &= false;
						}

						// for _old col


						int v = deltaUpdatedRow.getInt(selColName+"_old");
						compareValue = valueInt.compareTo(new Integer(v));

						if((operation.equals(">") && (compareValue>0))){
							eval_old &= true;
						}else if((operation.equals("<") && (compareValue<0))){
							eval_old &= true;
						}else if((operation.equals("=") && (compareValue==0))){
							eval_old &= true;
						}else{
							eval_old &= false;
						}

						break;

					case "varint":

						// for _new col
						s1 = deltaUpdatedRow.getVarint(selColName+"_new").toString();
						valueInt = new Integer(new BigInteger(s1).intValue());
						compareValue = valueInt.compareTo(new Integer(value));

						if((operation.equals(">") && (compareValue>0))){
							eval &= true;
						}else if((operation.equals("<") && (compareValue<0))){
							eval &= true;
						}else if((operation.equals("=") && (compareValue==0))){
							eval &= true;
						}else{
							eval &= false;
						}

						// for _old col
						BigInteger bigInt = deltaUpdatedRow.getVarint(selColName+"_old");
						if (bigInt != null) {
							valueInt = bigInt.intValue();
						} else {
							valueInt = 0;
						}			
						compareValue = valueInt.compareTo(new Integer(value));

						if((operation.equals(">") && (compareValue>0))){
							eval_old &= true;
						}else if((operation.equals("<") && (compareValue<0))){
							eval_old &= true;
						}else if((operation.equals("=") && (compareValue==0))){
							eval_old &= true;
						}else{
							eval_old &= false;
						}

						break;

					case "float":
						break;
					}
				}

				// if condition matching now & matched before
				if(eval && eval_old){
					vm.updateSelection(deltaUpdatedRow,(String)json.get("keyspace"),selecTable,baseTablePrimaryKey);

					// if matching now & not matching before
				}else if(eval && !eval_old){
					vm.updateSelection(deltaUpdatedRow,(String)json.get("keyspace"),selecTable,baseTablePrimaryKey);

					//if not matching now &  matching before
				}else if(!eval && eval_old){
					vm.deleteRowSelection(vm.getDeltaUpdatedRow(), (String)json.get("keyspace"), selecTable, baseTablePrimaryKey, json);

					//if not matching now & not before, ignore
				}
			}
		}


		//===================================================================================
		//2. for the delta table updated, get the depending preaggregation/agg tables
		//preagg tables hold all column values, hence they have to be updated

		int position = deltaTableName.indexOf("delta_"+(String) json.get("table"));

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


				//2.a after getting the preagg table name & neccessary parameters,
				//check if aggKey in delta (_old & _new ) is null
				//if null then dont update, else update

				boolean isNull = checkIfAggIsNull(AggKey,deltaUpdatedRow);

				if(! isNull){

					// by passing the whole delta Row, we have agg key value even if it is not in json
					vm.updatePreaggregation(deltaUpdatedRow,AggKey,AggKeyType,json,preaggTable,baseTablePrimaryKey,AggCol,AggColType,false);
				}

			}
		}else{
			System.out.println("No Preaggregation table for this delta table "+" delta_"+(String) json.get("table")+" available");
		}


		// ===================================================================================================================
		// 3. for the delta table updated, get update depending reverse join tables

		
		
		
		int cursor = 0;
		for (int j = 0; j < rjoins; j++) {
			
			// basetables
						int nrOfTables = Integer.parseInt(rj_nrDelta.get(j));

						String joinTable = rj_joinTables.get(j);
						
						// include only indices from 1 to nrOfTables
						// get basetables from name of rj table
						List<String> baseTables = Arrays.asList(
								rj_joinTables.get(j).split("_")).subList(1, nrOfTables + 1);
						
						String tableName = (String) json.get("table");
						String keyspace = (String) json.get("keyspace");

						int column = baseTables.indexOf(tableName) + 1;
						
						String joinKeyName = rj_joinKeys.get(cursor + column - 1);
						
						String aggKeyType = rj_joinKeyTypes.get(j);
			
			vm.updateReverseJoin( json,  cursor,  nrOfTables,  joinTable,  baseTables,  joinKeyName,
					 tableName,  keyspace,  aggKeyType,  column);
			
			
			
			
			
			//HERE UPDATE JOIN TABLES
			
			
			
			
			cursor += nrOfTables;
		}

		// ===================================================================================================================
		// 4. update Join tables

		String updatedReverseJoin = vm.getReverseJoinName();

		position = reverseTableName.indexOf(updatedReverseJoin);

		if(position!=-1){

			String temp= "mapping.unit(";
			temp+=Integer.toString(position);
			temp+=")";


			int nrJoin = VmXmlHandler.getInstance().getRjJoinMapping().
					getInt(temp+".nrJoin");

			for(int i=0;i<nrJoin;i++){

				String s = temp+".join("+Integer.toString(i)+")";
				String innerJoinTableName = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".innerJoin");
				String leftJoinTableName = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".leftJoin");
				String rightJoinTableName = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".rightJoin");

				String leftJoinTable = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".LeftTable");
				String rightJoinTable = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".RightTable");

				String tableName = (String)json.get("table");

				Boolean updateLeft = false;
				Boolean updateRight = false;

				if(tableName.equals(leftJoinTable)){
					updateLeft = true;
				}else{
					updateRight = true;
				}

				vm.updateJoinController(deltaUpdatedRow,innerJoinTableName,leftJoinTableName,rightJoinTableName,json,updateLeft,updateRight);

			}
		}else{
			System.out.println("No join table for this reverse join table "+updatedReverseJoin+" available");
		}

	}

	private boolean checkIfAggIsNull(String aggKey, Row deltaUpdatedRow) {

		if(deltaUpdatedRow!=null){	
			ColumnDefinitions colDef = deltaUpdatedRow.getColumnDefinitions();
			int indexNew = colDef.getIndexOf(aggKey+"_new");
			int indexOld = colDef.getIndexOf(aggKey+"_old");

			if(deltaUpdatedRow.isNull(indexNew) && deltaUpdatedRow.isNull(indexOld)){
				return true;
			}
		}

		return false;
	}



	public void cascadeDelete(JSONObject json){

		//===================================================================================

		//get position of basetable from xml list
		//retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json.get("table"));
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		Row deltaDeletedRow = null;

		// 1. delete from Delta Table
		// 1.a If successful, retrieve entire delta Row from Delta to pass on as streams

		if(vm.deleteRowDelta(json)){
			deltaDeletedRow = vm.getDeltaDeletedRow();	
		}

		//=================================================================================

		//===================================================================================
		//2. for the delta table updated, get the depending preaggregation/agg tables
		//preagg tables hold all column values, hence they have to be updated

		int position = deltaTableName.indexOf("delta_"+(String) json.get("table"));

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

				// by passing the whole delta Row, we have agg key value even if it is not in json
				vm.deleteRowPreaggAgg(deltaDeletedRow,baseTablePrimaryKey,json,preaggTable,AggKey,AggKeyType,AggCol,AggColType);

			}
		}else{
			System.out.println("No Preaggregation table for this delta table "+" delta_"+(String) json.get("table")+" available");
		}


		//===================================================================================
		//3. for the delta table updated, get the depending selection tables tables
		//check if condition is true based on selection true
		// if true, delete row from selection table

		int position1 = deltaTableName.indexOf("delta_"+(String) json.get("table"));

		if(position1!=-1){

			String temp4= "mapping.unit(";
			temp4+=Integer.toString(position1);
			temp4+=")";

			int nrConditions = VmXmlHandler.getInstance().getDeltaSelectionMapping().
					getInt(temp4+".nrCond");

			for(int i=0;i<nrConditions;i++){

				String s = temp4+".Cond("+Integer.toString(i)+")";
				String selecTable = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".name");

				String nrAnd = VmXmlHandler.getInstance().getDeltaSelectionMapping().
						getString(s+".nrAnd");

				boolean eval = false;

				for(int j=0;j<Integer.parseInt(nrAnd);j++) {

					String s11 = s+".And(";
					s11+=Integer.toString(j);
					s11+=")";

					String operation = VmXmlHandler.getInstance().getDeltaSelectionMapping().
							getString(s11+".operation");
					String value = VmXmlHandler.getInstance().getDeltaSelectionMapping().
							getString(s11+".value");
					String type = VmXmlHandler.getInstance().getDeltaSelectionMapping().
							getString(s11+".type");

					String selColName = VmXmlHandler.getInstance().getDeltaSelectionMapping().
							getString(s11+".selectionCol");


					switch (type) {

					case "text":

						if(operation.equals("=")){
							if(vm.getDeltaDeletedRow().getString(selColName+"_new").equals(value)){
								eval = true;
							}else{
								eval = false;
							}
						}else if(operation.equals("!=")){
							if(!vm.getDeltaDeletedRow().getString(selColName+"_new").equals(value)){
								eval = true;
							}else{
								eval = false;
							}
						}

						break;

					case "varchar":

						if(operation.equals("=")){
							if(vm.getDeltaDeletedRow().getString(selColName+"_new").equals(value)){
								eval = true;
							}else{
								eval = false;
							}
						}else if(operation.equals("!=")){
							if(!vm.getDeltaDeletedRow().getString(selColName+"_new").equals(value)){
								eval = true;
							}else{
								eval = false;
							}
						}

						break;	

					case "int":
						String s1 = Integer.toString(vm.getDeltaDeletedRow().getInt(selColName+"_new"));
						Integer valueInt = new Integer(s1);
						int compareValue = valueInt.compareTo(new Integer(value));

						if((operation.equals(">") && (compareValue<0))){
							eval = false;
						}else if((operation.equals("<") && (compareValue>0))){
							eval = false;
						}else if((operation.equals("=") && (compareValue!=0))){
							eval = false;
						}else{
							eval = true;
						}

						break;

					case "varint":

						s1 = vm.getDeltaDeletedRow().getVarint(selColName+"_new").toString();
						valueInt = new Integer(new BigInteger(s1).intValue());
						compareValue = valueInt.compareTo(new Integer(value));

						if((operation.equals(">") && (compareValue<0))){
							eval = false;
						}else if((operation.equals("<") && (compareValue>0))){
							eval = false;
						}else if((operation.equals("=") && (compareValue!=0))){
							eval = false;
						}else{
							eval = true;
						}

						break;

					case "float":
						break;
					}

				}

				if(eval)
					vm.deleteRowSelection(vm.getDeltaDeletedRow(),(String)json.get("keyspace"),selecTable,baseTablePrimaryKey,json);
			}

		}

		//==========================================================================================================================
		//4. reverse joins
		

		// check for rj mappings after updating delta
		int cursor = 0;

		// for each join
		for (int j = 0; j < rjoins; j++) {
			// basetables
						int nrOfTables = Integer.parseInt(rj_nrDelta.get(j));

						String joinTable = rj_joinTables.get(j);
			
						// include only indices from 1 to nrOfTables
						// get basetables from name of rj table
						List<String> baseTables = Arrays.asList(
								rj_joinTables.get(j).split("_")).subList(1, nrOfTables + 1);
						
						String tableName = (String) json.get("table");

						String keyspace = (String) json.get("keyspace");

						int column = baseTables.indexOf(tableName) + 1;
						
						String joinKeyName = rj_joinKeys.get(cursor + column - 1);

						String aggKeyType = rj_joinKeyTypes.get(j);
						
						
						vm.deleteReverseJoin(json,  cursor,  nrOfTables,  joinTable,  baseTables,  joinKeyName,
								 tableName,  keyspace,  aggKeyType,  column);
			
						
						
						
						//HERE DELETE FROM JOIN TABLES
						
						
						
						
			cursor += nrOfTables;
		}
		//==========================================================================================================================

		//5. delete from join tables

		String updatedReverseJoin = vm.getReverseJoinName();

		position = reverseTableName.indexOf(updatedReverseJoin);

		if(position!=-1){

			String temp= "mapping.unit(";
			temp+=Integer.toString(position);
			temp+=")";


			int nrJoin = VmXmlHandler.getInstance().getRjJoinMapping().
					getInt(temp+".nrJoin");

			for(int i=0;i<nrJoin;i++){

				String s = temp+".join("+Integer.toString(i)+")";
				String innerJoinTableName = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".innerJoin");
				String leftJoinTableName = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".leftJoin");
				String rightJoinTableName = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".rightJoin");

				String leftJoinTable = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".LeftTable");
				String rightJoinTable = VmXmlHandler.getInstance().getRjJoinMapping().
						getString(s+".RightTable");

				String tableName = (String)json.get("table");

				Boolean updateLeft = false;
				Boolean updateRight = false;

				if(tableName.equals(leftJoinTable)){
					updateLeft = true;
				}else{
					updateRight = true;
				}

				vm.deleteJoinController(deltaDeletedRow ,innerJoinTableName,leftJoinTableName,rightJoinTableName,json,updateLeft,updateRight);

			}
		}else{
			System.out.println("No join table for this reverse join table "+updatedReverseJoin+" available");
		}

	}

}
