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
	List<String> preaggTableNames;
	List<String> rj_joinTables ;
	List<String> rj_joinKeys ;
	List<String> rj_joinKeyTypes;
	List<String> rj_nrDelta ;
	int rjoins;


	public ViewManagerController() {

		connectToCluster();
		retrieveLoadXmlHandlers();
		parseXmlMapping();

		vm = new ViewManager(currentCluster);

	}

	private void retrieveLoadXmlHandlers() {
		baseTableKeysConfig = new XMLConfiguration();
		baseTableKeysConfig.setDelimiterParsingDisabled(true);

		try {
			baseTableKeysConfig
			.load("ViewManager/properties/baseTableKeys.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

	}

	public void parseXmlMapping() {
		baseTableName = baseTableKeysConfig.getList("tableSchema.table.name");
		pkName = baseTableKeysConfig.getList("tableSchema.table.pkName");
		deltaTableName = VmXmlHandler.getInstance().getDeltaPreaggMapping()
				.getList("mapping.unit.deltaTable");
		reverseTableName = VmXmlHandler.getInstance().getRjJoinMapping()
				.getList("mapping.unit.reverseJoin");

		rj_joinTables = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping().getList("mapping.unit.Join.name");

		rj_joinKeys = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping().getList("mapping.unit.Join.JoinKey");

		rj_joinKeyTypes = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping().getList("mapping.unit.Join.type");

		rj_nrDelta = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping().getList("mapping.unit.nrDelta");

		rjoins = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
				.getInt("mapping.nrUnit");

		preaggTableNames = VmXmlHandler.getInstance().getHavingPreAggMapping().getList("mapping.unit.preaggTable");
	}


	private void connectToCluster() {

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

	public void update(JSONObject json) {

		// ===================================================================================

		// get position of basetable from xml list
		// retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json
				.get("table"));
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		Row deltaUpdatedRow = null;

		// 1. update Delta Table
		// 1.a If successful, retrieve entire updated Row from Delta to pass on
		// as streams

		if (vm.updateDelta(json, indexBaseTableName, baseTablePrimaryKey)) {
			deltaUpdatedRow = vm.getDeltaUpdatedRow();
		}

		// ===================================================================================
		// update selection view
		// for each delta, loop on all selection views possible
		// check if selection condition is met based on selection key
		// if yes then update selection, if not ignore
		// also compare old values of selection condition, if they have changed
		// then delete row from table

		int position1 = deltaTableName.indexOf("delta_"
				+ (String) json.get("table"));

		if (position1 != -1) {

			String temp4 = "mapping.unit(";
			temp4 += Integer.toString(position1);
			temp4 += ")";

			int nrConditions = VmXmlHandler.getInstance()
					.getDeltaSelectionMapping().getInt(temp4 + ".nrCond");

			for (int i = 0; i < nrConditions; i++) {

				String s = temp4 + ".Cond(" + Integer.toString(i) + ")";
				String selecTable = VmXmlHandler.getInstance()
						.getDeltaSelectionMapping().getString(s + ".name");

				String nrAnd = VmXmlHandler.getInstance()
						.getDeltaSelectionMapping().getString(s + ".nrAnd");

				boolean eval = true;
				boolean eval_old = true;

				for (int j = 0; j < Integer.parseInt(nrAnd); j++) {

					String s11 = s + ".And(";
					s11 += Integer.toString(j);
					s11 += ")";

					String operation = VmXmlHandler.getInstance()
							.getDeltaSelectionMapping()
							.getString(s11 + ".operation");
					String value = VmXmlHandler.getInstance()
							.getDeltaSelectionMapping()
							.getString(s11 + ".value");
					String type = VmXmlHandler.getInstance()
							.getDeltaSelectionMapping()
							.getString(s11 + ".type");

					String selColName = VmXmlHandler.getInstance()
							.getDeltaSelectionMapping()
							.getString(s11 + ".selectionCol");

					switch (type) {

					case "text":

						if (operation.equals("=")) {
							if (deltaUpdatedRow.getString(selColName + "_new")
									.equals(value)) {
								eval &= true;
							} else {
								eval &= false;
							}

							if (deltaUpdatedRow.getString(selColName + "_old") == null) {
								eval_old = false;
							} else if (deltaUpdatedRow.getString(
									selColName + "_old").equals(value)) {
								eval_old &= true;
							} else {
								eval_old &= false;
							}
						} else if (operation.equals("!=")) {
							if (!deltaUpdatedRow.getString(selColName + "_new")
									.equals(value)) {
								eval = true;
							} else {
								eval = false;
							}

							if (deltaUpdatedRow.getString(selColName + "_old") == null) {
								eval_old = false;
							} else if (!deltaUpdatedRow.getString(
									selColName + "_old").equals(value)) {
								eval_old &= true;
							} else {
								eval_old &= false;
							}
						}

						break;

					case "varchar":

						if (operation.equals("=")) {
							if (deltaUpdatedRow.getString(selColName + "_new")
									.equals(value)) {
								eval &= true;
							} else {
								eval &= false;
							}

							if (deltaUpdatedRow.getString(selColName + "_old") == null) {
								eval_old = false;
							} else if (deltaUpdatedRow.getString(
									selColName + "_old").equals(value)) {
								eval_old &= true;
							} else {
								eval_old &= false;
							}
						} else if (operation.equals("!=")) {
							if (!deltaUpdatedRow.getString(selColName + "_new")
									.equals(value)) {
								eval &= true;
							} else {
								eval &= false;
							}

							if (deltaUpdatedRow.getString(selColName + "_old") == null) {
								eval_old = false;
							} else if (!deltaUpdatedRow.getString(
									selColName + "_old").equals(value)) {
								eval_old &= true;
							} else {
								eval_old &= false;
							}
						}

						break;

					case "int":

						// for _new col
						String s1 = Integer.toString(deltaUpdatedRow
								.getInt(selColName + "_new"));
						Integer valueInt = new Integer(s1);
						int compareValue = valueInt
								.compareTo(new Integer(value));

						if ((operation.equals(">") && (compareValue > 0))) {
							eval &= true;
						} else if ((operation.equals("<") && (compareValue < 0))) {
							eval &= true;
						} else if ((operation.equals("=") && (compareValue == 0))) {
							eval &= true;
						} else {
							eval &= false;
						}

						// for _old col

						int v = deltaUpdatedRow.getInt(selColName + "_old");
						compareValue = valueInt.compareTo(new Integer(v));

						if ((operation.equals(">") && (compareValue > 0))) {
							eval_old &= true;
						} else if ((operation.equals("<") && (compareValue < 0))) {
							eval_old &= true;
						} else if ((operation.equals("=") && (compareValue == 0))) {
							eval_old &= true;
						} else {
							eval_old &= false;
						}

						break;

					case "varint":

						// for _new col
						s1 = deltaUpdatedRow.getVarint(selColName + "_new")
						.toString();
						valueInt = new Integer(new BigInteger(s1).intValue());
						compareValue = valueInt.compareTo(new Integer(value));

						if ((operation.equals(">") && (compareValue > 0))) {
							eval &= true;
						} else if ((operation.equals("<") && (compareValue < 0))) {
							eval &= true;
						} else if ((operation.equals("=") && (compareValue == 0))) {
							eval &= true;
						} else {
							eval &= false;
						}

						// for _old col
						BigInteger bigInt = deltaUpdatedRow
								.getVarint(selColName + "_old");
						if (bigInt != null) {
							valueInt = bigInt.intValue();
						} else {
							valueInt = 0;
						}
						compareValue = valueInt.compareTo(new Integer(value));

						if ((operation.equals(">") && (compareValue > 0))) {
							eval_old &= true;
						} else if ((operation.equals("<") && (compareValue < 0))) {
							eval_old &= true;
						} else if ((operation.equals("=") && (compareValue == 0))) {
							eval_old &= true;
						} else {
							eval_old &= false;
						}

						break;

					case "float":
						break;
					}
				}

				// if condition matching now & matched before
				if (eval && eval_old) {
					vm.updateSelection(deltaUpdatedRow,
							(String) json.get("keyspace"), selecTable,
							baseTablePrimaryKey);

					// if matching now & not matching before
				} else if (eval && !eval_old) {
					vm.updateSelection(deltaUpdatedRow,
							(String) json.get("keyspace"), selecTable,
							baseTablePrimaryKey);

					// if not matching now & matching before
				} else if (!eval && eval_old) {
					vm.deleteRowSelection(vm.getDeltaUpdatedRow(),
							(String) json.get("keyspace"), selecTable,
							baseTablePrimaryKey, json);

					// if not matching now & not before, ignore
				}
			}
		}

		// ===================================================================================
		// 2. for the delta table updated, get the depending preaggregation/agg
		// tables
		// preagg tables hold all column values, hence they have to be updated

		int position = deltaTableName.indexOf("delta_"
				+ (String) json.get("table"));

		if (position != -1) {

			String temp = "mapping.unit(";
			temp += Integer.toString(position);
			temp += ")";

			int nrPreagg = VmXmlHandler.getInstance().getDeltaPreaggMapping()
					.getInt(temp + ".nrPreagg");

			for (int i = 0; i < nrPreagg; i++) {

				String s = temp + ".Preagg(" + Integer.toString(i) + ")";
				String AggKey = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".AggKey");
				String AggKeyType = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".AggKeyType");
				String preaggTable = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".name");
				String AggCol = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".AggCol");
				String AggColType = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".AggColType");

				// 2.a after getting the preagg table name & neccessary
				// parameters,
				// check if aggKey in delta (_old & _new ) is null
				// if null then dont update, else update

				boolean isNull = checkIfAggIsNull(AggKey, deltaUpdatedRow);

				if (!isNull) {

					// by passing the whole delta Row, we have agg key value
					// even if it is not in json
					vm.updatePreaggregation(deltaUpdatedRow, AggKey,
							AggKeyType, json, preaggTable, baseTablePrimaryKey,
							AggCol, AggColType, false,false);
				}

				//=========================================================================
				//2.1 update preaggregations with having clause

				//check if preagg has some having clauses or not
				position = preaggTableNames.indexOf(preaggTable);

				if (position1 != -1) {

					String temp4 = "mapping.unit(";
					temp4 += Integer.toString(position1);
					temp4 += ")";

					int nrConditions = VmXmlHandler.getInstance()
							.getHavingPreAggMapping().getInt(temp4 + ".nrCond");

					for (i = 0; i < nrConditions; i++) {

						String s1 = temp4 + ".Cond(" + Integer.toString(i) + ")";
						String havingTable = VmXmlHandler.getInstance()
								.getHavingPreAggMapping().getString(s1 + ".name");

						String nrAnd = VmXmlHandler.getInstance()
								.getHavingPreAggMapping().getString(s1 + ".nrAnd");

						boolean eval1 = true;
						boolean eval2 = true;

						for (int j = 0; j < Integer.parseInt(nrAnd); j++) {

							String s11 = s1 + ".And(";
							s11 += Integer.toString(j);
							s11 += ")";

							String aggFct = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".aggFct");
							String operation = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".operation");
							String value = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".value");
							String type = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".type");

							String selColName = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".selectionCol");

							Row PreagRow = vm.getUpdatedPreaggRow();
							Row PreagRowAK = vm.getUpdatedPreaggRowChangeAK();

							float min1 = PreagRow.getFloat("min");
							float max1 = PreagRow.getFloat("max");
							float average1 = PreagRow.getFloat("average");
							int sum1 = PreagRow.getInt("sum");
							int count1 = PreagRow.getInt("count");


							float min2= 0;
							float max2= 0;
							float average2 = 0;
							int sum2 = 0;
							int count2= 0;

							if(PreagRowAK!=null){
								min2 = PreagRowAK.getFloat("min");
								max2 = PreagRowAK.getFloat("max");
								average2 = PreagRowAK.getFloat("average");
								sum2 = PreagRowAK.getInt("sum");
								count2 = PreagRowAK.getInt("count");
							}


							if(aggFct.equals("sum")){


								int compareValue = new Integer(sum1)
								.compareTo(new Integer(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if(PreagRowAK!=null){

									compareValue = new Integer(sum2)
									.compareTo(new Integer(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval2 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval2 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval2 &= true;
									} else {
										eval2 &= false;
									}
								}

							}else if(aggFct.equals("average")){


								int compareValue = Float.compare(average1,Float.valueOf(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if(PreagRowAK!=null){

									compareValue = Float.compare(average2,Float.valueOf(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval1 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval1 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval1 &= true;
									} else {
										eval1 &= false;
									}
								}

							}else if(aggFct.equals("count")){

								int compareValue = new Integer(count1)
								.compareTo(new Integer(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if(PreagRowAK!=null){

									compareValue = new Integer(count2)
									.compareTo(new Integer(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval2 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval2 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval2 &= true;
									} else {
										eval2 &= false;
									}
								}

							}else if(aggFct.equals("min")){

								int compareValue = Float.compare(min1,Float.valueOf(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if(PreagRowAK!=null){

									compareValue = Float.compare(min2,Float.valueOf(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval1 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval1 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval1 &= true;
									} else {
										eval1 &= false;
									}
								}

							}else if(aggFct.equals("max")){

								int compareValue = Float.compare(max1,Float.valueOf(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if(PreagRowAK!=null){

									compareValue = Float.compare(max2,Float.valueOf(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval2 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval2 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval2 &= true;
									} else {
										eval2 &= false;
									}
								}
							}

							// if matching now & not matching before
							// if condition matching now & matched before
							if (eval1) {
								vm.updateHaving(deltaUpdatedRow,
										(String) json.get("keyspace"), havingTable,
										PreagRow);

								if(PreagRowAK!=null && eval2){
									vm.updateHaving(deltaUpdatedRow,
											(String) json.get("keyspace"), havingTable,
											PreagRowAK);
								}

								// if not matching now
							} else if (!eval1) {
								vm.deleteRowHaving(deltaUpdatedRow,
										(String) json.get("keyspace"), havingTable,
										PreagRow);

								if(PreagRowAK!=null && !eval2){
									vm.deleteRowHaving(deltaUpdatedRow,
											(String) json.get("keyspace"), havingTable,
											PreagRowAK);
								}

								// if not matching now & not before, ignore
							}

							Row deletedRow = vm.getUpdatedPreaggRowDeleted();
							if(deletedRow!=null){
								vm.deleteRowHaving(deltaUpdatedRow,
										(String) json.get("keyspace"), havingTable,
										deletedRow);
							}
						}
					}
				}else{
					System.out.println("No Having table for this preaggregation Table "
							+preaggTable+ " available");
				}
			}

		}
		// End of updating preagg with having clause		
		//============================================================================

		else {
			System.out.println("No Preaggregation table for this delta table "
					+ " delta_" + (String) json.get("table") + " available");
		}


		// ===================================================================================================================
		// 3. for the delta table updated, get update depending reverse join
		// tables

		int cursor = 0;
		for ( int j = 0; j < rjoins; j++) {

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

			String joinKeyType = rj_joinKeyTypes.get(j);

			if(column==0){
				System.out.println("No ReverseJoin for this delta update");
				continue;	
			}	

			vm.updateReverseJoin(json, cursor, nrOfTables, joinTable,
					baseTables, joinKeyName, tableName, keyspace, joinKeyType,
					column);

			// HERE UPDATE JOIN TABLES

			// ===================================================================================================================
			// 4. update Join tables

			String updatedReverseJoin = vm.getReverseJoinName();

			position = reverseTableName.indexOf(updatedReverseJoin);

			if (position != -1) {

				String temp = "mapping.unit(";
				temp += Integer.toString(position);
				temp += ")";

				int nrJoin = VmXmlHandler.getInstance().getRjJoinMapping()
						.getInt(temp + ".nrJoin");

				for ( int i = 0; i < nrJoin; i++) {

					String s = temp + ".join(" + Integer.toString(i) + ")";
					String innerJoinTableName = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".innerJoin");
					String leftJoinTableName = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".leftJoin");
					String rightJoinTableName = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".rightJoin");

					String leftJoinTable = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".LeftTable");
					String rightJoinTable = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".RightTable");

					tableName = (String) json.get("table");

					Boolean updateLeft = false;
					Boolean updateRight = false;

					if (tableName.equals(leftJoinTable)) {
						updateLeft = true;
					} else {
						updateRight = true;
					}

					vm.updateJoinController(deltaUpdatedRow,
							innerJoinTableName, leftJoinTableName,
							rightJoinTableName, json, updateLeft, updateRight, joinKeyType, joinKeyName);

				}
			} else {
				System.out.println("No join table for this reverse join table "
						+ updatedReverseJoin + " available");
			}

			// END OF UPATE JOIN TABLES

			//=====================================================================
			//Update JoinPreagg

			if (position != -1) {

				String temp = "mapping.unit(";
				temp += Integer.toString(position);
				temp += ")";

				String joinKey = temp+".joinKey";

				int nrJoinAgg = VmXmlHandler.getInstance().getJoinAggMapping()
						.getInt(temp + ".nrJoinAgg");

				for ( int i = 0; i < nrJoinAgg; i++) {

					String s = temp + ".joinAgg(" + Integer.toString(i) + ")";

					String basetable = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".basetable");
					String otherTable = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".othertable");
					String joinAggTableName = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".name");
					String joinType = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".joinType");
					Boolean leftTable = VmXmlHandler.getInstance()
							.getJoinAggMapping().getBoolean(s + ".leftTable");
					Boolean rightTable = VmXmlHandler.getInstance()
							.getJoinAggMapping().getBoolean(s + ".rightTable");
					String aggKey = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".AggKey");
					String aggKeyType = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".AggKeyType");

					String aggCol = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".AggCol");
					String aggColType = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".AggColType");

					Row oldReverseRow = vm.getReverseJoinUpdateOldRow();
					Row newReverseRow = vm.getReverseJoinUpdatedNewRow();

					tableName = (String) json.get("table");

					String aggKeyValue = "";

					if(aggKey.equals(joinKey)){

						switch(aggKeyType){

						case "int":
							aggKeyValue = Integer.toString(newReverseRow.getInt(aggKey));
							break;
						case "float":
							aggKeyValue = Float.toString(newReverseRow.getFloat(aggKey));
							break;
						case "varint":
							aggKeyValue = newReverseRow.getVarint(aggKey).toString();
							break;

						case "text":
							aggKeyValue = "'"+newReverseRow.getString(aggKey).toString()+"'";
							break;

						case "varchar":
							aggKeyValue = "'"+newReverseRow.getString(aggKey).toString()+"'";
							break;

						}
					}else{
						
						switch(aggKeyType){

						case "int":
							aggKeyValue = Integer.toString(deltaUpdatedRow.getInt(aggKey+"_new"));
							break;
						case "float":
							aggKeyValue = Float.toString(deltaUpdatedRow.getFloat(aggKey+"_new"));
							break;
						case "varint":
							aggKeyValue = deltaUpdatedRow.getVarint(aggKey+"_new").toString();
							break;

						case "text":
							aggKeyValue = "'"+deltaUpdatedRow.getString(aggKey+"_new").toString()+"'";
							break;

						case "varchar":
							aggKeyValue = "'"+deltaUpdatedRow.getString(aggKey+"_new").toString()+"'";
							break;

						}
					}

					boolean update = true;
					switch(joinType){

					case "left":

						if(otherTable.equals(tableName) && !newReverseRow.getMap("list_item2", String.class, String.class).isEmpty()){
							vm.deleteEntireRowWithPK((String)json.get("keyspace"), joinAggTableName, aggKey, aggKeyValue);
							update = false;
						}
						break;

					case "right":
						if(otherTable.equals(tableName) && !newReverseRow.getMap("list_item1", String.class, String.class).isEmpty()){
							vm.deleteEntireRowWithPK((String)json.get("keyspace"), joinAggTableName, aggKey, aggKeyValue);
							update = false;
						}
						break;

					case "inner":
						break;
					}

					if(update && !otherTable.equals(tableName) ){
						vm.updateJoinAgg(deltaUpdatedRow,json,joinAggTableName,aggKey,aggKeyType,aggCol,aggColType,oldReverseRow,newReverseRow,leftTable,false);
					}
				}
			} else {
				System.out.println("No agg table for this reverse join table "
						+ updatedReverseJoin + " available");
			}


			// END OF UPDATE JoinPreag

			cursor += nrOfTables;
		}


	}



	private boolean checkIfAggIsNull(String aggKey, Row deltaUpdatedRow) {

		if (deltaUpdatedRow != null) {
			ColumnDefinitions colDef = deltaUpdatedRow.getColumnDefinitions();
			int indexNew = colDef.getIndexOf(aggKey + "_new");
			int indexOld = colDef.getIndexOf(aggKey + "_old");

			if (deltaUpdatedRow.isNull(indexNew)
					&& deltaUpdatedRow.isNull(indexOld)) {
				return true;
			}
		}

		return false;
	}

	public void cascadeDelete(JSONObject json) {

		// ===================================================================================

		// get position of basetable from xml list
		// retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json
				.get("table"));
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		Row deltaDeletedRow = null;

		// 1. delete from Delta Table
		// 1.a If successful, retrieve entire delta Row from Delta to pass on as
		// streams

		if (vm.deleteRowDelta(json)) {
			deltaDeletedRow = vm.getDeltaDeletedRow();
		}

		// =================================================================================

		// ===================================================================================
		// 2. for the delta table updated, get the depending preaggregation/agg
		// tables
		// preagg tables hold all column values, hence they have to be updated

		int position = deltaTableName.indexOf("delta_"
				+ (String) json.get("table"));

		if (position != -1) {

			String temp = "mapping.unit(";
			temp += Integer.toString(position);
			temp += ")";

			int nrPreagg = VmXmlHandler.getInstance().getDeltaPreaggMapping()
					.getInt(temp + ".nrPreagg");

			for (int i = 0; i < nrPreagg; i++) {

				String s = temp + ".Preagg(" + Integer.toString(i) + ")";
				String AggKey = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".AggKey");
				String AggKeyType = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".AggKeyType");
				String preaggTable = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".name");
				String AggCol = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".AggCol");
				String AggColType = VmXmlHandler.getInstance()
						.getDeltaPreaggMapping().getString(s + ".AggColType");

				// by passing the whole delta Row, we have agg key value even if
				// it is not in json
				vm.deleteRowPreaggAgg(deltaDeletedRow, baseTablePrimaryKey,
						json, preaggTable, AggKey, AggKeyType, AggCol,
						AggColType);

				// update the corresponding preagg wih having clause

				position =preaggTableNames.indexOf(preaggTable);

				if (position != -1) {

					String temp4 = "mapping.unit(";
					temp4 += Integer.toString(position);
					temp4 += ")";

					int nrConditions = VmXmlHandler.getInstance()
							.getHavingPreAggMapping().getInt(temp4 + ".nrCond");

					for (i = 0; i < nrConditions; i++) {

						String s1 = temp4 + ".Cond(" + Integer.toString(i) + ")";
						String havingTable = VmXmlHandler.getInstance()
								.getHavingPreAggMapping().getString(s1 + ".name");

						String nrAnd = VmXmlHandler.getInstance()
								.getHavingPreAggMapping().getString(s1 + ".nrAnd");

						boolean eval1 = true;

						for (int j = 0; j < Integer.parseInt(nrAnd); j++) {

							String s11 = s1 + ".And(";
							s11 += Integer.toString(j);
							s11 += ")";

							String aggFct = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".aggFct");
							String operation = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".operation");
							String value = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".value");
							String type = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".type");

							String selColName = VmXmlHandler.getInstance()
									.getHavingPreAggMapping()
									.getString(s11 + ".selectionCol");

							Row DeletedPreagRow = vm.getDeletePreaggRow();
							Row DeletedPreagRowMapSize1 = vm.getDeletePreaggRowDeleted();


							float min1 = 0;
							float max1 = 0;
							float average1= 0;
							int sum1= 0;
							int count1= 0;

							if(DeletedPreagRow!=null){
								min1 = DeletedPreagRow.getFloat("min");
								max1 = DeletedPreagRow.getFloat("max");
								average1 = DeletedPreagRow.getFloat("average");
								sum1 = DeletedPreagRow.getInt("sum");
								count1 = DeletedPreagRow.getInt("count");
							}


							if(aggFct.equals("sum")){

								if(DeletedPreagRow!=null){


									int compareValue = new Integer(sum1)
									.compareTo(new Integer(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval1 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval1 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval1 &= true;
									} else {
										eval1 &= false;
									}
								}

							}else if(aggFct.equals("average")){

								if(DeletedPreagRow!=null){



									int compareValue = Float.compare(average1,Float.valueOf(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval1 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval1 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval1 &= true;
									} else {
										eval1 &= false;
									}

								}

							}else if(aggFct.equals("count")){

								if(DeletedPreagRow!=null){

									int compareValue = new Integer(count1)
									.compareTo(new Integer(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval1 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval1 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval1 &= true;
									} else {
										eval1 &= false;
									}

								}

							}else if(aggFct.equals("min")){


								if(DeletedPreagRow!=null){


									int compareValue = Float.compare(min1,Float.valueOf(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval1 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval1 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval1 &= true;
									} else {
										eval1 &= false;
									}

								}

							}else if(aggFct.equals("max")){

								if(DeletedPreagRow!=null){
									int compareValue = Float.compare(max1,Float.valueOf(value));

									if ((operation.equals(">") && (compareValue > 0))) {
										eval1 &= true;
									} else if ((operation.equals("<") && (compareValue < 0))) {
										eval1 &= true;
									} else if ((operation.equals("=") && (compareValue == 0))) {
										eval1 &= true;
									} else {
										eval1 &= false;
									}
								}							
							}

							if(DeletedPreagRow!=null) {
								if(eval1){
									vm.updateHaving(deltaDeletedRow,
											(String) json.get("keyspace"), havingTable,
											DeletedPreagRow);
								}else{
									vm.deleteRowHaving(deltaDeletedRow,
											(String) json.get("keyspace"), havingTable,
											DeletedPreagRow);
								}
							}else if (DeletedPreagRowMapSize1!=null){
								vm.deleteRowHaving(deltaDeletedRow,
										(String) json.get("keyspace"), havingTable,
										DeletedPreagRowMapSize1);
							}
						}
					}
				}

			}



		} else {
			System.out.println("No Preaggregation table for this delta table "
					+ " delta_" + (String) json.get("table") + " available");
		}

		// ===================================================================================
		// 3. for the delta table updated, get the depending selection tables
		// tables
		// check if condition is true based on selection true
		// if true, delete row from selection table

		int position1 = deltaTableName.indexOf("delta_"
				+ (String) json.get("table"));

		if (position1 != -1) {

			String temp4 = "mapping.unit(";
			temp4 += Integer.toString(position1);
			temp4 += ")";

			int nrConditions = VmXmlHandler.getInstance()
					.getDeltaSelectionMapping().getInt(temp4 + ".nrCond");

			for (int i = 0; i < nrConditions; i++) {

				String s = temp4 + ".Cond(" + Integer.toString(i) + ")";
				String selecTable = VmXmlHandler.getInstance()
						.getDeltaSelectionMapping().getString(s + ".name");

				String nrAnd = VmXmlHandler.getInstance()
						.getDeltaSelectionMapping().getString(s + ".nrAnd");

				boolean eval = false;

				for (int j = 0; j < Integer.parseInt(nrAnd); j++) {

					String s11 = s + ".And(";
					s11 += Integer.toString(j);
					s11 += ")";

					String operation = VmXmlHandler.getInstance()
							.getDeltaSelectionMapping()
							.getString(s11 + ".operation");
					String value = VmXmlHandler.getInstance()
							.getDeltaSelectionMapping()
							.getString(s11 + ".value");
					String type = VmXmlHandler.getInstance()
							.getDeltaSelectionMapping()
							.getString(s11 + ".type");

					String selColName = VmXmlHandler.getInstance()
							.getDeltaSelectionMapping()
							.getString(s11 + ".selectionCol");

					switch (type) {

					case "text":

						if (operation.equals("=")) {
							if (vm.getDeltaDeletedRow()
									.getString(selColName + "_new")
									.equals(value)) {
								eval = true;
							} else {
								eval = false;
							}
						} else if (operation.equals("!=")) {
							if (!vm.getDeltaDeletedRow()
									.getString(selColName + "_new")
									.equals(value)) {
								eval = true;
							} else {
								eval = false;
							}
						}

						break;

					case "varchar":

						if (operation.equals("=")) {
							if (vm.getDeltaDeletedRow()
									.getString(selColName + "_new")
									.equals(value)) {
								eval = true;
							} else {
								eval = false;
							}
						} else if (operation.equals("!=")) {
							if (!vm.getDeltaDeletedRow()
									.getString(selColName + "_new")
									.equals(value)) {
								eval = true;
							} else {
								eval = false;
							}
						}

						break;

					case "int":
						String s1 = Integer.toString(vm.getDeltaDeletedRow()
								.getInt(selColName + "_new"));
						Integer valueInt = new Integer(s1);
						int compareValue = valueInt
								.compareTo(new Integer(value));

						if ((operation.equals(">") && (compareValue < 0))) {
							eval = false;
						} else if ((operation.equals("<") && (compareValue > 0))) {
							eval = false;
						} else if ((operation.equals("=") && (compareValue != 0))) {
							eval = false;
						} else {
							eval = true;
						}

						break;

					case "varint":

						s1 = vm.getDeltaDeletedRow()
						.getVarint(selColName + "_new").toString();
						valueInt = new Integer(new BigInteger(s1).intValue());
						compareValue = valueInt.compareTo(new Integer(value));

						if ((operation.equals(">") && (compareValue < 0))) {
							eval = false;
						} else if ((operation.equals("<") && (compareValue > 0))) {
							eval = false;
						} else if ((operation.equals("=") && (compareValue != 0))) {
							eval = false;
						} else {
							eval = true;
						}

						break;

					case "float":
						break;
					}

				}

				if (eval)
					vm.deleteRowSelection(vm.getDeltaDeletedRow(),
							(String) json.get("keyspace"), selecTable,
							baseTablePrimaryKey, json);
			}

		}

		// ==========================================================================================================================
		// 4. reverse joins

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

			vm.deleteReverseJoin(json, cursor, nrOfTables, joinTable,
					baseTables, joinKeyName, tableName, keyspace, aggKeyType,
					column);

			// HERE DELETE FROM JOIN TABLES

			// 5. delete from join tables

			String updatedReverseJoin = vm.getReverseJoinName();

			position = reverseTableName.indexOf(updatedReverseJoin);

			if (position != -1) {

				String temp = "mapping.unit(";
				temp += Integer.toString(position);
				temp += ")";

				int nrJoin = VmXmlHandler.getInstance().getRjJoinMapping()
						.getInt(temp + ".nrJoin");

				for (int i = 0; i < nrJoin; i++) {

					String s = temp + ".join(" + Integer.toString(i) + ")";
					String innerJoinTableName = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".innerJoin");
					String leftJoinTableName = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".leftJoin");
					String rightJoinTableName = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".rightJoin");

					String leftJoinTable = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".LeftTable");
					String rightJoinTable = VmXmlHandler.getInstance()
							.getRjJoinMapping().getString(s + ".RightTable");

					tableName = (String) json.get("table");

					Boolean updateLeft = false;
					Boolean updateRight = false;

					if (tableName.equals(leftJoinTable)) {
						updateLeft = true;
					} else {
						updateRight = true;
					}

					vm.deleteJoinController(deltaDeletedRow,
							innerJoinTableName, leftJoinTableName,
							rightJoinTableName, json, updateLeft, updateRight);

				}
			} else {
				System.out.println("No join table for this reverse join table "
						+ updatedReverseJoin + " available");
			}

			// END OF DELETE FROM JOIN TABLES

			// delete operations on agg of joins based on each deletion update of reverse join

			//Update JoinPreagg

			if (position != -1) {

				String temp = "mapping.unit(";
				temp += Integer.toString(position);
				temp += ")";

				int nrJoinAgg = VmXmlHandler.getInstance().getJoinAggMapping()
						.getInt(temp + ".nrJoinAgg");

				for (int i = 0; i < nrJoinAgg; i++) {

					String s = temp + ".joinAgg(" + Integer.toString(i) + ")";

					String basetable = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".basetable");


					tableName = (String) json.get("table");
					if(!basetable.equals(tableName))
						continue;

					String joinAggTableName = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".name");
					Boolean leftTable = VmXmlHandler.getInstance()
							.getJoinAggMapping().getBoolean(s + ".leftTable");
					Boolean rightTable = VmXmlHandler.getInstance()
							.getJoinAggMapping().getBoolean(s + ".rightTable");
					String aggKey = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".AggKey");
					String aggKeyType1 = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".AggKeyType");

					String aggCol = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".AggCol");
					String aggColType = VmXmlHandler.getInstance()
							.getJoinAggMapping().getString(s + ".AggColType");



					Row oldReverseRow = vm.getRevereJoinDeleteOldRow();
					Row newReverseRow = vm.getReverseJoinDeleteNewRow();

					vm.deleteFromJoinAgg(deltaDeletedRow,json,joinAggTableName,aggKey,aggKeyType1,aggCol,aggColType,oldReverseRow,newReverseRow,leftTable);

				}
			} else {
				System.out.println("No agg table for this reverse join table "
						+ updatedReverseJoin + " available");
			}

			// END OF UPDATE JoinPreag


			cursor += nrOfTables;
		}
		// ==========================================================================================================================

	}

}
