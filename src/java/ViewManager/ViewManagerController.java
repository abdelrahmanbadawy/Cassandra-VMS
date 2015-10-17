package ViewManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.eclipse.jdt.core.dom.ThisExpression;
import org.json.simple.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;


public class ViewManagerController implements Runnable {

	Cluster currentCluster = null;
	ViewManager vm = null;
	private static XMLConfiguration baseTableKeysConfig;
	List<String> baseTableName;
	List<String> pkName;
	List<String> deltaTableName;
	List<String> reverseTablesNames_Join;
	List<String> reverseTablesNames_AggJoin;
	List<String> reverseTablesNames_AggJoinGroupBy;
	List<String> preaggJoinTableNames;
	List<String> rj_joinTables;
	List<String> rj_joinKeys;
	List<String> rj_joinKeyTypes;
	List<String> rj_nrDelta;
	int rjoins;
	List<String> pkType;
	Stream stream = null;
	TaskDistributor td;


	public ViewManagerController(ViewManager vm,Cluster cluster, TaskDistributor td) {	
		
		System.out.println("I'm up");
		retrieveLoadXmlHandlers();
		parseXmlMapping();
		stream = new Stream();
		this.vm = vm;
		currentCluster = cluster;
		this.td = td; 
		
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
		pkType = baseTableKeysConfig.getList("tableSchema.table.pkType");
		deltaTableName = VmXmlHandler.getInstance().getDeltaPreaggMapping()
				.getList("mapping.unit.deltaTable");
		reverseTablesNames_Join = VmXmlHandler.getInstance().getRjJoinMapping()
				.getList("mapping.unit.reverseJoin");

		reverseTablesNames_AggJoin = VmXmlHandler.getInstance()
				.getRjJoinMapping().getList("mapping.unit.reverseJoin");

		reverseTablesNames_AggJoinGroupBy = VmXmlHandler.getInstance()
				.getRJAggJoinGroupByMapping()
				.getList("mapping.unit.reverseJoin");

		rj_joinTables = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
				.getList("mapping.unit.Join.name");

		rj_joinKeys = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
				.getList("mapping.unit.Join.JoinKey");

		rj_joinKeyTypes = VmXmlHandler.getInstance()
				.getDeltaReverseJoinMapping().getList("mapping.unit.Join.type");

		rj_nrDelta = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
				.getList("mapping.unit.nrDelta");

		rjoins = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
				.getInt("mapping.nrUnit");

		preaggJoinTableNames = VmXmlHandler.getInstance()
				.getHavingJoinAggMapping().getList("mapping.unit.preaggTable");

		/*havingJoinGroupBy =  VmXmlHandler.getInstance().getRJAggJoinGroupByHavingMapping()
				.getList("mapping.unit.name");*/

	}


	public void update(JSONObject json) {

		// ===================================================================================

		// get position of basetable from xml list
		// retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json.get("table"));
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		String baseTablePrimaryKeyType = pkType.get(indexBaseTableName);

		stream = new Stream();

		stream.setBaseTable((String) json.get("table"));

		CustomizedRow deltaUpdatedRow = null;

		// 1. update Delta Table
		// 1.a If successful, retrieve entire updated Row from Delta to pass on
		// as streams

		if (vm.updateDelta(stream,json, indexBaseTableName, baseTablePrimaryKey)) {
			deltaUpdatedRow = stream.getDeltaUpdatedRow();
		}

		// ===================================================================================
		// update selection view
		// for each delta, loop on all selection views possible
		// check if selection condition is met based on selection key
		// if yes then update selection, if not ignore
		// also compare old values of selection condition, if they have changed
		// then delete row from table

		int position1 = deltaTableName.indexOf("delta_" + (String) json.get("table"));

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

				boolean myEval = true;
				boolean myEval_old = true;

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


					myEval &= Utils.evaluateCondition(stream.getDeltaUpdatedRow(), operation, value, type, selColName+"_new");
					myEval_old &= Utils.evaluateCondition(stream.getDeltaUpdatedRow(), operation, value, type, selColName+"_old");		
				}

				// if condition matching now & matched before
				if (myEval && myEval_old) {
					vm.updateSelection(stream.getDeltaUpdatedRow(),
							(String) json.get("keyspace"), selecTable,
							baseTablePrimaryKey);

					// if matching now & not matching before
				} else if (myEval && !myEval_old) {
					vm.updateSelection(stream.getDeltaUpdatedRow(),
							(String) json.get("keyspace"), selecTable,
							baseTablePrimaryKey);

					// if not matching now & matching before
				} else if (!myEval && myEval_old) {
					vm.deleteRowSelection((String) json.get("keyspace"), selecTable,
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

					// WHERE clause condition evaluation
					String condName = VmXmlHandler.getInstance()
							.getDeltaPreaggMapping()
							.getString(s + ".Cond.name");

					if (!condName.equals("none")) {

						String nrAnd = VmXmlHandler.getInstance()
								.getDeltaPreaggMapping()
								.getString(s + ".Cond.nrAnd");

						boolean eval = true;
						String operation = "";
						String value = "";
						String type = "";
						String colName = "";

						for (int j = 0; j < Integer.parseInt(nrAnd); j++) {
							String s11 = s + ".Cond.And(";
							s11 += Integer.toString(j);
							s11 += ")";

							operation = VmXmlHandler.getInstance()
									.getDeltaPreaggMapping()
									.getString(s11 + ".operation");
							value = VmXmlHandler.getInstance()
									.getDeltaPreaggMapping()
									.getString(s11 + ".value");
							type = VmXmlHandler.getInstance()
									.getDeltaPreaggMapping()
									.getString(s11 + ".type");
							colName = VmXmlHandler.getInstance()
									.getDeltaPreaggMapping()
									.getString(s11 + ".selectionCol");

							eval &= Utils.evaluateCondition(deltaUpdatedRow,
									operation, value, type, colName + "_new");

						}

						System.out.println((String) json.get("table")
								+ " condition is " + eval);

						// condition fulfilled
						if (eval) {
							// by passing the whole delta Row, we have agg key
							// value
							// even if it is not in json
							vm.updatePreaggregation(stream, AggKey,
									AggKeyType, json, preaggTable,
									baseTablePrimaryKey, AggCol, AggColType,
									false, false);
						} else {
							// cascade delete

							String pkVAlue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, baseTablePrimaryKey, baseTablePrimaryKeyType, "");
							boolean eval_old = Utils.evaluateCondition(deltaUpdatedRow, operation, value, type,colName + "_old");

							if (eval_old) {
								vm.deleteRowPreaggAgg(stream, pkVAlue, json, preaggTable, AggKey, AggKeyType, AggCol, AggColType);
								//cascadeDeleteHavingTables(json,preaggTable,AggKey,AggKeyType,pkVAlue,AggCol,AggColType);
							}
							// continue
							continue;
						}

					} else {
						// by passing the whole delta Row, we have agg key value
						// even if it is not in json
						vm.updatePreaggregation(stream, AggKey,
								AggKeyType, json, preaggTable,
								baseTablePrimaryKey, AggCol, AggColType, false,
								false);
					}
				}
				// =========================================================================
			}

		}
		// End of updating preagg with having clause
		// ============================================================================

		else {
			System.out.println("No Preaggregation table for this delta table "
					+ " delta_" + (String) json.get("table") + " available");
		}

		stream.resetPreaggregationRows();

		// ===================================================================================================================
		// 3. for the delta table updated, get update depending reverse join
		// tables

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

			String joinKeyType = rj_joinKeyTypes.get(j);

			if (column == 0) {
				System.out.println("No ReverseJoin for this delta update");
				continue;
			}

			// Check on where clause for join

			// WHERE clause condition evaluation

			position = VmXmlHandler.getInstance().getDeltaReverseJoinMapping()
					.getList("mapping.unit.Join.name").indexOf(joinTable);

			if (position != -1) {

				String temp = "mapping.unit(";
				temp += Integer.toString(position);
				temp += ").Join";

				String condName = VmXmlHandler.getInstance()
						.getDeltaReverseJoinMapping()
						.getString(temp + ".Cond.name");

				List<String> baseTableNames = new ArrayList<>();
				String otherTable = "";

				if (!condName.equals("none")) {
					baseTableNames = VmXmlHandler.getInstance()
							.getDeltaReverseJoinMapping()
							.getList(temp + ".Cond.table.name");

					otherTable = VmXmlHandler.getInstance()
							.getDeltaReverseJoinMapping()
							.getString(temp + ".Cond.otherTable");

					if (!baseTableNames.contains((String) json.get("table"))
							&& !otherTable.equals((String) json.get("table"))) {
						continue;
					}
				}

				// to override the next if condition,to update the reverse join
				if (otherTable.equals((String) json.get("table"))) {
					condName = "none";
				}

				if (!condName.equals("none")) {

					String nrAnd = VmXmlHandler.getInstance()
							.getDeltaReverseJoinMapping()
							.getString(temp + ".Cond.nrAnd");

					boolean eval = true;

					String operation = "";
					String value = "";
					String type = "";
					String colName = "";

					for (int jj = 0; jj < Integer.parseInt(nrAnd); jj++) {
						String s11 = temp + ".Cond.And(";
						s11 += Integer.toString(jj);
						s11 += ")";

						operation = VmXmlHandler.getInstance()
								.getDeltaReverseJoinMapping()
								.getString(s11 + ".operation");
						value = VmXmlHandler.getInstance()
								.getDeltaReverseJoinMapping()
								.getString(s11 + ".value");
						type = VmXmlHandler.getInstance()
								.getDeltaReverseJoinMapping()
								.getString(s11 + ".type");
						colName = VmXmlHandler.getInstance()
								.getDeltaReverseJoinMapping()
								.getString(s11 + ".selectionCol");

						eval &= Utils.evaluateCondition(deltaUpdatedRow, operation,
								value, type, colName + "_new");

					}

					System.out.println((String) json.get("table")
							+ " condition is " + eval);

					// condition fulfilled
					if (eval) {
						// by passing the whole delta Row, we have agg key
						// value
						// even if it is not in json
						vm.updateReverseJoin(stream,json, cursor, nrOfTables,
								joinTable, baseTables, joinKeyName, tableName,
								keyspace, joinKeyType, column);
					} else {

						String pkVAlue = Utils.getColumnValueFromDeltaStream(deltaUpdatedRow, baseTablePrimaryKey, baseTablePrimaryKeyType, "");

						boolean eval_old = Utils.evaluateCondition(deltaUpdatedRow,
								operation, value, type, colName + "_old");

						if (eval_old) {

							// 1. retrieve the row to be deleted from delta
							// table

							Row selecRow = Utils.selectAllStatement((String)json.get("keyspace"), "delta_" + json.get("table"), baseTablePrimaryKey, pkVAlue);

							// 2. set DeltaDeletedRow variable for streaming
							//vm.setDeltaDeletedRow(selectionResult.one());

							CustomizedRow crow = new CustomizedRow(selecRow);
							stream.setDeltaDeletedRow(crow);

							cascadeDeleteReverseJoin( json, j, cursor);

							stream.setDeltaDeletedRow(null);
							//cascadeDelete(json, false);


						}

						// continue
						continue;

					}
				} else {
					// by passing the whole delta Row, we have agg key value
					// even if it is not in json
					vm.updateReverseJoin(stream,json, cursor, nrOfTables, joinTable,
							baseTables, joinKeyName, tableName, keyspace,
							joinKeyType, column);
				}

			}

			stream.resetReverseJoinRows();

			cursor += nrOfTables;
		}

		stream.resetDeltaRows();
	}

	private void evaluateLeftorRightJoinAggHaving(String temp, String aggColPosition,
			int e, JSONObject json, String leftOrRight) {


		Integer nrHaving =  VmXmlHandler.getInstance()
				.getRJAggJoinMapping().getInt(temp + "." +aggColPosition+".c(" + e
						+ ")."+leftOrRight+".nrHaving");

		if(nrHaving!=0){

			List<String> havingTableName =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ")."+leftOrRight+".Having.name");

			List<String> aggFct =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ")."+leftOrRight+".Having.And.aggFct");

			List<String> havingType =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ")."+leftOrRight+".Having.And.type");
			List<String> operation =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ")."+leftOrRight+".Having.And.operation");
			List<String> value =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ")."+leftOrRight+".Having.And.value");

			for(int j=0;j<nrHaving;j++){

				if(stream.getLeftOrRightJoinAggDeleteRow()!=null){
					//boolean result = Utils.evalueJoinAggConditions(stream.getLeftOrRightJoinAggDeleteRow(), aggFct.get(j), operation.get(j), value.get(j));
					//if(result){
					String pkName = stream.getLeftOrRightJoinAggDeleteRow().getName(0);
					String pkType = stream.getLeftOrRightJoinAggDeleteRow().getType(0);
					String pkValue = Utils.getColumnValueFromDeltaStream(stream.getLeftOrRightJoinAggDeleteRow(), pkName, pkType, "");
					Utils.deleteEntireRowWithPK((String)json.get("keyspace"), havingTableName.get(j), pkName,pkValue);
					//}
				}

				if(stream.getLeftOrRightJoinAggUpdatedOldRow()!=null){
					boolean result = Utils.evalueJoinAggConditions(stream.getLeftOrRightJoinAggUpdatedOldRow(), aggFct.get(j), operation.get(j), value.get(j));
					if(result){
						JoinAggregationHelper.insertStatement(json, havingTableName.get(j), stream.getLeftOrRightJoinAggUpdatedOldRow());

					}else{
						String pkName = stream.getLeftOrRightJoinAggUpdatedOldRow().getName(0);
						String pkType = stream.getLeftOrRightJoinAggUpdatedOldRow().getType(0);
						String pkValue = Utils.getColumnValueFromDeltaStream(stream.getLeftOrRightJoinAggUpdatedOldRow(), pkName, pkType, "");
						Utils.deleteEntireRowWithPK((String)json.get("keyspace"), havingTableName.get(j), pkName,pkValue);
					}
				}

				if(stream.getLeftOrRightJoinAggNewRow()!=null){
					boolean result = Utils.evalueJoinAggConditions(stream.getLeftOrRightJoinAggNewRow(), aggFct.get(j), operation.get(j), value.get(j));
					if(result){
						JoinAggregationHelper.insertStatement(json, havingTableName.get(j), stream.getLeftOrRightJoinAggNewRow());
					}else{
						String pkName = stream.getLeftOrRightJoinAggNewRow().getName(0);
						String pkType = stream.getLeftOrRightJoinAggNewRow().getType(0);
						String pkValue = Utils.getColumnValueFromDeltaStream(stream.getLeftOrRightJoinAggNewRow(), pkName, pkType, "");
						Utils.deleteEntireRowWithPK((String)json.get("keyspace"), havingTableName.get(j), pkName,pkValue);
					}
				}
			}

		}		
	}


	private void evaluateInnerJoinAggHaving(String temp,String aggColPosition,int e,JSONObject json){

		Integer nrHaving =  VmXmlHandler.getInstance()
				.getRJAggJoinMapping().getInt(temp + "." +aggColPosition+".c(" + e
						+ ").inner.nrHaving");

		if(nrHaving!=0){

			List<String> innerHaving =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ").inner.Having.name");

			List<String> aggFct =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ").inner.Having.And.aggFct");

			List<String> innerHavingType =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ").inner.Having.And.type");
			List<String> operation =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ").inner.Having.And.operation");
			List<String> value =  VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getList(temp + "." +aggColPosition+".c(" + e
							+ ").inner.Having.And.value");

			for(int j=0;j<nrHaving;j++){

				if(stream.getInnerJoinAggDeleteRow()!=null){
					//boolean result = Utils.evalueJoinAggConditions(stream.getInnerJoinAggDeleteRow(), aggFct.get(j), operation.get(j), value.get(j));
					//if(result){
					String pkName = stream.getInnerJoinAggDeleteRow().getName(0);
					String pkType = stream.getInnerJoinAggDeleteRow().getType(0);
					String pkValue = Utils.getColumnValueFromDeltaStream(stream.getInnerJoinAggDeleteRow(), pkName, pkType, "");
					Utils.deleteEntireRowWithPK((String)json.get("keyspace"), innerHaving.get(j), pkName,pkValue);
					//}
				}

				if(stream.getInnerJoinAggUpdatedOldRow()!=null){
					boolean result = Utils.evalueJoinAggConditions(stream.getInnerJoinAggUpdatedOldRow(), aggFct.get(j), operation.get(j), value.get(j));
					if(result){
						JoinAggregationHelper.insertStatement(json, innerHaving.get(j), stream.getInnerJoinAggUpdatedOldRow());
					}else{
						String pkName = stream.getInnerJoinAggUpdatedOldRow().getName(0);
						String pkType = stream.getInnerJoinAggUpdatedOldRow().getType(0);
						String pkValue = Utils.getColumnValueFromDeltaStream(stream.getInnerJoinAggUpdatedOldRow(), pkName, pkType, "");
						Utils.deleteEntireRowWithPK((String)json.get("keyspace"), innerHaving.get(j), pkName,pkValue);
					}
				}

				if(stream.getInnerJoinAggNewRow()!=null){
					boolean result = Utils.evalueJoinAggConditions(stream.getInnerJoinAggNewRow(), aggFct.get(j), operation.get(j), value.get(j));
					if(result){
						JoinAggregationHelper.insertStatement(json, innerHaving.get(j), stream.getInnerJoinAggNewRow());
					}else{
						String pkName = stream.getInnerJoinAggNewRow().getName(0);
						String pkType = stream.getInnerJoinAggNewRow().getType(0);
						String pkValue = Utils.getColumnValueFromDeltaStream(stream.getInnerJoinAggNewRow(), pkName, pkType, "");
						Utils.deleteEntireRowWithPK((String)json.get("keyspace"), innerHaving.get(j), pkName,pkValue);
					}
				}
			}

		}
	}


	private boolean checkIfAggIsNull(String aggKey, CustomizedRow deltaUpdatedRow) {

		if (deltaUpdatedRow != null) {

			if (deltaUpdatedRow.isNull(aggKey + "_new") && deltaUpdatedRow.isNull(aggKey + "_old")) {
				return true;
			}
		}

		return false;
	}

	public void cascadeDelete(JSONObject json, boolean deleteOperation) {

		// boolean deleteOperation is set to false if this method is called from
		// the update method
		// i.e WHERE clause condition evaluates to fasle
		// ===================================================================================

		// get position of basetable from xml list
		// retrieve pk of basetable and delta from XML mapping file
		int indexBaseTableName = baseTableName.indexOf((String) json
				.get("table"));
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		stream = new Stream();
		stream.setBaseTable((String) json.get("table"));
		stream.setDeleteOperation(true);
		stream.setDeltaJSON(json);


		CustomizedRow deltaDeletedRow = null;

		// 1. delete from Delta Table
		// 1.a If successful, retrieve entire delta Row from Delta to pass on as
		// streams
		if (deleteOperation) {
			if (vm.deleteRowDelta(stream,json)) {			
				deltaDeletedRow = stream.getDeltaDeletedRow();
			}
		} else
			deltaDeletedRow = stream.getDeltaDeletedRow();

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
				vm.deleteRowPreaggAgg(stream, baseTablePrimaryKey,
						json, preaggTable, AggKey, AggKeyType, AggCol,
						AggColType);

			}

		} else {
			System.out.println("No Preaggregation table for this delta table "
					+ " delta_" + (String) json.get("table") + " available");
		}

		stream.resetPreaggregationRows();

		// ===================================================================================
		// 3. for the delta table updated, get the depending selection tables
		// tables
		// check if condition is true based on selection true
		// if true, delete row from selection table

		int position1 = deltaTableName.indexOf("delta_"
				+ (String) json.get("table"));

		if (deleteOperation && position1 != -1) {

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

					eval&= Utils.evaluateCondition(stream.getDeltaDeletedRow(), operation, value, type, selColName+"_new");

				}

				if (eval){
					vm.deleteRowSelection(
							(String) json.get("keyspace"), selecTable,
							baseTablePrimaryKey, json);
				}
			}

		}

		// ==========================================================================================================================
		// 4. reverse joins

		if (deleteOperation) {

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
						rj_joinTables.get(j).split("_")).subList(1,
								nrOfTables + 1);

				String tableName = (String) json.get("table");

				String keyspace = (String) json.get("keyspace");

				int column = baseTables.indexOf(tableName) + 1;

				String joinKeyName = rj_joinKeys.get(cursor + column - 1);

				String aggKeyType = rj_joinKeyTypes.get(j);

				stream.setDeltaJSON(json);
				vm.deleteReverseJoin(stream,json, cursor, nrOfTables, joinTable,
						baseTables, joinKeyName, tableName, keyspace,
						aggKeyType, column);



				stream.resetReverseJoinRows();
				cursor += nrOfTables;
			}
			// ==========================================================================================================================

			stream.resetDeltaRows();
		}
	}

	public boolean cascadeDeleteReverseJoin(JSONObject json, int j, int cursor){

		// basetables
		int nrOfTables = Integer.parseInt(rj_nrDelta.get(j));

		String joinTable = rj_joinTables.get(j);

		// include only indices from 1 to nrOfTables
		// get basetables from name of rj table
		List<String> baseTables = Arrays.asList(
				rj_joinTables.get(j).split("_")).subList(1,
						nrOfTables + 1);

		String tableName = (String) json.get("table");

		String keyspace = (String) json.get("keyspace");

		int column = baseTables.indexOf(tableName) + 1;

		String joinKeyName = rj_joinKeys.get(cursor + column - 1);

		String aggKeyType = rj_joinKeyTypes.get(j);

		vm.deleteReverseJoin(stream,json, cursor, nrOfTables, joinTable,
				baseTables, joinKeyName, tableName, keyspace,
				aggKeyType, column);

		stream.resetReverseJoinRows();

		return true;
	}


	public boolean propagateRJ(JSONObject rjjson) {

		JSONObject data = (JSONObject) rjjson.get("data");

		String bufferString = data.get("stream").toString();

		stream = Serialize.deserializeStream(bufferString);

		JSONObject json = stream.getDeltaJSON();

		String tableName = stream.getBaseTable();
		int indexBaseTableName = baseTableName.indexOf(stream.getBaseTable());
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		String baseTablePrimaryKeyType = pkType.get(indexBaseTableName);

		// String tableName = (String) json.get("table");
		String keyspace = (String) json.get("keyspace");

		String joinTable = rjjson.get("table").toString();

		// j
		int indexOfRJ = rj_joinTables.indexOf(joinTable);

		// basetables
		int nrOfTables = Integer.parseInt(rj_nrDelta.get(indexOfRJ));

		List<String> baseTables = Arrays.asList(joinTable.split("_")).subList(
				1, nrOfTables + 1);

		int column = baseTables.indexOf(tableName) + 1;

		String joinKeyName = rj_joinKeys.get(indexOfRJ * 2 + column - 1);

		String joinKeyType = rj_joinKeyTypes.get(indexOfRJ);

		int position = reverseTablesNames_Join.indexOf(joinTable);

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



				Boolean updateLeft = false;
				Boolean updateRight = false;

				if (tableName.equals(leftJoinTable)) {
					updateLeft = true;
				} else {
					updateRight = true;
				}

				vm.updateJoinController(stream, innerJoinTableName,
						leftJoinTableName, rightJoinTableName, json,
						updateLeft, updateRight, joinKeyType, joinKeyName,
						baseTablePrimaryKey);

			}
		} else {
			System.out.println("No join table for this reverse join table "
					+ joinTable + " available");
		}

		// UPDATE join agg

		int positionAgg = reverseTablesNames_AggJoin.indexOf(joinTable);

		if (positionAgg != -1) {

			String temp = "mapping.unit(";
			temp += Integer.toString(positionAgg);
			temp += ")";

			Boolean updateLeft = false;
			Boolean updateRight = false;

			String leftJoinTable = VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getString(temp + ".LeftTable");
			String rightJoinTable = VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getString(temp + ".RightTable");



			if (tableName.equals(leftJoinTable)) {
				updateLeft = true;
			} else {
				updateRight = true;
			}

			int nrLeftAggColumns = VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getInt(temp + ".leftAggColumns.nr");

			for (int e = 0; e < nrLeftAggColumns; e++) {

				String aggColName = VmXmlHandler.getInstance()
						.getRJAggJoinMapping()
						.getString(temp + ".leftAggColumns.c(" + e + ").name");
				String aggColType = VmXmlHandler.getInstance()
						.getRJAggJoinMapping()
						.getString(temp + ".leftAggColumns.c(" + e + ").type");
				String innerJoinAggTable = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e
								+ ").inner.name");
				String leftJoinAggTable = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").left.name");

				int index = VmXmlHandler.getInstance().getRJAggJoinMapping()
						.getInt(temp + ".leftAggColumns.c(" + e + ").index");

				if (updateLeft) {

					vm.updateJoinAgg_UpdateLeft_AggColLeftSide(stream,
							innerJoinAggTable, leftJoinAggTable, json,
							joinKeyType, joinKeyName, aggColName, aggColType);
				} else {
					vm.updateJoinAgg_UpdateRight_AggColLeftSide(stream,
							innerJoinAggTable, leftJoinAggTable, json,
							joinKeyType, joinKeyName, aggColName, aggColType,
							index);
				}

				if (!leftJoinAggTable.equals("false")) {
					evaluateLeftorRightJoinAggHaving(temp, "leftAggColumns", e,
							json, "left");
				}

				if (!innerJoinAggTable.equals("false")) {
					evaluateInnerJoinAggHaving(temp, "leftAggColumns", e, json);
				}

				stream.resetJoinAggRows();
			}

			int nrRightAggColumns = VmXmlHandler.getInstance()
					.getRJAggJoinMapping().getInt(temp + ".rightAggColumns.nr");

			for (int e = 0; e < nrRightAggColumns; e++) {

				String aggColName = VmXmlHandler.getInstance()
						.getRJAggJoinMapping()
						.getString(temp + ".rightAggColumns.c(" + e + ").name");
				String aggColType = VmXmlHandler.getInstance()
						.getRJAggJoinMapping()
						.getString(temp + ".rightAggColumns.c(" + e + ").type");
				String innerJoinAggTable = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e
								+ ").inner.name");
				String rightJoinAggTable = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e
								+ ").right.name");

				int index = VmXmlHandler.getInstance().getRJAggJoinMapping()
						.getInt(temp + ".rightAggColumns.c(" + e + ").index");

				if (updateLeft) {
					vm.updateJoinAgg_UpdateLeft_AggColRightSide(stream,
							innerJoinAggTable, rightJoinAggTable, json,
							joinKeyType, joinKeyName, aggColName, aggColType,
							index);
				} else {

					vm.updateJoinAgg_UpdateRight_AggColRightSide(stream,
							innerJoinAggTable, rightJoinAggTable, json,
							joinKeyType, joinKeyName, aggColName, aggColType);
				}

				if (!rightJoinAggTable.equals("false")) {
					evaluateLeftorRightJoinAggHaving(temp, "rightAggColumns",
							e, json, "right");
				}

				if (!innerJoinAggTable.equals("false")) {
					evaluateInnerJoinAggHaving(temp, "rightAggColumns", e, json);
				}

				stream.resetJoinAggRows();

			}
		}


		// ======================================================================================================
		// Update Group By Join Agg clauses

		int positionAggGroupBy = reverseTablesNames_AggJoinGroupBy
				.indexOf(joinTable);

		if (positionAggGroupBy != -1) {

			String temp = "mapping.unit(";
			temp += Integer.toString(positionAggGroupBy);
			temp += ")";

			Boolean updateLeft = false;
			Boolean updateRight = false;

			String leftJoinTable = VmXmlHandler.getInstance()
					.getRJAggJoinGroupByMapping()
					.getString(temp + ".LeftTable");
			String rightJoinTable = VmXmlHandler.getInstance()
					.getRJAggJoinGroupByMapping()
					.getString(temp + ".RightTable");

			tableName = (String) json.get("table");

			if (tableName.equals(leftJoinTable)) {
				updateLeft = true;
			} else {
				updateRight = true;
			}

			int nrLeftAggColumns = VmXmlHandler.getInstance()
					.getRJAggJoinGroupByMapping()
					.getInt(temp + ".leftAggColumns.nr");

			for (int e = 0; e < nrLeftAggColumns; e++) {

				String aggColName = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").name");
				String aggColType = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").type");

				int nrAgg = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getInt(temp + ".leftAggColumns.c(" + e + ").nrAgg");

				for (int i = 0; i < nrAgg; i++) {

					String innerJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").inner.name");
					String leftJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").left.name");

					String Key = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").Key");
					String KeyType = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").KeyType");

					int index = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getInt(temp + ".leftAggColumns.c(" + e
									+ ").index");

					int AggKeyIndex = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getInt(temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").aggKeyIndex");


					if (updateLeft) {

						vm.updateJoinAgg_UpdateLeft_AggColLeftSide_GroupBy(stream,
								innerJoinAggTable, leftJoinAggTable, json,
								KeyType, Key, aggColName, aggColType,
								joinKeyName, joinKeyType);

					} else {
						vm.updateJoinAgg_UpdateRight_AggColLeftSide_GroupBy(stream,
								innerJoinAggTable, leftJoinAggTable, json,
								joinKeyType, joinKeyName, aggColName,
								aggColType, index, Key, KeyType,
								AggKeyIndex);
					}

					stream.resetJoinAggGroupByUpRows();

				}
			}

			int nrRightAggColumns = VmXmlHandler.getInstance()
					.getRJAggJoinGroupByMapping()
					.getInt(temp + ".rightAggColumns.nr");

			for (int e = 0; e < nrRightAggColumns; e++) {

				String aggColName = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e + ").name");
				String aggColType = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e + ").type");

				int nrAgg = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getInt(temp + ".rightAggColumns.c(" + e
								+ ").nrAgg");

				for (int i = 0; i < nrAgg; i++) {

					String Key = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").Key");
					String KeyType = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").KeyType");

					int index = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getInt(temp + ".rightAggColumns.c(" + e
									+ ").index");

					String innerJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").inner");

					String rightJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").right");

					int AggKeyIndex = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getInt(temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").aggKeyIndex");

					if (updateLeft) {
						vm.updateJoinAgg_UpdateLeft_AggColRightSide_GroupBy(stream,
								innerJoinAggTable, rightJoinAggTable, json,
								joinKeyType, joinKeyName, aggColName,
								aggColType, index, Key, KeyType,
								AggKeyIndex);

					} else {

						vm.updateJoinAgg_UpdateRight_AggColRightSide_GroupBy(stream,
								innerJoinAggTable, rightJoinAggTable, json,
								KeyType, Key, aggColName, aggColType,
								joinKeyName, joinKeyType);

					}

					stream.resetJoinAggGroupByUpRows();

				}
			}

		}

		//END OF UPDATE JoinAgg Group By
		//===============================================


		return true;
	}


	public boolean propagateDeleteRJ(JSONObject rjjson) {

		JSONObject data = (JSONObject) rjjson.get("data");

		String bufferString = data.get("stream").toString();

		stream = Serialize.deserializeStream(bufferString);


		JSONObject json = stream.getDeltaJSON();

		String tableName = stream.getBaseTable();
		int indexBaseTableName = baseTableName.indexOf(stream.getBaseTable());
		String baseTablePrimaryKey = pkName.get(indexBaseTableName);
		String baseTablePrimaryKeyType = pkType.get(indexBaseTableName);

		// String tableName = (String) json.get("table");
		String keyspace = (String) json.get("keyspace");

		String joinTable = rjjson.get("table").toString();

		// j
		int indexOfRJ = rj_joinTables.indexOf(joinTable);

		// basetables
		int nrOfTables = Integer.parseInt(rj_nrDelta.get(indexOfRJ));

		List<String> baseTables = Arrays.asList(joinTable.split("_")).subList(
				1, nrOfTables + 1);

		int column = baseTables.indexOf(tableName) + 1;

		String joinKeyName = rj_joinKeys.get(indexOfRJ * 2 + column - 1);

		String joinKeyType = rj_joinKeyTypes.get(indexOfRJ);


		// HERE DELETE FROM JOIN TABLES

		String updatedReverseJoin = vm.getReverseJoinTableName();

		int position = reverseTablesNames_Join.indexOf(updatedReverseJoin);

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
						.getRjJoinMapping()
						.getString(s + ".RightTable");

				tableName = (String) json.get("table");

				Boolean updateLeft = false;
				Boolean updateRight = false;

				if (tableName.equals(leftJoinTable)) {
					updateLeft = true;
				} else {
					updateRight = true;
				}

				vm.deleteJoinController(stream,stream.getDeltaDeletedRow(),
						innerJoinTableName, leftJoinTableName,
						rightJoinTableName, json, updateLeft,
						updateRight);

			}
		} else {
			System.out
			.println("No join table for this reverse join table "
					+ updatedReverseJoin + " available");
		}

		// END OF DELETE FROM JOIN TABLES

		// UPDATE join aggregation
		int positionAgg = reverseTablesNames_AggJoin.indexOf(joinTable);



		if (positionAgg != -1) {

			String temp = "mapping.unit(";
			temp += Integer.toString(positionAgg);
			temp += ")";

			Boolean updateLeft = false;
			Boolean updateRight = false;

			String leftJoinTable = VmXmlHandler.getInstance()
					.getRJAggJoinMapping()
					.getString(temp + ".LeftTable");
			String rightJoinTable = VmXmlHandler.getInstance()
					.getRJAggJoinMapping()
					.getString(temp + ".RightTable");

			tableName = (String) json.get("table");

			if (tableName.equals(leftJoinTable)) {
				updateLeft = true;
			} else {
				updateRight = true;
			}

			int nrLeftAggColumns = VmXmlHandler.getInstance()
					.getRJAggJoinMapping()
					.getInt(temp + ".leftAggColumns.nr");

			for (int e = 0; e < nrLeftAggColumns; e++) {

				String aggColName = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").name");
				String aggColType = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").type");
				String innerJoinAggTable = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").inner.name");
				String leftJoinAggTable = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").left.name");

				int index = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getInt(temp + ".leftAggColumns.c(" + e + ").index");

				if (updateLeft) {

					vm.deleteJoinAgg_DeleteLeft_AggColLeftSide(stream,
							innerJoinAggTable, leftJoinAggTable, json,
							joinKeyType, joinKeyName, aggColName,
							aggColType);
				} else {


					vm.deleteJoinAgg_DeleteRight_AggColLeftSide(stream,leftJoinAggTable,
							innerJoinAggTable, json, joinKeyType,
							joinKeyName, aggColName, aggColType);
				}

				if(!leftJoinAggTable.equals("false")){
					evaluateLeftorRightJoinAggHaving(temp,"leftAggColumns", e, json,"left");
				}

				if(!innerJoinAggTable.equals("false")){
					evaluateInnerJoinAggHaving(temp, "leftAggColumns", e, json);
				}

				stream.resetJoinAggRows();

			}

			int nrRightAggColumns = VmXmlHandler.getInstance()
					.getRJAggJoinMapping()
					.getInt(temp + ".rightAggColumns.nr");

			for (int e = 0; e < nrRightAggColumns; e++) {

				String aggColName = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e + ").name");
				String aggColType = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e + ").type");
				String innerJoinAggTable = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e
								+ ").inner.name");
				String rightJoinAggTable = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e
								+ ").right.name");

				int index = VmXmlHandler
						.getInstance()
						.getRJAggJoinMapping()
						.getInt(temp + ".rightAggColumns.c(" + e
								+ ").index");

				if (updateLeft) {

					vm.deleteJoinAgg_DeleteLeft_AggColRightSide(stream,rightJoinAggTable,
							innerJoinAggTable, json, joinKeyType,
							joinKeyName, aggColName, aggColType);

				} else {

					vm.deleteJoinAgg_DeleteRight_AggColRightSide(stream,
							innerJoinAggTable, rightJoinAggTable, json,
							joinKeyType, joinKeyName, aggColName,
							aggColType);

				}

				if(!rightJoinAggTable.equals("false")){
					evaluateLeftorRightJoinAggHaving(temp,"rightAggColumns", e, json,"right");
				}

				if(!innerJoinAggTable.equals("false")){
					evaluateInnerJoinAggHaving(temp, "rightAggColumns", e, json);
				}

				stream.resetJoinAggRows();

			}

		}

		// ============================================================================================================
		int positionAggGroupBy = reverseTablesNames_AggJoinGroupBy
				.indexOf(joinTable);

		if (positionAggGroupBy != -1) {

			String temp = "mapping.unit(";
			temp += Integer.toString(positionAggGroupBy);
			temp += ")";

			Boolean updateLeft = false;
			Boolean updateRight = false;

			String leftJoinTable = VmXmlHandler.getInstance()
					.getRJAggJoinGroupByMapping()
					.getString(temp + ".LeftTable");
			String rightJoinTable = VmXmlHandler.getInstance()
					.getRJAggJoinGroupByMapping()
					.getString(temp + ".RightTable");

			tableName = (String) json.get("table");

			if (tableName.equals(leftJoinTable)) {
				updateLeft = true;
			} else {
				updateRight = true;
			}

			int nrLeftAggColumns = VmXmlHandler.getInstance()
					.getRJAggJoinGroupByMapping()
					.getInt(temp + ".leftAggColumns.nr");

			for (int e = 0; e < nrLeftAggColumns; e++) {

				String aggColName = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").name");
				String aggColType = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getString(
								temp + ".leftAggColumns.c(" + e + ").type");

				int nrAgg = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getInt(temp + ".leftAggColumns.c(" + e + ").nrAgg");

				for (int i = 0; i < nrAgg; i++) {

					String innerJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").inner.name");
					String leftJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").left.name");

					String aggKey = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").Key");
					String aggKeyType = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").KeyType");

					int index = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getInt(temp + ".leftAggColumns.c(" + e
									+ ").index");

					int AggKeyIndex = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getInt(temp + ".leftAggColumns.c(" + e
									+ ").Agg(" + i + ").aggKeyIndex");

					if (updateLeft) {

						vm.deleteJoinAgg_DeleteLeft_AggColLeftSide_GroupBy(stream,
								innerJoinAggTable, leftJoinAggTable,
								json, aggKeyType, aggKey, aggColName,
								aggColType,index);
					} else {
						vm.deleteJoinAgg_DeleteRight_AggColLeftSide_GroupBy(stream,
								innerJoinAggTable, leftJoinAggTable,
								json, aggKeyType, aggKey, aggColName,
								aggColType, AggKeyIndex, index);
					}

					stream.resetJoinAggGroupByUpRows();

				}
			}

			int nrRightAggColumns = VmXmlHandler.getInstance()
					.getRJAggJoinGroupByMapping()
					.getInt(temp + ".rightAggColumns.nr");

			for (int e = 0; e < nrRightAggColumns; e++) {

				String aggColName = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e + ").name");
				String aggColType = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getString(
								temp + ".rightAggColumns.c(" + e + ").type");

				int nrAgg = VmXmlHandler
						.getInstance()
						.getRJAggJoinGroupByMapping()
						.getInt(temp + ".rightAggColumns.c(" + e
								+ ").nrAgg");

				for (int i = 0; i < nrAgg; i++) {

					String aggKey = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").Key");
					String aggKeyType = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").KeyType");

					int index = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getInt(temp + ".rightAggColumns.c(" + e
									+ ").index");

					String innerJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").inner");

					String rightJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").right");

					int AggKeyIndex = VmXmlHandler
							.getInstance()
							.getRJAggJoinGroupByMapping()
							.getInt(temp + ".rightAggColumns.c(" + e
									+ ").Agg(" + i + ").aggKeyIndex");

					if (updateLeft) {
						vm.deleteJoinAgg_DeleteLeft_AggColRightSide_GroupBy(stream,
								innerJoinAggTable, rightJoinAggTable,
								json, aggKeyType, aggKey, aggColName,
								aggColType, AggKeyIndex, index);
					} else {

						vm.deleteJoinAgg_DeleteRight_AggColRightSide_GroupBy(stream,
								innerJoinAggTable, rightJoinAggTable,
								json, aggKeyType, aggKey, aggColName,
								aggColType,index);
					}

					stream.resetJoinAggGroupByUpRows();

				}
			}

		}


		return true;
	}

	@Override
	public void run() {

		while(true){

		
			while(!td.delta.isEmpty()){
				JSONObject head = td.delta.remove();
				decide(head);
			}
			
			try {
		        Thread.sleep(3000);
		    } catch (InterruptedException e) {
		        // We've been interrupted: no more messages.
		        return;
		    }
		}


	}

	private void decide(JSONObject json) {
		String type = json.get("type").toString();
		if (type.equals("insert")) {
			update(json);
		}

		if (type.equals("update")) {
			update(json);
		}

		if (type.equals("delete-row")) {
			cascadeDelete(json, true);
		}
	}


}