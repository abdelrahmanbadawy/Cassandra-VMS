package ViewManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
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
	List<String> rj_joinTables;
	List<String> rj_joinKeys;
	List<String> rj_joinKeyTypes;
	List<String> rj_nrDelta;
	int rjoins;
	List<String> pkType;
	List<String> vm_identifiers;
	int identifier_index;
	Stream stream = null;
	TaskDistributor td;

	boolean firstOperation = true;


	final static Logger timestamps = Logger.getLogger("ViewManagerController");  

	public ViewManagerController(ViewManager vm,Cluster cluster, TaskDistributor td, int identifier_index) {	

		System.out.println("Delta Controller is up");
		retrieveLoadXmlHandlers();
		this.vm = vm;
		this.identifier_index = identifier_index;
		parseXmlMapping();
		stream = new Stream();

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

		vm_identifiers = VmXmlHandler.getInstance().getVMProperties().getList("vm.identifier");
		identifier_index = vm_identifiers.indexOf(vm.getIdentifier());

	}


	public void update(JSONObject json) {
		//print time of very first operation
		if(firstOperation){
			timestamps.info(vm.getIdentifier()+"-"+"delta");
			firstOperation= false;
		}

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

		System.out.println("saving execPtr "+ json.get("readPtr").toString());


		if(json.get("recovery_mode").equals("off") || json.get("recovery_mode").equals("last_recovery_line")){


			timestamps.info(vm.getIdentifier()+" - "+"delta");

			VmXmlHandler.getInstance().getVMProperties().setProperty("vm("+identifier_index+").execPtr1", json.get("readPtr").toString());
			VmXmlHandler.getInstance().save(VmXmlHandler.getInstance().getVMProperties().getFile());


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

		//print time of very first operation
		if(firstOperation){
			timestamps.info(vm.getIdentifier()+" - "+"delta");
			firstOperation= false;
		}

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

		System.out.println("saving execPtr "+ json.get("readPtr").toString());


		if(json.get("recovery_mode").equals("off") || json.get("recovery_mode").equals("last_recovery_line")){

			timestamps.info(vm.getIdentifier()+" - "+"delta");

			VmXmlHandler.getInstance().getVMProperties().setProperty("vm("+identifier_index+").execPtr1", json.get("readPtr").toString());
			VmXmlHandler.getInstance().save(VmXmlHandler.getInstance().getVMProperties().getFile());
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



	@Override
	public void run() {

		while(true){

			if(!td.deltaQueues.get(identifier_index).isEmpty()){
				JSONObject head = td.deltaQueues.get(identifier_index).remove();
				decide(head);
			}

			try {
				Thread.sleep(1000);
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