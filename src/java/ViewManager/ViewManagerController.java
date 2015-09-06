package ViewManager;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
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
	List<String> reverseTablesNames_Join;
	List<String> reverseTablesNames_AggJoin;
	List<String> reverseTablesNames_AggJoinGroupBy;
	List<String> preaggTableNames;
	List<String> preaggJoinTableNames;
	List<String> rj_joinTables;
	List<String> rj_joinKeys;
	List<String> rj_joinKeyTypes;
	List<String> rj_nrDelta;
	int rjoins;
	List<String> pkType;

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

		preaggTableNames = VmXmlHandler.getInstance().getHavingPreAggMapping()
				.getList("mapping.unit.preaggTable");

		preaggJoinTableNames = VmXmlHandler.getInstance()
				.getHavingJoinAggMapping().getList("mapping.unit.preaggTable");

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
		String baseTablePrimaryKeyType = pkType.get(indexBaseTableName);

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

							eval &= evaluateCondition(deltaUpdatedRow,
									operation, value, type, colName + "_new");

						}

						System.out.println((String) json.get("table")
								+ " condition is " + eval);

						// condition fulfilled
						if (eval) {
							// by passing the whole delta Row, we have agg key
							// value
							// even if it is not in json
							vm.updatePreaggregation(deltaUpdatedRow, AggKey,
									AggKeyType, json, preaggTable,
									baseTablePrimaryKey, AggCol, AggColType,
									false, false);
						} else {
							// cascade delete

							String pkVAlue = "";

							switch (baseTablePrimaryKeyType) {

							case "int":
								pkVAlue = Integer.toString(deltaUpdatedRow
										.getInt(baseTablePrimaryKey));
								break;

							case "float":
								pkVAlue = Float.toString(deltaUpdatedRow
										.getFloat(baseTablePrimaryKey));
								break;

							case "varint":
								pkVAlue = deltaUpdatedRow.getVarint(
										baseTablePrimaryKey).toString();
								break;

							case "varchar":
								pkVAlue = deltaUpdatedRow
								.getString(baseTablePrimaryKey);
								break;

							case "text":
								pkVAlue = deltaUpdatedRow
								.getString(baseTablePrimaryKey);
								break;
							}

							boolean eval_old = evaluateCondition(
									deltaUpdatedRow, operation, value, type,
									colName + "_old");

							if (eval_old) {

								// 1. retrieve the row to be deleted from delta
								// table

								StringBuilder selectQuery = new StringBuilder(
										"SELECT *");
								selectQuery.append(" FROM ")
								.append(json.get("keyspace"))
								.append(".")
								.append("delta_" + json.get("table"))
								.append(" WHERE ")
								.append(baseTablePrimaryKey)
								.append(" = ").append(pkVAlue)
								.append(";");

								System.out.println(selectQuery);

								ResultSet selectionResult;

								try {

									Session session = currentCluster.connect();
									selectionResult = session
											.execute(selectQuery.toString());

								} catch (Exception e) {
									e.printStackTrace();
									return;
								}

								// 2. set DeltaDeletedRow variable for streaming
								vm.setDeltaDeletedRow(selectionResult.one());

								cascadeDelete(json, false);

							}

							// continue

							continue;
						}

					} else {
						// by passing the whole delta Row, we have agg key value
						// even if it is not in json
						vm.updatePreaggregation(deltaUpdatedRow, AggKey,
								AggKeyType, json, preaggTable,
								baseTablePrimaryKey, AggCol, AggColType, false,
								false);
					}

				}

				// =========================================================================
				// 2.1 update preaggregations with having clause

				// check if preagg has some having clauses or not
				position = preaggTableNames.indexOf(preaggTable);

				if (position1 != -1) {

					String temp4 = "mapping.unit(";
					temp4 += Integer.toString(position1);
					temp4 += ")";

					int nrConditions = VmXmlHandler.getInstance()
							.getHavingPreAggMapping().getInt(temp4 + ".nrCond");

					for (int m = 0; m < nrConditions; m++) {

						String s1 = temp4 + ".Cond(" + Integer.toString(m)
								+ ")";
						String havingTable = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s1 + ".name");

						String nrAnd = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s1 + ".nrAnd");

						boolean eval1 = true;
						boolean eval2 = true;

						for (int n = 0; n < Integer.parseInt(nrAnd); n++) {

							String s11 = s1 + ".And(";
							s11 += Integer.toString(n);
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
							float sum1 = PreagRow.getFloat("sum");
							int count1 = PreagRow.getInt("count");

							float min2 = 0;
							float max2 = 0;
							float average2 = 0;
							float sum2 = 0;
							int count2 = 0;

							if (PreagRowAK != null) {
								min2 = PreagRowAK.getFloat("min");
								max2 = PreagRowAK.getFloat("max");
								average2 = PreagRowAK.getFloat("average");
								sum2 = PreagRowAK.getFloat("sum");
								count2 = PreagRowAK.getInt("count");
							}

							if (aggFct.equals("sum")) {

								int compareValue = new Float(sum1)
								.compareTo(new Float(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if (PreagRowAK != null) {

									compareValue = new Float(sum2)
									.compareTo(new Float(value));

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

							} else if (aggFct.equals("average")) {

								int compareValue = Float.compare(average1,
										Float.valueOf(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if (PreagRowAK != null) {

									compareValue = Float.compare(average2,
											Float.valueOf(value));

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

							} else if (aggFct.equals("count")) {

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

								if (PreagRowAK != null) {

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

							} else if (aggFct.equals("min")) {

								int compareValue = Float.compare(min1,
										Float.valueOf(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if (PreagRowAK != null) {

									compareValue = Float.compare(min2,
											Float.valueOf(value));

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

							} else if (aggFct.equals("max")) {

								int compareValue = Float.compare(max1,
										Float.valueOf(value));

								if ((operation.equals(">") && (compareValue > 0))) {
									eval1 &= true;
								} else if ((operation.equals("<") && (compareValue < 0))) {
									eval1 &= true;
								} else if ((operation.equals("=") && (compareValue == 0))) {
									eval1 &= true;
								} else {
									eval1 &= false;
								}

								if (PreagRowAK != null) {

									compareValue = Float.compare(max2,
											Float.valueOf(value));

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
										(String) json.get("keyspace"),
										havingTable, PreagRow);

								if (PreagRowAK != null && eval2) {
									vm.updateHaving(deltaUpdatedRow,
											(String) json.get("keyspace"),
											havingTable, PreagRowAK);
								}

								// if not matching now
							} else if (!eval1) {
								vm.deleteRowHaving(deltaUpdatedRow,
										(String) json.get("keyspace"),
										havingTable, PreagRow);

								if (PreagRowAK != null && !eval2) {
									vm.deleteRowHaving(deltaUpdatedRow,
											(String) json.get("keyspace"),
											havingTable, PreagRowAK);
								}

								// if not matching now & not before, ignore
							}

							Row deletedRow = vm.getUpdatedPreaggRowDeleted();
							if (deletedRow != null) {
								vm.deleteRowHaving(deltaUpdatedRow,
										(String) json.get("keyspace"),
										havingTable, deletedRow);
							}
						}
					}
				} else {
					System.out
					.println("No Having table for this joinpreaggregation Table "
							+ preaggTable + " available");
				}

			}

		}
		// End of updating preagg with having clause
		// ============================================================================

		else {
			System.out.println("No Preaggregation table for this delta table "
					+ " delta_" + (String) json.get("table") + " available");
		}

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

						eval &= evaluateCondition(deltaUpdatedRow, operation,
								value, type, colName + "_new");

					}

					System.out.println((String) json.get("table")
							+ " condition is " + eval);

					// condition fulfilled
					if (eval) {
						// by passing the whole delta Row, we have agg key
						// value
						// even if it is not in json
						vm.updateReverseJoin(json, cursor, nrOfTables,
								joinTable, baseTables, joinKeyName, tableName,
								keyspace, joinKeyType, column);
					} else {

						String pkVAlue = "";

						switch (baseTablePrimaryKeyType) {

						case "int":
							pkVAlue = Integer.toString(deltaUpdatedRow
									.getInt(baseTablePrimaryKey));
							break;

						case "float":
							pkVAlue = Float.toString(deltaUpdatedRow
									.getFloat(baseTablePrimaryKey));
							break;

						case "varint":
							pkVAlue = deltaUpdatedRow.getVarint(
									baseTablePrimaryKey).toString();
							break;

						case "varchar":
							pkVAlue = deltaUpdatedRow
							.getString(baseTablePrimaryKey);
							break;

						case "text":
							pkVAlue = deltaUpdatedRow
							.getString(baseTablePrimaryKey);
							break;
						}

						boolean eval_old = evaluateCondition(deltaUpdatedRow,
								operation, value, type, colName + "_old");

						if (eval_old) {

							// 1. retrieve the row to be deleted from delta
							// table

							StringBuilder selectQuery = new StringBuilder(
									"SELECT *");
							selectQuery.append(" FROM ")
							.append(json.get("keyspace")).append(".")
							.append("delta_" + json.get("table"))
							.append(" WHERE ")
							.append(baseTablePrimaryKey).append(" = ")
							.append(pkVAlue).append(";");

							System.out.println(selectQuery);

							ResultSet selectionResult;

							try {

								Session session = currentCluster.connect();
								selectionResult = session.execute(selectQuery
										.toString());

							} catch (Exception e) {
								e.printStackTrace();
								return;
							}

							// 2. set DeltaDeletedRow variable for streaming
							vm.setDeltaDeletedRow(selectionResult.one());

							cascadeDelete(json, false);
						}

						// continue
						continue;

					}
				} else {
					// by passing the whole delta Row, we have agg key value
					// even if it is not in json
					vm.updateReverseJoin(json, cursor, nrOfTables, joinTable,
							baseTables, joinKeyName, tableName, keyspace,
							joinKeyType, column);
				}

			}

			// HERE UPDATE JOIN TABLES

			// ===================================================================================================================
			// 4. update Join tables

			String updatedReverseJoin = vm.getReverseJoinName();

			position = reverseTablesNames_Join.indexOf(updatedReverseJoin);

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

					vm.updateJoinController(deltaUpdatedRow,
							innerJoinTableName, leftJoinTableName,
							rightJoinTableName, json, updateLeft, updateRight,
							joinKeyType, joinKeyName, baseTablePrimaryKey);

				}
			} else {
				System.out.println("No join table for this reverse join table "
						+ updatedReverseJoin + " available");
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
									temp + ".leftAggColumns.c(" + e + ").inner");
					String leftJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinMapping()
							.getString(
									temp + ".leftAggColumns.c(" + e + ").left");

					int index = VmXmlHandler
							.getInstance()
							.getRJAggJoinMapping()
							.getInt(temp + ".leftAggColumns.c(" + e + ").index");

					if (updateLeft) {

						vm.updateJoinAgg_UpdateLeft_AggColLeftSide(
								innerJoinAggTable, leftJoinAggTable, json,
								joinKeyType, joinKeyName, aggColName,
								aggColType);
					} else {
						vm.updateJoinAgg_UpdateRight_AggColLeftSide(
								innerJoinAggTable, leftJoinAggTable, json,
								joinKeyType, joinKeyName, aggColName,
								aggColType, index);
					}

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
									+ ").inner");
					String rightJoinAggTable = VmXmlHandler
							.getInstance()
							.getRJAggJoinMapping()
							.getString(
									temp + ".rightAggColumns.c(" + e
									+ ").right");

					int index = VmXmlHandler
							.getInstance()
							.getRJAggJoinMapping()
							.getInt(temp + ".rightAggColumns.c(" + e
									+ ").index");

					if (updateLeft) {
						vm.updateJoinAgg_UpdateLeft_AggColRightSide(
								innerJoinAggTable, rightJoinAggTable, json,
								joinKeyType, joinKeyName, aggColName,
								aggColType, index);
					} else {

						vm.updateJoinAgg_UpdateRight_AggColRightSide(
								innerJoinAggTable, rightJoinAggTable, json,
								joinKeyType, joinKeyName, aggColName,
								aggColType);
					}

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
										+ ").Agg(" + i + ").inner");
						String leftJoinAggTable = VmXmlHandler
								.getInstance()
								.getRJAggJoinGroupByMapping()
								.getString(
										temp + ".leftAggColumns.c(" + e
										+ ").Agg(" + i + ").left");

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

							vm.updateJoinAgg_UpdateLeft_AggColLeftSide_GroupBy(
									innerJoinAggTable, leftJoinAggTable, json,
									KeyType, Key, aggColName, aggColType,
									joinKeyName, joinKeyType);
						} else {
							vm.updateJoinAgg_UpdateRight_AggColLeftSide_GroupBy(
									innerJoinAggTable, leftJoinAggTable, json,
									joinKeyType, joinKeyName, aggColName,
									aggColType, index, Key, KeyType,
									AggKeyIndex);
						}

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
							vm.updateJoinAgg_UpdateLeft_AggColRightSide_GroupBy(
									innerJoinAggTable, rightJoinAggTable, json,
									joinKeyType, joinKeyName, aggColName,
									aggColType, index, Key, KeyType,
									AggKeyIndex);
						} else {

							vm.updateJoinAgg_UpdateRight_AggColRightSide_GroupBy(
									innerJoinAggTable, rightJoinAggTable, json,
									KeyType, Key, aggColName, aggColType,
									joinKeyName, joinKeyType);
						}

					}
				}

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
		Row deltaDeletedRow = null;

		// 1. delete from Delta Table
		// 1.a If successful, retrieve entire delta Row from Delta to pass on as
		// streams
		if (deleteOperation) {
			if (vm.deleteRowDelta(json)) {
				deltaDeletedRow = vm.getDeltaDeletedRow();
			}
		} else
			deltaDeletedRow = vm.getDeltaDeletedRow();

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

				position = preaggTableNames.indexOf(preaggTable);

				if (position != -1) {

					String temp4 = "mapping.unit(";
					temp4 += Integer.toString(position);
					temp4 += ")";

					int nrConditions = VmXmlHandler.getInstance()
							.getHavingPreAggMapping().getInt(temp4 + ".nrCond");

					for (int r = 0; r < nrConditions; r++) {

						String s1 = temp4 + ".Cond(" + Integer.toString(r)
								+ ")";
						String havingTable = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s1 + ".name");

						String nrAnd = VmXmlHandler.getInstance()
								.getHavingPreAggMapping()
								.getString(s1 + ".nrAnd");

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
							Row DeletedPreagRowMapSize1 = vm
									.getDeletePreaggRowDeleted();

							float min1 = 0;
							float max1 = 0;
							float average1 = 0;
							float sum1 = 0;
							int count1 = 0;

							if (DeletedPreagRow != null) {
								min1 = DeletedPreagRow.getFloat("min");
								max1 = DeletedPreagRow.getFloat("max");
								average1 = DeletedPreagRow.getFloat("average");
								sum1 = DeletedPreagRow.getFloat("sum");
								count1 = DeletedPreagRow.getInt("count");
							}

							if (aggFct.equals("sum")) {

								if (DeletedPreagRow != null) {

									int compareValue = Float.compare(sum1,
											Float.valueOf(value));

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

							} else if (aggFct.equals("average")) {

								if (DeletedPreagRow != null) {

									int compareValue = Float.compare(average1,
											Float.valueOf(value));

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

							} else if (aggFct.equals("count")) {

								if (DeletedPreagRow != null) {

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

							} else if (aggFct.equals("min")) {

								if (DeletedPreagRow != null) {

									int compareValue = Float.compare(min1,
											Float.valueOf(value));

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

							} else if (aggFct.equals("max")) {

								if (DeletedPreagRow != null) {
									int compareValue = Float.compare(max1,
											Float.valueOf(value));

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

							if (DeletedPreagRow != null) {
								if (eval1) {
									vm.updateHaving(deltaDeletedRow,
											(String) json.get("keyspace"),
											havingTable, DeletedPreagRow);
								} else {
									vm.deleteRowHaving(deltaDeletedRow,
											(String) json.get("keyspace"),
											havingTable, DeletedPreagRow);
								}
							} else if (DeletedPreagRowMapSize1 != null) {
								vm.deleteRowHaving(deltaDeletedRow,
										(String) json.get("keyspace"),
										havingTable, DeletedPreagRowMapSize1);
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

				vm.deleteReverseJoin(json, cursor, nrOfTables, joinTable,
						baseTables, joinKeyName, tableName, keyspace,
						aggKeyType, column);

				// HERE DELETE FROM JOIN TABLES

				String updatedReverseJoin = vm.getReverseJoinName();

				position = reverseTablesNames_Join.indexOf(updatedReverseJoin);

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

						vm.deleteJoinController(deltaDeletedRow,
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

				String joinKeyType = rj_joinKeyTypes.get(j);

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
										temp + ".leftAggColumns.c(" + e
										+ ").name");
						String aggColType = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getString(
										temp + ".leftAggColumns.c(" + e
										+ ").type");
						String innerJoinAggTable = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getString(
										temp + ".leftAggColumns.c(" + e
										+ ").inner");
						String leftJoinAggTable = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getString(
										temp + ".leftAggColumns.c(" + e
										+ ").left");

						int index = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getInt(temp + ".leftAggColumns.c(" + e
										+ ").index");

						if (updateLeft) {

							vm.deleteJoinAgg_DeleteLeft_AggColLeftSide(
									innerJoinAggTable, leftJoinAggTable, json,
									joinKeyType, joinKeyName, aggColName,
									aggColType);
						} else {


							vm.deleteJoinAgg_DeleteRight_AggColLeftSide(
									innerJoinAggTable, json, joinKeyType,
									joinKeyName, aggColName, aggColType);
						}

					}

					int nrRightAggColumns = VmXmlHandler.getInstance()
							.getRJAggJoinMapping()
							.getInt(temp + ".rightAggColumns.nr");

					for (int e = 0; e < nrRightAggColumns; e++) {

						String aggColName = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getString(
										temp + ".rightAggColumns.c(" + e
										+ ").name");
						String aggColType = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getString(
										temp + ".rightAggColumns.c(" + e
										+ ").type");
						String innerJoinAggTable = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getString(
										temp + ".rightAggColumns.c(" + e
										+ ").inner");
						String rightJoinAggTable = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getString(
										temp + ".rightAggColumns.c(" + e
										+ ").right");

						int index = VmXmlHandler
								.getInstance()
								.getRJAggJoinMapping()
								.getInt(temp + ".rightAggColumns.c(" + e
										+ ").index");

						if (updateLeft) {

							vm.deleteJoinAgg_DeleteLeft_AggColRightSide(
									innerJoinAggTable, json, joinKeyType,
									joinKeyName, aggColName, aggColType);

						} else {

							vm.deleteJoinAgg_DeleteRight_AggColRightSide(
									innerJoinAggTable, rightJoinAggTable, json,
									joinKeyType, joinKeyName, aggColName,
									aggColType);

						}

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
										temp + ".leftAggColumns.c(" + e
										+ ").name");
						String aggColType = VmXmlHandler
								.getInstance()
								.getRJAggJoinGroupByMapping()
								.getString(
										temp + ".leftAggColumns.c(" + e
										+ ").type");

						int nrAgg = VmXmlHandler
								.getInstance()
								.getRJAggJoinGroupByMapping()
								.getInt(temp + ".leftAggColumns.c(" + e
										+ ").nrAgg");

						for (int i = 0; i < nrAgg; i++) {

							String innerJoinAggTable = VmXmlHandler
									.getInstance()
									.getRJAggJoinGroupByMapping()
									.getString(
											temp + ".leftAggColumns.c(" + e
											+ ").Agg(" + i + ").inner");
							String leftJoinAggTable = VmXmlHandler
									.getInstance()
									.getRJAggJoinGroupByMapping()
									.getString(
											temp + ".leftAggColumns.c(" + e
											+ ").Agg(" + i + ").left");

							String aggKey = VmXmlHandler
									.getInstance()
									.getRJAggJoinGroupByMapping()
									.getString(
											temp + ".leftAggColumns.c(" + e
											+ ").Agg(" + i + ").Key");
							aggKeyType = VmXmlHandler
									.getInstance()
									.getRJAggJoinGroupByMapping()
									.getString(
											temp + ".leftAggColumns.c(" + e
											+ ").Agg(" + i
											+ ").KeyType");

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

								vm.deleteJoinAgg_DeleteLeft_AggColLeftSide_GroupBy(
										innerJoinAggTable, leftJoinAggTable,
										json, aggKeyType, aggKey, aggColName,
										aggColType);
							} else {
								vm.deleteJoinAgg_DeleteRight_AggColLeftSide_GroupBy(
										innerJoinAggTable, leftJoinAggTable,
										json, aggKeyType, aggKey, aggColName,
										aggColType, AggKeyIndex, index);
							}

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
										temp + ".rightAggColumns.c(" + e
										+ ").name");
						String aggColType = VmXmlHandler
								.getInstance()
								.getRJAggJoinGroupByMapping()
								.getString(
										temp + ".rightAggColumns.c(" + e
										+ ").type");

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
							aggKeyType = VmXmlHandler
									.getInstance()
									.getRJAggJoinGroupByMapping()
									.getString(
											temp + ".rightAggColumns.c(" + e
											+ ").Agg(" + i
											+ ").KeyType");

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
								vm.deleteJoinAgg_DeleteLeft_AggColRightSide_GroupBy(
										innerJoinAggTable, rightJoinAggTable,
										json, aggKeyType, aggKey, aggColName,
										aggColType, AggKeyIndex, index);
							} else {

								vm.deleteJoinAgg_DeleteRight_AggColRightSide_GroupBy(
										innerJoinAggTable, rightJoinAggTable,
										json, aggKeyType, aggKey, aggColName,
										aggColType);
							}

						}
					}

				}

				cursor += nrOfTables;
			}
			// ==========================================================================================================================

		}
	}

	public boolean evaluateCondition(Row row, String operation, String value,
			String type, String colName) {

		boolean eval = true;

		if (row.isNull(colName)) {
			return false;
		}

		switch (type) {

		case "text":

			if (operation.equals("=")) {
				if (row.getString(colName).equals(value)) {
					eval &= true;
				} else {
					eval &= false;
				}

			} else if (operation.equals("!=")) {
				if (!row.getString(colName).equals(value)) {
					eval = true;
				} else {
					eval = false;
				}

			}

			break;

		case "varchar":

			if (operation.equals("=")) {
				if (row.getString(colName).equals(value)) {
					eval &= true;
				} else {
					eval &= false;
				}

			} else if (operation.equals("!=")) {
				if (!row.getString(colName).equals(value)) {
					eval &= true;
				} else {
					eval &= false;
				}

			}

			break;

		case "int":

			// for _new col
			String s1 = Integer.toString(row.getInt(colName));
			Integer valueInt = new Integer(s1);
			int compareValue = valueInt.compareTo(new Integer(value));

			if ((operation.equals(">") && (compareValue > 0))) {
				eval &= true;
			} else if ((operation.equals("<") && (compareValue < 0))) {
				eval &= true;
			} else if ((operation.equals("=") && (compareValue == 0))) {
				eval &= true;
			} else {
				eval &= false;
			}

			break;

		case "varint":

			// for _new col
			s1 = row.getVarint(colName).toString();
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

			break;

		case "float":

			compareValue = Float.compare(row.getFloat(colName),
					Float.valueOf(value));

			if ((operation.equals(">") && (compareValue > 0))) {
				eval &= true;
			} else if ((operation.equals("<") && (compareValue < 0))) {
				eval &= true;
			} else if ((operation.equals("=") && (compareValue == 0))) {
				eval &= true;
			} else {
				eval &= false;
			}

			break;
		}

		return eval;

	}

}