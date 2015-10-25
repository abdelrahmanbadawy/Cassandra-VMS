package ViewManager;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.simple.JSONObject;

import com.datastax.driver.core.Cluster;

public class ViewManagerRJController implements Runnable{

	private ViewManager vm;
	private Cluster cluster;
	Stream stream;
	TaskDistributor td;
	List<String> baseTableName;
	private static XMLConfiguration baseTableKeysConfig;
	List<String> pkName;
	List<String> pkType;
	List<String> rj_joinTables;
	List<String> rj_joinKeys;
	List<String> rj_joinKeyTypes;
	List<String> rj_nrDelta;
	List<String> reverseTablesNames_Join;
	List<String> reverseTablesNames_AggJoin;
	List<String> reverseTablesNames_AggJoinGroupBy;
	List<String> vm_identifiers;
	int identifier_index;


	int rjoins;

	public ViewManagerRJController(ViewManager vm,Cluster cluster, TaskDistributor taskDistributor, int identifier_index) {	
		System.out.println("RJ Controller is up");
		this.vm = vm;
		this.identifier_index=identifier_index;
		parseXML();	

		this.cluster = cluster;
		stream = new Stream();
		td = taskDistributor;
		vm_identifiers = VmXmlHandler.getInstance().getVMProperties().getList("vm.identifier");
		identifier_index = vm_identifiers.indexOf(vm.getIdentifier());

	}

	private void parseXML() {

		baseTableKeysConfig = new XMLConfiguration();
		baseTableKeysConfig.setDelimiterParsingDisabled(true);

		try {
			baseTableKeysConfig
			.load("ViewManager/properties/baseTableKeys.xml");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		baseTableName = baseTableKeysConfig.getList("tableSchema.table.name");

		pkName = baseTableKeysConfig.getList("tableSchema.table.pkName");
		pkType = baseTableKeysConfig.getList("tableSchema.table.pkType");
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

		reverseTablesNames_AggJoinGroupBy = VmXmlHandler.getInstance()
				.getRJAggJoinGroupByMapping()
				.getList("mapping.unit.reverseJoin");

		reverseTablesNames_Join = VmXmlHandler.getInstance().getRjJoinMapping()
				.getList("mapping.unit.reverseJoin");

		reverseTablesNames_AggJoin = VmXmlHandler.getInstance()
				.getRjJoinMapping().getList("mapping.unit.reverseJoin");

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
						JoinAggregationHelper.insertStatement(json, havingTableName.get(j), stream.getLeftOrRightJoinAggUpdatedOldRow(), vm.getIdentifier());

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
						JoinAggregationHelper.insertStatement(json, havingTableName.get(j), stream.getLeftOrRightJoinAggNewRow(), vm.getIdentifier());
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
						JoinAggregationHelper.insertStatement(json, innerHaving.get(j), stream.getInnerJoinAggUpdatedOldRow(), vm.getIdentifier());
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
						JoinAggregationHelper.insertStatement(json, innerHaving.get(j), stream.getInnerJoinAggNewRow(), vm.getIdentifier());
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


	public boolean propagateDeleteRJ(JSONObject rjjson) {

		String type = rjjson.get("type").toString();
		JSONObject data;
		if(type.equalsIgnoreCase("insert"))
			data = (JSONObject) rjjson.get("data");
		else
			data = (JSONObject) rjjson.get("set_data");

		String bufferString = data.get("stream").toString();

		stream = Serialize.deserializeStream(bufferString);


		JSONObject json = stream.getDeltaJSON();

		json.put("readPtr", rjjson.get("readPtr"));
		json.put("recovery_mode", rjjson.get("recovery_mode").toString());

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
		System.out.println("saving execPtrRJ "+ rjjson.get("readPtr").toString());

		if(rjjson.get("recovery_mode").equals("off") || rjjson.get("recovery_mode").equals("last_recovery_line")){
		VmXmlHandler.getInstance().getVMProperties().setProperty("vm("+identifier_index+").execPtrRJ", rjjson.get("readPtr").toString());

		VmXmlHandler.getInstance().save(VmXmlHandler.getInstance().getVMProperties().getFile());
		}

		return true;
	}


	public boolean propagateRJ(JSONObject rjjson) {

		String type = rjjson.get("type").toString();
		JSONObject data = null;

		if(type.equalsIgnoreCase("insert"))
			data = (JSONObject) rjjson.get("data");

		String bufferString = data.get("stream").toString();

		stream = Serialize.deserializeStream(bufferString);

		JSONObject json = stream.getDeltaJSON();

		json.put("readPtr", rjjson.get("readPtr"));
		json.put("recovery_mode", rjjson.get("recovery_mode").toString());

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

		System.out.println("saving execPtrRJ "+ rjjson.get("readPtr").toString());

		if(rjjson.get("recovery_mode").equals("off") || rjjson.get("recovery_mode").equals("last_recovery_line")){
		VmXmlHandler.getInstance().getVMProperties().setProperty("vm("+identifier_index+").execPtrRJ", rjjson.get("readPtr").toString());

		VmXmlHandler.getInstance().save(VmXmlHandler.getInstance().getVMProperties().getFile());
		}

		return true;
	}

	@Override
	public void run() {

		while(true){


			if(!td.rjQueues.get(identifier_index).isEmpty()){
				JSONObject head = td.rjQueues.get(identifier_index).remove();
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
		String table = json.get("table").toString();

		if (table.toLowerCase().contains("rj_")) {
			if (type.equalsIgnoreCase("insert")){
				JSONObject data = (JSONObject) json.get("data");
				String bufferString = data.get("stream").toString();
				Stream s = Serialize.deserializeStream(bufferString);
				if(s.isDeleteOperation())
					propagateDeleteRJ(json);
				else
					propagateRJ(json);
			}
		}

	}

}
