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

	public void insertEmployeeSelectView(JSONObject json) {
		JSONObject data = (JSONObject) json.get("data");
		int salary = Integer.parseInt(data.get("salary").toString());

		if (data != null && salary >= 2000) {
			Object[] hm = data.entrySet().toArray();

			StringBuilder columns = new StringBuilder();
			StringBuilder values = new StringBuilder();

			for (int i = 0; i < hm.length; i++) {
				// System.out.println(hm[i]);
				String[] split = hm[i].toString().split("=");

				columns.append(split[0]);
				values.append(split[1]);

				if (i < hm.length - 1) {
					columns.append(", ");
					values.append(", ");
				}

			}

			StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
			insertQuery.append(data.get("keyspace")).append(".")
			.append(json.get("table")).append("SelectView (")
			.append(columns).append(") VALUES (").append(values)
			.append(");");

			System.out.println(insertQuery);

			Session session = Client.getClusterInstance().connect();

			session.execute(insertQuery.toString());

		}
	}



	public boolean insertCourses_Faculty_AggView(JSONObject json) {

		JSONObject data = (JSONObject) json.get("data");

		String faculty = data.get("faculty").toString();

		StringBuilder selectQuery = new StringBuilder("SELECT * ");
		selectQuery.append(" FROM ").append(json.get("keyspace")).append(".")
		.append(json.get("table")).append("_faculty_AggView WHERE ")
		.append("faculty= ").append(faculty).append(" ;");

		System.out.println(selectQuery);

		ResultSet aggViewSelection;
		try {

			Session session = currentCluster.connect();
			aggViewSelection = session.execute(selectQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		// System.out.println("here" + aggViewSelection.all().size());

		StringBuilder insertQueryAgg;

		Row theRow = aggViewSelection.one();

		if (theRow == null) {
			// if result set is empty, insert a new row, pk = aggkey, sum
			// =aggCol, count = 1, av= sum/count

			insertQueryAgg = new StringBuilder("INSERT INTO ");
			insertQueryAgg.append(json.get("keyspace")).append(".").append(json.get("table")).append("_faculty_AggView ")
			.append(" ( ").append("faculty")
			.append(", sum, count, average) VALUES (").append(data.get("faculty"))
			.append(", ");

			int sum = Integer.parseInt(data.get("ects").toString());

			insertQueryAgg.append(sum).append(", 1, ").append(sum).append(");");

			System.out.println(insertQueryAgg);

		} else {
			// else insert with same pk, count ++, sum *= aggcol, avg =
			// sum/count

			// System.out.println("here" + aggViewSelection.all().size());

			System.out.println(theRow);

			int sum = theRow.getInt("sum");
			sum +=  Integer.parseInt(data.get("ects").toString());

			int count = theRow.getInt("count");
			count++;

			float avg = (float) sum / (float) count;

			insertQueryAgg = new StringBuilder("UPDATE ");
			insertQueryAgg.append(json.get("keyspace")).append(".").append(json.get("table"))
			.append("_faculty_AggView SET sum= ").append(sum).append(", average= ")
			.append(avg).append(", count= ").append(count)
			.append(" WHERE ").append("faculty").append("= ")
			.append(data.get("faculty")).append(";");

			System.out.println(insertQueryAgg);

		}

		// run insert query

		try {

			Session session = currentCluster.connect();
			aggViewSelection = session.execute(insertQueryAgg.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;

	}

	public void updateEmployeeSelectView(JSONObject json) {
		JSONObject condition = (JSONObject) json.get("condition");

		if (condition != null) {
			condition = (JSONObject) json.get("condition");

			JSONObject set_data = (JSONObject) json.get("set_data");
			int salary = Integer.parseInt(set_data.get("salary").toString());

			if (salary < 2000) {

				StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
				deleteQuery.append(json.get("keyspace")).append(".")
				.append(json.get("table")).append("SelectView WHERE ");

				if (condition != null) {
					Object[] hm = condition.entrySet().toArray();

					for (int i = 0; i < hm.length; i++) {
						// System.out.println(hm[i]);
						deleteQuery.append(hm[i]);
						if (i < hm.length - 1)
							deleteQuery.append(" AND ");
					}

				}
				deleteQuery.append(";");

				System.out.println(deleteQuery);

				Session session = Client.getClusterInstance().connect();

				session.execute(deleteQuery.toString());

			} else {

				StringBuilder updateQuery = new StringBuilder("UPDATE ");
				updateQuery.append(json.get("keyspace")).append(".")
				.append(json.get("table")).append("SelectView SET ");

				Object[] hm = set_data.entrySet().toArray();
				for (int i = 0; i < hm.length; i++) {
					// System.out.println(hm[i]);
					updateQuery.append(hm[i]);
					if (i < hm.length - 1)
						updateQuery.append(", ");
				}

				updateQuery.append(" WHERE ");

				if (condition != null) {
					Object[] cond = condition.entrySet().toArray();

					for (int i = 0; i < cond.length; i++) {
						// System.out.println(hm[i]);
						updateQuery.append(cond[i]);
						if (i < cond.length - 1)
							updateQuery.append(", ");
					}

				}
				updateQuery.append(";");

				System.out.println(updateQuery);

				Session session = Client.getClusterInstance().connect();

				session.execute(updateQuery.toString());

			}

		}

	}

	public void deleteEmployeeSelectView(JSONObject json) {
		JSONObject condition = (JSONObject) json.get("condition");

		StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
		deleteQuery.append(json.get("keyspace")).append(".")
		.append(json.get("table")).append("SelectView WHERE ");

		if (condition != null) {
			Object[] hm = condition.entrySet().toArray();

			for (int i = 0; i < hm.length; i++) {
				// System.out.println(hm[i]);
				deleteQuery.append(hm[i]);
				if (i < hm.length - 1)
					deleteQuery.append(" AND ");
			}

		}
		deleteQuery.append(";");

		System.out.println(deleteQuery);

		Session session = Client.getClusterInstance().connect();

		session.execute(deleteQuery.toString());
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
			insertQueryAgg.append(");");

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

			insertQueryAgg.append(");");
			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		System.out.println("Done Delta update");
		return true;

	}


}
