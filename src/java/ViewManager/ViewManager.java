package ViewManager;

import org.json.simple.JSONObject;

import client.client.Client;
import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class ViewManager {

	Cluster currentCluster = null;

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

	/*
	 * public void deleteAggView(JSONObject json){
	 * 
	 * // Agg
	 * 
	 * Session session = Client.getClusterInstance().connect();
	 * 
	 * JSONObject condition = (JSONObject) json.get("condition");
	 * 
	 * 
	 * StringBuilder selectQuery = new StringBuilder();
	 * selectQuery.append("SELECT * FROM test.empSumView WHERE id= ")
	 * .append(condition.get("id")).append(";");
	 * 
	 * Row deletedRow = session.execute(selectQuery.toString()).one();
	 * 
	 * int deletedValue = deletedRow.getVarint("salary").intValue();
	 * 
	 * 
	 * StringBuilder deleteQuery = new StringBuilder(); deleteQuery = new
	 * StringBuilder("DELETE FROM ");
	 * deleteQuery.append(json.get("keyspace")).append(".")
	 * .append(json.get("table")).append("SumView WHERE id= ")
	 * .append(condition.get("id")).append(";");
	 * 
	 * System.out.println(deleteQuery); session.execute(deleteQuery.toString());
	 * 
	 * selectQuery = new StringBuilder();
	 * selectQuery.append("SELECT * FROM test.AggView;");
	 * 
	 * Row sumRow = session.execute(selectQuery.toString()).one();
	 * 
	 * int delta = sumRow.getInt("value") - deletedValue;
	 * 
	 * StringBuilder updateQuery = new StringBuilder("UPDATE ");
	 * updateQuery.append(json.get("keyspace")).append(".")
	 * .append("AggView SET value= ").append(delta)
	 * .append(" WHERE name= 'sum';");
	 * 
	 * System.out.println(updateQuery);
	 * 
	 * session.execute(updateQuery.toString());
	 * 
	 * 
	 * }
	 */
}
