package ViewManager;

import org.json.simple.JSONObject;

import client.client.Client;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class ViewManager {



	
public void insertEmployeeSelectView(JSONObject json){
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
		insertQuery.append(json.get("table")).append("SelectView (")
				.append(columns).append(") VALUES (").append(values)
				.append(");");

		System.out.println(insertQuery);

		Session session = Client.getClusterInstance().connect();

		session.execute(insertQuery.toString());

	}
}

public void updateEmployeeSelectView(JSONObject json){
	JSONObject condition = (JSONObject) json.get("condition");

	if (condition != null) {
		condition = (JSONObject) json.get("condition");

		JSONObject set_data = (JSONObject) json.get("set_data");
		int salary = Integer
				.parseInt(set_data.get("salary").toString());

		if (salary < 2000) {

			StringBuilder deleteQuery = new StringBuilder(
					"DELETE FROM ");
			deleteQuery.append(json.get("keyspace")).append(".")
					.append(json.get("table"))
					.append("SelectView WHERE ");

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
					.append(json.get("table"))
					.append("SelectView SET ");

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

public void deleteEmployeeSelectView(JSONObject json){
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
public void deleteAggView(JSONObject json){
		
	// Agg

				Session session = Client.getClusterInstance().connect();
				
				JSONObject condition = (JSONObject) json.get("condition");
				
				
				StringBuilder selectQuery = new StringBuilder();
				selectQuery.append("SELECT * FROM test.empSumView WHERE id= ")
						.append(condition.get("id")).append(";");

				Row deletedRow = session.execute(selectQuery.toString()).one();

				int deletedValue = deletedRow.getVarint("salary").intValue();

				
				StringBuilder deleteQuery = new StringBuilder();
				deleteQuery = new StringBuilder("DELETE FROM ");
				deleteQuery.append(json.get("keyspace")).append(".")
						.append(json.get("table")).append("SumView WHERE id= ")
						.append(condition.get("id")).append(";");

				System.out.println(deleteQuery);
				session.execute(deleteQuery.toString());

				selectQuery = new StringBuilder();
				selectQuery.append("SELECT * FROM test.AggView;");

				Row sumRow = session.execute(selectQuery.toString()).one();

				int delta = sumRow.getInt("value") - deletedValue;

				StringBuilder updateQuery = new StringBuilder("UPDATE ");
				updateQuery.append(json.get("keyspace")).append(".")
						.append("AggView SET value= ").append(delta)
						.append(" WHERE name= 'sum';");

				System.out.println(updateQuery);

				session.execute(updateQuery.toString());

		
	}
	*/
}
