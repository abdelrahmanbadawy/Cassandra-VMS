package ViewManager;

import java.util.HashMap;

import client.client.XmlHandler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class ReverseJoinHelper {

	static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
					.getString("config.host.localhost"))
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
							.build();

	public static boolean insertStatement(String joinTable,String keyspace,String joinKeyName,String joinKeyValue, int column,HashMap<String, String> myMap, Stream stream, int counter){

		if(counter == -1){
			StringBuilder insertQuery = new StringBuilder("INSERT INTO ")
			.append(keyspace).append(".").append(joinTable).append(" (")
			.append(joinKeyName).append(", ").append("list_item" + column)
			.append(", stream, counter) VALUES (").append(joinKeyValue).append(", ?, ").append(Serialize.serializeStream2(stream))
			.append(", ").append((counter+1)).append(") IF NOT EXISTS;");

			Session session = currentCluster.connect();
			PreparedStatement statement = session.prepare(insertQuery.toString());
			BoundStatement boundStatement = new BoundStatement(statement);
			Row inserted = session.execute(boundStatement.bind(myMap)).one();

			System.out.println(insertQuery);


			if(inserted.getBool("[applied]"))
				return true;
			else
				return false;
		}
		else{


			StringBuilder updateQuery = new StringBuilder("UPDATE ");
			updateQuery.append(keyspace)
			.append(".").append(joinTable).append(" SET list_item").append((column)).append("= ?")
			.append(", counter= ").append(counter+1).append(", stream= ").append(Serialize.serializeStream2(stream))
			.append(" WHERE ").append(joinKeyName).append("= ").append(joinKeyValue)
			.append(" IF counter = ").append(counter).append(";");


			System.out.println(updateQuery);

			Row updated ;
			try {

				Session session = currentCluster.connect();
				PreparedStatement statement = session.prepare(updateQuery.toString());
				BoundStatement boundStatement = new BoundStatement(statement);
				updated = session.execute(boundStatement.bind(myMap)).one();

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

			if(updated.getBool("[applied]"))
				return true;
			else
				return false;

		}


	}

	public static void insertStatement(String joinTable,String keyspace,String joinKeyName,String joinKeyValue, int column,HashMap<String, String> myMap, Stream stream){


		StringBuilder insertQuery = new StringBuilder("INSERT INTO ")
		.append(keyspace).append(".").append(joinTable).append(" (")
		.append(joinKeyName).append(", ").append("list_item" + column)
		.append(", stream, counter) VALUES (").append(joinKeyValue).append(", ?, ").append(Serialize.serializeStream2(stream))
		.append(");");

		Session session = currentCluster.connect();
		PreparedStatement statement = session.prepare(insertQuery.toString());
		BoundStatement boundStatement = new BoundStatement(statement);
		Row inserted = session.execute(boundStatement.bind(myMap)).one();

		System.out.println(insertQuery);


	}

}
