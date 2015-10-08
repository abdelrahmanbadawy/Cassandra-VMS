package ViewManager;

import java.util.HashMap;

import client.client.XmlHandler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
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


	public static void insertStatement(String joinTable,String keyspace,String joinKeyName,String joinKeyValue, int column,HashMap<String, String> myMap, Stream stream){

		StringBuilder insertQuery = new StringBuilder("INSERT INTO ")
		.append(keyspace).append(".").append(joinTable).append(" (")
		.append(joinKeyName).append(", ").append("list_item" + column)
		.append(", stream) VALUES (").append(joinKeyValue).append(", ?, ").append(Serialize.serializeStream2(stream))
		.append(");");
		
		Session session = currentCluster.connect();
		PreparedStatement statement = session.prepare(insertQuery.toString());
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(myMap));
		
		System.out.println(insertQuery);

	}

}
