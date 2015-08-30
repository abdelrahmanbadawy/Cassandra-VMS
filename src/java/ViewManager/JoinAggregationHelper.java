package ViewManager;

import org.json.simple.JSONObject;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class JoinAggregationHelper {

	
	static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
							.getString("config.host.localhost"))
			.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
			.withLoadBalancingPolicy(
					new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
			.build();
	
	public static boolean insertStatement(Float sum, int count, Float avg, Float min, Float max, String key, String keyValue,
			String joinAggTable, JSONObject json){
		
		StringBuilder insertQueryAgg = new StringBuilder(
				"INSERT INTO ");
		insertQueryAgg.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable).append(" ( ")
		.append(key + ", ")
		.append("sum, count, average, min, max")
		.append(") VALUES (").append(keyValue + ", ")
		.append(sum).append(", ").append(count)
		.append(", ").append(avg).append(", ").append(min)
		.append(", ").append(max).append(");");
		
		try {

			Session session = currentCluster.connect();
			session.execute(insertQueryAgg.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
		
	}
	
	
	public static Row selectStatement(String key, String keyValue, String joinAggTable, JSONObject json){
		StringBuilder selectQuery1 = new StringBuilder(
				"SELECT ").append(key)
				.append(", sum, ").append("count, ")
				.append("average, min, max ");
		selectQuery1.append(" FROM ")
		.append((String) json.get("keyspace"))
		.append(".").append(joinAggTable)
		.append(" where ")
		.append(key + " = ")
		.append(keyValue).append(";");
		
		
		Row theRow = null;
		try {
			Session session = currentCluster.connect();
			theRow = session.execute(
					selectQuery1.toString()).one();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return theRow;
		
	}
	
}
