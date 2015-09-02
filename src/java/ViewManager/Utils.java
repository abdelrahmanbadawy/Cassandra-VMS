package ViewManager;

import java.math.BigInteger;

import client.client.XmlHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class Utils {
	
	static Cluster currentCluster = Cluster
			.builder()
			.addContactPoint(
					XmlHandler.getInstance().getClusterConfig()
							.getString("config.host.localhost"))
			.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
			.withLoadBalancingPolicy(
					new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
			.build();

	//delete row from a table with primary key
	public static boolean deleteEntireRowWithPK(String keyspace, String tableName,
			String pk, String pkValue) {

		StringBuilder deleteQuery = new StringBuilder("delete from ");
		deleteQuery.append(keyspace).append(".").append(tableName)
				.append(" WHERE ").append(pk + " = ").append(pkValue)
				.append(";");

		System.out.println(deleteQuery.toString());
		try {

			Session session = currentCluster.connect();
			session.execute(deleteQuery.toString());

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}
	
	
	//evaluates a condition
	public static boolean evaluateCondition(Row row, String operation, String value,
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
	
	public static String getColumnValueFromDeltaStream(Row stream, String name,String type, String suffix){

		String value = "";

		switch (type) {

		case "text":

			value = ("'"+ stream.getString(name + suffix) + "'");
			break;

		case "int":

			value = ("" + stream.getInt(name + suffix));
			break;

		case "varint":

			value = ("" + stream.getVarint(name + suffix));
			break;

		case "varchar":

			value = ("'"+ stream.getString(name + suffix) + "'");
			break;

		case "float":

			value = ("" + stream.getFloat(name + suffix));
			break;

		}

		return value;

	}

}
