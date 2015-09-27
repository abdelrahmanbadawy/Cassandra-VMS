package ViewManager;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Row;

public class CustomizedRow implements Serializable{

	private List<String> colNames;
	private List<String> colTypes;
	private List<Object> colValues;
	int colDefSize;

	private static final long serialVersionUID = 1L;

	public CustomizedRow(Row row){

		if(row!=null){

			colNames = new ArrayList<String>();
			colTypes = new ArrayList<String>();
			colValues = new ArrayList<Object>();

			for(int i=0;i<row.getColumnDefinitions().size();i++){

				colDefSize = row.getColumnDefinitions().size();
				colNames.add(row.getColumnDefinitions().getName(i));
				colTypes.add(row.getColumnDefinitions().getType(i).toString());

				if(row.getColumnDefinitions().getName(i).contains("agg_list")){
					colValues.add(row.getList(row.getColumnDefinitions().getName(i), Float.class));
					continue;
				}

				if(row.getColumnDefinitions().getName(i).contains("list_item")){
					colValues.add(row.getMap(row.getColumnDefinitions().getName(i), String.class, String.class));
					continue;
				}

				switch(row.getColumnDefinitions().getType(i).toString()){

				case "float":
					colValues.add(row.getFloat(i));
					break;
				case "int":
					colValues.add(row.getInt(i));
					break;
				case "varchar":
					colValues.add(row.getString(i));
					break;
				case "text":
					colValues.add(row.getString(i));
					break;
				case "varint":
					colValues.add(row.getVarint(i));
					break;

				}

			}
		}
	}

	public String getString(String s){

		int index = colNames.indexOf(s);
		return (String)colValues.get(index);
	}

	public int getInt(String s){

		int index = colNames.indexOf(s);
		return (int) colValues.get(index);
	}

	public BigInteger getVarint(String s){

		int index = colNames.indexOf(s);
		return (BigInteger) colValues.get(index);
	}

	public float getFloat(String s){

		int index = colNames.indexOf(s);
		return (float) colValues.get(index);
	}

	public boolean isNull(String s) {

		int index = colNames.indexOf(s);
		if(colValues.get(index)==(null))
			return true;
		else
			return false;
	}

	public String getType(int i){
		return colTypes.get(i);
	}

	public String getName(int i){
		return colNames.get(i);
	}

	public int getIndexOf(String s){
		return colNames.indexOf(s);
	}

	public float getFloat(int i) {	
		return (float)colValues.get(i);
	}

	public float getInt(int i) {	
		return (int)colValues.get(i);
	}

	public BigInteger getVarint(int i) {	
		return (BigInteger)colValues.get(i);
	}

	public String getString(int i) {	
		return (String)colValues.get(i);
	}

	public Map<String, String> getMap(String s){
		int index = colNames.indexOf(s);
		return (Map<String, String>) colValues.get(index);
	}

	public List<Float> getList(String s){
		int index = colNames.indexOf(s);
		return (List<Float>) colValues.get(index);
	}
}
