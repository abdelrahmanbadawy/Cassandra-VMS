package ViewManager;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Row;

public class CustomizedRow implements Serializable{

	private List<String> colNames;
	private List<String> colTypes;
	private List<Object> colValues;
	int colDefSize;

	private static final long serialVersionUID = 1L;

	public CustomizedRow(){
		colNames = new ArrayList<String>();
		colTypes = new ArrayList<String>();
		colValues = new ArrayList<Object>();
	}

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

	public int getInt(int i) {	
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

	public void setColNames(List<String> names){
		
		this.colNames.addAll(names);
	}

	public void setColTypes(List<String> types){
		this.colTypes.addAll(types);
	}

	public void setColValues(List<Object> values){
		this.colValues.addAll(values);
	}

	public void setColDefSize(int value){
		this.colDefSize = value;
	}

	public static CustomizedRow constructUpdatedPreaggRow(String aggKey, String aggKeyValue, Map<String,String> myList, float sum, int count, float average, float min, float max,String blob){
		CustomizedRow crow = new CustomizedRow();
		List<String> names = new ArrayList<String>(Arrays.asList(aggKey,"list_item","sum","average","min","max","count","stream"));
		crow.setColNames(names);

		List<String> types = new ArrayList<String>(Arrays.asList("text","Map.class","float","float","float","float","int","blob"));
		crow.setColTypes(types);

		List<Object> values = new ArrayList<Object>(Arrays.asList(aggKeyValue,myList,sum,average,min,max,count,blob));
		crow.setColValues(values);

		crow.setColDefSize(values.size());

		return crow;
	}
}
