package client.testExperiment1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class CalculateThroughput {
	
	public static Map<TimeUnit,Long> computeDiff(Date date1, Date date2) {
	    long diffInMillies = date2.getTime() - date1.getTime();
	    List<TimeUnit> units = new ArrayList<TimeUnit>(EnumSet.allOf(TimeUnit.class));
	    Collections.reverse(units);
	    Map<TimeUnit,Long> result = new LinkedHashMap<TimeUnit,Long>();
	    long milliesRest = diffInMillies;
	    for ( TimeUnit unit : units ) {
	        long diff = unit.convert(milliesRest,TimeUnit.MILLISECONDS);
	        long diffInMilliesForUnit = unit.toMillis(diff);
	        milliesRest = milliesRest - diffInMilliesForUnit;
	        result.put(unit,diff);
	    }
	    return result;
	}

	public static void main(String [] args) throws IOException, ParseException{

	
		int param = 1;
		
		BufferedReader br = new BufferedReader(new FileReader("experiment_logs/"+args[param]));
		
		Timestamp [] start = new Timestamp [Integer.parseInt(args[0])];
		
		int [] operations = new int [Integer.parseInt(args[0])];
		
		Timestamp [] timestamps = new Timestamp [Integer.parseInt(args[0])];
		
		
		String line = br.readLine();
		
		while(line !=null){
			
			String [] split = line.split(" - ");
			
			if(split[2].equals("exec")){
				int vm = Integer.parseInt(split[1].split("vm")[1]) -1;
				
				operations[vm]++;
			}
			else{
				
				
				
				int vm = Integer.parseInt(split[1].split("vm")[1]) -1;
				
				
				
				Timestamp ts = Timestamp.valueOf(split[0]);
				
				if(start[vm]==null)
					start[vm] = ts;
				
				if(timestamps[vm]==null){
					timestamps[vm]=ts;
				}else{
					//if(ts.after(timestamps[vm]))
						timestamps[vm]=ts;
				}
				
			}
			
			line = br.readLine();
			
			if(line == null){
				param ++;
				if(param < args.length){
					System.out.println(args[param]);
					br = new BufferedReader(new FileReader("experiment_logs/"+args[param]));
					line = br.readLine();
				}
			}
			
			
			
		}
		
		for(int i=0; i< Integer.parseInt(args[0]); i++){
			
			
			Date date1 = new Date(start[i].getTime());
			Date date2 = new Date(timestamps[i].getTime());
			 
			
			System.out.println("VM"+(i+1) +" start "+start[i].toString()+" end "+timestamps[i].toString() );
			System.out.println(operations[i]);
			System.out.println(computeDiff(date1, date2));
		}
	
		
	}
	
}
