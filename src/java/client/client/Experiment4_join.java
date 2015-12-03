package client.client;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class Experiment4_join {


	static ArrayList<String> courseName = new ArrayList<String>(
			Arrays.asList("Astronomy","Chemistry","Metallurgy","Physics","French","German","Arabic","Slavonic Studies","Archaeology","Social Anthropology","Biological Anthropology","Asian Studies","American Studies","Development Studies","Psychology","Plant Sciences","Pathology","Genetics","Biochemistry","Clinical pharmacology","Transfusion medicine","Civil engineering","turbomachinery","Computer Laboratory","Lab1","Lab2","Lab3","Lab4","Energy","Fluids","Information engineering","Business Research","Acadamic Research","Scientific Research","Brain mapping unit","Orthopaedic Surgery","Trauma and Orthopaedic Surgery","The Polar Museum","Earth Sciences","Modern Greek","Neo-Latin"));

	static ArrayList<String> studentName = new ArrayList<String>(
			Arrays.asList("Mona","Sara","Lisa","Marie","Maria","David","Kevin","Aziz","Azza","Amina","Ziko","Wenbo","Jan","Adler","Schmidt","Sayahn","Zara","Mango","Orsay","Pimkie","Zero","HM","CA","Muller","Adam","Adisson","Dina","Akram","Alei","Ahmed","Adele","Adelaine","Baily","Barbie","Barbie","Kuku","Caroline","Carolina","Karim","Kate","Caty","Lisa Marie","Charles","Charly","Lina","Darcy","Darien","Dave","Dawn","Aragon","Batman","IronMan","Catwoman","Foxy","Derek","elaine","Fox","Cassandra","Hbase","BigTables","Dynamo","Azure","Amazone","Arizona","Kaley","hiberante","logback","logger","hesham","azza","Monica","Shady","xenia","Bombo","kiwi","rocky","simsim","misho","dudu"));

	
	static String [] shared_faculty;
	static String [] courses_faculty;
	static String [] student_faculty;
	
	private static void generateCsvFile(String sFileName, int percentage)
	{
		
		
		int shared_size = 3000*percentage/100;
		
		shared_faculty = new String [shared_size];
		courses_faculty = new String[3000];
		student_faculty = new String[3000];
		
		for(int i = 0; i < shared_size; i++){
			shared_faculty[i]="x"+i;
			courses_faculty[i]="x"+i;
			student_faculty[i]="x"+i;
		}
		
		for(int i= shared_size; i < 3000 ;i++){
			courses_faculty[i]="y"+i;
			student_faculty[i]="z"+i;
		}
		
		//1,'Distributed Systems',4,'Informatics'
		int counter = 1;
		FileWriter writer = null;
		try {
			writer = new FileWriter(sFileName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		
		Random rn = new Random();
		

		//40000 insertions that are passing
		//for(int i=0;i<40000;i++){
		for(int i=0;i<3000;i++){
			try {
				writer.append(String.valueOf(counter));
				writer.append(',');
				counter++;

				int r = rn.nextInt((courseName.size()-1) - 0 + 1) + 0;
				writer.append("'"+courseName.get(r)+"'");
				writer.append(',');

				r = rn.nextInt(1000 - 5 + 1) + 5;
				writer.append(String.valueOf(r));
				writer.append(',');

				
				//r = rn.nextInt(4999 - 0 + 1) + 0;
				writer.append("'"+courses_faculty[i]+"'");
				

				writer.append('\n');

				//generate whatever data you want

				writer.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		for(int i=3000;i<4000;i++){
			try {
				
				int r = rn.nextInt(4999 - 1 + 1) + 1;
				writer.append(String.valueOf(r));
				writer.append(',');

				r = rn.nextInt((courseName.size()-1) - 0 + 1) + 0;
				writer.append("'"+courseName.get(r)+"'");
				writer.append(',');

				r = rn.nextInt(1000 - 5 + 1) + 5;
				writer.append(String.valueOf(r));
				writer.append(',');

				r = rn.nextInt(2999 - 0 + 1) + 0;
				writer.append("'"+courses_faculty[r]+"'");

				writer.append('\n');

				//generate whatever data you want

				writer.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
		
		//800001-802500 insertions that are passing
		for(int i=4000;i<5000;i++){

			try {

				int r = rn.nextInt(2999 - 1 + 1) + 1;
				writer.append(String.valueOf(r));

				writer.append('\n');

				//generate whatever data you want

				writer.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		////// student
		//1,'Sally Whittaker',3.2,'Informatics',19
		counter = 1;
		for(int i=0;i<3000;i++){
			try {
				writer.append(String.valueOf(counter));
				writer.append(',');
				counter++;

				int r = rn.nextInt((studentName.size()-1) - 0 + 1) + 0;
				writer.append("'"+studentName.get(r)+"'");
				writer.append(',');

				double finalX = rn.nextFloat() * (4.0 - 1.0) + 1.0;
				writer.append(String.valueOf(finalX));
				writer.append(',');

				//r = rn.nextInt(2999 - 0 + 1) + 0;
				writer.append("'"+student_faculty[i]+"'");
				writer.append(',');

				r = rn.nextInt(50 - 10 + 1) + 10;
				writer.append(String.valueOf(r));

				writer.append('\n');
				//generate whatever data you want

				writer.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		for(int i=3000;i<4000;i++){
			try {
				
				int r = rn.nextInt(4999 - 1 + 1) + 1;
				writer.append(String.valueOf(r));
				writer.append(',');

				 r = rn.nextInt((studentName.size()-1) - 0 + 1) + 0;
				writer.append("'"+studentName.get(r)+"'");
				writer.append(',');

				double finalX = rn.nextFloat() * (4.0 - 1.0) + 1.0;
				writer.append(String.valueOf(finalX));
				writer.append(',');

				r = rn.nextInt(2999 - 0 + 1) + 0;
				writer.append("'"+student_faculty[r]+"'");
				writer.append(',');

				r = rn.nextInt(50 - 10 + 1) + 10;
				writer.append(String.valueOf(r));
				
				writer.append('\n');

				//generate whatever data you want

				writer.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for(int i=4000;i<5000;i++){

			try {

				int r = rn.nextInt(2999 - 1 + 1) + 1;
				writer.append(String.valueOf(r));

				writer.append('\n');

				//generate whatever data you want

				writer.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		

		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String [] args){

		int x = 10;
		generateCsvFile("src/java/client/data/ex4-data/ex4-courses-join-"+x+"precent.csv",x); 

	}

}
