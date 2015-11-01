package client.client;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class Experiment1 {

	static ArrayList<String> faculty = new ArrayList<String>(
			Arrays.asList("Informatics", "Bioinformatics","BWL","Biology","Dental Medicine","Design","Mathematics","Physics","Chemistry","Medicine","Art","Mechanical Engineering","Architecture","Construction","Electrical Engineering","Biochemistry","Biophysics","Tiermedizin","English Literature","Arabic Literature","German Literature","Islamic Studies","Political Studies","Finance","VWL","Neuroscience","Classics","English","History of Art","History","Science","Social Science","Economics","Education","Law","Languages","Music","Philosophy","Modern Languages","Medival Languages","Clinical Medicine","Medicine","Geography","Earth Sciences","Business","Management","Engineering"));
	static ArrayList<String> courseName = new ArrayList<String>(
			Arrays.asList("Astronomy","Chemistry","Metallurgy","Physics","French","German","Arabic","Slavonic Studies","Archaeology","Social Anthropology","Biological Anthropology","Asian Studies","American Studies","Development Studies","Psychology","Plant Sciences","Pathology","Genetics","Biochemistry","Clinical pharmacology","Transfusion medicine","Civil engineering","turbomachinery","Computer Laboratory","Lab1","Lab2","Lab3","Lab4","Energy","Fluids","Information engineering","Business Research","Acadamic Research","Scientific Research","Brain mapping unit","Orthopaedic Surgery","Trauma and Orthopaedic Surgery","The Polar Museum","Earth Sciences","Modern Greek","Neo-Latin"));
	static ArrayList<String> studentName = new ArrayList<String>(
			Arrays.asList("Mona","Sara","Lisa","Marie","Maria","David","Kevin","Aziz","Azza","Amina","Ziko","Wenbo","Jan","Adler","Schmidt","Sayahn","Zara","Mango","Orsay","Pimkie","Zero","HM","CA","Muller","Adam","Adisson","Dina","Akram","Alei","Ahmed","Adele","Adelaine","Baily","Barbie","Barbie","Kuku","Caroline","Carolina","Karim","Kate","Caty","Lisa Marie","Charles","Charly","Lina","Darcy","Darien","Dave","Dawn","Aragon","Batman","IronMan","Catwoman","Foxy","Derek","elaine","Fox","Cassandra","Hbase","BigTables","Dynamo","Azure","Amazone","Arizona","Kaley","hiberante","logback","logger","hesham","azza","Monica","Shady","xenia","Bombo","kiwi","rocky","simsim","misho","dudu"));


	private static void generateCsvFile(String sFileName)
	{
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

		for(int i=0;i<100;i++){

			try {
				writer.append(String.valueOf(counter));
				writer.append(',');
				counter++;

				int r = rn.nextInt((courseName.size()-1) - 0 + 1) + 0;
				writer.append("'"+courseName.get(r)+"'");
				writer.append(',');

				r = rn.nextInt(10 - 1 + 1) + 1;
				writer.append(String.valueOf(r));
				writer.append(',');


				r = rn.nextInt((faculty.size()-1) - 0 + 1) + 0;
				writer.append("'"+faculty.get(r)+"'");

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


	private static void generateCsvFile2(String sFileName)
	{
		//1,'Distributed Systems',4,'Informatics'
		int counter = 1;
		FileWriter writer = null;
		try {
			writer = new FileWriter(sFileName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		//1,'Sally Whittaker',3.2,'Informatics',19

		Random rn = new Random();
		counter = 1;

		for(int i=0;i<100;i++){

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

				r = rn.nextInt((faculty.size()-1) - 0 + 1) + 0;
				writer.append("'"+faculty.get(r)+"'");
				writer.append(',');

				r = rn.nextInt(50 - 10 + 1) + 10;
				writer.append(String.valueOf(r));

				writer.append('\n');
				//generate whatever data you want


			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public static void main(String [] args){

		generateCsvFile("src/java/client/data/ex1-courses.csv"); 
		generateCsvFile2("src/java/client/data/ex1-student.csv"); 



	}

}
