package client.client;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class Experiment2_Selec {

	
	static ArrayList<String> faculty = new ArrayList<String>(
			Arrays.asList("Informatics", "Bioinformatics","BWL","Biology","Dental Medicine","Design","Mathematics","Physics","Chemistry","Medicine","Art","Mechanical Engineering","Architecture","Construction","Electrical Engineering","Biochemistry","Biophysics","Tiermedizin","English Literature","Arabic Literature","German Literature","Islamic Studies","Political Studies","Finance","VWL","Neuroscience","Classics","English","History of Art","History","Science","Social Science","Economics","Education","Law","Languages","Music","Philosophy","Modern Languages","Medival Languages","Clinical Medicine","Medicine","Geography","Earth Sciences","Business","Management","Engineering"));
	static ArrayList<String> courseName = new ArrayList<String>(
			Arrays.asList("Astronomy","Chemistry","Metallurgy","Physics","French","German","Arabic","Slavonic Studies","Archaeology","Social Anthropology","Biological Anthropology","Asian Studies","American Studies","Development Studies","Psychology","Plant Sciences","Pathology","Genetics","Biochemistry","Clinical pharmacology","Transfusion medicine","Civil engineering","turbomachinery","Computer Laboratory","Lab1","Lab2","Lab3","Lab4","Energy","Fluids","Information engineering","Business Research","Acadamic Research","Scientific Research","Brain mapping unit","Orthopaedic Surgery","Trauma and Orthopaedic Surgery","The Polar Museum","Earth Sciences","Modern Greek","Neo-Latin"));
	

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

		//40000 insertions that are passing
		for(int i=0;i<40000;i++){

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
		
		
		//40001-800000 insertions that are passing
		for(int i=40000;i<800000;i++){

			try {
				writer.append(String.valueOf(counter));
				writer.append(',');
				counter++;

				int r = rn.nextInt((courseName.size()-1) - 0 + 1) + 0;
				writer.append("'"+courseName.get(r)+"'");
				writer.append(',');

				r = rn.nextInt(4 - 1 + 1) + 1;
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
		
		
		
		for(int i=802500;i<900000;i++){

			try {
				
				int r = rn.nextInt(800000 - 41000 + 1) + 41000;
				writer.append(String.valueOf(r));
				writer.append(',');
				
				r = rn.nextInt((courseName.size()-1) - 0 + 1) + 0;
				writer.append("'"+courseName.get(r)+"'");
				writer.append(',');

				r = rn.nextInt(4 - 1 + 1) + 1;
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
	

		//800001-802500 insertions that are passing
				for(int i=800000;i<802500;i++){

					try {
						
				
						int r = rn.nextInt(40000 - 1 + 1) + 1;
						writer.append(String.valueOf(r));
						writer.append(',');

						r = rn.nextInt((courseName.size()-1) - 0 + 1) + 0;
						writer.append("'"+courseName.get(r)+"'");
						writer.append(',');

						r = rn.nextInt(1000 - 5 + 1) + 5;
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
				
				for(int i=900000;i<902500;i++){

					try {
						

						int r = rn.nextInt(40000 - 1 + 1) + 1;
						writer.append(String.valueOf(r));
						
						writer.append('\n');

						//generate whatever data you want

						writer.flush();

					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				for(int i=902500;i<1000000;i++){

					try {
						

						int r = rn.nextInt(800000 - 41000 + 1) + 41000;
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

		generateCsvFile("src/java/client/data/ex2-courses-selec.csv"); 
		
	}
	
}
