package client.client;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class Experiment3_agg {

	/*static ArrayList<String> faculty = new ArrayList<String>(
			Arrays.asList("Informatics", "Bioinformatics","BWL","Biology","Dental Medicine","Design","Mathematics","Physics","Chemistry","Medicine","Art","Mechanical Engineering","Architecture","Construction","Electrical Engineering","Biochemistry","Biophysics","Tiermedizin","English Literature","Arabic Literature","German Literature","Islamic Studies","Political Studies","Finance","VWL","Neuroscience","Classics","English","History of Art","History","Science","Social Science","Economics","Education","Law","Languages","Music","Philosophy","Modern Languages","Medival Languages","Clinical Medicine","Medicine","Geography","Earth Sciences","Business","Management","Engineering"));
	 */
	 //10 keys
	/*static ArrayList<String> faculty = new ArrayList<String>(
			Arrays.asList("Informatics", "Bioinformatics","BWL","Biology","Dental Medicine","Design","Mathematics","Physics","Chemistry","Medicine",
					"Informatics1", "Bioinformat1ics","B1WL","B1iology","Den1tal Medicine","Des1ign","Mathema1tics","Phy1sics","Chemist1ry","Medi1cine",
					"Info2rmatics", "Bioi2nformatics","BW2L","Biol2ogy","Dent2al Medicine","Des2ign","Mathe2matics","Phys2ics","Chemist2ry","Medi2cine",
					"Inform3atics", "Bioinforma3tics","BW3L","Bio3logy","Dental 3Medicine","Des3ign","Mat3hematics","Phy3sics","Che3mistry","Me3dicine",
					"Info4rmatics", "Bioinform4atics","BW4L","B4iology","Den4tal Medicine","De4sign","Math4ematics","Physic4s","Che4mistry","Med4icine"));
*/

		 
	static ArrayList<String> faculty = new ArrayList<String>(
			Arrays.asList("Informatics", "Bioinformatics","BWL","Biology","Dental Medicine","Design","Mathematics","Physics","Chemistry","Medicine",
					"Informatics", "Bioinformatics1","BWL1","Biology1","Dental Medicine1","Design1","Mathematics1","Physics1","Chemistry1","Medicine1",
					"Informatics2", "Bioinformatics2","BWL2","Biology2","Dental Medicine2","Design2","Mathematics2","Physics2","Chemistry2","Medicine2",
					"Informat3ics", "Bioinf3ormatics","BW3L","Biol3ogy","Dental Medici3ne","Des33ign","Mathemat3ics","Phys3ics","Chemistry3","Medicine3",
					"Informat34ics", "Bioinf43ormatics","B4W3L","Bio4l3ogy","Den4tal Medici3ne","Des334ign","Mathema4t3ics","Phy4s3ics","Chemist4ry3","Medic4ine3",
					"Informat34i5cs", "Bioinf43orm5atics","B45W3L","Bio54l3ogy","Den54tal Medici3ne","Des5334ign","Mat5hema4t3ics","Phy54s3ics","Che5mist4ry3","Me5dic4ine3",
					"Info6rmat34i5cs", "Bioinf643orm5atics","B465W3L","Bi6o54l3ogy","Den654tal Medici3ne","De6s5334ign","Ma6t5hema4t3ics","P6hy54s3ics","Ch6e5mist4ry3","Me65dic4ine3",
					"Info6rmat374i5cs", "Bioinf7643orm5atics","B4675W3L","Bi6o754l3ogy","Den6754tal Medici3ne","De6s75334ign","Ma6t5he7ma4t3ics","P6hy574s3ics","Ch6e57mist4ry3","Me65dic74ine3",
					"Info6rm8at374i5cs", "Bi8oinf7643orm5atics","B48675W3L","Bi68o754l3ogy","Den68754tal Medici3ne","De6s753834ign","Ma6t5h8e7ma4t3ics","P6hy5784s3ics","Ch68e57mist4ry3","Me65d8ic74ine3",
					"Info6rm98at374i5cs", "Bi8oinf97643orm5atics","B498675W3L","Bi68o9754l3ogy","Den687594tal Medici3ne","De6s7953834ign","Ma6t5h8e7ma4t93ics","P6hy5784s39ics","Ch698e57mist4ry3","Me659d8ic74ine3",
					"Inform11atics", "Bioi11nformatics","11BWL","Bi11ology","De11ntal Medicine","Des11ign","Mathe11matics","Physi11cs","Chemis11try","Medic11ine",
					"Informati22cs", "Bioinf22ormatics1","B22WL1","22Biology1","Denta22l Medicine1","Desi22gn1","Ma22thematics1","Phys22ics1","Chemis22try1","Medic22ine1",
					"Infor33matics2", "Bioinfo33rmatics2","BW33L2","Biol33ogy2","Den33tal Medicine2","De33sign2","Mathe33matics2","Phys33ics2","Chem33istry2","Medici33ne2",
					"Infor44mat3ics", "Bi44oinf3ormatics","B44W3L","Biol443ogy","Dent44al Medici3ne","De44s33ign","M44athemat3ics","Phys443ics","Che44mistry3","Med44icine3",
					"In554format34ics", "Bioi55nf43ormatics","B455W3L","Bio554l3ogy","Den4t55al Medici3ne","Des35534ign","Mathe55ma4t3ics","P55hy4s3ics","Chemi55st4ry3","Me55dic4ine3",
					"Inform66at34i5cs", "Bioinf6643orm5atics","B4665W3L","Bio5664l3ogy","Den54tal Me66dici3ne","Des665334ign","Mat566hema4t3ics","Phy54s3i66cs","Che5mi66st4ry3","Me5dic664ine3",
					"Info6rmat3477i5cs", "Bioinf77643orm5atics","B46775W3L","Bi6o5477l3ogy","Den654ta77l Medici3ne","De776s5334ign","Ma6t775hema4t3ics","P776hy54s3ics","Ch776e5mist4ry3","Me7765dic4ine3",
					"Info6rma88t374i5cs", "Bi88oinf7643orm5atics","B488675W3L","Bi6o75488l3ogy","Den675488tal Medici3ne","De6s7885334ign","Ma6t5he7ma884t3ics","P6hy574s883ics","Ch6e57mist884ry3","Me65dic8874ine3",
					"Info6rm899at374i5cs", "Bi8oi99nf7643orm5atics","B4899675W3L","Bi68o99754l3ogy","Den9968754tal Medici3ne","De699s753834ign","Ma6t5h899e7ma4t3ics","P6hy599784s3ics","Ch68e57mis99t4ry3","Me65d899ic74ine3",
					"Info6r100m98at374i5cs", "Bi8100oinf97643orm5atics","B410098675W3L","Bi68o9710054l3ogy","Den687100594tal Medici3ne","De6s7100953834ign","Ma6t5h8e7m100a4t93ics","P6hy5784100s39ics","Ch698e51007mist4ry3","Me659d1008ic74ine3"
					
					
					));
	
	// 3 keys
	/*static ArrayList<String> faculty = new ArrayList<String>(
			Arrays.asList("Informatics", "Bioinformatics","BWL"));*/


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
		int value = 1;

		//40000 insertions that are passing
		//for(int i=0;i<40000;i++){
		for(int i=0;i<5000;i++){
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


				if(value==4)
					value = 1;
				
				writer.append("'"+"fac"+value+"'");
				value++;
				

				writer.append('\n');

				//generate whatever data you want

				writer.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		for(int i=5000;i<8000;i++){
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

				if(value==4)
					value = 1;
				
				writer.append("'"+"fac"+value+"'");
				value++;

				writer.append('\n');

				//generate whatever data you want

				writer.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
		
		//800001-802500 insertions that are passing
		for(int i=8000;i<10000;i++){

			try {

				int r = rn.nextInt(4999 - 1 + 1) + 1;
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

		generateCsvFile("src/java/client/data/ex3-courses-agg-3.csv"); 

	}

}
