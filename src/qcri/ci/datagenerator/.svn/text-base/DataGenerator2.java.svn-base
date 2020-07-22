package qcri.ci.datagenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class DataGenerator2 {
	
	/**
	 * A->B, but B not->A
	 */
	public static void generate()
	{
		String schema = "A(String),B(String)";
		
		String inputDCPath = "Experiments/ExpDataGenerator2/inputDB";
		
		try {
			PrintWriter out = new PrintWriter(new FileWriter(inputDCPath));
			
			out.println(schema);
			
			
			int numTuples = 1000;
			/*//b = a + 4;
			for(int i = 0 ; i < numTuples; i++)
			{
				StringBuilder sb = new StringBuilder();
				int a = new Random().nextInt(100);
				int b = a + 100;
				sb.append(a + "," + b);
				
				
				out.println(sb);
			}
			*/
		
			
			out.println(1 + "," + 2);
			out.println(1 + "," + 2);
			out.println(2 + "," + 3);
			out.println(2 + "," + 3);
			
			
			//some b to be equal, but a not equal
			out.println(10000 +","+ 100000);
			out.println(10001 +","+ 100000);
			out.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
