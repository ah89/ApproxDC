package qcri.ci.datagenerator;

import java.io.*;
import java.util.Random;
public class DataGenerator1 {

	public static void generate()
	{
		String schema = "A(Integer),B(Integer),C(Integer)";
		
		String inputDCPath = "Experiments/ExpDataGenerator1/inputDB";
		
		try {
			PrintWriter out = new PrintWriter(new FileWriter(inputDCPath));
			
			out.println(schema);
			//B = A + 4, C = A + 8;
			//So, A,B,C, they inter-determine each other
			
			int numTuples = 30;
			for(int i = 0 ; i < numTuples; i++)
			{
				StringBuilder sb = new StringBuilder();
				int a = new Random().nextInt(10);
				int b = a + 4;
				int c = a + 8;
				sb.append(a + "," + b + "," + c);
				out.println(sb);
			}
		
			
			out.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
