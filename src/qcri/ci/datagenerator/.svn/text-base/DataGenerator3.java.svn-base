package qcri.ci.datagenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class DataGenerator3 {
	public static void generate()
	{
		String schema = "A(String),B(String),C(String)";
		
		String inputDCPath = "Experiments/ExpDataGenerator3/inputDB";
		
		//a = 1, b - > C
		try {
			PrintWriter out = new PrintWriter(new FileWriter(inputDCPath));
			
			out.println(schema);
			
			out.println("1,b1,c1");
			out.println("1,b1,c1");
			out.println("1,b2,c2");
			out.println("1,b1,c1");
			out.println("2,b1,c2");
			out.println("2,b2,c2");
			
			out.println("1,b1,c1");
			out.println("1,b1,c1");
			out.println("1,b2,c2");
			out.println("1,b1,c1");
			out.println("2,b1,c2");
			out.println("2,b2,c2");
			
			
		
			
			out.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
