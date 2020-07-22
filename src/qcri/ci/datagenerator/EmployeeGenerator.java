package qcri.ci.datagenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class EmployeeGenerator {

	public static void generate()
	{
		int numManager = 10;
		int[] mids = new int[numManager];
		int[] midSal = new int[numManager];	
		for(int i = 0; i < mids.length; i++)
		{
			mids[i] = i;
			midSal[i] = (10 + new Random().nextInt(20)) * 10; //100-300
		}
		String schema = "ID(String),ManagerID(String),Salary(Integer),Tax(Integer)";
		String inputDCPath = "Experiments/ExpEmployee/inputDB";
		int numTuples = 10000;
		try {
			PrintWriter out = new PrintWriter(new FileWriter(inputDCPath));
			out.println(schema);
			for(int i = 0; i < numManager; i++)
			{
				StringBuilder sb = new StringBuilder();
				sb.append(i);
				sb.append(",");
				
				sb.append(-1);
				sb.append(",");
				
				//salary
			
				int sal = midSal[i];
				sb.append(sal);
				sb.append(",");
				
				//tax
				int tax = genTaxFromSalary(sal);
				sb.append(tax);
				out.println(sb);
			}
			
			//Generate employee
			
			for(int i = numManager; i < numTuples; i++)
			{
				StringBuilder sb = new StringBuilder();
				sb.append(i);
				sb.append(",");
				
				int mid = mids[new Random().nextInt(numManager)];
				sb.append(mid);
				sb.append(",");
				
				//salary
				int manageSal = midSal[mid];
				int sal = new Random().nextInt(manageSal);
				sal -= sal%10;
				sb.append(sal);
				sb.append(",");
				
				//tax
				int tax = genTaxFromSalary(sal);
				sb.append(tax);
				out.println(sb);
				
			}
			
			out.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public static int genTaxFromSalary(int sal)
	{
		if(sal >= 0 && sal <= 100)
		{
			int tax = (int)(0.1 * sal);
			return tax;
		}
		else if (sal >= 100 && sal <= 200)
		{
			int tax = (int)(0.3* sal - 20);
			return tax;
		}
		else if(sal >=200 && sal <= 300)
		{
			int tax = (int)(0.5 * sal - 60);
			return tax;
		}
		else
			return (int)(0.5* sal);
		
	}

}
