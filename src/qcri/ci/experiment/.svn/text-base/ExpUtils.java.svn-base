package qcri.ci.experiment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class ExpUtils {

	
	static String head =  "Setting,NumOfTuples,NumOfAttrs,RunningTime(s),NumOfPres,DFSTimePerDC(ms),WastedWork,NumMinimalDCs,timeInitTuplePair(s),timeDFS(s)," +
			"PrecisionTop5,RecallTop5," +  
			"PrecisionTop10,RecallTop10," + 
			"PrecisionTop15,RecallTop15," + 
			"PrecisionTop20,RecallTop20"  
			; 
	static int startingPrecision = 10;
	
	public static String getExpFolder(String dataset)
	{
		return "Experiments/" + dataset + "/";
	}

	
	public static void randomSample(int total, int sampleSize, int[] perm)
	{
		int M = sampleSize;    // choose this many elements
        int N = total;    // from 0, 1, ..., N-1

        // create permutation 0, 1, ..., N-1
        //int[] perm = new int[N];
        assert(perm.length == total);
        
        for (int i = 0; i < N; i++)
            perm[i] = i;

        // create random sample in perm[0], perm[1], ..., perm[M-1]
        for (int i = 0; i < M; i++)  {

            // random integer between i and N-1
            int r = i + (int) (Math.random() * (N-i-1));

            // swap elements at indices i and r
            int t = perm[r];
            perm[r] = perm[i];
            perm[i] = t;
        }

        // print results
       /* for (int i = 0; i < M; i++)
            System.out.print(perm[i] + " ");
        System.out.println();*/
	}
	
	public static void sampleInputDB(String dataset, int numTuples, ArrayList<Integer> whichAttrs) throws IOException
	{
		
		String srcFile = "Experiments/" + dataset + "/" + "inputDBAll";
		String desFile = "Experiments/" + dataset + "/" + "inputDB";
		
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		PrintWriter out = new PrintWriter(new FileWriter(desFile));
		
		ArrayList<String> dbs = new ArrayList<String>();
		int count = -1;
		String line = null;
		String head = null;
		while((line = br.readLine())!=null)
		{
			if(count == -1)
			{
				count ++;
				head = line;
			}
				
			dbs.add(line);
		}
		
		
		
		
		int[] perm = new int[dbs.size()];
		randomSample(dbs.size(), numTuples, perm);
		int numAttri = whichAttrs.size();
		//Write the result back
		for(int j = -1; j < numTuples; j++)
		{
			if(j == -1)
			{
				line = head;
			}
			else
			{
				line = dbs.get(perm[j]);
			}
			String[] temp = line.split(",");
			StringBuilder sb = new StringBuilder();
			if(numAttri > temp.length)
				numAttri = temp.length;
			for(int i = 0 ; i < numAttri; i++)
			{
				sb.append(temp[whichAttrs.get(i)]);
				if(i != numAttri -1)
					sb.append(",");
			}
			out.println(sb);
			
		}
		
		
		
		br.close();
		out.close();
	}
	
	
	
	
	
	
	
	
	public static void getInputDB(String dataset, int numTuples, int numAttri)
	{
		//include the header
		String srcFile = "Experiments/" + dataset + "/" + "inputDBAll";
		String desFile = "Experiments/" + dataset + "/" + "inputDB";
		try {
			qcri.ci.utils.FileUtil.firstKTuples(srcFile, desFile, numTuples,numAttri);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
	}
	
	
	public static void genInputDBRandomAttri(String dataset, int numTuples, ArrayList<Integer> whichAttrs) throws IOException
	{
		
		String srcFile = "Experiments/" + dataset + "/" + "inputDBAll";
		String desFile = "Experiments/" + dataset + "/" + "inputDB";
		
		
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		PrintWriter out = new PrintWriter(new FileWriter(desFile));
		
		
		int count = -1;
		String line = null;
		int numAttri = whichAttrs.size();
		
		while((line = br.readLine())!=null)
		{
			if(count <= numTuples)
			{
				if(numAttri == -1)
					out.println(line);
				else
				{
					
					String[] temp = line.split(",");
					StringBuilder sb = new StringBuilder();
					if(numAttri > temp.length)
						numAttri = temp.length;
					for(int i = 0 ; i < numAttri; i++)
					{
						sb.append(temp[whichAttrs.get(i)]);
						if(i != numAttri -1)
							sb.append(",");
					}
					out.println(sb);
					
				}
				count++;
			}
		}
		br.close();
		out.close();
	}
	
	
	public static void cleanStock(String srcFile, String desFile) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		PrintWriter out = new PrintWriter(new FileWriter(desFile));
		
		int count = -1;
		String line = null;
		int numAttri;
		while((line = br.readLine())!=null)
		{
		
			
			if(count == -1)
			{
				out.println(line);
				numAttri = line.split(",").length;
			}
			else
			{
				String[] temp = line.split(",");
				
				double open = Double.valueOf(temp[2]);
				double high = Double.valueOf(temp[3]);
				double low = Double.valueOf(temp[4]);
				double close = Double.valueOf(temp[5]);
				
				if(high < open || high < low || high < close)
					continue;
				if(low > open || low > high || low > close)
					continue;
				
				
				
				out.println(line);
				
			}
				
			count++;
			
		}
		br.close();
		out.close();
		
	}
}
