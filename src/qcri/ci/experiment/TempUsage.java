package qcri.ci.experiment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

import qcri.ci.utils.Config;

public class TempUsage {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		rankingFunction();

	}
	private static void rankingFunction() throws Exception
	{
		String input = "Experiments/Z_RankingFunction/RankingResult_TaxGenerator";
		String rkPre = "Experiments/Z_RankingFunction/Precision.csv";
		String rkRecall = "Experiments/Z_RankingFunction/Recall.csv";
		String rkFMeasure = "Experiments/Z_RankingFunction/F-Measure.csv";
		
		
		
		BufferedReader br = new BufferedReader(new FileReader(input));
		PrintWriter out = new PrintWriter(new FileWriter(rkPre));
		String line = null;
		StringBuilder sb1 = new StringBuilder();
		sb1.append("Weight a,");
		for(int i = 0; i < Config.numTopks; i++)
		{
			sb1.append("G-PrecisionForTop-" + Config.grak * (i+1));
			if( i != Config.numTopks - 1)
				sb1.append(",");
		}
		out.println(sb1);
		br.readLine();//discard first line
		while((line = br.readLine()) != null)
		{
			StringBuilder sb = new StringBuilder();
			String[] temp = line.split(",");
			sb.append(temp[0].substring(temp[0].indexOf("_")+1, temp[0].lastIndexOf("_")) + ",");
			for(int i = 0; i < Config.numTopks; i++)
			{
				sb.append(temp[1 + i * 2]);
				if(i != Config.numTopks - 1)
					sb.append(",");
			}
			out.println(sb);
		}
		br.close();
		out.close();
		
		//Recall
		br = new BufferedReader(new FileReader(input));
		out = new PrintWriter(new FileWriter(rkRecall));
		line = null;
		sb1 = new StringBuilder();
		sb1.append("Weight a,");
		for(int i = 0; i < Config.numTopks; i++)
		{
			sb1.append("G-RecallForTop-" + Config.grak * (i+1));
			if( i != Config.numTopks - 1)
				sb1.append(",");
		}
		out.println(sb1);
		br.readLine();//discard first line
		while((line = br.readLine()) != null)
		{
			StringBuilder sb = new StringBuilder();
			String[] temp = line.split(",");
			sb.append(temp[0].substring(temp[0].indexOf("_")+1, temp[0].lastIndexOf("_")) + ",");
			for(int i = 0; i < Config.numTopks; i++)
			{
				sb.append(temp[2 + i * 2]);
				if(i != Config.numTopks - 1)
					sb.append(",");
			}

			out.println(sb);
		}
		br.close();
		out.close();
		
		//F-
		//Recall
		br = new BufferedReader(new FileReader(input));
		out = new PrintWriter(new FileWriter(rkFMeasure));
		line = null;
		sb1 = new StringBuilder();
		sb1.append("Weight a,");
		for(int i = 0; i < Config.numTopks; i++)
		{
			sb1.append("G-F-MeasureForTop-" + Config.grak * (i+1));
			if( i != Config.numTopks - 1)
				sb1.append(",");
		}
		out.println(sb1);
		br.readLine();//discard first line
		while((line = br.readLine()) != null)
		{
			StringBuilder sb = new StringBuilder();
			String[] temp = line.split(",");
			sb.append(temp[0].substring(temp[0].indexOf("_")+1, temp[0].lastIndexOf("_")) + ",");
			for(int i = 0; i < Config.numTopks; i++)
			{
				double p = Double.valueOf(temp[1 + i * 2]);
				double r = Double.valueOf(temp[2 + i * 2]);
				double f = 2 * p * r / (p + r);
				sb.append(f);
				if(i != Config.numTopks - 1)
					sb.append(",");
			}

			out.println(sb);
		}
		br.close();
		out.close();
	}


}
