package qcri.ci.utils;

import java.util.*;
import java.io.*;

import javax.imageio.ImageIO;

import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.NamedPlotColor;
import com.panayotis.gnuplot.style.PlotColor;
import com.panayotis.gnuplot.style.Style;
import com.panayotis.gnuplot.terminal.ImageTerminal;
import com.panayotis.gnuplot.terminal.PostscriptTerminal;
public class MyPlotInfo {

	public boolean YSetLogScale = false;
	public boolean XSetLogScale = false;
	
	
	
	public String inputFile;
	public String xName;
	public String yName;
	
	public int numPlots;
	public ArrayList<double[][]> datas = new ArrayList<double[][]>();
	public ArrayList<String> yNames = new ArrayList<String>();
	
	public MyPlotInfo(String inputFile) throws IOException
	{
		this.inputFile = inputFile;
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = null;
		boolean isFirstLine = true;
		String firstLine = null;
		int[] numRowsEachPlot = null;
		
		while((line = br.readLine())!=null)
		{
			if(isFirstLine)
			{
				isFirstLine = false;
				firstLine = line;
				String[] temp = firstLine.split(",");
				numPlots = temp.length - 1;
				xName = temp[0];
				numRowsEachPlot = new int[numPlots];
				for(int i = 0 ; i < numPlots; i++)
				{
					yNames.add(temp[i+1]);
				}
				yName = yNames.get(0).split("For")[0];
				//yName = yNames.get(0);
			}
			else
			{
				String[] ss1 = line.split(",");
				for(int i = 0 ; i < numPlots; i++)
				{
					if(!ss1[i+1].equals("NA"))
					{
						numRowsEachPlot[i]++;
					}
				}
			}
		}
		br.close();
		
		for(int i = 0 ; i < numPlots; i++)
		{
			datas.add(new double[numRowsEachPlot[i]][2]);
		}
		
		
		//fill in the data
		isFirstLine = true;
		int lineNum = 0;
		br = new BufferedReader(new FileReader(inputFile));
		while((line = br.readLine())!=null)
		{
			if(isFirstLine)
			{
				isFirstLine = false;
			}
			else
			{
				String[] ss1 = line.split(",");
				for(int i = 0 ; i < numPlots; i++)
				{
					if(lineNum < numRowsEachPlot[i])
					{
						datas.get(i)[lineNum][0] = Double.valueOf(ss1[0]);
						datas.get(i)[lineNum][1] = Double.valueOf(ss1[i+1]);
					}
					
				}
				lineNum++;
			}
		}
		br.close();
		
	}
	
	public void startPlot() throws IOException
	{
		int FONT_SIZE = 35;
		//JavaPlot plot = new JavaPlot();
		JavaPlot plot = new JavaPlot("/Applications/Gnuplot.app/Contents/Resources/bin/gnuplot");

		PlotColor[]	 colors = new PlotColor[11];
		colors[0] = NamedPlotColor.BLACK;
		colors[1] = NamedPlotColor.BLUE;
		colors[2] = NamedPlotColor.RED;
		colors[3] = NamedPlotColor.GREEN;
		colors[4] = NamedPlotColor.YELLOW;
		colors[5] = NamedPlotColor.CORAL;
		colors[6] = NamedPlotColor.AQUAMARINE;
		colors[7] = NamedPlotColor.CYAN;
		colors[8] = NamedPlotColor.DARK_GOLDENROD;
		colors[9] = NamedPlotColor.DARK_BLUE;
		colors[10] = NamedPlotColor.DARK_GREY;
		//colors = NamedPlotColor.values();
		
		if(inputFile.contains("parallel"))
		{
			
			xName = xName + " (*1M)";
		}
		
		else if(inputFile.contains("VaryingNumTuple")
				|| inputFile.contains("sampling"))
		{
			xName = xName + " (*1K)";
		}
		if(inputFile.contains("NoiseToleranceLevel")
				)
			{
				
				//xName = xName + " ( * 0.001)";
				xName = xName + " (*0.000001)";
			}
		
		if(inputFile.contains("Y_FixNumberTuplesNoiseTolerance_VaryingNoiseLevel"))
		{
			xName = xName + " ( * 0.001)";
			
		}
		
		
		
		for(int i = 0 ; i < numPlots; i++)
		{
			double[][] data = datas.get(i);
			
			
			if(inputFile.contains("parallel")
					)
				{
					for(int t = 0 ; t < data.length; t++)
					{
						data[t][0] = data[t][0] / 1000000;
						
						
					}
					//xName = xName + " (*1000)";
				}

			else if(inputFile.contains("VaryingNumTuple")
					|| inputFile.contains("sampling"))
			{
				for(int t = 0 ; t < data.length; t++)
				{
					data[t][0] = data[t][0] / 1000;
					
					
				}
				//xName = xName + " (*1000)";
			}
			
			
			if(inputFile.contains("NoiseToleranceLevel")
					)
				{
					for(int t = 0 ; t < data.length; t++)
					{
						data[t][0] = data[t][0] * 1000;
						
						
					}
					//xName = xName + " (*1000)";
				}
			
			if(inputFile.contains("Y_FixNumberTuplesNoiseTolerance_VaryingNoiseLevel"))
			{
				for(int t = 0 ; t < data.length; t++)
				{
					data[t][0] = data[t][0] * 1000;
					
					
				}
			}
			
			
						
			//Plus 1 for log scale
			if(YSetLogScale)
			{
				for(int t = 0 ; t < data.length; t++)
				{
					data[t][1] = data[t][1] + 1;
					
				}
			}
			
			
			
			
			// plot a line
			DataSetPlot singlePlot = new DataSetPlot(data);
			
			// set the style as a line
			//singlePlot.getPlotStyle().setStyle(Style.LINES);
	
			singlePlot.getPlotStyle().setStyle(Style.LINESPOINTS);
			
			
			
			// set size
			singlePlot.getPlotStyle().setLineWidth(8);
			// set color
			singlePlot.getPlotStyle().setLineType(colors[i]);
			//points
			singlePlot.getPlotStyle().setPointSize(3);
			//singlePlot.getPlotStyle().setPointType(i);
			
			
			singlePlot.setTitle(yNames.get(i).split("For")[1]);
			
			plot.addPlot(singlePlot);
			// set the label and the font
			
			plot.getAxis("x").setBoundaries(data[0][0], data[data.length-1][0]);
			
			
			
			
			//if(data[0][1] != data[data.length-1][1])
				//plot.getAxis("y").setBoundaries(data[0][1], data[data.length-1][1]);
			
			plot.getAxis("x").setLabel(xName, "Arial",FONT_SIZE);
			
			
			if(XSetLogScale)
				plot.getAxis("x").setLogScale(true);
			
			if(YSetLogScale)
				plot.getAxis("y").setLogScale(true);
			
			
			
			boolean setY01 = true;
			for(int t = 0 ; t < data.length; t++)
			{
				if(data[t][1] < 0 || data[t][1] > 1)
				{
					setY01 = false;
					break;
				}
			}
			if(setY01)
				plot.getAxis("y").setBoundaries(0, 1);
			//plot.getAxis("y").setBoundaries(1, 10);
		}
		//plot.setTitle("XuDemo");
		plot.getAxis("y").setLabel(yName, "Arial", FONT_SIZE);

		
		//plot.set("xtics", "'10'");
		//plot.set("key font", "'0','20'");
		//plot.set("xtics font", "',14'");

		//key, center, left, right, bottom, center right, bottom left
		plot.set("key", "center right");
		
		
		//plot.set("size", "1,1");
		
		//set global settings
		plot.set("terminal", "postscript enhanced \"Helvetica\" 30");

		
		String outputFile = inputFile + "_Plots.eps";
		// set the output file
		PostscriptTerminal term = new PostscriptTerminal(outputFile);
		term.setEPS(true);
		plot.setTerminal(term);
		plot.plot();
		
		
		
		
	}
	
}
