package qcri.ci.experiment;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.List;

import com.sun.corba.se.spi.orb.StringPair;
import com.sun.rowset.internal.Row;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.renderer.category.BoxAndWhiskerRenderer;
import org.jfree.data.statistics.DefaultBoxAndWhiskerCategoryDataset;
import qcri.ci.ConstraintDiscovery;
import qcri.ci.ConstraintDiscovery3;
import qcri.ci.generaldatastructure.constraints.DenialConstraint;
import qcri.ci.generaldatastructure.constraints.NewDenialConstraint;
import qcri.ci.generaldatastructure.constraints.NewPredicate;
import qcri.ci.generaldatastructure.constraints.Predicate;
import qcri.ci.generaldatastructure.db.NewTable;
import qcri.ci.generaldatastructure.db.NewTuple;
import qcri.ci.generaldatastructure.db.Table;
import qcri.ci.generaldatastructure.db.Tuple;
import qcri.ci.instancedriven.*;
import qcri.ci.utils.Config;
import qcri.ci.utils.FileUtil;
import qcri.ci.utils.IntegerPair;

import javax.imageio.ImageIO;
import javax.swing.*;

public class Test {
	private static boolean compareDC(NewDenialConstraint d1, NewDenialConstraint d2) {
		for (NewPredicate p : d1.getPredicates()) {
			boolean exist = false;
			for (NewPredicate p2: d2.getPredicates()) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					exist = true;
				}
			}
			if (!exist) {
				return false;
			}
		}



		for (NewPredicate p : d2.getPredicates()) {
			boolean exist = false;
			for (NewPredicate p2: d1.getPredicates()) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					exist = true;
				}
			}
			if (!exist) {
				return false;
			}
		}

		return true;
	}

	private static boolean compareDC(DenialConstraint d1, NewDenialConstraint d2) {
		for (Predicate p : d1.getPredicates()) {
			boolean exist = false;
			for (NewPredicate p2: d2.getPredicates()) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					exist = true;
				}
			}
			if (!exist) {
				return false;
			}
		}

		for (NewPredicate p : d2.getPredicates()) {
			boolean exist = false;
			for (Predicate p2: d1.getPredicates()) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					exist = true;
				}
			}
			if (!exist) {
				return false;
			}
		}

		return true;
	}

	private static boolean compareDC(NewDenialConstraint d1, DenialConstraint d2) {
		for (NewPredicate p : d1.getPredicates()) {
			boolean exist = false;
			for (Predicate p2: d2.getPredicates()) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					exist = true;
				}
			}
			if (!exist) {
				return false;
			}
		}

		for (Predicate p : d2.getPredicates()) {
			boolean exist = false;
			for (NewPredicate p2: d1.getPredicates()) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					exist = true;
				}
			}
			if (!exist) {
				return false;
			}
		}

		return true;
	}


	private static boolean compareHS(Set<Predicate> first, Set<Predicate> second) {
		for (Predicate p : first) {
			boolean exist = false;
			for (Predicate p2: second) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					exist = true;
				}
			}
			if (!exist) {
				return false;
			}
		}

		for (Predicate p : second) {
			boolean exist = false;
			for (Predicate p2: first) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					exist = true;
				}
			}
			if (!exist) {
				return false;
			}
		}

		return true;
	}

	public static void approximate_Varying_NoiseToleranceLevel(String dataset) throws IOException
		{
			List<List<NewDenialConstraint>> results = new ArrayList<>();
			//for a fixed num of tuples, compute the pair wise info
			int numTuples = 10000;
			//Config.enableCrossColumn = true;
			Config.noiseLevel = 0.1;



			//insertNoise(dataset,numTuples,0.5);
			//Config.howInit = 2;

			//ConstraintDiscovery cd = null;
			//cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDBNoise",1,3,1,numTuples);
			//cd.initHeavyWork(Config.howInit);

			ConstraintDiscovery cd = null;
			ConstraintDiscovery3 cd3 = null;
			//cd3 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDBNoise",1,3,1,numTuples);
			//cd3.initHeavyWork(Config.howInit);

			//int[] times = new int[]{2,4,6,8,10};

			//double[] times = new double[]{1,3,5,7,9,11,13,15,17,19};
			double[] times = new double[]{1,2,3,4,5,6,7,8,9,10};
			//double[] times = new double[]{1};


			for(int timeIndex = 0; timeIndex < times.length; timeIndex++)
			{
				//Get the exact output
				double time = times[timeIndex];

				Config.noiseTolerance = Config.noiseLevel * 0.1 * time;

				System.out.println("Noise Tolerance Level: " + Config.noiseLevel * 0.1 * time );

				//Config.dfsLevel = 4;

				//Config.howInit = 2;

				List<NewDenialConstraint> curRes;

				cd3 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
						1,3,1,numTuples,1);
				//cd.discoverEXCHKS();


				cd3.initHeavyWork(Config.howInit);
				curRes = cd3.discover();
				//cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDBNoise",1,3,1,numTuples);
				//cd.initHeavyWork(Config.howInit); cd.discover();

				results.add(curRes);

			}

			//Plot: x: noisy tolerance level   y is the Top-10 pre/recall
			String[] prfs = new String[]{"Precision","Recall","F-Measure","Time (s)ForDFS"};
			for(String prf: prfs)
			{
				int timeIndex = 0;
				BufferedReader br = new BufferedReader(new FileReader("Experiments/ExpReport.CSV"));
				String fileName = null;

				if(prf.equals("Precision"))
				{
					fileName = ExpUtils.getExpFolder(dataset)+ "VaryingNoiseToleranceLevel_10k_Precision.csv";
				}
				else if(prf.equals("Recall"))
				{
					fileName = ExpUtils.getExpFolder(dataset)+ "VaryingNoiseToleranceLevel_10k_Recall.csv";
				}
				else if(prf.equals("F-Measure"))
				{
					fileName = ExpUtils.getExpFolder(dataset)+ "VaryingNoiseToleranceLevel_10k_FMeasure.csv";
				}
				else
				{
					fileName = ExpUtils.getExpFolder(dataset)+ "VaryingNoiseToleranceLevel_10k_DFSTime.csv";
				}
				PrintWriter out = new PrintWriter(new FileWriter(fileName));
				String thisHead = "Approx. Level,";
				if(prf.equals("Precision"))
				{
					for(int i = 0 ; i < Config.numTopks; i++)
					{
						thisHead += "G-PrecisionForTop-" + Config.grak * (i+1);
						if( i!= Config.numTopks - 1)
							thisHead += ",";
					}
				}
				else if(prf.equals("Recall"))
				{
					for(int i = 0 ; i < Config.numTopks; i++)
					{
						thisHead += "G-RecallForTop-" + Config.grak * (i+1);
						if( i!= Config.numTopks - 1)
							thisHead += ",";
					}
				}
				else if(prf.equals("F-Measure"))
				{
					for(int i = 0 ; i < Config.numTopks; i++)
					{
						thisHead += "G-F-MeasureForTop-" + Config.grak * (i+1);
						if( i!= Config.numTopks - 1)
							thisHead += ",";
					}
				}
				else
				{
					thisHead += "Time (s)ForDFS";
				}
				out.println(thisHead);

				String line = null;
				int count = -1;
				while((line = br.readLine()) != null)
				{
					if(count == -1)
					{
						count++;
						continue;
					}
					String[] values = line.split(",");
					StringBuilder sb = new StringBuilder();
					sb.append(Config.noiseLevel * times[timeIndex] + ",");
					timeIndex ++;
					if(prf.equals("Time (s)ForDFS"))
					{
						sb.append(values[9]);
					}
					else
					{
						for(int i = 0; i < Config.numTopks; i++)
						{

							if(prf.equals("Precision"))
							{
								sb.append(values[10 + i * 2]);
							}
							else if(prf.equals("Recall"))
							{
								sb.append(values[11 + i * 2]);
							}
							else if(prf.equals("F-Measure"))
							{
								double f = 2 * Double.valueOf(values[10 + i * 2]) * Double.valueOf(values[11 + i * 2])
										/ (Double.valueOf(values[10 + i * 2]) + Double.valueOf(values[11 + i * 2]));
								sb.append(f);
							}


							if(i != Config.numTopks - 1)
								sb.append(",");
						}
					}



					out.println(sb);
				}

				br.close();
				out.close();
			}

			List<String> columns;
			List<List<List<Integer>>> data;
			DefaultBoxAndWhiskerCategoryDataset datasetbox;
			CategoryPlot plot;
			ChartPanel chartPanel;
			JPanel controlPanel;
			int start = 0;

			columns = new ArrayList<String>(10);
			data = new ArrayList<>();
			for (int i = 1; i <= 10; i++) {
				String name = String.valueOf(i * 0.01);
				columns.add(name);
				List<List<Integer>> list = new ArrayList<>();
				List<Integer> curList = new ArrayList<>();
				for (NewDenialConstraint dc : results.get(i-1)) {
					curList.add(dc.getPredicates().size());
				}
				list.add(curList);
				data.add(list);
			}

			datasetbox = new DefaultBoxAndWhiskerCategoryDataset();
			for (int i = 0; i < 10; i++) {
				List<List<Integer>> list = data.get(i);
				int row = 0;
				for (List<Integer> values : list) {
					String category = columns.get(i);
					datasetbox.add(values, "s" + row++, category);
				}
			}

			CategoryAxis xAxis = new CategoryAxis("Approximation Level");
			NumberAxis yAxis = new NumberAxis("Number of Predicates");
			BoxAndWhiskerRenderer renderer = new BoxAndWhiskerRenderer();
			plot = new CategoryPlot(datasetbox, xAxis, yAxis, renderer);
			JFreeChart chart = new JFreeChart("DC size", plot);
			chartPanel = new ChartPanel(chart);

			controlPanel = new JPanel();

			renderer.setFillBox(true);
			renderer.setMeanVisible(false);
			renderer.setSeriesPaint(0, Color.lightGray);

			chart.setBackgroundPaint(Color.white);
			plot.setBackgroundPaint(Color.white);
			plot.setDomainGridlinePaint(Color.white);
			plot.setDomainGridlinesVisible(true);
			plot.setRangeGridlinePaint(Color.white);
			//plot.getRangeAxis().setRange(0, 10.5);


			/*EventQueue.invokeLater(new Runnable() {

				@Override
				public void run() {
					JFrame frame = new JFrame();
					frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
					frame.add(chartPanel, BorderLayout.CENTER);
					frame.add(controlPanel, BorderLayout.SOUTH);
					frame.pack();
					frame.setLocationRelativeTo(null);
					frame.setVisible(true);
				}
			});*/

			BufferedImage image = chart.createBufferedImage( 300, 200);

			File outputFile = new File("boxPlot.png");
    		ImageIO.write(image, "png", outputFile);
		}

	private static boolean isSymmetricDC(NewDenialConstraint dc1, NewDenialConstraint dc2) {
		String dc1string = dc1.toString();
		String dc2string = dc2.toString();

		if (dc1string.length() != dc2string.length())
			return false;

		for (int i = 0; i < dc1string.length(); i++) {
			char c1 = dc1string.charAt(i);
			char c2 = dc2string.charAt(i);

			if (c1 != '=' && c1 !='!' && c1 != '<' && c1 != '>' && c2 != '=' && c2 !='!' && c2 != '<' && c2 != '>' && c1 != c2)
				return false;

			if (c1 == '=' && c2 != '=')
				return false;

			if (c1 != '=' && c2 == '=')
				return false;

			if (c1 == '!' && c2 != '!')
				return false;

			if (c1 != '!' && c2 == '!')
				return false;

			if (c1 == '<' && c2 != '>')
				return false;

			if (c1 != '<' && c2 == '>')
				return false;

			if (c1 == '>' && c2 != '<')
				return false;

			if (c1 != '>' && c2 == '<')
				return false;
		}

		return true;
	}

	public static void printDiagram1(List<List<NewDenialConstraint>> results) {
		List<String> columns;
		List<List<List<Double>>> data;
		DefaultBoxAndWhiskerCategoryDataset datasetbox;
		CategoryPlot plot;
		ChartPanel chartPanel;
		JPanel controlPanel;
		int start = 0;

		columns = new ArrayList<String>(10);
		data = new ArrayList<>();
		for (int i = 1; i <= results.size(); i++) {
			String name = String.valueOf(0.8+(i-1)*0.05);
			columns.add(name);
			List<List<Double>> list = new ArrayList<>();
			List<Double> curList = new ArrayList<>();
			for (NewDenialConstraint dc : results.get(i-1)) {
				curList.add(dc.interestingness);
			}
			list.add(curList);
			data.add(list);
		}

		datasetbox = new DefaultBoxAndWhiskerCategoryDataset();
		for (int i = 0; i < results.size(); i++) {
			List<List<Double>> list = data.get(i);
			int row = 0;
			for (List<Double> values : list) {
				String category = columns.get(i);
				datasetbox.add(values, "s" + row++, category);
			}
		}

		CategoryAxis xAxis = new CategoryAxis("Approximation Level");
		NumberAxis yAxis = new NumberAxis("Number of Predicates");
		BoxAndWhiskerRenderer renderer = new BoxAndWhiskerRenderer();
		plot = new CategoryPlot(datasetbox, xAxis, yAxis, renderer);
		JFreeChart chart = new JFreeChart("DC size", plot);
		chartPanel = new ChartPanel(chart);

		controlPanel = new JPanel();

		renderer.setFillBox(true);
		renderer.setMeanVisible(false);
		renderer.setSeriesPaint(0, Color.lightGray);

		chart.setBackgroundPaint(Color.white);
		plot.setBackgroundPaint(Color.white);
		plot.setDomainGridlinePaint(Color.white);
		plot.setDomainGridlinesVisible(true);
		plot.setRangeGridlinePaint(Color.white);
		//plot.getRangeAxis().setRange(0, 10.5);


		/*EventQueue.invokeLater(new Runnable() {

			@Override
			public void run() {
				JFrame frame = new JFrame();
				frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				frame.add(chartPanel, BorderLayout.CENTER);
				frame.add(controlPanel, BorderLayout.SOUTH);
				frame.pack();
				frame.setLocationRelativeTo(null);
				frame.setVisible(true);
			}
		});*/

		BufferedImage image = chart.createBufferedImage( 300, 200);

		File outputFile = new File("boxPlot1.png");
		try {
			ImageIO.write(image, "png", outputFile);
		}
		catch (Exception e) {

		}
	}

	public static void printDiagram2(List<List<NewDenialConstraint>> results) {
		List<String> columns;
		List<List<List<Double>>> data;
		DefaultBoxAndWhiskerCategoryDataset datasetbox;
		CategoryPlot plot;
		ChartPanel chartPanel;
		JPanel controlPanel;
		int start = 0;

		columns = new ArrayList<String>(10);
		data = new ArrayList<>();
		for (int i = 1; i <= results.size(); i++) {
			String name = String.valueOf(0.8+(i-1)*0.05);
			columns.add(name);
			List<List<Double>> list = new ArrayList<>();
			List<Double> curList = new ArrayList<>();
			for (NewDenialConstraint dc : results.get(i-1)) {
				curList.add(dc.coverage);
			}
			list.add(curList);
			data.add(list);
		}

		datasetbox = new DefaultBoxAndWhiskerCategoryDataset();
		for (int i = 0; i < results.size(); i++) {
			List<List<Double>> list = data.get(i);
			int row = 0;
			for (List<Double> values : list) {
				String category = columns.get(i);
				datasetbox.add(values, "s" + row++, category);
			}
		}

		CategoryAxis xAxis = new CategoryAxis("Approximation Level");
		NumberAxis yAxis = new NumberAxis("Number of Predicates");
		BoxAndWhiskerRenderer renderer = new BoxAndWhiskerRenderer();
		plot = new CategoryPlot(datasetbox, xAxis, yAxis, renderer);
		JFreeChart chart = new JFreeChart("DC size", plot);
		chartPanel = new ChartPanel(chart);

		controlPanel = new JPanel();

		renderer.setFillBox(true);
		renderer.setMeanVisible(false);
		renderer.setSeriesPaint(0, Color.lightGray);

		chart.setBackgroundPaint(Color.white);
		plot.setBackgroundPaint(Color.white);
		plot.setDomainGridlinePaint(Color.white);
		plot.setDomainGridlinesVisible(true);
		plot.setRangeGridlinePaint(Color.white);
		//plot.getRangeAxis().setRange(0, 10.5);


		/*EventQueue.invokeLater(new Runnable() {

			@Override
			public void run() {
				JFrame frame = new JFrame();
				frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				frame.add(chartPanel, BorderLayout.CENTER);
				frame.add(controlPanel, BorderLayout.SOUTH);
				frame.pack();
				frame.setLocationRelativeTo(null);
				frame.setVisible(true);
			}
		});*/

		BufferedImage image = chart.createBufferedImage( 300, 200);

		File outputFile = new File("boxPlot2.png");
		try {
			ImageIO.write(image, "png", outputFile);
		}
		catch (Exception e) {

		}
	}

	public static void printDiagram3(List<List<NewDenialConstraint>> results) {
		List<String> columns;
		List<List<List<Double>>> data;
		DefaultBoxAndWhiskerCategoryDataset datasetbox;
		CategoryPlot plot;
		ChartPanel chartPanel;
		JPanel controlPanel;
		int start = 0;

		columns = new ArrayList<String>(10);
		data = new ArrayList<>();
		for (int i = 1; i <= results.size(); i++) {
			String name = String.valueOf(0.8+(i-1)*0.05);
			columns.add(name);
			List<List<Double>> list = new ArrayList<>();
			List<Double> curList = new ArrayList<>();
			for (NewDenialConstraint dc : results.get(i-1)) {
				curList.add(dc.mdl);
			}
			list.add(curList);
			data.add(list);
		}

		datasetbox = new DefaultBoxAndWhiskerCategoryDataset();
		for (int i = 0; i < results.size(); i++) {
			List<List<Double>> list = data.get(i);
			int row = 0;
			for (List<Double> values : list) {
				String category = columns.get(i);
				datasetbox.add(values, "s" + row++, category);
			}
		}

		CategoryAxis xAxis = new CategoryAxis("Approximation Level");
		NumberAxis yAxis = new NumberAxis("Number of Predicates");
		BoxAndWhiskerRenderer renderer = new BoxAndWhiskerRenderer();
		plot = new CategoryPlot(datasetbox, xAxis, yAxis, renderer);
		JFreeChart chart = new JFreeChart("DC size", plot);
		chartPanel = new ChartPanel(chart);

		controlPanel = new JPanel();

		renderer.setFillBox(true);
		renderer.setMeanVisible(false);
		renderer.setSeriesPaint(0, Color.lightGray);

		chart.setBackgroundPaint(Color.white);
		plot.setBackgroundPaint(Color.white);
		plot.setDomainGridlinePaint(Color.white);
		plot.setDomainGridlinesVisible(true);
		plot.setRangeGridlinePaint(Color.white);
		//plot.getRangeAxis().setRange(0, 10.5);


		/*EventQueue.invokeLater(new Runnable() {

			@Override
			public void run() {
				JFrame frame = new JFrame();
				frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				frame.add(chartPanel, BorderLayout.CENTER);
				frame.add(controlPanel, BorderLayout.SOUTH);
				frame.pack();
				frame.setLocationRelativeTo(null);
				frame.setVisible(true);
			}
		});*/

		BufferedImage image = chart.createBufferedImage( 300, 200);

		File outputFile = new File("boxPlot3.png");
		try {
			ImageIO.write(image, "png", outputFile);
		}
		catch (Exception e) {

		}
	}

	public static void printDiagram4(List<List<NewDenialConstraint>> results) {
		List<String> columns;
		List<List<List<Double>>> data;
		DefaultBoxAndWhiskerCategoryDataset datasetbox;
		CategoryPlot plot;
		ChartPanel chartPanel;
		JPanel controlPanel;
		int start = 0;

		columns = new ArrayList<String>(10);
		data = new ArrayList<>();
		for (int i = 1; i <= results.size(); i++) {
			String name = String.valueOf(0.8+(i-1)*0.05);
			columns.add(name);
			List<List<Double>> list = new ArrayList<>();
			List<Double> curList = new ArrayList<>();
			for (NewDenialConstraint dc : results.get(i-1)) {
				curList.add(dc.numVios);
			}
			list.add(curList);
			data.add(list);
		}

		datasetbox = new DefaultBoxAndWhiskerCategoryDataset();
		for (int i = 0; i < results.size(); i++) {
			List<List<Double>> list = data.get(i);
			int row = 0;
			for (List<Double> values : list) {
				String category = columns.get(i);
				datasetbox.add(values, "s" + row++, category);
			}
		}

		CategoryAxis xAxis = new CategoryAxis("Approximation Level");
		NumberAxis yAxis = new NumberAxis("Number of Predicates");
		BoxAndWhiskerRenderer renderer = new BoxAndWhiskerRenderer();
		plot = new CategoryPlot(datasetbox, xAxis, yAxis, renderer);
		JFreeChart chart = new JFreeChart("DC size", plot);
		chartPanel = new ChartPanel(chart);

		controlPanel = new JPanel();

		renderer.setFillBox(true);
		renderer.setMeanVisible(false);
		renderer.setSeriesPaint(0, Color.lightGray);

		chart.setBackgroundPaint(Color.white);
		plot.setBackgroundPaint(Color.white);
		plot.setDomainGridlinePaint(Color.white);
		plot.setDomainGridlinesVisible(true);
		plot.setRangeGridlinePaint(Color.white);
		//plot.getRangeAxis().setRange(0, 10.5);


		/*EventQueue.invokeLater(new Runnable() {

			@Override
			public void run() {
				JFrame frame = new JFrame();
				frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				frame.add(chartPanel, BorderLayout.CENTER);
				frame.add(controlPanel, BorderLayout.SOUTH);
				frame.pack();
				frame.setLocationRelativeTo(null);
				frame.setVisible(true);
			}
		});*/

		BufferedImage image = chart.createBufferedImage( 300, 200);

		File outputFile = new File("boxPlot4.png");
		try {
			ImageIO.write(image, "png", outputFile);
		}
		catch (Exception e) {

		}
	}

	public static void printDiagram(List<List<NewDenialConstraint>> results) {
		List<String> columns;
		List<List<List<Integer>>> data;
		DefaultBoxAndWhiskerCategoryDataset datasetbox;
		CategoryPlot plot;
		ChartPanel chartPanel;
		JPanel controlPanel;
		int start = 0;

		columns = new ArrayList<String>(10);
		data = new ArrayList<>();
		for (int i = 1; i <= results.size(); i++) {
			String name = String.valueOf(0.8+(i-1)*0.05);
			columns.add(name);
			List<List<Integer>> list = new ArrayList<>();
			List<Integer> curList = new ArrayList<>();
			for (NewDenialConstraint dc : results.get(i-1)) {
				curList.add(dc.getPredicates().size());
			}
			list.add(curList);
			data.add(list);
		}

		datasetbox = new DefaultBoxAndWhiskerCategoryDataset();
		for (int i = 0; i < results.size(); i++) {
			List<List<Integer>> list = data.get(i);
			int row = 0;
			for (List<Integer> values : list) {
				String category = columns.get(i);
				datasetbox.add(values, "s" + row++, category);
			}
		}

		CategoryAxis xAxis = new CategoryAxis("Approximation Level");
		NumberAxis yAxis = new NumberAxis("Number of Predicates");
		BoxAndWhiskerRenderer renderer = new BoxAndWhiskerRenderer();
		plot = new CategoryPlot(datasetbox, xAxis, yAxis, renderer);
		JFreeChart chart = new JFreeChart("DC size", plot);
		chartPanel = new ChartPanel(chart);

		controlPanel = new JPanel();

		renderer.setFillBox(true);
		renderer.setMeanVisible(false);
		renderer.setSeriesPaint(0, Color.lightGray);

		chart.setBackgroundPaint(Color.white);
		plot.setBackgroundPaint(Color.white);
		plot.setDomainGridlinePaint(Color.white);
		plot.setDomainGridlinesVisible(true);
		plot.setRangeGridlinePaint(Color.white);
		//plot.getRangeAxis().setRange(0, 10.5);


		/*EventQueue.invokeLater(new Runnable() {

			@Override
			public void run() {
				JFrame frame = new JFrame();
				frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				frame.add(chartPanel, BorderLayout.CENTER);
				frame.add(controlPanel, BorderLayout.SOUTH);
				frame.pack();
				frame.setLocationRelativeTo(null);
				frame.setVisible(true);
			}
		});*/

		BufferedImage image = chart.createBufferedImage( 300, 200);

		File outputFile = new File("boxPlot.png");
		try {
			ImageIO.write(image, "png", outputFile);
		}
		catch (Exception e) {

		}
	}

	public static int checkViolations(NewTable table, NewDenialConstraint dc) {
		int numViolations = 0;
		ArrayList<NewPredicate> preds = dc.getPredicates();
		for (int i = 0; i < table.getNumRows() - 1; i++) {
			for (int j = i + 1; j < table.getNumRows(); j++) {
				NewTuple t1 = table.getTuple(i);
				NewTuple t2 = table.getTuple(j);

				boolean violated = true;
				for (NewPredicate pd : preds) {
					if (!pd.check(t1, t2)) {
						violated = false;
						break;
					}
				}

				if (violated) {
					numViolations++;
				}
			}
		}

		return numViolations;
	}

	public static void checkDCs(ArrayList<NewDenialConstraint> dcs) {
		int count = 0;
		String dataset = "TaxGenerator";
		String srcFile = "Experiments/" + dataset + "/" + "inputDB";
		NewTable table = new NewTable(srcFile,10000);

		ArrayList<NewDenialConstraint> bestDCs = new ArrayList<NewDenialConstraint>(dcs.subList(0,50));

		for (NewDenialConstraint dc : bestDCs) {
			System.out.println("Currrent DC is " + dc.toString() + " with score " + (0.5 * dc.mdl + 0.5 *  dc.coverage));
			System.out.println("Number of violations of DC is " + checkViolations(table, dc));
			//System.out.println("Number of violations of DC is " + dc.numVios);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {


		//String dataset = "TaxGenerator";
		//String dataset = "ExpHospital";
		//String dataset = "Test";
		String dataset = "SPStock";

		int function = 1;
		//int function = 2;
		//int function = 3;

		ArrayList<Integer> cols = new ArrayList<Integer>();
		FileUtil.clearExpReportCSVFile(ExpUtils.head, "Experiments/ExpReport.CSV");
		ConstraintDiscovery cd;
		ConstraintDiscovery3 cd2;

		Config.sc = 0;
		Config.howInit = 1;
		//Config.enableCrossColumn = false;
		Config.kfre = 0.01;
		Config.noiseTolerance = 0;

		Config.enableMixedDcs  = false;

		if(dataset.equals("SPStock"))
			Config.enableCrossColumn = true;
		else
			Config.enableCrossColumn = false;

		//cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1, 3);
		//cd.discoverEXCHKS();


		//Config.noiseLevel = 0.001;

		//insertNoise(dataset,10000,0.5);
		//
		//
		//cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",
		//		1,3,1,10);
		//cd.discoverEXCHKS();


		//cd.initHeavyWork(Config.howInit);
		//cd.discover();

		/*for (int i=0;i<1000000;i++) {
			System.out.println("aa");
		}*/

		/*cd2 = new ConstraintMining3Approx("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		List<NewDenialConstraint> res1 = cd2.discover();*/

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
		//	1,3,1,100000);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//cd2.discover();

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
		//	1,3,1,200000);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//cd2.discover();

		/*cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,300000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,400000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,500000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();*/

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
		//	1,3,1,600000);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//cd2.discover();

		/*List<List<NewDenialConstraint>> results = new ArrayList<>();
		List<NewDenialConstraint> curRes;

		Config.noiseTolerance = 0.2;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		curRes = cd2.discover();
		results.add(curRes);

		Config.noiseTolerance = 0.15;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		curRes = cd2.discover();
		results.add(curRes);

		Config.noiseTolerance = 0.1;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		curRes = cd2.discover();
		results.add(curRes);

		Config.noiseTolerance = 0.05;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		curRes = cd2.discover();
		results.add(curRes);

		Config.noiseTolerance = 0;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		curRes = cd2.discover();
		results.add(curRes);


		printDiagram(results);
		printDiagram1(results);
		printDiagram2(results);
		printDiagram3(results);
		printDiagram4(results);*/

		/*cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		Config.noiseTolerance = 0.001;

		cd3 = new ConstraintMining4ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000);
		//cd2.discoverEXCHKS();


		cd3.initHeavyWork(Config.howInit);
		cd3.discover();

		Config.noiseTolerance = 0.01;

		cd3 = new ConstraintMining4ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000);
		//cd2.discoverEXCHKS();


		cd3.initHeavyWork(Config.howInit);
		cd3.discover();*/

		/*Config.noiseTolerance = 0.0001;

		cd3 = new ConstraintMining4ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000);
		//cd2.discoverEXCHKS();


		cd3.initHeavyWork(Config.howInit);
		cd3.discover();

		Config.noiseTolerance = 0.2;

		cd3 = new ConstraintMining4ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000);
		//cd2.discoverEXCHKS();


		cd3.initHeavyWork(Config.howInit);
		cd3.discover();

		Config.noiseTolerance = 0.3;

		cd3 = new ConstraintMining4ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000);
		//cd2.discoverEXCHKS();


		cd3.initHeavyWork(Config.howInit);
		cd3.discover();*/

		/*cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		Config.noiseTolerance = 0.1;

		cd3 = new ConstraintMining4ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100);
		//cd2.discoverEXCHKS();


		cd3.initHeavyWork(Config.howInit);
		cd3.discover();*/

		/*cd5 = new ConstraintMining5ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100);
		//cd2.discoverEXCHKS();


		cd5.initHeavyWork(Config.howInit);
		cd5.discover();

		Config.noiseTolerance = 0.1;

		cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,100);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,100);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

				cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,1000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

				cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,10000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,10000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

				cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,25000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,25000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

				cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,50000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,50000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		/*		cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,200000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();


		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,300000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,300000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,400000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,400000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,500000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();*/

		//cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,500000);
		//cd.initHeavyWork(Config.howInit);
		//cd.discover();

		/*cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,600000);
		//cd2.discoverEXCHKS();
		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
		//	1,3,1,800000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();*/

		//Config.noiseTolerance = 0.1;

		int numTuples = 10000;
		double precision = 0.0;
		double recall = 0.0;

		//Config.noiseLevel = 0.01;
		//Map<Integer, Map<Integer, StringPair>> noise = insertNoise(dataset,numTuples,0.5);

		//Config.noiseTolerance = 0.1;

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
		//	1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//ArrayList<NewDenialConstraint> realdcs = cd2.discover();

		//Config.noiseTolerance = 0.01;

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
		//	1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//realdcs = cd2.discover();

		//dataset = "ExpHospital";
		//Config.enableCrossColumn = false;


		//####################

		ArrayList<NewDenialConstraint> realdcs;

		for (int i=0; i<1; i++) {

		System.out.println("NEWNEWNEWNEW");

		Random rand = new Random();
		int n = rand.nextInt(100000-numTuples);
		n = n+1;

		//Config.noiseLevel = 0.001;
		//Map<Integer, Map<Integer, StringPair>> noise = insertNoise(dataset,numTuples,0.5, n);

		Config.noiseTolerance = 0.1;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples, function);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

		Config.noiseTolerance = 0.01;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples, function);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

		Config.noiseTolerance = 0.001;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples, function);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

				Config.noiseTolerance = 0.0001;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples, function);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

				Config.noiseTolerance = 0.00001;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples,function);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

		Config.noiseTolerance = 0.000001;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples,function);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();
		}

		/*for(int j=0; j<1;j++) {
		System.out.println("NEWNEWNEWNEW");

				Random rand = new Random();
		int n = rand.nextInt(100000-numTuples);
		n = n+1;

		Config.noiseLevel = 0.001;
		insertNoiseRows(dataset,numTuples,0.5,0.01,n);

		/*Config.noiseTolerance = 0.1;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoiseRows",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

				Config.noiseTolerance = 0.01;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoiseRows",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

				Config.noiseTolerance = 0.001;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoiseRows",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

				Config.noiseTolerance = 0.0001;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoiseRows",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();

				Config.noiseTolerance = 0.00001;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoiseRows",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();*/

		/*Config.noiseTolerance = 0.000001;

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoiseRows",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		realdcs = cd2.discover();
		}*/






		//##########################################################################




		//Config.noiseTolerance = 0.001;

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
		//	1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//cd2.discover();

		//Config.noiseTolerance = 0.01;

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
		//	1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//realdcs = cd2.discover();

		//Config.noiseTolerance = 0.1;

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
		//	1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//ArrayList<NewDenialConstraint> appdcs = cd2.discover();

		//checkquality1(realdcs,appdcs);



/*
		Config.noiseTolerance = 0.1;
		Config.noiseLevel = 0.1;
		Map<Integer, Map<Integer, StringPair>> noise = insertNoise(dataset,numTuples,0.5);

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		ArrayList<NewDenialConstraint> sampledcs = cd2.discover();

		precision += calcPrecision(realdcs,sampledcs);
		recall += calcRecall(realdcs,sampledcs);

		System.out.println("Precision is " + precision);
		System.out.println("Recall is " + recall);

		noise = insertNoise(dataset,numTuples,0.5);

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		sampledcs = cd2.discover();

		precision += calcPrecision(realdcs,sampledcs);
		recall += calcRecall(realdcs,sampledcs);

		System.out.println("Precision is " + precision);
		System.out.println("Recall is " + recall);

		noise = insertNoise(dataset,numTuples,0.5);

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		sampledcs = cd2.discover();

		precision += calcPrecision(realdcs,sampledcs);
		recall += calcRecall(realdcs,sampledcs);

				System.out.println("Precision is " + precision);
		System.out.println("Recall is " + recall);

				noise = insertNoise(dataset,numTuples,0.5);

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		sampledcs = cd2.discover();

		precision += calcPrecision(realdcs,sampledcs);
		recall += calcRecall(realdcs,sampledcs);

				System.out.println("Precision is " + precision);
		System.out.println("Recall is " + recall);

				noise = insertNoise(dataset,numTuples,0.5);

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
			1,3,1,numTuples);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		sampledcs = cd2.discover();

		precision += calcPrecision(realdcs,sampledcs);
		recall += calcRecall(realdcs,sampledcs);

		System.out.println("Precision is " + precision);
		System.out.println("Recall is " + recall);



		//Config.noiseTolerance = 0.00001;

		//cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDBNoise",
		//	1,3,1,1000);
		//cd2.discoverEXCHKS();


		//cd2.initHeavyWork(Config.howInit);
		//dcs = cd2.discover();

		//checkDCs(dcs);

		/*cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,5000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,30000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,40000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,50000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,60000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,70000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();*/



		//cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,700000);
		//cd.initHeavyWork(Config.howInit);
		//cd.discover();

		/*cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,300000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();*/

		/*cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,800000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,900000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,900000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		cd2 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd = new ConstraintMining2("Experiments/" + dataset + "/" + "inputDB",1,3,1,1000000);
		cd.initHeavyWork(Config.howInit);
		cd.discover();

		/*cd3 = new ConstraintMining3ApproxNew("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,4);
		//cd2.discoverEXCHKS();


		cd3.initHeavyWork(Config.howInit);
		List<NewDenialConstraint> res2 = cd3.discover();*/

		/*for (NewDenialConstraint d : res2) {
			boolean found = false;
			for (NewDenialConstraint d2 : res1) {
				if (compareDC(d,d2) || isSymmetricDC(d,d2)) {
					found = true;
					break;
				}
			}
			if(!found) {
				System.out.println("Missing: " + d.toString());
			}
		}*/

		/*cd2 = new ConstraintMining3Approx("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,200000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,300000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,400000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,500000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,600000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,700000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,800000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,900000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2.initHeavyWork(Config.howInit);
		cd2.discover();

		cd2 = new ConstraintMining3("Experiments/" + dataset + "/" + "inputDB",
			1,3,1,1000000);
		//cd2.discoverEXCHKS();


		cd2.initHeavyWork(Config.howInit);
		cd2.discover();*/

		//approximate_Varying_NoiseToleranceLevel(dataset);

		//cd3 = new ConstraintMining3Approx("Experiments/" + dataset + "/" + "inputDB",
		//	1,3,1,100);
		//cd.discoverEXCHKS();


		//cd3.initHeavyWork(Config.howInit);
		//cd3.discover();

		//approximate_Varying_NoiseToleranceLevel(dataset);

		//Config.enableCrossColumn = false;
		//FASTDC fastdc = new FASTDC("Experiments/" + dataset + "/" + "inputDB",5);
		//Set<Set<Predicate>> result1 = fastdc.discover();
		//fastdc.dc2File();

		//Config.enableCrossColumn = false;
		//ExperimentalVeryFastDC vfastdc = new ExperimentalVeryFastDC("Experiments/" + dataset + "/" + "inputDB",3);
		//List<NewDenialConstraint> dc3 = vfastdc.generateDCs();
		//Set<Set<Predicate>> result2 = vfastdc.discover();
		//vfastdc.dc2File();

		//Config.enableCrossColumn = false;
		//FasterDC2 fasterdc = new FasterDC2("Experiments/" + dataset + "/" + "inputDB",100000);
		//Set<Set<Predicate>> result2 = fasterdc.discover();
		//fastdc.dc2File();


		/*boolean found = false;
		for (Set<NewPredicate> d : dc3) {
			for (Set<NewPredicate> d2 : dcs2) {
				if (d.size() != d2.size())
					continue;

				int predAppear = 0;
				for (NewPredicate p : d) {
					if (d2.contains(p))
						predAppear++;
				}

				if (predAppear == d.size() && d.size() > 0) {
					found = true;
					break;
				}
			}
			if(!found) {
				System.out.println("Does not appear in other version: " + d.toString());
			}

			found = false;
		}*/

		/*for (NewDenialConstraint d : dcs2) {
			boolean found = false;
			for (NewDenialConstraint d2 : dc3) {
				if (compareDC(d,d2)) {
					found = true;
				}
			}
			if(!found) {
				System.out.println("Missing: " + d.toString());
			}
		}*/
	}


	public static double checkquality1(ArrayList<NewDenialConstraint> real, ArrayList<NewDenialConstraint> app) {
		double res = 0.0;
		double avg = 0.0;
		for (NewDenialConstraint dc1 : app) {
			for (NewDenialConstraint dc2 : real) {
				int nump = dc1.getPredicates().size();
				int found = 0;
				for (NewPredicate p : dc1.getPredicates()) {
					for (NewPredicate p2 : dc2.getPredicates()) {
						if (p.equals(p2)) {
							found++;
							break;
						}
					}
				}
				if (found == nump) {
					avg += dc1.getPredicates().size();
					res++;
					break;
				}
			}
		}


		System.out.println("found dcs:  " + res);
		System.out.println("avg size of related predicates is " + avg/res);

		return res;
	}

	public static double calcPrecision(ArrayList<NewDenialConstraint> real, ArrayList<NewDenialConstraint> sample) {
		double res = 0.0;
		for (NewDenialConstraint dc1 : sample) {
			for (NewDenialConstraint dc2 : real) {
				if (dc1.equals(dc2)) {
					res = res + 1;
					break;
				}
			}
		}

		return res/real.size();
	}

	public static double calcRecall(ArrayList<NewDenialConstraint> real, ArrayList<NewDenialConstraint> sample) {
		double res = 0.0;
		for (NewDenialConstraint dc : sample) {
			if (real.contains(dc))
				res = res + 1;
		}

		return res/sample.size();
	}

	public static boolean comparePred(List<NewPredicate> set1, List<Predicate> set2) {
		if (set1.size() != set2.size())
			return false;

		int matches = 0;
		for (NewPredicate p : set1) {
			for (Predicate p2 : set2) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					matches++;
					break;
				}
			}
		}

		return matches == set1.size();
	}

	public static boolean comparePred2(List<Predicate> set1, List<NewPredicate> set2) {
		if (set1.size() != set2.size())
			return false;

		int matches = 0;
		for (Predicate p : set1) {
			for (NewPredicate p2 : set2) {
				if (p.toString().equalsIgnoreCase(p2.toString())) {
					matches++;
					break;
				}
			}
		}

		return matches == set1.size();
	}

	//Randomly insert noise into the tax data generator
		public static Map<Integer, Map<Integer, StringPair>> insertNoise(String dataset, int numTuples, double type, int start) throws IOException
		{

			String srcFile = "Experiments/" + dataset + "/" + "inputDB";
			String desFile = "Experiments/" + dataset + "/" + "inputDBNoise";


			Table table = new Table(srcFile,numTuples,start);
			Map<Integer, Map<Integer, StringPair>> result = table.insertNoise(type);

			table.dump2File(desFile);

			return result;
		}

				public static Map<Integer, Map<Integer, StringPair>> insertNoiseRows(String dataset, int numTuples, double type, double percent, int start) throws IOException
		{

			String srcFile = "Experiments/" + dataset + "/" + "inputDB";
			String desFile = "Experiments/" + dataset + "/" + "inputDBNoiseRows";


			Table table = new Table(srcFile,numTuples,start);
			Map<Integer, Map<Integer, StringPair>> result = table.insertNoiseRows(type,percent);

			table.dump2File(desFile);

			return result;
		}

				public static Map<Integer, Map<Integer, StringPair>> insertNoiseColumns(String dataset, int numTuples, double type, double percent) throws IOException
		{

			String srcFile = "Experiments/" + dataset + "/" + "inputDB";
			String desFile = "Experiments/" + dataset + "/" + "inputDBNoise";


			Table table = new Table(srcFile,numTuples);
			Map<Integer, Map<Integer, StringPair>> result = table.insertNoiseColumns(type,percent);

			table.dump2File(desFile);

			return result;
		}


}
