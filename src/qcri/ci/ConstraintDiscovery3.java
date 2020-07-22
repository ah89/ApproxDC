package qcri.ci;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.sun.tools.jdi.InternalEventHandler;
import qcri.ci.generaldatastructure.constraints.DBProfiler;
import qcri.ci.generaldatastructure.constraints.NewDenialConstraint;
import qcri.ci.generaldatastructure.constraints.NewPredicate;
import qcri.ci.generaldatastructure.db.NewTable;
import qcri.ci.generaldatastructure.db.NewTuple;
import qcri.ci.utils.BooleanPair;
import qcri.ci.utils.Config;
import qcri.ci.utils.IntegerPair;

import org.jfree.data.statistics.BoxAndWhiskerCategoryDataset;

public class ConstraintDiscovery3 {

	protected long startingTime;

	protected long numOfIterations = 0;
	protected long numOfSuccessfulIterations = 0;
	protected long numOfFailures = 0;
	protected long minimalityFailed = 0;

	protected double minDCsize = Double.MAX_VALUE;
	protected double maxDCsize = 0;
	protected double avgDCsize = 0;

	public NewTable originalTable;
	protected NewTable table1,table2;

	//public Set<Set<NewPredicate>> iterResults = new HashSet<>();

	protected ArrayList<NewDenialConstraint> totalDCs = new ArrayList<>();
	//public ArrayList<NewDenialConstraint> exchkDCs = new ArrayList<NewDenialConstraint>(); //extended check constraints

	protected Set<Long> uncovered;
	protected Set<Long> cannotBeCovered = new HashSet<>();
	//protected Map<Long, Set<IntegerPair>> uncoveredMap = new HashMap<>();
	protected Map<Integer, Set<Long>> crit = new HashMap<>();

	protected boolean[] candidates;

	protected List<NewPredicate> presMap = new ArrayList<>();

	protected Long evidenceNum = 0L;
	//protected Map<Long, Set<NewPredicate>> evidenceMap = new HashMap<>();
	protected Map<List<Boolean>, Long> evidenceMapRev = new HashMap<>();
	protected Map<Integer, Set<Long>> evidenceForPred = new HashMap<>();
	protected Map<Integer, Long> evidenceForPredRemaining = new HashMap<>();
	protected Map<Long, Long> evidenceSet = new HashMap<>();
	protected Map<Long, List<Boolean>> evidences = new HashMap<>();
	//protected Set<Set<NewPredicate>> evidences = new HashSet<>();

	protected Map<Long, Map<Integer,Integer>> evidenceToPairs = new HashMap<>();
	protected ConcurrentHashMap<List<Boolean>, Set<Integer>> evidenceEdges = new ConcurrentHashMap<>();

	//public ArrayList<NewPredicate> presMap = new ArrayList<NewPredicate>();//NewPredicate with both variables
	//public ArrayList<NewPredicate> preSpace = new ArrayList<NewPredicate>(); //The space of all predicates

	public ArrayList<NewPredicate> consPres = new ArrayList<NewPredicate>();// The set of all k-frequent constant predicate


	//public ArrayList<NewPredicate> singleNewTupleVarPre = new ArrayList<NewPredicate>(); //NewPredicate in a single tuple

	//public static Object mutexLock = new Object();

	//From each set of constant predicates, to the set of DCs, having those constant predicates
	private Map<Set<NewPredicate>,ArrayList<NewDenialConstraint>> consPresMap = new HashMap<>();

	//From each set of constant predicates, to the pairwise tuple info!
	private Map<Set<NewPredicate>,Map<List<Boolean>,Long>> cons2PairInfo = new HashMap<>();


	private Map<Set<NewPredicate>,Integer> cons2NumNewTuples = new HashMap<Set<NewPredicate>,Integer>();


	public DBProfiler dbPro;

	public ConstraintDiscovery3(String inputDBPath, int numRows)
	{
		startingTime = System.currentTimeMillis();
		originalTable = new NewTable(inputDBPath, numRows);
		table1 = originalTable;
		table2 = originalTable;
		
		System.out.println("before find all predicate");
		findAllPredicates();
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
				"Number of variable predicates: " + presMap.size());
		
		
		findAllConsPres();
		System.out.println( (System.currentTimeMillis() - startingTime )/ 1000 + 
				": Done initialization of table and predicate space");
		
		
		
	}
	
	
	/*public double expectedInitTime()
	{
		double totalExpTime = 0;
		
		double k10Time = 0;
		if(originalTable.getTableName().equals("TaxGenerator"))
		{
			k10Time = 220.89;
		}
		else if(originalTable.getTableName().equals("ExpHospital"))
		{
			k10Time = 53.53;
		}
		else if(originalTable.getTableName().equals("SPStock"))
		{
			k10Time = 235;
		}
		
		
		for(Set<NewPredicate> limit: cons2PairInfo.keySet())
		{
			
			ArrayList<NewTuple> curTuples = new ArrayList<NewTuple>();
			for(NewTuple tuple: originalTable.getTuples())
			{
				boolean good = true;
				for(NewPredicate pre: limit)
				{
					if(!pre.check(tuple))
					{
						good = false;
						break;
					}
				}
				if(good)
				{
					curTuples.add(tuple);
				}
			}
			totalExpTime += ((double)curTuples.size() / 10000 )
					* ((double)curTuples.size() / 10000 ) * k10Time;
					
		}
		
		return totalExpTime;
	}*/
	
	public void initHeavyWork(int howInit)
	{
		if(howInit == 1 || howInit == 0)
		{
			//initAllNewTupleWiseInfoParallel();
			//initAllNewTupleWiseInfoParallel2();
			
			for(Set<NewPredicate> cons: cons2PairInfo.keySet())
			{
				this.buildingEviUtil(cons, presMap, cons2PairInfo.get(cons));
				
				StringBuilder sb = new StringBuilder();
				for(NewPredicate pre: cons)
					sb.append(pre.toString() + " &");
				System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
						"Done initialization evidence for " + sb);
			}
			
			
			System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
					"Done initialization tuple pair wise information using one machine");
		}
		/*else if(howInit == 2)
		{
			//Ian's code!...
			String preInfoPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("preInfo.txt"));		
			try {
				synchronized(mutexLock){
					// only one thread is allowed to go in everytime
					DistributedExecutor executor = new DistributedExecutor();
					executor.execute(originalTable.getCurrentFolder(),this.originalTable, this.getAllVarNewPredicates(),preInfoPath);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			//At the this point, i should have the predicate info file
			//Name is "preInfo.txt", under directory preInfoPath
			
			parsePreInfo(preInfoPath);
			
			
			System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
					"Done initialization tuple pair wise information using Cluster");
		}*/
		
		
		String preInfoPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("preInfo.txt"));		
		try {
			PrintWriter out = new PrintWriter(preInfoPath);
			StringBuilder sb = new StringBuilder();
			for(NewPredicate pre: presMap)
			{
				sb.append(pre.toString());
				sb.append(":");
			}
			sb.append("count");
			out.println(sb);
			//Write the preInfo into a text File
			//check if the init has been done correctly
			for(Set<NewPredicate> cons: cons2PairInfo.keySet())
			{
				if(cons.size()!=0)
					continue;
				Map<List<Boolean>, Long> value = cons2PairInfo.get(cons);
				long total = 0;
				for(List<Boolean> key: value.keySet())
				{
					
					sb = new StringBuilder();
					for(NewPredicate pre: presMap)
					{
						if(key.get(presMap.indexOf(pre)))
						{
							sb.append("1" + ":");
						}
						else
						{
							sb.append("0" + ":");
						}
					}
					sb.append(value.get(key));
					out.println(sb);
					
					total += value.get(key);
				}
				System.out.println("Total Evi1: " + total);
				System.out.println("Total Evi2: " + (long) originalTable.getNumRows() * (originalTable.getNumRows() - 1));
				
			}
			out.close();
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Output the preinfo sorted:
		preInfoPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("preInfoSorted.txt"));		
		try {
			PrintWriter out = new PrintWriter(preInfoPath);
			StringBuilder sb = new StringBuilder();
			for(NewPredicate pre: presMap)
			{
				sb.append(pre.toString());
				sb.append(":");
			}
			sb.append("count");
			out.println(sb);
			//Write the preInfo into a text File
			//check if the init has been done correctly
			for(Set<NewPredicate> cons: cons2PairInfo.keySet())
			{
				if(cons.size()!=0)
					continue;
				Map<List<Boolean>, Long> value = cons2PairInfo.get(cons);
				//Sort them
				ArrayList<List<Boolean>> sortedKeys = new ArrayList<>();
				for(List<Boolean> key: value.keySet())
				{
					int i = 0;
					for(i = 0; i < sortedKeys.size(); i++)
					{
						if(value.get(key) > value.get(sortedKeys.get(i)))
						{
							sortedKeys.add(i, key);
							break;
						}
					}
					if( i == sortedKeys.size())
					{
						sortedKeys.add(key);
					}
				}
				
				
				long total = 0;
				for(List<Boolean> key: sortedKeys)
				{
					
					sb = new StringBuilder();
					for(NewPredicate pre: presMap)
					{
						if(key.get(presMap.indexOf(pre)))
						{
							sb.append("1" + ":");
						}
						else
						{
							sb.append("0" + ":");
						}
					}
					sb.append(value.get(key));
					out.println(sb);
					
					total += value.get(key);
				}
				System.out.println("Total Evi1: " + total);
				System.out.println("Total Evi2: " + (long) originalTable.getNumRows() * (originalTable.getNumRows() - 1));
				
			}
			out.close();
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*private void parsePreInfo(String preInfoPath)
	{
		
		Map<boolean[],Long> tupleWiseInfo = cons2PairInfo.get(new boolean[presMap.size()]);
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(preInfoPath));
			String line = null;
			int count = -1;
			while((line = br.readLine())!=null)
			{
				if (line.equals(""))
					continue;
				if(count == -1)//first line is the head
				{
					count++;
					continue;
				}
				String[] temp = line.split(":");
				assert(temp.length == presMap.size() + 1);
				
				Set<NewPredicate> key = new HashSet<NewPredicate>();
				for(int i = 0 ;i < temp.length - 1; i++)
				{
					if(temp[i].equals("1"))
					{
						key.add(presMap.get(i));
					}
				}
				tupleWiseInfo.put(key, Long.valueOf(temp[temp.length - 1]));
				
			}
			br.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}*/
	
	
	/**
	 * This is the function where you can add all predicates that you are interested in
	 * The set of operators you are interested in
	 * Allowing comparison within a tuple?
	 * Allowing comparison cross columns?
	 */
	private void findAllPredicates()
	{
		/*String[] ops = Config.ops;
		
		//Order the ops such that it is paired, each pair is like (EQ,IQ) (GT,LTE)
		ArrayList<String> orderedOps = new ArrayList<String>();
		for(int i = 0 ; i < ops.length; i++)
		{
			String opName = ops[i];
			if(orderedOps.contains(opName))
				continue;
			String reverseName = OperatorMapping.getReserveName(opName);
			orderedOps.add(opName);
			orderedOps.add(reverseName);
			
		}
		*/
		//1. EQ, IQ for each column
		//A=,A!,B=,B!=
		int numCols = originalTable.getNumCols();
		for(int col = 1; col <= numCols; col++)
		{
			
			NewPredicate pre = new NewPredicate(originalTable,0,1,2,col,col);
			presMap.add(pre);
			pre = new NewPredicate(originalTable,1,1,2,col,col);
			presMap.add(pre);
			
			
			int type = originalTable.getColumnMapping().positionToType(col);
			if(type == 0 || type == 1)
			{
				pre = new NewPredicate(originalTable,2,1,2,col,col);
				presMap.add(pre);
				pre = new NewPredicate(originalTable,3,1,2,col,col);
				presMap.add(pre);

				pre = new NewPredicate(originalTable,4,1,2,col,col);
				presMap.add(pre);
				
				pre = new NewPredicate(originalTable,5,1,2,col,col);
				presMap.add(pre);
			}
			
		}
		
		//2. GL, LTE for each column with numerical values (LT, GTE)?
		for(int col =1 ; col <= numCols; col++)
		{
			/*int type = originalTable.getColumnMapping().positionToType(col);
			if(type == 0 || type == 1)
			{
				NewPredicate pre = new NewPredicate(originalTable,"GT",1,2,col,col);
				presMap.add(pre);
				pre = new NewPredicate(originalTable,"LTE",1,2,col,col);
				presMap.add(pre);

				pre = new NewPredicate(originalTable,"LT",1,2,col,col);
				presMap.add(pre);
				
				pre = new NewPredicate(originalTable,"GTE",1,2,col,col);
				presMap.add(pre);
			}*/
		}
		
		dbPro = new DBProfiler(originalTable);
		
		//preSpace.addAll(presMap);
		
		if(Config.enableCrossColumn)
		{
			Set<String> eqCompa = dbPro.getEquComCols2();
			Set<String> orderCompa = dbPro.getOrderComCols2();
			
			int[] row1s = null;
			int[] row2s = null;
			if(Config.ps == 1)
			{
				row1s = new int[]{1,1};
				row2s = new int[]{1,2};
			}
			else if(Config.ps == 2)
			{
				row1s = new int[]{1,2,1,2};
				row2s = new int[]{1,2,2,1};
			}
			
			
			
			//3. EQ, IQ for column pairs 	
			for(String cols: eqCompa)
			{
				System.out.println("Joinale columns: " + cols);
				String[] colsTemp = cols.split(",");
				int col1 = originalTable.getColumnMapping().NametoPosition(colsTemp[0]);
				int col2 = originalTable.getColumnMapping().NametoPosition(colsTemp[1]);
				int row1 = 1;
					int row2 = 1;
					NewPredicate pre = new NewPredicate(originalTable,0,row1,row2,col1,col2);
					presMap.add(pre);
					//singleNewTupleVarPre.add(pre);
					pre = new NewPredicate(originalTable,1,row1,row2,col1,col2);
					presMap.add(pre);
					//singleNewTupleVarPre.add(pre);
			}
			// 4. GL, LTE for each column pair with numerical values
			for(String cols: orderCompa)
			{
				System.out.println("Order Comparable columns: " + cols);
				String[] colsTemp = cols.split(",");
				int col1 = originalTable.getColumnMapping().NametoPosition(colsTemp[0]);
				int col2 = originalTable.getColumnMapping().NametoPosition(colsTemp[1]);
					int row1 = 1;
					int row2 = 1;
					NewPredicate pre = new NewPredicate(originalTable,2,row1,row2,col1,col2);
					presMap.add(pre);
					pre = new NewPredicate(originalTable,3,row1,row2,col1,col2);
					presMap.add(pre);
					pre = new NewPredicate(originalTable,4,row1,row2,col1,col2);
					presMap.add(pre);
					pre = new NewPredicate(originalTable,5,row1,row2,col1,col2);
					presMap.add(pre);
			}
			
			//Add all predicates to predicate space
			//row1s = new int[]{1,2,1,2};
			//row2s = new int[]{1,2,2,1};
			//3. EQ, IQ for column pairs 	
			/*for(String cols: eqCompa)
			{
				System.out.println("Joinale columns: " + cols);
				String[] colsTemp = cols.split(",");
				int col1 = originalTable.getColumnMapping().NametoPosition(colsTemp[0]);
				int col2 = originalTable.getColumnMapping().NametoPosition(colsTemp[1]);
				for(int i = 0; i <  row1s.length; i++)
				{
					int row1 = row1s[i];
					int row2 = row2s[i];
					NewPredicate pre = new NewPredicate(originalTable,0,row1,row2,col1,col2);
					preSpace.add(pre);
					//singleNewTupleVarPre.add(pre);
					pre = new NewPredicate(originalTable,1,row1,row2,col1,col2);
					preSpace.add(pre);
					//singleNewTupleVarPre.add(pre);
				}	
			}
			// 4. GL, LTE for each column pair with numerical values
			for(String cols: orderCompa)
			{
				System.out.println("Order Comparable columns: " + cols);
				String[] colsTemp = cols.split(",");
				int col1 = originalTable.getColumnMapping().NametoPosition(colsTemp[0]);
				int col2 = originalTable.getColumnMapping().NametoPosition(colsTemp[1]);
				for(int i = 0; i <  row1s.length; i++)
				{
					int row1 = row1s[i];
					int row2 = row2s[i];
					NewPredicate pre = new NewPredicate(originalTable,2,row1,row2,col1,col2);
					preSpace.add(pre);
					pre = new NewPredicate(originalTable,3,row1,row2,col1,col2);
					preSpace.add(pre);				
					pre = new NewPredicate(originalTable,4,row1,row2,col1,col2);
					preSpace.add(pre);
					pre = new NewPredicate(originalTable,5,row1,row2,col1,col2);
					preSpace.add(pre);
				}
			}*/
			
		}
		
		candidates = new boolean[presMap.size()];
	}
	
	private void findAllConsPres()
	{
		
		Set<Set<NewPredicate>> consPresInputs = new HashSet<Set<NewPredicate>>();
		consPresInputs.add(new HashSet<NewPredicate>());
		consPres.clear();
		consPresMap.clear();
		cons2PairInfo.clear();
		
		
		
		if(Config.enableMixedDcs)
			dbPro.findkfrequentconstantpredicateEQOnly2(consPresInputs);
		
		
		for(Set<NewPredicate> temp: consPresInputs)
		{
			StringBuilder sb = new StringBuilder();
			for(NewPredicate pre: temp)
				sb.append(pre.toString() + " &");
			System.out.println("Cons for Mixed: " +  sb);
			consPresMap.put(temp, new ArrayList<NewDenialConstraint>());
			cons2PairInfo.put(temp, new HashMap<List<Boolean>, Long>());
			
		}
		
		System.out.println("We will do this number of FASTDCs: " + consPresInputs.size());

	}
	
	
	
	/**
	 * Initialize all the tuple pair wise information for each set of constant predicates 
	 */
	private void buildingEviUtil(Set<NewPredicate> limit, final List<NewPredicate> allPres,  Map<List<Boolean>,Long> evidence)
	{
		//Limit the database according to limit
		final ArrayList<NewTuple> curTuples = new ArrayList<NewTuple>();
		for(NewTuple tuple: originalTable.getTuples())
		{
			boolean good = true;
			for(NewPredicate pre: limit)
			{
				if(!pre.check(tuple))
				{
					good = false;
					break;
				}
			}
			if(good)
			{
				curTuples.add(tuple);
			}
		}
		
		cons2NumNewTuples.put(limit, curTuples.size());

		int numThreads =  Runtime.getRuntime().availableProcessors();

		ArrayList<Thread> threads = new ArrayList<Thread>();

		final ArrayList<Map<List<Boolean>, Long>> tempPairInfo = new ArrayList<>();
		final ArrayList<Map<List<Boolean>, Map<Integer,Integer>>> tempPairInfo2 = new ArrayList<>();
		//final ArrayList<Map<List<Boolean>, Set<Integer>>> tempPairInfo3 = new ArrayList<>();

		System.out.println("We are going to use " + numThreads + " threads");

		final int numRows = curTuples.size();
		int chunk = numRows / numThreads;

		for(int k = 0 ; k < numThreads; k++)
		{
			final int kTemp = k;
			final int startk = k * chunk;
			final int finalk = (k==numThreads-1)? numRows:(k+1)*chunk;
			//tempInfo.add(new HashMap<Set<NewPredicate>,Long>());

			Map<List<Boolean>, Long> tempPairInfok = new HashMap();
			Map<List<Boolean>,Map<Integer,Integer>> tempPairInfok2 = new HashMap<>();
			//Map<List<Boolean>,Set<Integer>> tempPairInfok3 = new HashMap<>();

			tempPairInfo.add(tempPairInfok);
			tempPairInfo2.add(tempPairInfok2);
			//tempPairInfo3.add(tempPairInfok3);
			Thread thread = new Thread(new Runnable()
			{
				public void run() {
					Map<List<Boolean>, Long> tempPairInfok = tempPairInfo.get(kTemp);
					Map<List<Boolean>, Map<Integer,Integer>> tempPairInfok2 = tempPairInfo2.get(kTemp);
					//Map<List<Boolean>, Set<Integer>> tempPairInfok3 = tempPairInfo3.get(kTemp);

					int numCols = originalTable.getNumCols();
					Boolean[] cur1 = new Boolean[presMap.size()];
					Boolean[] cur2 = new Boolean[presMap.size()];
					for(int t1 = startk ; t1 < finalk; t1++)
						for(int t2 = t1 ; t2 < numRows; t2++)
						{
							for (int i=0; i<presMap.size();i++) {
								cur1[i] = false;
								cur2[i] = false;
							}

							NewTuple tuple1 = curTuples.get(t1);
							NewTuple tuple2 = curTuples.get(t2);

							//The naive way

							int p = 0;

							if(Config.qua == 1)
							{
								for(int col = 1; col <= numCols; col++)
								{
									int type = originalTable.getColumnMapping().positionToType(col);

									if(type == 2)
									{
										if(tuple1.tid == tuple2.tid) {
											cur1[p] = true;
											cur2[p] = true;

											cur1[p+1] = true;
											cur2[p+1] = true;
										}
										else{
										if(dbPro.equalOrNot2(col-1, tuple1.getCell(col-1).getStringValue(), tuple2.getCell(col-1).getStringValue()))
										{

											cur1[p] = true;
											cur2[p] = true;
										}
										else
										{
											cur1[p+1] = true;
											cur2[p+1] = true;
										}}
										p = p + 2;
									}
									else if(type == 0 || type == 1)
									{
										if(tuple1.tid == tuple2.tid) {
												cur1[p]=true;
												cur1[p+1] = true;
												cur1[p+2] = true;
												cur1[p+3] = true;
												cur1[p+4] = true;
												cur1[p+5] = true;

												cur2[p] = true;
												cur2[p+1] = true;
												cur2[p+2] = true;
												cur2[p+3] = true;
												cur2[p+4] = true;
												cur2[p+5] = true;
										}
										// if >
										else if(presMap.get(p+2).check(tuple1,tuple2))
										{
											cur1[p+1] = true;
											cur1[p+2] = true;
											cur1[p+5] = true;

											cur2[p+1] = true;
											cur2[p+3] = true;
											cur2[p+4] = true;
										}
										else // <=
										{
											// =
											if(presMap.get(p).check(tuple1, tuple2))
											{
												cur1[p] = true;
												cur1[p+3] = true;
												cur1[p+5] = true;

												cur2[p] = true;
												cur2[p+3] = true;
												cur2[p+5] = true;
											}
											else
											{
												cur1[p+1] = true;
												cur1[p+3] = true;
												cur1[p+4] = true;

												cur2[p+1] = true;
												cur2[p+2] = true;
												cur2[p+5] = true;

											}

										}

										p = p + 6;
									}
									else
									{
										System.out.println("Type not supported");
									}

								}
							}

							for(p = p ; p < presMap.size(); p+=1) {
								if (tuple1.tid != tuple2.tid) {
									cur1[p] = true;
									cur2[p] = true;
								} else {
									NewPredicate pre = presMap.get(p);
									if (pre.check(tuple1, tuple2)) {
										cur1[p] = true;
									}
								}
							}


							List<Boolean> cur1list = new ArrayList<Boolean>(Arrays.asList(cur1));
							List<Boolean> cur2list = new ArrayList<Boolean>(Arrays.asList(cur2));
							if (tempPairInfok.containsKey(cur1list))
							{
								tempPairInfok.put(cur1list, tempPairInfok.get(cur1list)+1);
								//tempPairInfok.put(cur1, tempPairInfok.get(cur1)+1);
							}
							else
							{
								//Set<IntegerPair> curSet = new HashSet<>();
								//curSet.add(new IntegerPair(t1, t2));
								tempPairInfok.put(cur1list, (long)1);

							}

							if (tempPairInfok.containsKey(cur2list) && tuple1.tid != tuple2.tid)
							{
								//tempPairInfok.put(cur2, tempPairInfok.get(cur2)+1);
								tempPairInfok.put(cur2list, tempPairInfok.get(cur2list)+1);

							}
							else if (tuple1.tid != tuple2.tid)
							{
								//tempPairInfok.put(cur2, (long) 1);

								//Set<IntegerPair> curSet = new HashSet<>();
								//curSet.add(new IntegerPair(t2, t1));
								tempPairInfok.put(cur2list, (long)1);

							}

							if (tempPairInfok2.containsKey(cur1list))
							{
								if (tempPairInfok2.get(cur1list).containsKey(t1)) {
									int old = tempPairInfok2.get(cur1list).get(t1);
									tempPairInfok2.get(cur1list).put(t1, old+1);
								}
								else {
									tempPairInfok2.get(cur1list).put(t1, 1);
								}

								if (t1 != t2 && tempPairInfok2.get(cur1list).containsKey(t2)) {
									int old = tempPairInfok2.get(cur1list).get(t2);
									tempPairInfok2.get(cur1list).put(t2, old+1);
								}
								else if (t1 != t2) {
									tempPairInfok2.get(cur1list).put(t2, 1);
								}
							}
							else
							{
								tempPairInfok2.put(cur1list, new HashMap<>());
								tempPairInfok2.get(cur1list).put(t1, 1);
								tempPairInfok2.get(cur1list).put(t2, 1);
							}

							if (tempPairInfok2.containsKey(cur2list) && tuple1.tid != tuple2.tid)
							{
								if (tempPairInfok2.get(cur2list).containsKey(t1)) {
									int old = tempPairInfok2.get(cur2list).get(t1);
									tempPairInfok2.get(cur2list).put(t1, old+1);
								}
								else {
									tempPairInfok2.get(cur2list).put(t1, 1);
								}

								if (tempPairInfok2.get(cur2list).containsKey(t2)) {
									int old = tempPairInfok2.get(cur2list).get(t2);
									tempPairInfok2.get(cur2list).put(t2, old+1);
								}
								else {
									tempPairInfok2.get(cur2list).put(t2, 1);
								}

							}
							else if (tuple1.tid != tuple2.tid)
							{
								tempPairInfok2.put(cur2list, new HashMap<>());
								tempPairInfok2.get(cur2list).put(t1, 1);
								tempPairInfok2.get(cur2list).put(t2, 1);
							}

							/*if (evidenceEdges.containsKey(cur1list))
							{
								evidenceEdges.get(cur1list).add(t1*numRows+t2);
							}
							else
							{
								evidenceEdges.put(cur1list, new HashSet<>());
								evidenceEdges.get(cur1list).add(t1*numRows+t2);
							}

							if (evidenceEdges.containsKey(cur2list) && tuple1.tid != tuple2.tid)
							{
								evidenceEdges.get(cur2list).add(t2*numRows+t1);

							}
							else if (tuple1.tid != tuple2.tid)
							{
								evidenceEdges.put(cur2list, new HashSet<>());
								evidenceEdges.get(cur2list).add(t2*numRows+t1);
							}*/
						}
				}

			});
			thread.start();
			threads.add(thread);
		}
		for(int k = 0 ; k < numThreads; k++)
		{
			try {
				threads.get(k).join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Long evidenceSetIndex = 0L;
		for(int k = 0 ; k < numThreads; k++)
		{
			Map<List<Boolean>, Long> tempPairInfok = tempPairInfo.get(k);
			for(List<Boolean> cur: tempPairInfok.keySet())
			{
				//System.out.println(cur.toString());
				evidenceNum = evidenceNum + tempPairInfok.get(cur);
				if (evidenceMapRev.keySet().contains(cur)) {
					evidence.put(cur, evidence.get(cur) + tempPairInfok.get(cur));
					Long currSize = evidenceSet.get(evidenceMapRev.get(cur));
					evidenceSet.put(evidenceMapRev.get(cur), currSize+tempPairInfok.get(cur));
					Map<Integer,Integer> currentmap = evidenceToPairs.get(evidenceMapRev.get(cur));
					for (Integer key : tempPairInfo2.get(k).get(cur).keySet()) {
						if (currentmap.containsKey(key)) {
							int oldvalue = currentmap.get(key);
							currentmap.put(key, oldvalue + tempPairInfo2.get(k).get(cur).get(key));
						}
						else {
							currentmap.put(key, tempPairInfo2.get(k).get(cur).get(key));
						}
					}
					//evidenceEdges.get(evidenceMapRev.get(cur)).addAll(new HashSet<Integer>(tempPairInfo3.get(k).get(cur)));
					//evidenceEdges.get(evidenceMapRev.get(cur)).addAll(new HashSet<ArrayList<Integer>>(tempPairInfo3.get(k).get(cur)));
					//uncoveredMap.get(evidenceMapRev.get(cur)).addAll(tempPairInfok.get(cur));
					continue;
				}

				evidenceSetIndex = evidenceSetIndex + 1;
				//evidenceMap.put(evidenceSetIndex, cur);
				evidenceMapRev.put(cur, evidenceSetIndex);
				//uncovered.add(evidenceSetIndex);
				evidenceSet.put(evidenceSetIndex,tempPairInfok.get(cur));
				evidences.put(evidenceSetIndex, cur);
				evidence.put(cur, tempPairInfok.get(cur));

				evidenceToPairs.put(evidenceSetIndex,new HashMap<>(tempPairInfo2.get(k).get(cur)));

				//evidenceEdges.put(evidenceSetIndex,new HashSet<>());
				//evidenceEdges.get(evidenceSetIndex).addAll(new HashSet<Integer>(tempPairInfo3.get(k).get(cur)));

				//Set<IntegerPair> curSet = new HashSet<>();
				//curSet.addAll(tempPairInfok.get(cur));
				//uncoveredMap.put(evidenceSetIndex, curSet);

				for (int i=0; i<cur.size();i++) {
					if (!cur.get(i))
						continue;

					if (evidenceForPred.get(i) == null) {
						evidenceForPred.put(i, new HashSet<>());
						evidenceForPredRemaining.put(i, (long) 0);
					}

					evidenceForPred.get(i).add(evidenceSetIndex);
					evidenceForPredRemaining.put(i, evidenceForPredRemaining.get(i)+1);
				}
			}

			tempPairInfok.clear();
		}

		uncovered = new HashSet<>(evidenceSet.keySet());

		Long covered = 0L;
		/*for (Long key : uncovered) {
			canBeCovered.put(key, true);
		}*/
	}
	/**
	 * Initialize all the tuple pair wise information for each set of constant predicates 
	 */
	/*private void initAllNewTupleWiseInfoParallel()
	{
		
		int numThreads =  Runtime.getRuntime().availableProcessors();
		
		ArrayList<Thread> threads = new ArrayList<Thread>();
		final ArrayList<HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Long>>> tempcons2PairInfo 
		= new ArrayList<HashMap<Set<NewPredicate>,Map<Set<NewPredicate>,Long>>>();
		
		//final ArrayList<HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Set<TP>>>> tempcons2PairInfoTP 
		//= new ArrayList<HashMap<Set<NewPredicate>,Map<Set<NewPredicate>,Set<TP>>>>();
		
		//final ArrayList<Map<Set<NewPredicate>,Long>> tempInfo = new ArrayList<Map<Set<NewPredicate>,Long> >();
		System.out.println("We are going to use " + numThreads + " threads");
		final int numRows = originalTable.getNumRows();
		int chunk = numRows / numThreads;
		
		for(int k = 0 ; k < numThreads; k++)
		{
			final int kTemp = k;
			final int startk = k * chunk;
			final int finalk = (k==numThreads-1)? numRows:(k+1)*chunk;
			//tempInfo.add(new HashMap<Set<NewPredicate>,Long>());
			
			HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Long>> tempcons2PairInfok = new HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Long>>();
			//HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Set<TP>>> tempcons2PairInfoTPk = new HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Set<TP>>>();
			
			for(Set<NewPredicate> consPres: consPresMap.keySet())
			{
				tempcons2PairInfok.put(consPres, new HashMap<Set<NewPredicate>,Long>());
				//tempcons2PairInfoTPk.put(consPres, new HashMap<Set<NewPredicate>,Set<TP>>());
			}
			tempcons2PairInfo.add(tempcons2PairInfok);
			//tempcons2PairInfoTP.add(tempcons2PairInfoTPk );
			Thread thread = new Thread(new Runnable()
			{
				public void run() {
					HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Long>> tempcons2PairInfok = tempcons2PairInfo.get(kTemp);
					//HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Set<TP>>> tempcons2PairInfoTPk = tempcons2PairInfoTP.get(kTemp);
					//Map<Set<NewPredicate>,Long> tupleWiseInfok = tempInfo.get(kTemp);
					int numCols = originalTable.getNumCols();
					for(int t1 = startk ; t1 < finalk; t1++)
						for(int t2 = 0 ; t2 < numRows; t2++)
						{

							NewTuple tuple1 = originalTable.getTuple(t1);
							NewTuple tuple2 = originalTable.getTuple(t2);
							
							if(tuple1.tid == tuple2.tid)
								continue;
						
							Set<NewPredicate> cur = new HashSet<NewPredicate>();
							
							
							
							//The naive way
						
							int p = 0;
							
							if(Config.qua == 1)
							{
								for(int col = 1; col <= numCols; col++)
								{
									
									NewPredicate pre = presMap.get(p);
									
									int type = originalTable.getColumnMapping().positionToType(col);
									
									if(type == 2)
									{
										
										int rel = 0; // 0- EQ, -1 -> IQ, 1- > , 2- <
										if(dbPro.equalOrNot(col-1, tuple1.getCell(col-1).getStringValue(), tuple2.getCell(col-1).getStringValue()))
										{
								
											rel = 0;
											cur.add(pre);
											
										}
										else
										{
											cur.add(presMap.get(p+1));
											
										
											rel = -1;
										}
										p = p + 2;
									}
									else if(type == 0 || type == 1)
									{
										// if > 
										if(presMap.get(p+2).check(tuple1,tuple2))
										{
											cur.add(presMap.get(p+1));
											cur.add(presMap.get(p+2));
											cur.add(presMap.get(p+5));
											
											
											
										}
										else // <=
										{
											// =
											if(presMap.get(p).check(tuple1, tuple2))
											{
												cur.add(presMap.get(p));
												cur.add(presMap.get(p+3));
												cur.add(presMap.get(p+5));
												
												
												
											}
											else
											{
												cur.add(presMap.get(p+1));
												cur.add(presMap.get(p+3));
												cur.add(presMap.get(p+4));
												
												
												
											}
											
										}
									
										p = p + 6;
									}
									else
									{
										System.out.println("Type not supported");
									}
									
								}								
							}
							
							
							for(p = p ; p < presMap.size(); p+=2)
							{
								
								NewPredicate pre = presMap.get(p);
								if(pre.check(tuple1,tuple2))
								{
									cur.add(pre);
								}
								else
									cur.add(presMap.get(p+1));
								
								
								
							}
							
							
							//increaseOne(cur);
							for(Set<NewPredicate> consPres: consPresMap.keySet())
							{
								if(!satisfyConsPres(tuple1,tuple2,consPres))
									continue;
								Map<Set<NewPredicate>,Long> tupleWiseInfo = tempcons2PairInfok.get(consPres);
								if (tupleWiseInfo.containsKey(cur))
								{
									tupleWiseInfo.put(cur, tupleWiseInfo.get(cur)+1);
									
									
								}
								else
								{
									tupleWiseInfo.put(cur, (long) 1);
									
								}
								
							
							}
							
							
						}
				}
				
			});
			thread.start();
			threads.add(thread);
		}
		for(int k = 0 ; k < numThreads; k++)
		{
			try {
				threads.get(k).join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for(int k = 0 ; k < numThreads; k++)
		{
			HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Long>> tempcons2PairInfok = tempcons2PairInfo.get(k);
			//HashMap<Set<NewPredicate>, Map<Set<NewPredicate>, Set<TP>>> tempcons2PairInfoTPk = tempcons2PairInfoTP.get(k);
			for(Set<NewPredicate> consPres: consPresMap.keySet())
			{
				Map<boolean[],Long> tupleWiseInfok = tempcons2PairInfok.get(consPres);
				//Map<Set<NewPredicate>,Set<TP>> tupleWiseInfoTPk = tempcons2PairInfoTPk.get(consPres);
				Map<boolean[], Long> aa = cons2PairInfo.get(consPres);
				//Map<Set<NewPredicate>, Set<TP>> bb = cons2PairInfoTP.get(consPres);
				for(boolean[] cur: tupleWiseInfok.keySet())
				{
					if (aa.containsKey(cur))
					{
						aa.put(cur, aa.get(cur) + tupleWiseInfok.get(cur));
						
					}
					else
					{
						aa.put(cur, tupleWiseInfok.get(cur));
					}
					/*if(bb.containsKey(cur))
					{
						bb.get(cur).addAll(tupleWiseInfoTPk.get(cur));
					}
					else 
					{
						bb.put(cur, new HashSet<TP>(tupleWiseInfoTPk.get(cur)));
					}*/
				//}
			//}
			
		//}
		
		
		
		
		
	//}

	
	/**
	 * Test if the two tuples satisfy the constant predicates
	 * @param t1
	 * @param t2
	 * @param cons
	 * @return
	 */
	private boolean satisfyConsPres(NewTuple t1, NewTuple t2, Set<NewPredicate> cons)
	{
		if(cons.size() == 0)
			return true;
		
		for(NewPredicate consPre: cons)
		{
			int row = consPre.getRows()[0];
			int col = consPre.getCols()[0];
			String value = consPre.getCons();
			String cellV = null;
			if(row == 1)
			{
				cellV = t1.getCell(col-1).getStringValue();
				
			}
			else if(row == 2)
			{
				cellV = t2.getCell(col-1).getStringValue();
			}
			int op = consPre.getOperator();
			assert(op == 0);
			
			if(!cellV.equals(value))
				return false;
		}
		return true;
	}
	
	public List<NewPredicate> getAllVarNewPredicates()
	{
		return presMap;
	}
	
	/**
	 * Get the reverse predicate of all predicates
	 * @param pre
	 * @return
	 */
	protected NewPredicate getReverseNewPredicate(Integer pre)
	{
		if(presMap.get(pre) != null)
		{
			if(pre %2 == 0)
				return presMap.get(pre+1);
			else
				return presMap.get(pre - 1);
		}
		/*else if(consPres.contains(pre))
		{
			int index = consPres.indexOf(pre);
			if(index %2 == 0)
				return consPres.get(index+1);
			else
				return consPres.get(index - 1);
		}*/
		else
		{
			//System.out.println("ERROR: Getting the reverse of a predicate.");
			return null;
		}
	}

	protected NewPredicate getReverseNewPredicate(NewPredicate pre)
	{
		if(presMap.contains(pre))
		{
			int index = presMap.indexOf(pre);
			if(index %2 == 0)
				return presMap.get(index+1);
			else
				return presMap.get(index - 1);
		}
		else if(consPres.contains(pre))
		{
			int index = consPres.indexOf(pre);
			if(index %2 == 0)
				return consPres.get(index+1);
			else
				return consPres.get(index - 1);
		}
		else
		{
			//System.out.println("ERROR: Getting the reverse of a predicate.");
			return null;
		}
	}
	
	private Map<NewPredicate,Set<NewPredicate>> impliedMap = new HashMap<NewPredicate,Set<NewPredicate>>();
	/**
	 * Get the set of predicates implied by the passed-in predicate, does not include the pre itself
	 * @param pre
	 * @return
	 */
	public Set<NewPredicate> getImpliedNewPredicates(NewPredicate pre)
	{
		if(pre == null)
			return new HashSet<NewPredicate>();
		
		if(!impliedMap.containsKey(pre))
		{
			
			if(pre.isSecondCons())
			{
				Set<NewPredicate> result = new HashSet<NewPredicate>();
				for(NewPredicate temp: consPres)
				{
					if(temp == pre)
						continue;
					if(pre.implied(temp))
						result.add(temp);
				}
				impliedMap.put(pre, result);
				return result;
			}
			else
			{
				Set<NewPredicate> result = new HashSet<NewPredicate>();
				for(NewPredicate temp: presMap)
				{
					if(temp == pre)
						continue;
					if(pre.implied(temp))
						result.add(temp);
				}
				impliedMap.put(pre, result);
				return result;
			}
			
			
		}
		else
			return impliedMap.get(pre);
	}
	
	
	/**
	 * This function is to compute the transitive closure of the set of predicates
	 * @param pres
	 */
	private void allImplied(Set<NewPredicate> pres)
	{
		for(NewPredicate left: pres)
			for(NewPredicate right: pres)
			{
				if(left == right)
					continue;
				NewPredicate t = transitive(left,right);
				if(t != null)
				{
					pres.add(t);
					return;
				}
				
				
			}
	}
	
	private NewPredicate transitive(NewPredicate left, NewPredicate right)
	{
		if(left == null || right == null)
			return null;
		
		
		int[] rowsleft = left.getRows();
		int[] rowsright = right.getRows();
		int[] colsleft = left.getCols();
		int[] colsright = right.getCols();
		int operatorLeft = left.getOperator();
		int operatorRight = right.getOperator();
		
		String lc1 = rowsleft[0] + "." + colsleft[0];
		String lc2 = rowsleft[1] + "." + colsleft[1];
		String rc1 = rowsright[0] + "." + colsright[0];
		String rc2 = rowsright[1] + "." + colsright[1];
		
		String resultc1 = null;
		String resultc2 = null;
		int resultn = -1;
		
		if(lc2.equals(rc1))
		{
			resultc1 = lc1;
			resultc2 = rc2;
			if(operatorLeft == 0 && operatorRight == 0)
			{
				resultn = 0;
			}
			else if(operatorLeft == 0 && operatorRight == 2)
			{
				resultn = 2;
			}
			else if(operatorLeft == 0 && operatorRight == 4)
			{
				resultn = 4;
			}
			else if(operatorLeft == 0 && operatorRight == 1)
			{
				resultn = 1;
			}
			else if(operatorLeft == 1 && operatorRight == 0)
			{
				resultn = 1;
			}
			else if(operatorLeft == 2 && operatorRight == 0)
			{
				resultn = 2;
			}
			else if(operatorLeft == 2 && operatorRight == 2)
			{
				resultn = 2;
			}
			else if(operatorLeft == 4 && operatorRight == 0)
			{
				resultn = 4;
			}
			else if(operatorLeft == 4 && operatorRight == 4)
			{
				resultn = 4;
			}
			
		}
		else if(lc2.equals(rc2))
		{
			resultc1 = lc1;
			resultc2 = rc1;
			if(operatorLeft == 0 && operatorRight == 0)
			{
				resultn = 0;
			}
			else if(operatorLeft == 0 && operatorRight == 2)
			{
				resultn = 4;
			}
			else if(operatorLeft == 0 && operatorRight == 4)
			{
				resultn = 2;
			}
			else if(operatorLeft == 0 && operatorRight == 1)
			{
				resultn = 1;
			}
			else if(operatorLeft == 1 && operatorRight == 0)
			{
				resultn = 1;
			}
			else if(operatorLeft == 2 && operatorRight == 0)
			{
				resultn = 4;
			}
			else if(operatorLeft == 2 && operatorRight == 4)
			{
				resultn = 2;
			}
			else if(operatorLeft == 4 && operatorRight == 0)
			{
				resultn = 4;
			}
			else if(operatorLeft == 4 && operatorRight == 2)
			{
				resultn = 4;
			}
		}
		else if(lc1.equals(rc1))
		{
			resultc1 = lc2;
			resultc2 = rc2;
			if(operatorLeft == 0 && operatorRight == 0)
			{
				resultn = 0;
			}
			else if(operatorLeft == 0 && operatorRight == 2)
			{
				resultn = 2;
			}
			else if(operatorLeft == 0 && operatorRight == 4)
			{
				resultn = 4;
			}
			else if(operatorLeft == 0 && operatorRight == 1)
			{
				resultn = 1;
			}
			else if(operatorLeft == 1 && operatorRight == 0)
			{
				resultn = 1;
			}
			else if(operatorLeft == 2 && operatorRight == 0)
			{
				resultn = 4;
			}
			else if(operatorLeft == 2 && operatorRight == 4)
			{
				resultn = 4;
			}
			else if(operatorLeft == 4 && operatorRight == 0)
			{
				resultn = 2;
			}
			else if(operatorLeft == 4 && operatorRight == 2)
			{
				resultn = 2;
			}
		}
		else if(lc2.equals(rc2))
		{
			resultc1 = lc1;
			resultc2 = rc1;
			if(operatorLeft == 0 && operatorRight == 0)
			{
				resultn = 0;
			}
			else if(operatorLeft == 0 && operatorRight == 2)
			{
				resultn = 4;
			}
			else if(operatorLeft == 0 && operatorRight == 4)
			{
				resultn = 2;
			}
			else if(operatorLeft == 0 && operatorRight == 1)
			{
				resultn = 1;
			}
			else if(operatorLeft == 1 && operatorRight == 0)
			{
				resultn = 1;
			}
			else if(operatorLeft == 2 && operatorRight == 0)
			{
				resultn = 2;
			}
			else if(operatorLeft == 2 && operatorRight == 4)
			{
				resultn = 2;
			}
			else if(operatorLeft == 4 && operatorRight == 0)
			{
				resultn = 4;
			}
			else if(operatorLeft == 4 && operatorRight == 2)
			{
				resultn = 4;
			}
		}
		
		if(resultc1 == null || resultc2 == null || resultn == -1)
			return null;
		
		for(NewPredicate pre: presMap)
		{
			String c1 = pre.getRows()[0] + "." + pre.getCols()[0];
			String c2 = pre.getRows()[1] + "." + pre.getCols()[1];
			int op = pre.getOperator();
			if(c1.equals(resultc1) && c2.equals(resultc2) && op == resultn)
			{
				return pre;
			}
			if(c1.equals(resultc2) && c2.equals(resultc1))
			{
				if(op == 0 && resultn == 0)
					return pre;
				else if(op == 1 && resultn == 1)
					return pre;
				else if(op == 2 && resultn == 4)
					return pre;
				else if(op == 4 && resultn == 2)
					return pre;
			}
		}
		return null;
	}
	
	
	
	
	public boolean linearImplication(Collection<NewDenialConstraint> dcs, NewDenialConstraint conse)
	{

		/*Set<NewPredicate> premise = new HashSet<NewPredicate>(conse.getNewPredicates());
		for(int k = 0; k < conse.getNewPredicates().size(); k++)
		{
			NewPredicate kpre = conse.getNewPredicates().get(k);
			premise.remove(kpre);
			Set<NewPredicate> closure = linearGetClosure(dcs,premise);
			if(closure.contains(this.getReverseNewPredicate(kpre)))
				return true;
			premise.add(kpre);
		}
		return false;*/
		
		if(conse == null)
			return false;
		
		//if(conse.isTrivial())
		//	return true;
		
	/*	Set<NewDenialConstraint> dcs_all = new HashSet<NewDenialConstraint>(dcs);
		for(NewDenialConstraint dc: dcs)
		{
			NewDenialConstraint temp = this.getSymmetricDC(dc);
			dcs_all.add(temp);
		}*/
		
		Set<NewPredicate> premise = new HashSet<NewPredicate>(conse.getPredicates());
		Set<NewPredicate> closure = linearGetClosure(dcs,premise);
		if(closure.contains(null))
			return true;
		else
			return false;
	}
	
	private Set<NewPredicate> linearGetClosure(Collection<NewDenialConstraint> dcs, Set<NewPredicate> premise)
	{
		//Init of the result
		Set<NewPredicate> result = new HashSet<NewPredicate>(premise);
		for(NewPredicate temp: premise)
		{
			result.addAll(this.getImpliedNewPredicates(temp));
		}
		//allImplied(result);
			
		
		//A list of candidate DCs, n-1 predicates are already in result
		List<NewDenialConstraint> canDCs = new ArrayList<NewDenialConstraint>();
		
		//From predicate, to a set of DCs, that contain the key predicate
		Map<NewPredicate,Set<NewDenialConstraint>> p2dcs = new HashMap<NewPredicate,Set<NewDenialConstraint>>();
		//From DC, to a set of predicates, that are not yet included in the closure
		Map<NewDenialConstraint,Set<NewPredicate>> dc2ps = new HashMap<NewDenialConstraint,Set<NewPredicate>>();
		
		
		//for(NewPredicate pre: presMap)
			//p2dcs.put(pre, new HashSet<NewDenialConstraint>());
		//Init two maps
		for(NewDenialConstraint dc: dcs)
		{
			for(NewPredicate pre: dc.getPredicates())
			{
				if(p2dcs.containsKey(pre))
				{
					Set<NewDenialConstraint> value = p2dcs.get(pre);
					value.add(dc);
					p2dcs.put(pre, value);
				}
				else
				{
					Set<NewDenialConstraint> value = new HashSet<NewDenialConstraint>();
					value.add(dc);
					p2dcs.put(pre, value);
				}
				
				
			}
			dc2ps.put(dc, new HashSet<NewPredicate>());
		}
		for(NewPredicate pre: p2dcs.keySet())
		{
			
			if(result.contains(pre))
				continue;
			
			
			for(NewDenialConstraint tempDC: p2dcs.get(pre))
			{
				Set<NewPredicate> value = dc2ps.get(tempDC);
				value.add(pre);
				dc2ps.put(tempDC, value);
			}
		}
		//Init the candidate list
		for(NewDenialConstraint dc: dcs)
		{
			if(dc2ps.get(dc).size() == 1)
				canDCs.add(dc);
			if(dc2ps.get(dc).size() == 0)
			{
				result.add(null);
				return result;
			}
		}
		
		//Do the main loop of adding to the list
		while(!canDCs.isEmpty())
		{
			NewDenialConstraint canDC = canDCs.remove(0);
			
			Set<NewPredicate> tailSet = dc2ps.get(canDC);
			
			//Why can be 0?
			if(tailSet.size() == 0)
				continue;
			
			NewPredicate tail = tailSet.iterator().next();

			NewPredicate reverseTail = this.getReverseNewPredicate(presMap.indexOf(tail));
			
			
			Set<NewPredicate> toBeProcessed = new HashSet<NewPredicate>();
			toBeProcessed.add(reverseTail);
			toBeProcessed.addAll(this.getImpliedNewPredicates(reverseTail));
			toBeProcessed.removeAll(result);
			

			for(NewPredicate reverseTailImp: toBeProcessed)
			{
				if(p2dcs.containsKey(reverseTailImp))
				{
					Set<NewDenialConstraint> covered = p2dcs.get(reverseTailImp);
					Set<NewDenialConstraint> toBeRemoved = new HashSet<NewDenialConstraint>();
					for(NewDenialConstraint tempDC: covered)
					{
						Set<NewPredicate> value = dc2ps.get(tempDC);
						value.remove(reverseTailImp);
						if(value.size() == 1)
						{
							canDCs.add(tempDC);
							toBeRemoved.add(tempDC);
						}
						else if(value.size() == 0)
						{
							result.add(null);
							return result;
						}
							
					}
					covered.removeAll(toBeRemoved);
				}
				

				
				
			}
		
			result.addAll(toBeProcessed);
			
			//allImplied(result);
			
		}
		
		
		
		return result;
	}
	
	/**
	 * Get the minimal DC for each DC
	 * @param dcs
	 */
	/*protected int initMinimalDC(List<NewDenialConstraint> dcs)
	{
		
		int result = 0;
		for(NewDenialConstraint dc: dcs)
		{
			
			boolean temp = dc.initMostSuc();
			if(temp)
			{
				System.out.println(dc.toString() + " is affected!");
				result++;
			}
				
		}
		return result;
	}*/
	
	/**
	 * Remove from the set, the set of trivial DCs
	 * @param dcs
	 * @return the number of trivial DCs
	 */
	/*protected int removeTriviality(List<NewDenialConstraint> dcs)
	{
		Set<NewDenialConstraint> toBeRemoved = new HashSet<NewDenialConstraint>();
		for(NewDenialConstraint dc: dcs)
		{
			if(dc.isTrivial())
				toBeRemoved.add(dc);
		}
		dcs.removeAll(toBeRemoved);
		return toBeRemoved.size();
	}*/
	
	
	/**
	 * Remove from the set, the set of DCs that is a superset of some other DC in the set
	 * @param dcs
	 * @return the number of DCs removed
	 */
	/*protected int removeSubset(List<NewDenialConstraint> dcs)
	{
		
		Set<NewDenialConstraint> toBeRemoved = new HashSet<NewDenialConstraint>();
		int result  = 0;
		boolean changed = true;
		while(changed)
		{
			changed = false;
			for(NewDenialConstraint dc1: dcs)
			{
				for(NewDenialConstraint dc2: dcs)
				{
					if(dc1 == dc2)
						continue;
					
					if(dc2.getNewPredicates().containsAll(dc1.getNewPredicates()))
					{
						toBeRemoved.add(dc2);
					}
				}
				
				if(toBeRemoved.size() > 0)
				{
					dcs.removeAll(toBeRemoved);
					result += toBeRemoved.size();
					toBeRemoved.clear();
					changed = true;
					break;
				}
				
			}
		}
		
	
		return result;
	}*/
	
	/**
	 * 
	 * @param dcs
	 * @return
	 */
	protected int minimalCover(List<NewDenialConstraint> dcs)
	{
		int result = 0;
		
		
		int sizeBefore = -1;
		while(sizeBefore != dcs.size())
		{
			sizeBefore = dcs.size();
			for(int i = 0 ; i < dcs.size(); i++)
			{
				Set<NewDenialConstraint> ante = new HashSet<NewDenialConstraint>(dcs);
				ante.remove(dcs.get(i));
				//if(implicationTesting(ante, dcs.get(i)))
				if(linearImplication(ante, dcs.get(i)))
				{
					//System.out.println("The DC has been removed due to implication: " + dcs.get(i));
					dcs.remove(i);
					result++;
					break;
				}
			}
		}
		
		//The following approach is obviously wrong
		/*Set<NewDenialConstraint> toBeRemoved = new HashSet<NewDenialConstraint>();
		Set<NewDenialConstraint> ante = new HashSet<NewDenialConstraint>(dcs);
		for(NewDenialConstraint dc: dcs)
		{
			ante.remove(dc);
			if(linearImplication(ante,dc))
			{
				toBeRemoved.add(dc);
			}
			ante.add(dc);
		}
		dcs.removeAll(toBeRemoved);
		result = toBeRemoved.size();*/
		
		return result;
	}
	
	protected int minimalCoverAccordingtoInterestingess(List<NewDenialConstraint> dcs)
	{
		int result = 0;
		Set<NewDenialConstraint> toBeRemoved = new HashSet<NewDenialConstraint>();
		Set<NewDenialConstraint> tempDCs = new HashSet<NewDenialConstraint>(dcs);
		for(int i = dcs.size() - 1; i >=0; i--)
		{
			tempDCs.remove(dcs.get(i));
			if(this.linearImplication(tempDCs, dcs.get(i)))
			{
				toBeRemoved.add(dcs.get(i));
			}
		}
		
		dcs.removeAll(toBeRemoved);
		result = toBeRemoved.size();
		return result;
	}
	
	private Map<NewPredicate,NewPredicate> symMap = new HashMap<NewPredicate,NewPredicate>();
	private NewPredicate getSymmetricPredicate(NewPredicate pre)
	{
		if(symMap.containsKey(pre))
			return symMap.get(pre);
		
		
		NewPredicate result = null;
		
		if(pre.getOperator() == 0 || pre.getOperator() == 1)
			return pre;
		
		for(NewPredicate temp: presMap)
		{
			if(pre.equalExceptOp(temp))
			{
				if(pre.getOperator() == 2 && temp.getOperator() == 4)
					result =  temp;
				else if(pre.getOperator() == 4 && temp.getOperator() == 2)
					result = temp;
				else if(pre.getOperator() == 5 && temp.getOperator() == 3)
					result =  temp;
				else if(pre.getOperator() == 3 && temp.getOperator() == 5)
					result = temp;
				else
					continue;
			}
		}
		if(result != null)
		{
			symMap.put(pre, result);
			symMap.put(result, pre);
		}
		return result;
		
	}
	
	/**
	 * Get symmetric predicate using substitution
	 * @param pre
	 * @return
	 */
	private NewPredicate getSymmetricNewPredicate2(NewPredicate pre)
	{
		if(symMap.containsKey(pre))
			return symMap.get(pre);
		
		
		NewPredicate result = null;
		
		//int[] rows = pre.getRows();

		if(pre.getOperator() == 0 || pre.getOperator() == 1)
			return pre;

		for(NewPredicate temp: presMap)
		{
			if(pre.equalExceptOp(temp))
			{
				if(pre.getOperator() == 2 && temp.getOperator() == 4)
					result =  temp;
				else if(pre.getOperator() == 4 && temp.getOperator() == 2)
					result = temp;
				else if(pre.getOperator() == 5 && temp.getOperator() == 3)
					result =  temp;
				else if(pre.getOperator() == 3 && temp.getOperator() == 5)
					result = temp;
				else
					continue;
			}
		}
	
		if(result != null)
		{
			symMap.put(pre, result);
			symMap.put(result, pre);
		}
		return result;
		
	}
	
	protected int removeSymmetryDCs(List<NewDenialConstraint> dcs)
	{
		Set<NewDenialConstraint> toBeRemoved = new HashSet<NewDenialConstraint>();
		for(int i = 0 ; i < dcs.size(); i++)
			for(int k = 0; k < dcs.size(); k++)
			{
				if(k > i)
				{
					NewDenialConstraint curDC = dcs.get(i);
					NewDenialConstraint afterDC = dcs.get(k);
					
					
					//Determine if curDC and afterDC are symmetric
					boolean tag = true;
					ArrayList<NewPredicate> pres1 = curDC.getPredicates();
					ArrayList<NewPredicate> pres2 = afterDC.getPredicates();
					
					if(pres1.size() != pres2.size())
						continue;
					
					for(int j = 0 ; j < pres1.size(); j++)
					{
						NewPredicate pre1 = pres1.get(j);
						
						NewPredicate symPre1 = this.getSymmetricNewPredicate2(pre1);
						if(!pres2.contains(symPre1))
						{
							tag = false;
							break;
						}	
					}
					
					if(tag == true)
					{
						toBeRemoved.add(curDC);
						break;
					}
						
				}
			}
		
		dcs.removeAll(toBeRemoved);
		return toBeRemoved.size();
	}
	protected NewDenialConstraint getSymmetricDC(NewDenialConstraint dc)
	{
		ArrayList<NewPredicate> curPres = new ArrayList<NewPredicate>();
		for(NewPredicate pre: dc.getPredicates())
		{
			NewPredicate sym = this.getSymmetricNewPredicate2(pre);
			curPres.add(sym);
		}
		
		return new NewDenialConstraint(2,curPres,this);
	}
	
	/**
	 * Add all DCs from each constant predicate, to all DCs, and do a subset pruning
	 */
	private void postprocess()
	{
		for(Set<NewPredicate> consPresInput: consPresMap.keySet())
		{
			//The set of implied var predicates, by constant predicates.
			//Example: t1.A = a, t2.A = a, implied t1.A = t2.A
			Set<NewPredicate> impliedVarPres = new HashSet<NewPredicate>();
			Set<Integer> cols = new HashSet<Integer>();
			for(NewPredicate pre: consPresInput)
			{
				int col = pre.getCols()[0];
				cols.add(col);
			}
			for(NewPredicate pre: presMap)
			{
				if(pre.getOperator() == 0 && pre.getCols()[0] == pre.getCols()[1] && cols.contains(pre.getCols()[0]) )
				{
					impliedVarPres.add(pre);
				}
			}

			//Add implied var predicates to DCs
			ArrayList<NewDenialConstraint> dcs = consPresMap.get(consPresInput);
			for(NewDenialConstraint dc: dcs)
			{
				dc.addPredicates(consPresInput);
				dc.addPredicates(impliedVarPres);
			}

			//Eliminate of trivial constant DCs
			//this.removeTriviality(dcs);
			//remove implied var predicates from Dcs
			/*for(DenialConstraint dc: dcs)
			{
				dc.removePredicates(impliedVarPres);
			}*/

			//Add them to the total DCs

			//Modify the interestingness of DCs
			for(NewDenialConstraint dc: dcs)
			{
				//double rate = (double)cons2NumTuples.get(consPresInput) / originalTable.getNumRows();
				double rate = 1;
				dc.interestingness = dc.interestingness * Math.sqrt(rate);
			}

			totalDCs.addAll(dcs);
		}
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
				"FASTDC: The total number of DCs before subset pruning is: " + totalDCs.size());
		//remove subset from totalDCs
		//this.removeSubset(totalDCs);
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
				"FASTDC: The total number of DCs after subset pruning is: " + totalDCs.size());

		//Rank the totalDCs according to their interestingness score
		Collections.sort(totalDCs, new Comparator<NewDenialConstraint>()
		{
			public int compare(NewDenialConstraint arg0,
					NewDenialConstraint arg1) {
				if(arg0.interestingness > arg1.interestingness)
					return -1;
				else if(arg0.interestingness < arg1.interestingness)
					return 1;
				else
					return 0;

			}

		});

		//Remove Symmetric DCs
		//int symReduction = this.removeSymmetryDCs(totalDCs);
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
				"FASTDC: The total number of DCs after symmetric reduction is: " + totalDCs.size());

	}
	
	protected long runningTime; // in seconds
	public ArrayList<NewDenialConstraint> discover()
	{
		ArrayList<NewDenialConstraint> result = new ArrayList<>();
		for(Set<NewPredicate> consPresInput: consPresMap.keySet())
		{
			ArrayList<NewDenialConstraint> dcs = consPresMap.get(consPresInput);
			Map<List<Boolean>,Long> tuplewiseinfo = cons2PairInfo.get(consPresInput);
			//Map<Set<NewPredicate>,Set<TP>> tuplewiseinfoTP = cons2PairInfoTP.get(consPresInput);
			
			discoverInternal(dcs,tuplewiseinfo);	
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ":" + 
					"--------Cons Done-----");
		
		}
		postprocess();
		
		runningTime = (System.currentTimeMillis() - startingTime )/ 1000;
		
		dc2File();
		writeStats();
		
		System.out.println((System.currentTimeMillis() - startingTime)/1000 + ":" + 
				"*************Done Constraints Discovery******************");

		return totalDCs;
	}
	
	
	/*public void discoverEXCHKS()
	{
		dbPro.getEXCHKDCs(exchkDCs,consPres);
		
		
		
		//Get the interestingness score for each DCs
		for(NewDenialConstraint dc: exchkDCs)
		{
			double coverage = 0;
			
			for(NewTuple tuple: originalTable.getTuples())
			{
				int satNumPres = 0;
				for(NewPredicate pre: dc.getPredicates())
					if(pre.check(tuple))
					{
						satNumPres++;
					}

				coverage += (double)(satNumPres+1) / dc.getPredicates().size();
			}
			coverage = coverage / originalTable.getNumRows();
			
			
			if(coverage > 1)
				coverage = 1;
			
			//just coverage
			dc.interestingness = coverage;
			
			
			//System.out.println(dc.toString());
		}
		//rank the DCs according to their interestingness score
		Collections.sort(exchkDCs, new Comparator<NewDenialConstraint>()
				{
					public int compare(NewDenialConstraint arg0,
							NewDenialConstraint arg1) {
						if(arg0.interestingness > arg1.interestingness)
							return -1;
						else if(arg0.interestingness < arg1.interestingness)
							return 1;
						else
							return 0;
							
					}
			
				});
		
		
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
				": Number of Extend check constraints Before Implication: " + exchkDCs.size());
		
		this.minimalCoverAccordingtoInterestingess(exchkDCs);
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
				": Number of Extend check constraints After first Implication: " + exchkDCs.size());
		minimalCover(exchkDCs);
		
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
				": Number of Extend check constraints After total Implication: " + exchkDCs.size());
		
		
		dc2File();
	}*/
	
	/**
	 * Child class must override this method
	 * @param
	 */
	protected void discoverInternal(ArrayList<NewDenialConstraint> dcs, Map<List<Boolean>,Long> tuplewiseinfo)
	{
	}
	
	/**
	 * Add all DCs from each constant predicate, to all DCs, and do a subset pruning
	 */
	/*private void postprocess()
	{
		for(Set<NewPredicate> consPresInput: consPresMap.keySet())
		{
			//The set of implied var predicates, by constant predicates.
			//Example: t1.A = a, t2.A = a, implied t1.A = t2.A
			Set<NewPredicate> impliedVarPres = new HashSet<NewPredicate>();
			Set<Integer> cols = new HashSet<Integer>();
			for(NewPredicate pre: consPresInput)
			{
				int col = pre.getCols()[0];
				cols.add(col);
			}
			for(NewPredicate pre: presMap)
			{
				if(pre.getOperator() == 0 && pre.getCols()[0] == pre.getCols()[1] && cols.contains(pre.getCols()[0]) )
				{
					impliedVarPres.add(pre);
				}
			}
			
			//Add implied var predicates to DCs
			ArrayList<NewDenialConstraint> dcs = consPresMap.get(consPresInput);
			for(NewDenialConstraint dc: dcs)
			{
				dc.addPredicates(consPresInput);
				dc.addPredicates(impliedVarPres);
			}
			
			//Eliminate of trivial constant DCs
			//this.removeTriviality(dcs);
			//remove implied var predicates from Dcs
			for(NewDenialConstraint dc: dcs)
			{
				dc.removeNewPredicates(impliedVarPres);
			}
			
			//Add them to the total DCs
			
			//Modify the interestingness of DCs 
			for(NewDenialConstraint dc: dcs)
			{
				//double rate = (double)cons2NumNewTuples.get(consPresInput) / originalTable.getNumRows();
				double rate = 1; 
				dc.interestingness = dc.interestingness * Math.sqrt(rate);
			}
			
			totalDCs.addAll(dcs);
		}
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
				"FASTDC: The total number of DCs before subset pruning is: " + totalDCs.size());
		//remove subset from totalDCs
		//this.removeSubset(totalDCs);
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
				"FASTDC: The total number of DCs after subset pruning is: " + totalDCs.size());
		
		//Rank the totalDCs according to their interestingness score
		Collections.sort(totalDCs, new Comparator<NewDenialConstraint>()
				{
					public int compare(NewDenialConstraint arg0,
							NewDenialConstraint arg1) {
						if(arg0.interestingness > arg1.interestingness)
							return -1;
						else if(arg0.interestingness < arg1.interestingness)
							return 1;
						else
							return 0;
							
					}
			
				});
		
		//Remove Symmetric DCs
		int symReduction = this.removeSymmetryDCs(totalDCs);
		System.out.println((System.currentTimeMillis() - startingTime )/ 1000 + 
				"FASTDC: The total number of DCs after symmetric reduction is: " + totalDCs.size());
		
	}*/
	
	/*int numTotalEvi = 0;
	public int getTotalEvi()
	{
		
			return numTotalEvi;
	}*/
	public Set<NewDenialConstraint> getAllDCs()
	{
		return new HashSet<NewDenialConstraint>(totalDCs);
	}
	private void dc2File()
	{
		
		String dcPath = null;
		dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("ALLDCS"));
		/*if(originalTable.inputDBPath.endsWith("inputDBGolden"))
		{
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_g"));
		}
		else if(originalTable.inputDBPath.endsWith("inputDB"))
		{
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_s"));
		}
		else if(originalTable.inputDBPath.endsWith("inputDBAll"))
		{
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_s"));
		}
		else if(originalTable.inputDBPath.endsWith("inputDBNoise"))
		{
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_s_noise"));
		}*/
		
		try {
			PrintWriter out = new PrintWriter(new FileWriter(dcPath));
			//out.println("The set of One tuple Constant DCs: ");
			//for(NewDenialConstraint dc: exchkDCs)
			//{
			//	out.println(dc.interestingness + ":" + dc.toString());
			//}
			//out.println("********************************************");
			out.println("The set of Denial Constraints:");
			for(NewDenialConstraint dc: totalDCs)
			{
				out.println(dc.interestingness + ":" + dc.toString());
			}
			out.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected void writeStats()
	{
	}
	
	protected class PR
	{
		public double precision;
		public double recall;
		
		public PR(double p, double r)
		{
			this.precision = p;
			this.recall = r;
		}
	}
	protected PR getPRTopk(int k,String dcFile, ArrayList<NewDenialConstraint> dcs)
	{
		//Compare golden and sample output
		//Read from golden standard, and read from the current dcs
		String golden = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_Human"));
		//String sample  = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_AfterImp"));
		String sample = dcFile;
		
		
		Set<String> allGoodDCs = new HashSet<String>();
		int count = -1;
		try {
			BufferedReader brg = new BufferedReader(new FileReader(golden));
			String line = null;
			while((line = brg.readLine())!=null)
			{
				if(count == -1)//first line is the head
				{
					count++;
					continue;
				}
				allGoodDCs.add(line.split(":")[1]);
			}
			brg.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Construct dcs from human input
		Set<NewDenialConstraint> allGoodDCs_dcs = new HashSet<NewDenialConstraint>();
		for(String dc: allGoodDCs)
		{
			String[] pres_s = dc.substring(4,dc.length()-1).split("&");
			ArrayList<NewPredicate> dcs_Pres = new ArrayList<NewPredicate>();
			for(String pre_s: pres_s)
			{
				for(NewPredicate pre: presMap)
				{
					if(pre.toString().equals(pre_s))
					{
						dcs_Pres.add(pre);
						break;
					}
					
				}
			}
			allGoodDCs_dcs.add(new NewDenialConstraint(2,dcs_Pres,this));
			
		}
		
		
		/*Set<NewDenialConstraint> allGoodDCs_dcs_sym = new HashSet<NewDenialConstraint>(allGoodDCs_dcs);
		for(NewDenialConstraint dc: allGoodDCs_dcs)
		{
			NewDenialConstraint temp = this.getSymmetricDC(dc);
			allGoodDCs_dcs_sym.add(temp);
		}*/

		int i = 0;
		for (NewDenialConstraint gooddc : allGoodDCs_dcs) {
			for (NewDenialConstraint dc : dcs) {
				if (dc.equals(gooddc)) {
					System.out.println("GOOD DC: " + gooddc.toString());
					i++;
					continue;
				}
			}
		}

		System.out.println("Number of good DCs: " + allGoodDCs_dcs.size());
		System.out.println("Number of good DCs satisfied: " + i);

		return null;

		/*int numGoodInK = 0;
		count = -1;
		try {
			BufferedReader brs = new BufferedReader(new FileReader(sample));
			String line = null;
			while((line = brs.readLine())!=null)
			{
				if(count == -1)//first line is the head
				{
					count++;
					continue;
				}
				if(count < k)
				{
					String cur = line.split(":")[1];
					NewDenialConstraint thisDC = null;
					for(NewDenialConstraint dc: dcs)
					{
						if(dc.toString().equals(cur))
						{
							thisDC = dc;
							break;
						}
					}
					//assert(totalDCs.get(count).toString().equals(cur));
					//if(allGoodDCs.contains(cur))
					if(this.linearImplication(allGoodDCs_dcs_sym, thisDC))
					{
						
						for(NewDenialConstraint dc: allGoodDCs_dcs_sym)
						{
							if(thisDC.getPredicates().containsAll(dc.getPredicates())
									&& dc.getPredicates().size() == thisDC.getPredicates().size())
							{
								
								//System.out.println("K " + k + ":" + thisDC.toString());
								numGoodInK ++;
								break;
							}
						}
						
						
					}
					count++;
				}
				
			}
			brs.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		double p = (double)numGoodInK / k;
		double r = (double)numGoodInK / allGoodDCs.size();
		if(p > 1)
			p = 1;
		if(r > 1)
			r = 1;
		return new PR(p,r  ); 
	}
	
	protected PR getPRTopk(int k,String dcFile)
	{
		//Compare golden and sample output
		//Read from golden standard, and read from the current dcs
		String golden = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_Human"));
		//String sample  = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_AfterImp"));
		String sample = dcFile;
		
		
		Set<String> allGoodDCs = new HashSet<String>();
		int count = -1;
		try {
			BufferedReader brg = new BufferedReader(new FileReader(golden));
			String line = null;
			while((line = brg.readLine())!=null)
			{
				if(count == -1)//first line is the head
				{
					count++;
					continue;
				}
				allGoodDCs.add(line.split(":")[1]);
			}
			brg.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Construct dcs from human input
		Set<NewDenialConstraint> allGoodDCs_dcs = new HashSet<NewDenialConstraint>();
		for(String dc: allGoodDCs)
		{
			String[] pres_s = dc.substring(4,dc.length()-1).split("&");
			ArrayList<NewPredicate> dcs_Pres = new ArrayList<NewPredicate>();
			for(String pre_s: pres_s)
			{
				for(NewPredicate pre: presMap)
				{
					if(pre.toString().equals(pre_s))
					{
						dcs_Pres.add(pre);
						break;
					}
					
				}
			}
			allGoodDCs_dcs.add(new NewDenialConstraint(2,dcs_Pres,this));
			
		}
		
		if(k == Config.grak)
		{
			long hahaha = (long) (originalTable.getNumRows() * originalTable.getNumRows() * Config.noiseTolerance);
			System.out.println("The max number of vios is " + hahaha);
			//System.out.println("The max number of vios is " + originalTable.getNumRows() * 0.002);
			for(NewDenialConstraint dc: allGoodDCs_dcs)
			{
				int numVios = 0;
				for(List<Boolean> info: cons2PairInfo.get(new HashSet<NewPredicate>()).keySet())
				{
					Set<NewPredicate> temp = new HashSet<NewPredicate>(dc.getPredicates());

					Set<NewPredicate> toRemove = new HashSet<>();
					for (NewPredicate p : temp) {
						if (!info.get(presMap.indexOf(p)))
							toRemove.add(p);
					}
					temp.removeAll(toRemove);
					
					//this is a NE
					if(temp.size() == dc.getPredicates().size())
					{
						//count = count - originalTable.getNumRows();
						numVios += cons2PairInfo.get(new HashSet<NewPredicate>()).get(info);
						continue;
					}
					
				}
				System.out.println("The number of vios for " + dc.toString() + " is " + numVios);
				
			}
		}
		
		
		Set<NewDenialConstraint> allGoodDCs_dcs_sym = new HashSet<NewDenialConstraint>(allGoodDCs_dcs);
		for(NewDenialConstraint dc: allGoodDCs_dcs)
		{
			NewDenialConstraint temp = this.getSymmetricDC(dc);
			allGoodDCs_dcs_sym.add(temp);
		}
		
		int numGoodInK = 0;
		count = -1;
		NewDenialConstraint thisDC = null;
		try {
			BufferedReader brs = new BufferedReader(new FileReader(sample));
			String line = null;
			while((line = brs.readLine())!=null)
			{
				if(count == -1)//first line is the head
				{
					count++;
					continue;
				}
				if(count < k)
				{
					String cur = line.split(":")[1];
					for(NewDenialConstraint dc: totalDCs)
					{
						if(dc.toString().equals(cur))
						{
							thisDC = dc;
							break;
						}
					}
					//assert(totalDCs.get(count).toString().equals(cur));
					//if(allGoodDCs.contains(cur))
					if(this.linearImplication(allGoodDCs_dcs_sym, thisDC))
					{
						for(NewDenialConstraint dc: allGoodDCs_dcs_sym)
						{
							if(thisDC.getPredicates().containsAll(dc.getPredicates())
									&& dc.getPredicates().size() == thisDC.getPredicates().size())
							{
								
								System.out.println("K " + k + ":" + thisDC.toString());
								numGoodInK ++;
								break;
							}
						}
						
						//System.out.println("Top" + k + ": " + cur + " : is implied by golden standard");
						//System.out.println("KKKK " + k + ":" + thisDC.toString());
						//numGoodInK ++;
					}
					count++;
				}
				
			}
			brs.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//System.out.println("This DC Exception : "  + thisDC.toString());
			e.printStackTrace();
		}
		
		double p = (double)numGoodInK / k;
		double r = (double)numGoodInK / allGoodDCs.size();
		if(p > 1)
			p = 1;
		if(r > 1)
			r = 1;
		return new PR(p,r  ); */
	}

	
}
