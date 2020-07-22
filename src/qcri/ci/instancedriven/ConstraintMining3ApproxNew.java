package qcri.ci.instancedriven;

import javafx.util.Pair;
import qcri.ci.ConstraintDiscovery3;
import qcri.ci.generaldatastructure.constraints.DenialConstraint;
import qcri.ci.generaldatastructure.constraints.NewDenialConstraint;
import qcri.ci.generaldatastructure.constraints.NewPredicate;
import qcri.ci.utils.Config;
import qcri.ci.utils.IntegerPair;

import javax.swing.text.StyledEditorKit;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Stream;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.stream.Collectors.*;
import static java.util.Map.Entry.*;

public class ConstraintMining3ApproxNew extends ConstraintDiscovery3{

		int positive = 0;
		int negative = 0;

		long totalEvi;
		Map<List<Boolean>,Long> tupleWiseInfo;
		//Map<Set<NewPredicate>,Set<TP>> tupleWiseInfoTP;

		//The followings are parameters
		int initParallel;
		int mvcParallel;
		int dynamicOrdering;
		int function;

		long lastResultTime = 0;
		double avgTimeBetweenResults = 0;

		//The following are program running stats
		public long timeInitTuplePair = 0;
		public long timeGetOrdering = 0;
		public long timeDFS = 0;
		public long timeDFSPruning = 0;
		public long timePostProcessing = 0;


		public ConstraintMining3ApproxNew(String inputDBPath, int initParallel, int mvcParallel, int dynamicOrdering, int numRows, int function) {
			super(inputDBPath, numRows);
			System.out.println("METHOD: DFS SEARCH: initParallel " + initParallel + " mvcParallel " + mvcParallel + " dynamicOrdering: " + dynamicOrdering);
			this.initParallel = initParallel;
			this.mvcParallel = mvcParallel;
			this.dynamicOrdering = dynamicOrdering;
			this.function = function;
		}


		public long getMaxVios()
		{
			//int aa =  (int) (totalEvi * Config.noiseTolerance * Config.noiseTolerance);
			//return (int) Math.sqrt(aa);
			return  ((long)(originalTable.getNumRows()  * Config.noiseTolerance) )
					*  originalTable.getNumRows();
		}

		public long getMaxVios(NewPredicate pre)
		{
			//int aa =  (int) (totalEvi * Config.noiseTolerance * Config.noiseTolerance);
			//return (int) Math.sqrt(aa);
			int[] rows = pre.getRows();
			if(rows[0] == rows[1])
			{


				return  ((long)(originalTable.getNumRows() * originalTable.getNumRows()
						* Config.noiseTolerance))
						* originalTable.getNumRows() ;


			}
			else
			{
				double temp = (originalTable.getNumRows()  * Config.noiseTolerance);
				long result = (long) (temp *  originalTable.getNumRows());
				return  result;
			}

		}
		public long getMaxVios(Set<NewPredicate> pres, NewPredicate pre)
		{
			boolean twoTuple = false;
			for(NewPredicate temp: pres)
			{
				int[] rows = temp.getRows();
				if(rows[0] != rows[1])
				{
					twoTuple = true;
					break;
				}
			}
			if(pre != null)
			{
				int[] rows = pre.getRows();
				if(rows[0] != rows[1])
				{
					twoTuple = true;

				}
			}
			if(twoTuple)
			{
				double temp = (originalTable.getNumRows()  * Config.noiseTolerance);
				long result = (long) (temp *  originalTable.getNumRows());
				return  result;
			}
			else
				return  ((long)(originalTable.getNumRows() * originalTable.getNumRows()
						* Config.noiseTolerance))
						* originalTable.getNumRows() ;
		}


		Map<NewPredicate,Map<Set<NewPredicate>,Long>> eviModPre = new HashMap<NewPredicate,Map<Set<NewPredicate>,Long>>();
		Map<NewPredicate,ArrayList<NewDenialConstraint>> pre2DCs = new HashMap<>();
		Set<NewPredicate> special = new HashSet<NewPredicate>();
		/*private void calculateEviModulePre(ArrayList<NewDenialConstraint> dcs )
		{
			//Step 1: the basics
			for(NewPredicate curPre: allVarPre)
			{
				Map<Set<NewPredicate>,Long> curPreInfoSet = new HashMap<Set<NewPredicate>,Long>();
				for(Set<NewPredicate> info: tupleWiseInfo.keySet())
				{
					if(!info.contains(curPre))
						continue;
					Set<NewPredicate> temp = new HashSet<NewPredicate>(info);
					temp.remove(curPre);
					curPreInfoSet.put(temp, tupleWiseInfo.get(info));
				}

				eviModPre.put(curPre, curPreInfoSet);
				pre2DCs.put(curPre, new ArrayList<NewDenialConstraint>());


				//System.out.println(curPre.toString() +":" + eviModPre.get(curPre).size() );

			}

			//Step 2: Optimization technique
			for(NewPredicate curPre: allVarPre)
			{


				if(eviModPre.containsKey(curPre))
				{
					if(Config.noiseTolerance !=0 )
					{
						long numVios = 0;
						for(long value: eviModPre.get(curPre).values())
						{
							numVios += value;
						}

						if(curPre.toString().equals("t1.Provider Number=t2.Provider Number"))
						{
							long a = eviModPre.get(curPre).size();
							System.out.println(a);
						}

						//System.out.println("The max Vios for " + curPre.toString() + "is: " + getMaxVios(curPre));
						if (numVios <= getMaxVios(curPre))
						{
							System.out.println("Appro NumVios: " + numVios +  curPre.toString());
							//Approximately good
						}
						else
						{
							continue;
						}
						ArrayList<NewPredicate> tempPres = new ArrayList<NewPredicate>();
						tempPres.add(curPre);
						DenialConstraint tempDC = new DenialConstraint(2,tempPres,this);
						if(coverage(tempDC) > 0.8)
						{

						}
						else
						{
							continue;
						}
					}
					else
					{
						if(eviModPre.get(curPre).size() != 0)
							continue;
					}


					if(curPre.getOperator() == 5 || curPre.getOperator() ==3
							//||(curPre.getName().equals("IQ")&& !originalTable.getColumnMapping().positionToType(curPre.getCols()[0]).equals("String"))
							)
						continue;

					//Add a DC for this predicate
					ArrayList<NewPredicate> temp = new ArrayList<NewPredicate>();
					temp.add(curPre);
					NewDenialConstraint dc = new NewDenialConstraint(2, temp,this);
					if(consPres.size() != 0)
					{
						for(NewPredicate consPre: consPres)
							dc.addPreciate(consPre);
					}
					//dcs.add(dc);
					pre2DCs.get(curPre).add(dc);
					super.removeTriviality(pre2DCs.get(curPre));
					super.removeSubset(pre2DCs.get(curPre));
					dcs.addAll(pre2DCs.get(curPre));



					//Get Rid of the unnecessary DFS search
					eviModPre.remove(curPre);
					NewPredicate reverse = super.getReverseNewPredicate(curPre);
					eviModPre.remove(reverse);
					for(NewPredicate reverseImp: super.getImpliedNewPredicates(reverse))
					{
						eviModPre.remove(reverseImp);

						//This is further pruning, to be verified!
						eviModPre.remove(super.getReverseNewPredicate(reverseImp));
						special.add(super.getReverseNewPredicate(reverseImp));

					}

					special.add(reverse);
					special.addAll(super.getImpliedNewPredicates(reverse));

					special.add(curPre);

				}
			}
			System.out.println("The size of the allVarPre is: "  + allVarPre.size());
			System.out.println("The size of special predicates is: " + special.size());
			System.out.println("The size of the keys in eviModPre is: " + eviModPre.size());

		}*/

		@Override
		protected void discoverInternal(ArrayList<NewDenialConstraint> dcs,Map<List<Boolean>,Long> tuplewiseinfo)
		{

			//Step 1: Initialization of tuple pair wise information
			System.out.println( (System.currentTimeMillis() - startingTime )/ 1000 +
					": Done reading DB to memory");
			long time1 = System.currentTimeMillis();

			this.tupleWiseInfo = tuplewiseinfo;
			//this.tupleWiseInfoTP = tuplewiseinfoTP;
			totalEvi = 0;
			for(List<Boolean> temp: tupleWiseInfo.keySet())
			{
				totalEvi += tupleWiseInfo.get(temp);
			}
			timeInitTuplePair = 0;
			timeGetOrdering = 0;
			timeDFS = 0;
			timeDFSPruning = 0;
			timePostProcessing = 0;


			long time2 = System.currentTimeMillis();

			this.timeInitTuplePair += (System.currentTimeMillis() - startingTime) / 1000;;



			if(Config.testScoringFunctionOnly)
			{
				//Read DCs from the file
				String dcs_Before = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_All"));
				try {
					BufferedReader br = new BufferedReader(new FileReader(dcs_Before));
					String line = null;
					int count = -1;
					while((line = br.readLine())!=null)
					{
						if(count == -1)//first line is the head
						{
							count++;
							continue;
						}

						String dc = line.split(":")[1];
						String dcReal = (String) dc.subSequence(4, dc.length()-1);
						String[] presString = dcReal.split("&");
						ArrayList<NewPredicate> presInDC = new ArrayList<NewPredicate>();
						for(NewPredicate pre: presMap)
						{
							for(String preString: presString)
							{
								if(pre.toString().equals(preString))
								{
									presInDC.add(pre);
									break;
								}
							}
						}
						dcs.add(new NewDenialConstraint(2,presInDC,this));
					}
					br.close();


				} catch (Exception e) {
					e.printStackTrace();
				}
				postprocess(dcs);
				return;
			}




			if(this.mvcParallel == 4)
			{
				//this.calculateEviModulePre(dcs);

				//discoverInternal_Opt2(dcs);
			}
			else
			{
				//Step 2: Calculate the evidence module each predicate
				//this.calculateEviModulePre(dcs);


				//Step 3: Do a DFS search for each predicate
				if(this.mvcParallel == 3)
					lastResultTime = System.currentTimeMillis();
					generateDCs(dcs);
			}




			long time3 = System.currentTimeMillis();
			this.timeDFS += (time3 - time2);


			//Print out some stats
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": We have " + dcs.size() + " after all dcs are added");
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": Time Init Tuple Pair: " + timeInitTuplePair);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": Time DFS: " + timeDFS);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": Time DFSPruning: " + timeDFSPruning);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": Time get Ordering: " + timeGetOrdering);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": CountPruningInDFS--Triviality Pruning: " + countPruningTrivial);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": CountPruningInDFS--Subset Pruning: " + countPruningInDFS1);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": CountPruningInDFS--Transitive Pruning: " + countPruningInDFS2);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": CountPruningInDFS--Subset Pruning across DFS tree: " + countPruningInDFS3);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": WastedWork--Nonminimal DCs " + wastedWork);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": WastedWork2--Nonminimal DCs " + wastedWork2);


			getPRTopk(0,"", dcs);

			postprocess(dcs);


		}

	private boolean updateCritUncov( Integer currPred, Set<Integer> currSet, Map<Integer, Set<Long>> newCrit, Map<Integer, Long> newEvidenceForPred) {
		for (Integer p : crit.keySet()) {
			newCrit.put(p, new HashSet<>(crit.get(p)));
		}

		Set<Long> setsToRemove = new HashSet<>();
		Set<Long> uncovToRemove = new HashSet<>();

		//Set<Long> newCovered = new HashSet<>(evidenceForPred.get(currPred));

		/*for ( Long evidenceSet : evidenceForPred.get(currPred) ) {
			if (!uncovered.contains(evidenceSet)) {
				setsToRemove.add(evidenceSet);
			}
		}*/

		//for (Integer p : currSet) {
		//	newCrit.get(p).removeAll(newCovered);
		//	if (newCrit.get(p).isEmpty())
		//		return false;
		//}

		//newCovered.retainAll(uncovered);
		//uncovered.removeAll(newCovered);

		Set<Long> newCovered = evidenceForPred.get(currPred);

		// update crit
		for (Integer p : currSet) {
			newCrit.get(p).removeAll(newCovered);
			if (newCrit.get(p).isEmpty())
				return false;
		}

		// update uncovered
		for ( Long evidenceSet : evidenceForPred.get(currPred) ) {
			if (uncovered.contains(evidenceSet)) {
				uncovToRemove.add(evidenceSet);
			}
			//else {
			//	setsToRemove.add(evidenceSet);
			//}
		}

		uncovered.removeAll(uncovToRemove);
		newCrit.put(currPred, new HashSet<>());
		newCrit.get(currPred).addAll(uncovToRemove);

		for (Integer p : evidenceForPredRemaining.keySet())
			newEvidenceForPred.put(p, evidenceForPredRemaining.get(p));

		Set<Integer> keys = new HashSet<>(newEvidenceForPred.keySet());
		for (Integer p : keys) {
			for (Long l : uncovToRemove) {
				if (evidenceForPred.get(p).contains(l)) {
					newEvidenceForPred.put(p, newEvidenceForPred.get(p)-1);
				}
			}

			if (newEvidenceForPred.get(p) < 1)
				newEvidenceForPred.remove(p);
		}

		return true;
	}

	private boolean checkNewHS(NewPredicate oldPred, NewPredicate newPred1, NewPredicate newPred2) {
		Set<Long> critical = crit.get(oldPred);

		Set<Long> evidence1 = evidenceForPred.get(newPred1);
		Set<Long> evidence2 = evidenceForPred.get(newPred2);

		if (evidence1 == null && evidence2 == null) {
			return false;
		}

		boolean result = true;

		if (evidence1 != null) {
			for (Long set : critical) {
				if (!evidence1.contains(set)) {
					result = false;
					break;
				}
			}

			if (result) {
				return true;
			}
		}

		result = true;

		if (evidence2 != null) {
			for (Long set : critical) {
				if (!evidence2.contains(set)) {
					result = false;
					break;
				}
			}
		}

		return result;
	}

	private Long findMaxSAT(boolean[] maxSat) {
		Long max = Long.MIN_VALUE;
		Long setRes = -1L;

		for (Long set : uncovered) {
			if (cannotBeCovered.contains(set))
				continue;

			Long num = 0L;

			boolean[] ev = new boolean[presMap.size()];
			List<Boolean> curEv = evidences.get(set);
			for (int i=0;i<ev.length;i++) {
				ev[i] = curEv.get(i);
			}

			for (int p = 0; p < presMap.size();p++) {
				if (ev[p] && candidates[p]) {
					if (evidenceForPredRemaining.containsKey(p))
						num = num + evidenceForPredRemaining.get(p);
					//num = num + 1;
				}
			}

			if (num > max) {
				max = num;
				for (int i=0;i<ev.length;i++)
					maxSat[i] = ev[i];
				setRes = set;
			}
		}

		return setRes;
	}

	private Long findMinSAT(boolean[] minSAT) {
		int min = Integer.MAX_VALUE;
		Long setRes = -1L;

		for (Long set : uncovered) {
			if (cannotBeCovered.contains(set))
				continue;

			int num = 0;

			boolean[] ev = new boolean[presMap.size()];
			List<Boolean> curEv = evidences.get(set);
			for (int i=0;i<ev.length;i++) {
				ev[i] = curEv.get(i);
			}

			for (int p = 0; p < presMap.size();p++) {
				if (ev[p] && candidates[p]) {
					num++;
				}
			}

			if (num < min) {
				min = num;
				for (int i=0;i<ev.length;i++)
					minSAT[i] = ev[i];
				setRes = set;
			}
		}

		return setRes;
	}

	/*private Long findMinSAT(boolean[] minSAT) {
		while (uncovered.iterator().hasNext()) {
			Long set = uncovered.iterator().next();
			if (!canBeCovered.get(set))
				continue;

			for (Integer p : evidenceForPred.keySet()) {
				if (evidenceForPred.get(p).contains(set))
					minSAT[p] = true;
			}

			return set;
		}

		return 0L;
	}*/

	private Map<Integer,Set<Integer>> sameAttMap = new HashMap<>();
	public Set<Integer> getNewPredicatesSameAtt(Integer pre)
	{
		if(pre == null)
			return new HashSet<>();

		if(!sameAttMap.containsKey(pre))
		{
			Set<Integer> result = new HashSet<>();
			for(NewPredicate temp: presMap)
			{
				if (Arrays.equals(temp.getCols(), presMap.get(pre).getCols())) {
					result.add(presMap.indexOf(temp));
				}
			}
			sameAttMap.put(pre, result);
			return result;
		}
		else
			return sameAttMap.get(pre);
	}

	private boolean isCovered1() {
		long uncoveredSize = 0L;
		for (Long l : uncovered) {
			uncoveredSize = uncoveredSize + evidenceSet.get(l);
		}

		return ((double)(uncoveredSize) / (double)evidenceNum) <= Config.noiseTolerance;
	}

	private boolean isCovered1(Set<Integer> curr, Integer removedPred) {
		Set<Long> notLongerCovered = crit.get(removedPred);
		Set<Long> tempUncovered = new HashSet<>(uncovered);
		tempUncovered.addAll(notLongerCovered);

		long uncoveredSize = 0L;
		for (Long l : tempUncovered) {
			uncoveredSize = uncoveredSize + evidenceSet.get(l);
		}

		return ((double)(uncoveredSize) / (double)evidenceNum) <= Config.noiseTolerance;
		/*Set<Long> tempCovered = new HashSet<>();
		for (Integer p : curr) {
			tempCovered.addAll(evidenceForPred.get(p));
		}

		Set<Long> tempUncovered = new HashSet<>(evidenceSet.keySet());
		tempUncovered.removeAll(tempCovered);

		long uncoveredSize = 0L;
		for (Long l : tempUncovered) {
			uncoveredSize = uncoveredSize + evidenceSet.get(l);
		}

		return ((double)(uncoveredSize) / (double)evidenceNum) < 0.1;*/
	}

	private boolean noPredicateToCover(Long l) {
		for (Integer p : evidenceForPred.keySet()) {
			if (candidates[p] && evidenceForPred.get(p).contains(l))
				return false;
		}

		return true;
	}

	private boolean isCovered1Mid() {
		Long uncoveredSize = 0L;
		for (Long l : uncovered) {
			if (cannotBeCovered.contains(l))
				uncoveredSize = uncoveredSize + evidenceSet.get(l);
		}

		return ((double)(uncoveredSize) / (double)evidenceNum) <= Config.noiseTolerance;
	}

	private boolean isCovered2Mid() {
		Set<Integer> problematic = new HashSet<>();

		for (Long l : uncovered) {
			if (cannotBeCovered.contains(l)) {
				for (Integer p : evidenceToPairs.get(l).keySet()) {
					problematic.add(p);
				}
			}
		}

		return ((double)(problematic.size()) / (double)originalTable.getNumRows()) <= Config.noiseTolerance;
	}


	private boolean isCovered2() {
		Set<Integer> problematic = new HashSet<>();

		for (Long l : uncovered) {
			for (Integer p : evidenceToPairs.get(l).keySet()) {
				problematic.add(p);
			}
		}

		return ((double)(problematic.size()) / (double)originalTable.getNumRows()) <= Config.noiseTolerance;
	}

	private boolean isCovered2(Set<Integer> curr, Integer removedPred) {
		Set<Long> notLongerCovered = crit.get(removedPred);
		Set<Long> tempUncovered = new HashSet<>(uncovered);
		tempUncovered.addAll(notLongerCovered);

		Set<Integer> problematic = new HashSet<>();

		for (Long l : tempUncovered) {
			for (Integer p : evidenceToPairs.get(l).keySet()) {
				problematic.add(p);
			}
		}

		return ((double)(problematic.size()) / (double)originalTable.getNumRows()) <= Config.noiseTolerance;
	}

	private boolean isCovered4() {
		Map<Integer, Integer> problematic = new HashMap<>();
		Long totalVio = new Long(0);

		for (Long l : uncovered) {
			totalVio += evidenceSet.get(l);
		}

		for (Long l : uncovered) {
			for (Integer p : evidenceToPairs.get(l).keySet()) {
				if (problematic.containsKey(p)) {
					Integer old = problematic.get(p);
					problematic.put(p, old + evidenceToPairs.get(l).get(p));
				}
				else {
					problematic.put(p,evidenceToPairs.get(l).get(p));
				}
			}
		}

		Map<Integer,Integer> sorted = problematic
        .entrySet()
        .stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .collect(
            toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                LinkedHashMap::new));

		Long resolved = new Long(0);
		Set<Integer> result = new HashSet<>();
		for (Integer p : sorted.keySet()) {
			resolved += problematic.get(p) - 2*result.size();
			result.add(p);
			if (resolved >= totalVio)
				break;
		}

		return ((double)(result.size()) / (double)originalTable.getNumRows()) <= Config.noiseTolerance;
	}

		private boolean isCovered4Mid() {
		Map<Integer, Integer> problematic = new HashMap<>();
		Long totalVio = new Long(0);

		for (Long l : uncovered) {
			if (cannotBeCovered.contains(l)) {
				totalVio += evidenceSet.get(l);
			}
		}

		for (Long l : uncovered) {
			if (cannotBeCovered.contains(l)) {
				for (Integer p : evidenceToPairs.get(l).keySet()) {
				if (problematic.containsKey(p)) {
					Integer old = problematic.get(p);
					problematic.put(p, old + evidenceToPairs.get(l).get(p));
				}
				else {
					problematic.put(p,evidenceToPairs.get(l).get(p));
				}
			}
			}
		}

		Map<Integer,Integer> sorted = problematic
        .entrySet()
        .stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .collect(
            toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                LinkedHashMap::new));

		Long resolved = new Long(0);
		Set<Integer> result = new HashSet<>();
		for (Integer p : sorted.keySet()) {
			resolved += problematic.get(p) - 2*result.size();
			result.add(p);
			if (resolved >= totalVio)
				break;
		}

		return ((double)(result.size()) / (double)originalTable.getNumRows()) <= Config.noiseTolerance;
	}

	private boolean isCovered4(Set<Integer> curr, Integer removedPred) {
		Set<Long> notLongerCovered = crit.get(removedPred);
		Set<Long> tempUncovered = new HashSet<>(uncovered);
		tempUncovered.addAll(notLongerCovered);

		Map<Integer, Integer> problematic = new HashMap<>();
		Long totalVio = new Long(0);

		for (Long l : tempUncovered) {
			totalVio += evidenceSet.get(l);
		}

		for (Long l : tempUncovered) {
			for (Integer p : evidenceToPairs.get(l).keySet()) {
				if (problematic.containsKey(p)) {
					Integer old = problematic.get(p);
					problematic.put(p, old + evidenceToPairs.get(l).get(p));
				}
				else {
					problematic.put(p,evidenceToPairs.get(l).get(p));
				}
			}
		}

		Map<Integer,Integer> sorted = problematic
        .entrySet()
        .stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .collect(
            toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                LinkedHashMap::new));

		Long resolved = new Long(0);
		Set<Integer> result = new HashSet<>();
		for (Integer p : sorted.keySet()) {
			resolved += problematic.get(p) - 2*result.size();
			result.add(p);
			if (resolved >= totalVio)
				break;
		}

		return ((double)(result.size()) / (double)originalTable.getNumRows()) <= Config.noiseTolerance;
	}


	private boolean isCovered3() {
		Set<Integer> minCover = new HashSet<>();
		Set<Integer> edges = new HashSet<>();

		for (Long l : uncovered) {
				for (Integer p : evidenceEdges.get(evidences.get(l))) {
					edges.add(p);
			}
		}

		int numRows = originalTable.getNumRows();
		int next;
		int t1;
		int t2;
		Boolean continueLoop = true;
		while (continueLoop) {
			next = edges.iterator().next();
			t1 = next/numRows;
			t2 = next - t1*numRows;
			if (minCover.contains(t1) || minCover.contains(t2)) {
				edges.remove(next);
				if (edges.isEmpty())
					continueLoop = false;
				continue;
			}

			minCover.add(t1);
			minCover.add(t2);
		}

		return ((double)(minCover.size()) / (double)originalTable.getNumRows()) <= 2*Config.noiseTolerance;
	}

	private boolean isCovered3Mid() {
		Set<Integer> minCover = new HashSet<>();
		Set<Integer> edges = new HashSet<>();

		for (Long l : uncovered) {
			if (cannotBeCovered.contains(l)) {
				for (Integer p : evidenceEdges.get(evidences.get(l))) {
					edges.add(p);
				}
			}
		}

		int numRows = originalTable.getNumRows();
		int next;
		int t1;
		int t2;
		Boolean continueLoop = true;
		while (continueLoop) {
			next = edges.iterator().next();
			t1 = next/numRows;
			t2 = next - t1*numRows;
			if (minCover.contains(t1) || minCover.contains(t2)) {
				edges.remove(next);
				if (edges.isEmpty())
					continueLoop = false;
				continue;
			}

			minCover.add(t1);
			minCover.add(t2);
		}

		return ((double)(minCover.size()) / (double)originalTable.getNumRows()) <= 2*Config.noiseTolerance;
	}

		private boolean isCovered3(Set<Integer> curr, Integer removedPred) {
		Set<Integer> minCover = new HashSet<>();
		Set<Integer> edges = new HashSet<>();

		Set<Long> notLongerCovered = crit.get(removedPred);
		Set<Long> tempUncovered = new HashSet<>(uncovered);
		tempUncovered.addAll(notLongerCovered);

		Set<Integer> problematic = new HashSet<>();

		for (Long l : tempUncovered) {
			for (Integer p : evidenceEdges.get(evidences.get(l))) {
				edges.add(p);
			}
		}

		int numRows = originalTable.getNumRows();
		int next;
		int t1;
		int t2;
		Boolean continueLoop = true;
		while (continueLoop) {
			next = edges.iterator().next();
			t1 = next/numRows;
			t2 = next - t1*numRows;
			if (minCover.contains(t1) || minCover.contains(t2)) {
				edges.remove(next);
				if (edges.isEmpty())
					continueLoop = false;
				continue;
			}

			minCover.add(t1);
			minCover.add(t2);
		}

		return ((double)(minCover.size()) / (double)originalTable.getNumRows()) <= 2*Config.noiseTolerance;
	}
	/*private boolean isCovered2(Set<NewPredicate> mhs) {
		int mhsSize = mhs.size();

		if (mhsSize == 0) {
			return evidenceNum == 0L;
		}

		Long uncoveredSize = 0L;
		for (Long l : uncovered) {
			uncoveredSize = uncoveredSize + evidenceSet.get(l);
		}

		if (mhsSize == 1) {
			return ((double)(evidenceNum -uncoveredSize) / (double)evidenceNum) >= 0.999999;
		}

		Map<Integer, List<Pair<Set<Long>, Integer>>> union = new HashMap();
		union.put(1, new ArrayList<>());

		int index = 0;
		for (NewPredicate p : mhs) {
			union.get(1).add(new Pair<Set<Long>,Integer>(evidenceForPred.get(p), index));
			index++;
		}

		for (int k=2; k < mhsSize; k++) {

			union.put(k, new ArrayList<>());
			int currSize = union.get(k-1).size();

			for (int i = 0; i < currSize; i++) {
				Set<Long> first = union.get(k-1).get(i).getKey();
				int from = union.get(k-1).get(i).getValue();

				for (int j = from+1; j < currSize; j++) {

					Pair<List<Long>, Integer> result = new Pair<>(new ArrayList<>(), j);
					Set<Long> second = union.get(1).get(j).getKey();

					int ai = 0, bi = 0, ci = 0;
					while (ai < first.size() && bi < second.size()) {
						if (first.get(ai) < second.get(bi)) {
							ai++;
						}
						else if (first.get(ai) > second.get(bi)) {
							bi++;
						}
						else {
							result.getKey().add(first.get(ai));
							ci++;
							ai++; bi++;
						}
					}

					union.get(k).add(result);
				}
			}
		}

		List<List<Long>> finalResults = new ArrayList<>();
		for (Pair<List<Long>, Integer> pair : union.get(mhsSize-1)) {
			finalResults.add(pair.getKey());
		}

		Set<Long> result = new HashSet<>();
		for (List<Long> list : finalResults) {
			result.addAll(list);
		}

		Long numOfCovers = 0L;
		for (Long l : result) {
			numOfCovers = numOfCovers + evidenceSet.get(l);
		}

 		return ((double)(evidenceNum - uncoveredSize) / (double)numOfCovers) >= 0.999999;
	}*/

	private NewPredicate getSymmetricNewPredicate(NewPredicate pre)
	{
		if(pre.getOperator() == 0 || pre.getOperator() == 1)
			return pre;

		for(NewPredicate temp: presMap)
		{
			if(pre.equalExceptOp(temp))
			{
				if(pre.getOperator() == 2 && temp.getOperator() == 4)
					return temp;
				if(pre.getOperator() == 4 && temp.getOperator() == 2)
					return temp;
				if(pre.getOperator() == 5 && temp.getOperator() == 3)
					return temp;
				if(pre.getOperator() == 3 && temp.getOperator() == 5)
					return temp;
			}
		}

		return null;
	}

	private boolean areSymmetric(NewDenialConstraint dc1, NewDenialConstraint dc2) {
		ArrayList<NewPredicate> pres1 = dc1.getPredicates();
		ArrayList<NewPredicate> pres2 = dc2.getPredicates();

		if(pres1.size() != pres2.size())
			return false;

		for(int j = 0 ; j < pres1.size(); j++)
		{
			NewPredicate pre1 = pres1.get(j);

			NewPredicate symPre1 = this.getSymmetricNewPredicate(pre1);
			if(!pres2.contains(symPre1))
			{
				return false;
			}
		}

		return true;
	}

	private boolean isSymmetricToOtherDC(List<NewDenialConstraint> dcs, NewDenialConstraint dc) {
		for (NewDenialConstraint dc2 : dcs) {
			if (areSymmetric(dc, dc2))
				return true;
		}

		return false;
	}

	private void updateUncovered(boolean[] curr) {
		Set<Long> noCover = new HashSet<>();
		outerloop:
		for (Long set : uncovered) {
			if (cannotBeCovered.contains(set))
				continue;

			for (int i = 0; i < candidates.length; i++) {
				if (candidates[i] && evidenceForPred.get(i).contains(set))
					continue outerloop;
			}

			noCover.add(set);
		}

		cannotBeCovered.addAll(noCover);

		for (int i=0;i<candidates.length;i++) {
			if (!candidates[i])
				evidenceForPredRemaining.remove(i);
		}
	}

	private void generateMHSF1(ArrayList<NewDenialConstraint> dcs, Set<Integer> curr ) {
		numOfIterations++;
		if (isCovered1()) { // all sets are covered - minimal hitting set ####################################
			//numOfSuccessfulIterations++;
			/*if (isSymmetricToOtherDC(dcs, currDC)) {
				return;
			}*/

			// check if minimal
			Set<Integer> temp = new HashSet<>(curr);
			for (Integer p : curr) {
				temp.remove(p);
				if (isCovered1(temp,p)) //#######################################
					return;
				temp.add(p);
			}

			avgTimeBetweenResults = avgTimeBetweenResults + (double) ((System.currentTimeMillis() - lastResultTime)) / (double) 1000;
			lastResultTime = System.currentTimeMillis();

			ArrayList<NewPredicate> reversedMVC = new ArrayList<>();
			for (Integer pre : curr) {
				NewPredicate rPre = getReverseNewPredicate(pre);
				reversedMVC.add(rPre);
			}
			NewDenialConstraint currDC = new NewDenialConstraint(2, reversedMVC);

			dcs.add(currDC);
			//totalDCs.add(currDC);

			if (currDC.getPredicates().size() > maxDCsize)
				maxDCsize = currDC.getPredicates().size();
			if (currDC.getPredicates().size() < minDCsize)
				minDCsize = currDC.getPredicates().size();
			avgDCsize += currDC.getPredicates().size();

			return;
		}

		if (curr.size() == 3)
			return;

		//else
		//iterResults.add(new HashSet<>(curr));

		boolean[] oldCandidates = Arrays.copyOf(candidates, presMap.size());

		boolean[] uncovSet = new boolean[presMap.size()];
		Long uncovSetIndex = findMaxSAT(uncovSet);
		if (uncovSetIndex == -1)
			return;

		// option one - do not cover this set
		Set<Long> cannotBeCoveredOld = new HashSet<>();
		for (Long l : cannotBeCovered) {
			cannotBeCoveredOld.add(l);
		}

		/*Map<Integer, Long> evidenceForPreRemOld = new HashMap<>();
		for (Integer i : evidenceForPredRemaining.keySet()) {
			evidenceForPreRemOld.put(i, evidenceForPredRemaining.get(i));
		}*/

		cannotBeCovered.add(uncovSetIndex);
		for (int i = 0; i < uncovSet.length; i++) {
			if (uncovSet[i])
				candidates[i] = false;

		}
		updateUncovered(uncovSet);
		if (isCovered1Mid()) { //################################################
			negative++;
			generateMHSF1(dcs, curr);
		}

		// option 2 - cover this set
		candidates = Arrays.copyOf(oldCandidates, presMap.size());
		cannotBeCovered = new HashSet<>();
		for (Long l : cannotBeCoveredOld) {
			cannotBeCovered.add(l);
		}
		/*evidenceForPredRemaining = new HashMap<>();
		for (Integer i : evidenceForPreRemOld.keySet()) {
			evidenceForPredRemaining.put(i, evidenceForPreRemOld.get(i));
		}*/

		Set<Integer> currCand = new HashSet<>(); // currCand - all candidates that belong to uncovSet
		for (int pred = 0; pred < presMap.size(); pred++) {
			if (uncovSet[pred] && candidates[pred]) {
				currCand.add(pred);
				candidates[pred] = false; // candidates = candidates \ currCand
			}
		}

		Set<Long> uncoveredOld = new HashSet<>(uncovered);
		Map<Integer, Set<Long>> critOld;
		Map<Integer, Long> evidenceForPredOld;

		for (Integer pred : currCand) { // try to add to the current set each element of CurrCand
			Map<Integer, Set<Long>> newCrit = new HashMap<>();
			Map<Integer, Long> newEvidenceForPred = new HashMap<>();
			boolean critResult = updateCritUncov(pred, curr, newCrit, newEvidenceForPred);

			if (critResult) {
				critOld = crit;
				evidenceForPredOld = evidenceForPredRemaining;

				crit = newCrit;
				evidenceForPredRemaining = newEvidenceForPred;

				Set<Integer> candPredToRemove = getNewPredicatesSameAtt(pred);
				Set<Integer> predToRemove = new HashSet<>();
				for (Integer p : candPredToRemove) {
					if (candidates[p]) {
						predToRemove.add(p);
					}
				}
				for (Integer i : predToRemove)
					candidates[i] = false;
				curr.add(pred); // recursive call with the new set containting the current predicate

				// check if the current candidates are enough to cover the rest of the sets
				/*Long needToCover = (long)0;
				Long numCovers = (long)0;
				for (Long set : uncovered) {
					boolean canCover = false;
					needToCover = needToCover + evidenceSet.get(set);

					for (int i=0; i < candidates.length;i++) {
						if (!candidates[i])
							continue;

						if (evidenceForPred.get(i).contains(set)) {
							canCover = true;
							break;
						}
					}

					if (canCover) {
						numCovers = numCovers + evidenceSet.get(set);
					}
				}

				if (numCovers.equals(needToCover))*/
				//if (isCovered1Mid()) {

					generateMHSF1(dcs, curr);
					positive++;
				//}
				//else {
				//	System.out.println("aa");
				//}
				//generateMHS(dcs, curr);

				/*boolean willCover = true;
				for (Long set : uncovered) {
					boolean canCover = false;

					for (int i=0; i < candidates.length;i++) {
						if (!candidates[i])
							continue;

						if (evidenceForPred.get(i).contains(set)) {
							canCover = true;
							break;
						}
					}

					if (!canCover) {
						willCover = false;
						break;
					}
				}

				if (willCover)
					generateMHS( dcs, curr );*/

				for (Integer i : predToRemove)
					candidates[i] = true;
				candidates[pred] = true;
				curr.remove(pred);

				uncovered = new HashSet<>(uncoveredOld);
				crit = critOld;
				evidenceForPredRemaining = evidenceForPredOld;
			}
		}

			candidates = Arrays.copyOf(oldCandidates, presMap.size());
	}

		private void generateMHSF2(ArrayList<NewDenialConstraint> dcs, Set<Integer> curr ) {
		numOfIterations++;
		if (isCovered2()) { // all sets are covered - minimal hitting set ####################################
			//numOfSuccessfulIterations++;
			/*if (isSymmetricToOtherDC(dcs, currDC)) {
				return;
			}*/

			// check if minimal
			Set<Integer> temp = new HashSet<>(curr);
			for (Integer p : curr) {
				temp.remove(p);
				if (isCovered2(temp,p)) //#######################################
					return;
				temp.add(p);
			}

			avgTimeBetweenResults = avgTimeBetweenResults + (double) ((System.currentTimeMillis() - lastResultTime)) / (double) 1000;
			lastResultTime = System.currentTimeMillis();

			ArrayList<NewPredicate> reversedMVC = new ArrayList<>();
			for (Integer pre : curr) {
				NewPredicate rPre = getReverseNewPredicate(pre);
				reversedMVC.add(rPre);
			}
			NewDenialConstraint currDC = new NewDenialConstraint(2, reversedMVC);

			dcs.add(currDC);
			//totalDCs.add(currDC);

			if (currDC.getPredicates().size() > maxDCsize)
				maxDCsize = currDC.getPredicates().size();
			if (currDC.getPredicates().size() < minDCsize)
				minDCsize = currDC.getPredicates().size();
			avgDCsize += currDC.getPredicates().size();

			return;
		}

		if (curr.size() == 3)
			return;

		//else
		//iterResults.add(new HashSet<>(curr));

		boolean[] oldCandidates = Arrays.copyOf(candidates, presMap.size());

		boolean[] uncovSet = new boolean[presMap.size()];
		Long uncovSetIndex = findMaxSAT(uncovSet);
		if (uncovSetIndex == -1)
			return;

		// option one - do not cover this set
		Set<Long> cannotBeCoveredOld = new HashSet<>();
		for (Long l : cannotBeCovered) {
			cannotBeCoveredOld.add(l);
		}

		/*Map<Integer, Long> evidenceForPreRemOld = new HashMap<>();
		for (Integer i : evidenceForPredRemaining.keySet()) {
			evidenceForPreRemOld.put(i, evidenceForPredRemaining.get(i));
		}*/

		cannotBeCovered.add(uncovSetIndex);
		for (int i = 0; i < uncovSet.length; i++) {
			if (uncovSet[i])
				candidates[i] = false;

		}
		updateUncovered(uncovSet);
		if (isCovered2Mid()) { //################################################
			negative++;
			generateMHSF1(dcs, curr);
		}

		// option 2 - cover this set
		candidates = Arrays.copyOf(oldCandidates, presMap.size());
		cannotBeCovered = new HashSet<>();
		for (Long l : cannotBeCoveredOld) {
			cannotBeCovered.add(l);
		}
		/*evidenceForPredRemaining = new HashMap<>();
		for (Integer i : evidenceForPreRemOld.keySet()) {
			evidenceForPredRemaining.put(i, evidenceForPreRemOld.get(i));
		}*/

		Set<Integer> currCand = new HashSet<>(); // currCand - all candidates that belong to uncovSet
		for (int pred = 0; pred < presMap.size(); pred++) {
			if (uncovSet[pred] && candidates[pred]) {
				currCand.add(pred);
				candidates[pred] = false; // candidates = candidates \ currCand
			}
		}

		Set<Long> uncoveredOld = new HashSet<>(uncovered);
		Map<Integer, Set<Long>> critOld;
		Map<Integer, Long> evidenceForPredOld;

		for (Integer pred : currCand) { // try to add to the current set each element of CurrCand
			Map<Integer, Set<Long>> newCrit = new HashMap<>();
			Map<Integer, Long> newEvidenceForPred = new HashMap<>();
			boolean critResult = updateCritUncov(pred, curr, newCrit, newEvidenceForPred);

			if (critResult) {
				critOld = crit;
				evidenceForPredOld = evidenceForPredRemaining;

				crit = newCrit;
				evidenceForPredRemaining = newEvidenceForPred;

				Set<Integer> candPredToRemove = getNewPredicatesSameAtt(pred);
				Set<Integer> predToRemove = new HashSet<>();
				for (Integer p : candPredToRemove) {
					if (candidates[p]) {
						predToRemove.add(p);
					}
				}
				for (Integer i : predToRemove)
					candidates[i] = false;
				curr.add(pred); // recursive call with the new set containting the current predicate

				// check if the current candidates are enough to cover the rest of the sets
				/*Long needToCover = (long)0;
				Long numCovers = (long)0;
				for (Long set : uncovered) {
					boolean canCover = false;
					needToCover = needToCover + evidenceSet.get(set);

					for (int i=0; i < candidates.length;i++) {
						if (!candidates[i])
							continue;

						if (evidenceForPred.get(i).contains(set)) {
							canCover = true;
							break;
						}
					}

					if (canCover) {
						numCovers = numCovers + evidenceSet.get(set);
					}
				}

				if (numCovers.equals(needToCover))*/
				//if (isCovered1Mid()) {

					generateMHSF1(dcs, curr);
					positive++;
				//}
				//else {
				//	System.out.println("aa");
				//}
				//generateMHS(dcs, curr);

				/*boolean willCover = true;
				for (Long set : uncovered) {
					boolean canCover = false;

					for (int i=0; i < candidates.length;i++) {
						if (!candidates[i])
							continue;

						if (evidenceForPred.get(i).contains(set)) {
							canCover = true;
							break;
						}
					}

					if (!canCover) {
						willCover = false;
						break;
					}
				}

				if (willCover)
					generateMHS( dcs, curr );*/

				for (Integer i : predToRemove)
					candidates[i] = true;
				candidates[pred] = true;
				curr.remove(pred);

				uncovered = new HashSet<>(uncoveredOld);
				crit = critOld;
				evidenceForPredRemaining = evidenceForPredOld;
			}
		}

			candidates = Arrays.copyOf(oldCandidates, presMap.size());
	}

		private void generateMHSF3(ArrayList<NewDenialConstraint> dcs, Set<Integer> curr ) {
		numOfIterations++;
		if (isCovered4()) { // all sets are covered - minimal hitting set ####################################
			//numOfSuccessfulIterations++;
			/*if (isSymmetricToOtherDC(dcs, currDC)) {
				return;
			}*/

			// check if minimal
			Set<Integer> temp = new HashSet<>(curr);
			for (Integer p : curr) {
				temp.remove(p);
				if (isCovered4(temp,p)) //#######################################
					return;
				temp.add(p);
			}

			avgTimeBetweenResults = avgTimeBetweenResults + (double) ((System.currentTimeMillis() - lastResultTime)) / (double) 1000;
			lastResultTime = System.currentTimeMillis();

			ArrayList<NewPredicate> reversedMVC = new ArrayList<>();
			for (Integer pre : curr) {
				NewPredicate rPre = getReverseNewPredicate(pre);
				reversedMVC.add(rPre);
			}
			NewDenialConstraint currDC = new NewDenialConstraint(2, reversedMVC);

			dcs.add(currDC);
			//totalDCs.add(currDC);

			if (currDC.getPredicates().size() > maxDCsize)
				maxDCsize = currDC.getPredicates().size();
			if (currDC.getPredicates().size() < minDCsize)
				minDCsize = currDC.getPredicates().size();
			avgDCsize += currDC.getPredicates().size();

			return;
		}

		if (curr.size() == 3)
			return;

		//else
		//iterResults.add(new HashSet<>(curr));

		boolean[] oldCandidates = Arrays.copyOf(candidates, presMap.size());

		boolean[] uncovSet = new boolean[presMap.size()];
		Long uncovSetIndex = findMaxSAT(uncovSet);
		if (uncovSetIndex == -1)
			return;

		// option one - do not cover this set
		Set<Long> cannotBeCoveredOld = new HashSet<>();
		for (Long l : cannotBeCovered) {
			cannotBeCoveredOld.add(l);
		}

		/*Map<Integer, Long> evidenceForPreRemOld = new HashMap<>();
		for (Integer i : evidenceForPredRemaining.keySet()) {
			evidenceForPreRemOld.put(i, evidenceForPredRemaining.get(i));
		}*/

		cannotBeCovered.add(uncovSetIndex);
		for (int i = 0; i < uncovSet.length; i++) {
			if (uncovSet[i])
				candidates[i] = false;

		}
		updateUncovered(uncovSet);
		if (isCovered4Mid()) { //################################################
			negative++;
			generateMHSF1(dcs, curr);
		}

		// option 2 - cover this set
		candidates = Arrays.copyOf(oldCandidates, presMap.size());
		cannotBeCovered = new HashSet<>();
		for (Long l : cannotBeCoveredOld) {
			cannotBeCovered.add(l);
		}
		/*evidenceForPredRemaining = new HashMap<>();
		for (Integer i : evidenceForPreRemOld.keySet()) {
			evidenceForPredRemaining.put(i, evidenceForPreRemOld.get(i));
		}*/

		Set<Integer> currCand = new HashSet<>(); // currCand - all candidates that belong to uncovSet
		for (int pred = 0; pred < presMap.size(); pred++) {
			if (uncovSet[pred] && candidates[pred]) {
				currCand.add(pred);
				candidates[pred] = false; // candidates = candidates \ currCand
			}
		}

		Set<Long> uncoveredOld = new HashSet<>(uncovered);
		Map<Integer, Set<Long>> critOld;
		Map<Integer, Long> evidenceForPredOld;

		for (Integer pred : currCand) { // try to add to the current set each element of CurrCand
			Map<Integer, Set<Long>> newCrit = new HashMap<>();
			Map<Integer, Long> newEvidenceForPred = new HashMap<>();
			boolean critResult = updateCritUncov(pred, curr, newCrit, newEvidenceForPred);

			if (critResult) {
				critOld = crit;
				evidenceForPredOld = evidenceForPredRemaining;

				crit = newCrit;
				evidenceForPredRemaining = newEvidenceForPred;

				Set<Integer> candPredToRemove = getNewPredicatesSameAtt(pred);
				Set<Integer> predToRemove = new HashSet<>();
				for (Integer p : candPredToRemove) {
					if (candidates[p]) {
						predToRemove.add(p);
					}
				}
				for (Integer i : predToRemove)
					candidates[i] = false;
				curr.add(pred); // recursive call with the new set containting the current predicate

				// check if the current candidates are enough to cover the rest of the sets
				/*Long needToCover = (long)0;
				Long numCovers = (long)0;
				for (Long set : uncovered) {
					boolean canCover = false;
					needToCover = needToCover + evidenceSet.get(set);

					for (int i=0; i < candidates.length;i++) {
						if (!candidates[i])
							continue;

						if (evidenceForPred.get(i).contains(set)) {
							canCover = true;
							break;
						}
					}

					if (canCover) {
						numCovers = numCovers + evidenceSet.get(set);
					}
				}

				if (numCovers.equals(needToCover))*/
				//if (isCovered1Mid()) {

					generateMHSF1(dcs, curr);
					positive++;
				//}
				//else {
				//	System.out.println("aa");
				//}
				//generateMHS(dcs, curr);

				/*boolean willCover = true;
				for (Long set : uncovered) {
					boolean canCover = false;

					for (int i=0; i < candidates.length;i++) {
						if (!candidates[i])
							continue;

						if (evidenceForPred.get(i).contains(set)) {
							canCover = true;
							break;
						}
					}

					if (!canCover) {
						willCover = false;
						break;
					}
				}

				if (willCover)
					generateMHS( dcs, curr );*/

				for (Integer i : predToRemove)
					candidates[i] = true;
				candidates[pred] = true;
				curr.remove(pred);

				uncovered = new HashSet<>(uncoveredOld);
				crit = critOld;
				evidenceForPredRemaining = evidenceForPredOld;
			}
		}

			candidates = Arrays.copyOf(oldCandidates, presMap.size());
	}

	public void generateDCs (ArrayList<NewDenialConstraint> dcs) {
		System.out.println("Generating DCs...");
		System.out.println("Number of distinct sets in evidence set: " + uncovered.size());
		for (Integer i : evidenceForPred.keySet())
			candidates[i] = true;
		if (function == 1)
			generateMHSF1( dcs, new HashSet<>() );
		else if (function == 2)
			generateMHSF2( dcs, new HashSet<>() );
		else
			generateMHSF3( dcs, new HashSet<>() );
		/*for (NewDenialConstraint dc : dcs) {
			midloop:
			for (NewDenialConstraint dc2 : dcs) {
				if (dc == dc2)
					continue;
				for (NewPredicate p : dc.getPredicates()) {
					if (!dc2.getPredicates().contains(p))
						continue midloop;
				}

				System.out.println("contained");
			}
		}*/

		System.out.println("Negative: " + negative);
		System.out.println("Positive: " + positive);

		avgTimeBetweenResults = avgTimeBetweenResults / (double)dcs.size();
		System.out.println("Time between results: " + avgTimeBetweenResults);

		System.out.println("Iterations: " + numOfIterations);
		System.out.println("Success: " + numOfSuccessfulIterations);

		System.out.println("Max DC Size: " + maxDCsize);
		System.out.println("Min DC Size: " + minDCsize);
		System.out.println("Avg DC size: " + avgDCsize / (double) dcs.size());
	}




		int wastedWork = 0;
		int wastedWork2 = 0;
		int numImpliedDCsBeforeAdding = 0;
		private void discoverInternal_Opt1( ArrayList<NewDenialConstraint> dcs)
		{
			System.out.println("We are in DFS search Opitimized mode : subset pruning across DFS search trees");
			Set<NewPredicate> done = new HashSet<NewPredicate>();
			done.addAll(eviModPre.keySet());
			//for(NewPredicate curPre: eviModPre.keySet())
			for(NewPredicate curPre: presMap)
			{
				if(!eviModPre.containsKey(curPre))
					continue;

				if(curPre.getOperator() == 5 || curPre.getOperator() == 3
						//||(curPre.getName().equals("IQ")&& !originalTable.getColumnMapping().positionToType(curPre.getCols()[0]).equals("String"))
						)
					continue;

				done.remove(curPre);

				Map<Set<NewPredicate>,Long> curPreInfoSet = eviModPre.get(curPre);
				Set<Set<NewPredicate>> allMVC = new HashSet<Set<NewPredicate>>();

				if(curPreInfoSet.size()!=0)
					getAllMVCDFS(allMVC,curPre, curPreInfoSet,done,dcs);
				else
					allMVC.add(new HashSet<NewPredicate>());

				for(Set<NewPredicate> mvc: allMVC)
				{
					//System.out.println(curPre.toString() + "MVC: " + mvc.iterator().next().toString());
					//Reverse the predicate
					Set<NewPredicate> reversedMVC = new HashSet<NewPredicate>();
					for(NewPredicate pre: mvc)
					{
						NewPredicate aaa = super.getReverseNewPredicate(presMap.indexOf(pre));
						reversedMVC.add(aaa);
					}

					reversedMVC.add(curPre);

					NewDenialConstraint dc = new NewDenialConstraint(2, new ArrayList<NewPredicate>(reversedMVC),this);
					/*if(consPres.size() != 0)
					{
						for(NewPredicate consPre: consPres)
							dc.addPreciate(consPre);
					}*/

					//dcs.add(dc);
					//Test implication before adding
					//if(!super.linearImplication(new HashSet<NewDenialConstraint>(dcs),  dc))
					 pre2DCs.get(curPre).add(dc);

				}

				//super.removeTriviality(pre2DCs.get(curPre));
				//int aa = super.removeSubset(pre2DCs.get(curPre));
				//wastedWork += aa;
				dcs.addAll(pre2DCs.get(curPre));

				//When done on this predicate, then this predicate cannot be used to get MVC
				//done.add(curPre);

			}
		}



		/**
		 * Order the predicates in the keysets, such that
		 * @param keysets
		 * @return
		 */
		private ArrayList<NewPredicate> orderTaxonomyTree(Set<NewPredicate> keysets)
		{
			ArrayList<NewPredicate> result = new ArrayList<NewPredicate>();
			return result;
		}

		//Do one shot DFS search
		/*private void discoverInternal_Opt2( ArrayList<NewDenialConstraint> dcs)
		{
			//Cannot find DCs containing predicate from done
			Set<NewPredicate> done = new HashSet<NewPredicate>();
			done.addAll(special);
			/*for(NewPredicate curPre: eviModPre.keySet())
			{

				if(curPre.getName().equals("GTE") || curPre.getName().equals("LTE")
						//||(curPre.getName().equals("IQ")&& !originalTable.getColumnMapping().positionToType(curPre.getCols()[0]).equals("String"))
						)
					done.add(curPre);
			}*/

		/*	for(NewPredicate curPre: presMap)
			{
				if(curPre.getOperator() == 5 || curPre.getOperator() == 3
						//||(curPre.getName().equals("IQ")&& !originalTable.getColumnMapping().positionToType(curPre.getCols()[0]).equals("String"))
						)
					done.add(curPre);
			}

			Set<Set<NewPredicate>> allMVC = new HashSet<Set<NewPredicate>>();
			if(tupleWiseInfo.size()!=0)
				getAllMVCDFS(allMVC,null, tupleWiseInfo,done,dcs);
			else
				allMVC.add(new HashSet<NewPredicate>());

			for(Set<NewPredicate> mvc: allMVC)
			{
				//Reverse the predicate
				Set<NewPredicate> reversedMVC = new HashSet<NewPredicate>();
				for(NewPredicate pre: mvc)
				{
					NewPredicate aaa = super.getReverseNewPredicate(pre);
					reversedMVC.add(aaa);
				}

				//reversedMVC.add(curPre);

				NewDenialConstraint dc = new NewDenialConstraint(2, new ArrayList<NewPredicate>(reversedMVC),this);
				/*if(consPres.size() != 0)
				{
					for(NewPredicate consPre: consPres)
						dc.addPreciate(consPre);
				}*/

				/*	dcs.add(dc);
				//pre2DCs.get(curPre).add(dc);
			}
			System.out.println((System.currentTimeMillis() - startingTime )/1000 + ": Before subset pruning");
			//int aa = super.removeSubset(dcs);
			//wastedWork += aa;
		}*/

		/**
		 * This should be a DFS search
		 * @param
		 * @return
		 */
		protected void getAllMVCDFS(Set<Set<NewPredicate>> allMVCs, NewPredicate currentPre, Map<Set<NewPredicate>,Long> toBeCovered,Set<NewPredicate> myDone,
				ArrayList<NewDenialConstraint> dcs)
		{
			//Get the ordering of predicate, based on their coverage
			Set<NewPredicate> curMVC = new HashSet<NewPredicate>();
			ArrayList<NewPredicate> candidates = new ArrayList<NewPredicate>();
			for(NewPredicate temp: presMap)
			{
				for(Set<NewPredicate> key: toBeCovered.keySet())
					if(key.contains(temp))
					{
						NewPredicate reverse = super.getReverseNewPredicate(presMap.indexOf(temp));
						if(myDone.contains(reverse))
						{
							//This predicate cannot be used to generate cover
							break;
						}
						else
						{
							//OK, this is a valid candidate
							candidates.add(temp);
							break;
						}

					}
			}
			ArrayList<NewPredicate> ordering = getOrdering2(toBeCovered,candidates,curMVC, currentPre);
			//Map<Set<NewPredicate>,Long> alreadyCovered2curMVCOverlapping = new HashMap<Set<NewPredicate>,Long>();
			getAllMVCDFSRecursive(allMVCs,currentPre,toBeCovered,ordering,curMVC,myDone,dcs,null,toBeCovered);
		}
		int countPruningInDFS1 = 0;
		int countPruningInDFS2 = 0;
		int countPruningTrivial = 0;


		Set<NewDenialConstraint> dcsClosure = new HashSet<NewDenialConstraint>();

		private boolean pruningInDFS(Set<Set<NewPredicate>> allMVCs, Set<NewPredicate> curMVC,NewPredicate lastMVC,NewPredicate currentPre)
		{
			//Triviality Pruning
			/*NewPredicate reverse = super.getReverseNewPredicate(lastMVC);
			Set<NewPredicate> revImplied = super.getImpliedNewPredicates(reverse);
			//revImplied.add(super.getReverseNewPredicate(lastMVC));
			if(reverse == super.getReverseNewPredicate(currentPre)
					 || revImplied.contains(super.getReverseNewPredicate(currentPre))
					)
			{
				countPruningTrivial++;
				return true;
			}

			for(NewPredicate temp: curMVC)
			{
				if(temp == lastMVC)
					continue;
				if(reverse == temp || revImplied.contains(temp))
				{
					countPruningTrivial++;
					return true;
				}
			}*/


			//subset pruning
			for(Set<NewPredicate> tempMVC: allMVCs)
			{
				if(curMVC.containsAll(tempMVC))
				{
					countPruningInDFS1++;
					return true;
				}

			}
			//Transitive pruning
			/*Set<NewPredicate> tempCurMVC = new HashSet<NewPredicate>(curMVC);
			for(NewPredicate temp: curMVC)
			{
				tempCurMVC.remove(temp);
				NewPredicate reversedTemp = super.getReverseNewPredicate(temp);
				tempCurMVC.add(reversedTemp);
				for(Set<NewPredicate> tempMVC: allMVCs)
				{
					if(tempCurMVC.containsAll(tempMVC))
					{
						countPruningInDFS2++;
						return true;
					}
				}

				tempCurMVC.remove(reversedTemp);
				tempCurMVC.add(temp);
			}

			//Transitive Pruning for Order NewPredicate
			for(NewPredicate temp: curMVC)
			{
				tempCurMVC.remove(temp);
				Set<NewPredicate> tempImplied = super.getImpliedNewPredicates(temp);
				Set<NewPredicate> tempImpliedReversed = new HashSet<NewPredicate>();
				for(NewPredicate pp: tempImplied)
					tempImpliedReversed.add(super.getReverseNewPredicate(pp));
				tempCurMVC.addAll(tempImpliedReversed);
				for(Set<NewPredicate> tempMVC: allMVCs)
				{
					if(tempCurMVC.containsAll(tempMVC))
					{
						countPruningInDFS2++;
						return true;
					}
				}
				tempCurMVC.removeAll(tempImpliedReversed);
				tempCurMVC.add(temp);
			}*/



			return false;
		}

		int countPruningInDFS3 = 0;



		/**
		 *
		 * @param curMVC
		 * @return
		 */
		private boolean pruningInDFS2( Set<NewPredicate> curMVC, ArrayList<NewDenialConstraint> dcs, NewPredicate currentPre)
		{
			Set<NewPredicate> reversedMVC = new HashSet<NewPredicate>();
			for(NewPredicate pre: curMVC)
				reversedMVC.add(super.getReverseNewPredicate(presMap.indexOf(pre)));
			for(NewDenialConstraint dc: dcs)
			{
				if(reversedMVC.containsAll(dc.getPredicates()))
				{
					 countPruningInDFS3++;
					 return true;
				}
			}


			return false;
		}



		private boolean pruningUsingDCClosure(Set<NewPredicate> curMVC,  NewPredicate currentPre)
		{
			ArrayList<NewPredicate> reversedMVC = new ArrayList<NewPredicate>();
			for(NewPredicate pre: curMVC)
				reversedMVC.add(super.getReverseNewPredicate(pre));
			if(currentPre!=null)
				reversedMVC.add(currentPre);

			for(NewDenialConstraint dc: dcsClosure)
			{
				if(reversedMVC.containsAll(dc.getPredicates()))
				{
					 return true;
				}
			}

			/*DenialConstraint curDC = new DenialConstraint(2, reversedMVC, this);
			if(this.linearImplication(dcsClosure, curDC))
			{
				dcsClosure.add(curDC);
				return true;
			}
			*/
			return false;
		}

		long xxx = 0;
		protected void getAllMVCDFSRecursive(Set<Set<NewPredicate>> allMVCs,  NewPredicate currentPre,Map<Set<NewPredicate>,Long> toBeCovered,
				ArrayList<NewPredicate> ordering,Set<NewPredicate> curMVC,Set<NewPredicate> myDone, ArrayList<NewDenialConstraint> dcs,NewPredicate lastMVC,
				Map<Set<NewPredicate>,Long> OriginalToBeCovered)
		{


			//If the size of the branch gets too long, return
			if(Config.dfsLevel != -1)
			{
				if(curMVC.size() + 1 > Config.dfsLevel)
					return;
			}


			//Pruning of the branch!
			//if(this.dynamicOrdering != 0)
			long time1 = System.currentTimeMillis();

			boolean tag = pruningInDFS(allMVCs,curMVC,lastMVC,currentPre);
			if(!tag)
				tag = pruningInDFS2(curMVC,dcs,currentPre);


			//boolean tag = pruningUsingDCClosure(curMVC,currentPre);

			long time2 = System.currentTimeMillis();
			this.timeDFSPruning += (time2 - time1);


			if(tag)
				return;

			//int maxVios = getMaxNumVios();
			long maxVios = 0;
			if(Config.noiseTolerance != 0)
				maxVios = getMaxVios(curMVC,currentPre);

			long remainingVios = 0;
			for(Long value: toBeCovered.values())
			{
				remainingVios += value;
			}

			/*if(currentPre.toString().equals("t1.State!=t2.State") &&
					curMVC.size() == 1 )
			{

				System.out.println(curMVC.iterator().next().toString()   + " : Remainig vios: " + remainingVios);
			}
			if(currentPre.toString().equals("t1.ZIP Code=t2.ZIP Code") &&
					curMVC.size() == 1 )
			{

				if(curMVC.iterator().next().toString().equals("t1.State=t2.State"))
					System.out.println(curMVC.iterator().next().toString()   + " : Remainig vios: " + remainingVios);
			}*/
			if(ordering.size() == 0 && remainingVios > maxVios)
			{
				//System.out.println(" No more predicates left");
				return;
			}

			if(remainingVios <= maxVios)
				//toBeCovered.size() == 0)
			{

				if(!minimalityTest(curMVC,OriginalToBeCovered,currentPre))
				//if(false)
				{
					wastedWork2++;
					return;
				}

				Set<NewPredicate> newMVC = new HashSet<NewPredicate>(curMVC);

				allMVCs.add(newMVC);

				//Create a new DC, and add it to the closure
				ArrayList<NewPredicate> reversedMVC = new ArrayList<NewPredicate>();
				for(NewPredicate pre: newMVC)
				{
					NewPredicate aaa = super.getReverseNewPredicate(pre);
					reversedMVC.add(aaa);
				}
				if(currentPre!=null)
					reversedMVC.add(currentPre);

				NewDenialConstraint dc = new NewDenialConstraint(2, reversedMVC,this);
				dcsClosure.add(dc);


				return;

			}
			else
			{
				if(Config.topkDCPruning)
				{
					long xxxS = System.currentTimeMillis();
					//See if I can prune them according to the ranking functions
					double mdlUpper = this.mdlUpper(curMVC);
					double coverageUpper = this.coverageUpper2(curMVC,currentPre);
					//double coverageUpper = 1;

					double interUpper = (1-Config.coverageA) * mdlUpper + Config.coverageA * coverageUpper;
					long xxxE = System.currentTimeMillis();
					xxx += (xxxE-xxxS);
					if(interUpper <= Config.topkDCPruningThre)
					{
						System.out.println("XX bounding interstingness :" + mdlUpper + "----"
								+ coverageUpper);
						return;
					}
					//If the remaining vios > the # evid, last NewPredicate cover,
					// then need at least two predicates

				}


			}


			ArrayList<NewPredicate> remaining = new ArrayList<NewPredicate>(ordering);
			for(int i = 0 ; i < ordering.size(); i++)
			{

				NewPredicate curPre = ordering.get(i);
				remaining.remove(curPre);


				Map<Set<NewPredicate>,Long> nextToBeCovered = new HashMap<Set<NewPredicate>,Long>();

				for(Set<NewPredicate> temp2: toBeCovered.keySet())
				{
					if(!temp2.contains(curPre))
					{
						nextToBeCovered.put(temp2, toBeCovered.get(temp2));

					}
				}


				//Copy MVC each time vs only keep one MVC copy
				curMVC.add(curPre);
				ArrayList<NewPredicate> nextOrdering = getOrdering2(nextToBeCovered,remaining, curMVC, currentPre);
				getAllMVCDFSRecursive(allMVCs,currentPre,nextToBeCovered,nextOrdering,
						curMVC,myDone,dcs,curPre,OriginalToBeCovered);
				curMVC.remove(curPre);


			}
		}


		private ArrayList<NewPredicate> getOrdering2(Map<Set<NewPredicate>,Long> toBeCovered,ArrayList<NewPredicate> candidate, Set<NewPredicate> curMVC, NewPredicate currentPre)
		{
			if(this.dynamicOrdering == 0)
			{
				ArrayList<NewPredicate> result = new ArrayList<NewPredicate>();
				for(NewPredicate pre: candidate)
				{
					//Get rid of trivial DC at this point
					boolean tagTrivial = false;
					NewPredicate reverse = super.getReverseNewPredicate(pre);
					Set<NewPredicate> revImplied = super.getImpliedNewPredicates(reverse);
					//revImplied.add(super.getReverseNewPredicate(lastMVC));
					if(reverse == super.getReverseNewPredicate(currentPre)
							 || revImplied.contains(super.getReverseNewPredicate(currentPre))
							)
					{
						tagTrivial = true;
					}

					for(NewPredicate temp: curMVC)
					{
						if(temp == pre)
							continue;
						if(reverse == temp || revImplied.contains(temp))
						{
							tagTrivial = true;
						}
					}
					if(tagTrivial)
						continue;


					long count = 0;
					for(Set<NewPredicate> temp: toBeCovered.keySet())
						if(temp.contains(pre))
						{
							if(Config.noiseTolerance == 0)
								count++;
							else
								count += toBeCovered.get(temp);
						}

					if(count == 0)
					{
						//assert(false);
						continue;
					}


					result.add(pre);
				}
				return result;
			}
			else
			{
				ArrayList<NewPredicate> result = new ArrayList<NewPredicate>();
				ArrayList<Long> coverage = new ArrayList<Long>();
				for(NewPredicate pre: candidate)
				{
					//Get rid of trivial DC at this point
					boolean tagTrivial = false;
					NewPredicate reverse = super.getReverseNewPredicate(pre);
					Set<NewPredicate> revImplied = super.getImpliedNewPredicates(reverse);
					//revImplied.add(super.getReverseNewPredicate(lastMVC));
					if(reverse == super.getReverseNewPredicate(currentPre)
							 || revImplied.contains(super.getReverseNewPredicate(currentPre))
							)
					{
						tagTrivial = true;
					}

					for(NewPredicate temp: curMVC)
					{
						if(temp == pre)
							continue;
						if(reverse == temp || revImplied.contains(temp))
						{
							tagTrivial = true;
						}
					}
					if(tagTrivial)
						continue;


					long count = 0;
					for(Set<NewPredicate> temp: toBeCovered.keySet())
						if(temp.contains(pre))
						{
							if(Config.noiseTolerance == 0)
								count++;
							else
								count += toBeCovered.get(temp);
						}

					if(count == 0)
					{
						//assert(false);
						continue;
					}


					int i = 0;
					for(i = 0 ; i < result.size(); i++)
					{
						if(count > coverage.get(i))
						{
							coverage.add(i, count);
							result.add(i,pre);
							break;
						}
					}
					if(i == result.size())
					{
						result.add(pre);
						coverage.add(count);
					}
				}
				return result;
			}


		}


		/**
		 * To test whether there is a (n-1) subset of curMVC, that covers toBeCovered
		 * @param curMVC
		 * @param OriginalToBeCovered
		 * @return
		 */
		private boolean minimalityTest(Set<NewPredicate> curMVC, Map<Set<NewPredicate>,Long> OriginalToBeCovered,NewPredicate currentPre)
		{
			if(Config.noiseTolerance != 0)
			{
				Set<NewPredicate> tempMVC = new HashSet<NewPredicate>(curMVC);
				for(NewPredicate leftOut: curMVC)
				{
					tempMVC.remove(leftOut);
					//Test whether temMVC is a cover or not
					long numVios = 0;
					for(Set<NewPredicate> key: OriginalToBeCovered.keySet())
					{
						boolean thisKeyCovered = false;
						for(NewPredicate pre: tempMVC)
							if(key.contains(pre))
							{
								thisKeyCovered = true;
								break;
							}
						if(!thisKeyCovered)
						{
							numVios += OriginalToBeCovered.get(key);
						}
					}
					if(numVios < getMaxVios(curMVC,currentPre))
					{
						return false;
					}

					tempMVC.add(leftOut);
				}
				return true;
			}
			else
			{
				Set<NewPredicate> tempMVC = new HashSet<NewPredicate>(curMVC);
				for(NewPredicate leftOut: curMVC)
				{
					tempMVC.remove(leftOut);
					//Test whether temMVC is a cover or not
					boolean isCover = true;
					for(Set<NewPredicate> key: OriginalToBeCovered.keySet())
					{
						boolean thisKeyCovered = false;
						for(NewPredicate pre: tempMVC)
							if(key.contains(pre))
							{
								thisKeyCovered = true;
								break;
							}
						if(!thisKeyCovered)
						{
							isCover = false;
							break;
						}
					}
					if(isCover)
					{
						return false;
					}

					tempMVC.add(leftOut);
				}
				return true;
			}


		}



		int numMinimalDCs = 0;
		/**
		 * 1. Elimination of not meaningful DC
		 */
		protected void postprocess(ArrayList<NewDenialConstraint> dcs)
		{
			System.out.println("XXXXX : " + xxx);
			System.out.println( (System.currentTimeMillis() - startingTime)/1000+  ": We are inside post processing");
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": We have " + dcs.size() + " DCs");
			/*System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": We have " + dcs.size() + " weak minimal DCs");

			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": Num of Implied DCs before adding: " + this.numImpliedDCsBeforeAdding);



			assert(super.removeTriviality(dcs) == 0);

			assert(super.removeSubset(dcs) == 0);

			int numSuc = super.initMinimalDC(dcs);

			System.out.println("The number of DCs affected by most succinct form is: " + numSuc);

			int countrule1 = super.removeTriviality(dcs);

			int countrule2 = super.removeSubset(dcs);

			System.out.println("Triviality:" + countrule1 + " subset pruning: " + countrule2);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": We have " + dcs.size() + " strong minimal DCs");
			 */


			numMinimalDCs = dcs.size();

			//Associate an interesting score
			/*for(NewDenialConstraint dc: dcs)
			{
				dc.interestingness = scoring(dc);
				if(dc.interestingness <  0)
				{
					System.out.println(dc.toString());
				}
			}*/

			//Randomly shuffle the list
			//Collections.shuffle(dcs);

			//Rank the totalDCs according to their interestingness score
			/*Collections.sort(dcs, new Comparator<NewDenialConstraint>()
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

					});*/





			//myOwnWriteDC2File("dcs_BeforeImp",dcs);
			//int countImpli = super.minimalCover(dcs);
			//int countImpli = super.minimalCoverAccordingtoInterestingess(dcs);
			//System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": We have applied Implication for "  + countImpli + " times before writing to files" );


			//Remove Symmetric DCs
			int symReduction = this.removeSymmetryDCs(dcs);
			System.out.println("Symmetrica DC Reduction:  " + symReduction);

			//myOwnWriteDC2File("dcs_AfterImp",dcs);


			//ranking(dcs);

			/*if(!Config.testScoringFunctionOnly)
			{
				String srFile = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_BeforeImp"));
				String dtFile = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_All"));
				FileUtil.copyfile(srFile, dtFile);
			}*/
		}

	public void  ranking(ArrayList<NewDenialConstraint> dcs)
	{
		PrintWriter xu = null;
		try {
			xu = new PrintWriter(new FileWriter("Experiments/RankingResult_" + originalTable.tableName + ".csv"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String line = "RankingType" +  "," + Config.getTopkHead();
			/*"PrecisionTop5,RecallTop5," +
					"PrecisionTop10,RecallTop10," +
					"PrecisionTop15,RecallTop15," +
					"PrecisionTop20,RecallTop20"
					;
			*/
		xu.println(line);




		//interestingness
		////System.out.println("Ranking according to interestingness");
		String dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_Inter"));
		try {
			PrintWriter out = new PrintWriter(new FileWriter(dcPath));
			out.println("The set of Denial Constraints:");
			for(NewDenialConstraint dc: dcs)
			{
				out.println( dc.interestingness + ":" + dc.toString());
			}
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}


		StringBuilder sb = new StringBuilder();
		sb.append("Inter,");
		for(int i = 0 ; i < Config.numTopks; i++)
		{
			int topk = Config.grak * (i+1);
			PR pr = getPRTopk(topk,dcPath,dcs);
			//PR pr = new PR(0,0);
			if( i != Config.numTopks -1)
				sb.append(pr.precision +  "," + pr.recall + ",");
			else
				sb.append(pr.precision +  "," + pr.recall );

		}
		xu.println(sb);



		//Rank the totalDCs according to their # vios
		//System.out.println("Ranking according to num Vios");
		Collections.sort(dcs, new Comparator<NewDenialConstraint>()
		{
			public int compare(NewDenialConstraint arg0,
					NewDenialConstraint arg1) {
				if(arg0.numVios < arg1.numVios)
					return -1;
				else if(arg0.numVios > arg1.numVios)
					return 1;
				else
					return 0;

			}

		});

		dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_NumVios"));
		try {
			PrintWriter out = new PrintWriter(new FileWriter(dcPath));
			out.println("The set of Denial Constraints:");
			for(NewDenialConstraint dc: dcs)
			{
				out.println( dc.numVios+ ":" + dc.toString());
			}
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		sb = new StringBuilder();
		sb.append("NumVios,");
		for(int i = 0 ; i < Config.numTopks; i++)
		{
			int topk = Config.grak * (i+1);
			PR pr = getPRTopk(topk,dcPath,dcs);
			//PR pr = new PR(0,0);
			if( i != Config.numTopks -1)
				sb.append(pr.precision +  "," + pr.recall + ",");
			else
				sb.append(pr.precision +  "," + pr.recall );

		}
		xu.println(sb);


		//Rank the totalDCs according to their # vios * interestingness
		//System.out.println("Ranking according to # vios * interestingness");
		Collections.sort(dcs, new Comparator<NewDenialConstraint>()
		{
			public int compare(NewDenialConstraint arg0,
					NewDenialConstraint arg1) {
				if(numViosTimesInter(arg0) > numViosTimesInter(arg1))
					return -1;
				else if(numViosTimesInter(arg0) < numViosTimesInter(arg1))
					return 1;
				else
					return 0;

			}

		});

		dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_NumViosTimesInterestingness"));
		try {
			PrintWriter out = new PrintWriter(new FileWriter(dcPath));
			out.println("The set of Denial Constraints:");
			for(NewDenialConstraint dc: dcs)
			{
				out.println( numViosTimesInter(dc) + ":" + dc.toString());
			}
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		sb = new StringBuilder();
		sb.append("NumViosTimesInterestingness,");
		for(int i = 0 ; i < Config.numTopks; i++)
		{
			int topk = Config.grak * (i+1);
			PR pr = getPRTopk(topk,dcPath,dcs);
			//PR pr = new PR(0,0);
			if( i != Config.numTopks -1)
				sb.append(pr.precision +  "," + pr.recall + ",");
			else
				sb.append(pr.precision +  "," + pr.recall );

		}
		xu.println(sb);


		//Rank the totalDCs according to their mdl
		//System.out.println("Ranking according to mdl");
		Collections.sort(dcs, new Comparator<NewDenialConstraint>()
		{
			public int compare(NewDenialConstraint arg0,
					NewDenialConstraint arg1) {
				if(arg0.mdl > arg1.mdl)
					return -1;
				else if(arg0.mdl < arg1.mdl)
					return 1;
				else
					return 0;

			}

		});

		dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_Succ"));
		try {
			PrintWriter out = new PrintWriter(new FileWriter(dcPath));
			out.println("The set of Denial Constraints:");
			for(NewDenialConstraint dc: dcs)
			{
				out.println( dc.mdl+ ":" + dc.toString());
			}
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		sb = new StringBuilder();
		sb.append("Succ,");
		for(int i = 0 ; i < Config.numTopks; i++)
		{
			int topk = Config.grak * (i+1);
			PR pr = getPRTopk(topk,dcPath,dcs);
			//PR pr = new PR(0,0);
			if( i != Config.numTopks -1)
				sb.append(pr.precision +  "," + pr.recall + ",");
			else
				sb.append(pr.precision +  "," + pr.recall );

		}
		xu.println(sb);

		//Rank the totalDCs according to their coverage
		//System.out.println("Ranking according to coverage");
		Collections.sort(dcs, new Comparator<NewDenialConstraint>()
		{
			public int compare(NewDenialConstraint arg0,
					NewDenialConstraint arg1) {
				if(arg0.coverage > arg1.coverage)
					return -1;
				else if(arg0.coverage < arg1.coverage)
					return 1;
				else
					return 0;

			}

		});

		dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_Coverage"));
		try {
			PrintWriter out = new PrintWriter(new FileWriter(dcPath));
			out.println("The set of Denial Constraints:");
			for(NewDenialConstraint dc: dcs)
			{
				out.println(dc.coverage+ ":" + dc.toString());
			}
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		sb = new StringBuilder();
		sb.append("Coverage,");
		for(int i = 0 ; i < Config.numTopks; i++)
		{
			int topk = Config.grak * (i+1);
			PR pr = getPRTopk(topk,dcPath,dcs);
			//PR pr = new PR(0,0);
			if( i != Config.numTopks -1)
				sb.append(pr.precision +  "," + pr.recall + ",");
			else
				sb.append(pr.precision +  "," + pr.recall );

		}
		xu.println(sb);


		double[] as = new double[]{0,0.2,0.4,0.6,0.8,1.0};
		for(final double a: as)
		{
			//System.out.println("Ranking according to a*coverage+1-a*mdl : " + a);
			Collections.sort(dcs, new Comparator<NewDenialConstraint>()
			{
				public int compare(NewDenialConstraint arg0,
						NewDenialConstraint arg1) {
					if(scoringExp(arg0,a) > scoringExp(arg1,a))
						return -1;
					else if(scoringExp(arg0,a) < scoringExp(arg1,a))
						return 1;
					else
						return 0;

				}

			});
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_Inter_" + a));
			try {
				PrintWriter out = new PrintWriter(new FileWriter(dcPath));
				out.println("The set of Denial Constraints:");
				for(NewDenialConstraint dc: dcs)
				{
					out.println("Interesting_" + scoringExp(dc,a)+ ":" + dc.toString());
				}
				out.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

			sb = new StringBuilder();
			sb.append("Inter_" + a + "_,");
			for(int i = 0 ; i < Config.numTopks; i++)
			{
				int topk = Config.grak * (i+1);
				PR pr = getPRTopk(topk,dcPath,dcs);
				//PR pr = new PR(0,0);
				if( i != Config.numTopks -1)
					sb.append(pr.precision +  "," + pr.recall + ",");
				else
					sb.append(pr.precision +  "," + pr.recall );

			}
			xu.println(sb);
		}

		xu.close();
	}
		
		private void myOwnWriteDC2File(String fileName,ArrayList<NewDenialConstraint> dcs)
		{
			String aa = fileName;
			
			String dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append(aa));
		
			try {
				PrintWriter out = new PrintWriter(new FileWriter(dcPath));
				out.println("The set of Denial Constraints:");
				for(NewDenialConstraint dc: dcs)
				{
					out.println(dc.interestingness + ":" + dc.toString());
				}
				out.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
	
				
		}
		
		/*public void  ranking(ArrayList<NewDenialConstraint> dcs)
		{
			PrintWriter xu = null;
			try {
				  xu = new PrintWriter(new FileWriter("Experiments/RankingResult_" + originalTable.tableName + ".csv"));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			String line = "RankingType" +  "," + Config.getTopkHead();
			"PrecisionTop5,RecallTop5," +
					"PrecisionTop10,RecallTop10," + 
					"PrecisionTop15,RecallTop15," + 
					"PrecisionTop20,RecallTop20"  
					; 

			xu.println(line);
			
			
			
			
			//interestingness
			////System.out.println("Ranking according to interestingness");
			String dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_Inter"));
			try {
				PrintWriter out = new PrintWriter(new FileWriter(dcPath));
				out.println("The set of Denial Constraints:");
				for(NewDenialConstraint dc: dcs)
				{
					out.println( dc.interestingness + ":" + dc.toString());
				}
				out.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
			StringBuilder sb = new StringBuilder();
			sb.append("Inter,");
			for(int i = 0 ; i < Config.numTopks; i++)
			{
				int topk = Config.grak * (i+1);
				PR pr = getPRTopk(topk,dcPath,dcs);
				//PR pr = new PR(0,0);
				if( i != Config.numTopks -1)
					sb.append(pr.precision +  "," + pr.recall + ",");
				else
					sb.append(pr.precision +  "," + pr.recall );
					
			}
			xu.println(sb);
			
			
			
			//Rank the totalDCs according to their # vios
			//System.out.println("Ranking according to num Vios");
			Collections.sort(dcs, new Comparator<NewDenialConstraint>()
					{
						public int compare(DenialConstraint arg0,
								DenialConstraint arg1) {
							if(arg0.numVios < arg1.numVios)
								return -1;
							else if(arg0.numVios > arg1.numVios)
								return 1;
							else
								return 0;
								
						}
				
					});
			
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_NumVios"));
			try {
				PrintWriter out = new PrintWriter(new FileWriter(dcPath));
				out.println("The set of Denial Constraints:");
				for(DenialConstraint dc: dcs)
				{
					out.println( dc.numVios+ ":" + dc.toString());
				}
				out.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			sb = new StringBuilder();
			sb.append("NumVios,");
			for(int i = 0 ; i < Config.numTopks; i++)
			{
				int topk = Config.grak * (i+1);
				PR pr = getPRTopk(topk,dcPath,dcs);
				//PR pr = new PR(0,0);
				if( i != Config.numTopks -1)
					sb.append(pr.precision +  "," + pr.recall + ",");
				else
					sb.append(pr.precision +  "," + pr.recall );
					
			}
			xu.println(sb);
			
			
			//Rank the totalDCs according to their # vios * interestingness
			//System.out.println("Ranking according to # vios * interestingness");
			Collections.sort(dcs, new Comparator<NewDenialConstraint>()
					{
						public int compare(DenialConstraint arg0,
								DenialConstraint arg1) {
							if(numViosTimesInter(arg0) > numViosTimesInter(arg1))
								return -1;
							else if(numViosTimesInter(arg0) < numViosTimesInter(arg1))
								return 1;
							else
								return 0;
								
						}
				
					});
			
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_NumViosTimesInterestingness"));
			try {
				PrintWriter out = new PrintWriter(new FileWriter(dcPath));
				out.println("The set of Denial Constraints:");
				for(DenialConstraint dc: dcs)
				{
					out.println( numViosTimesInter(dc) + ":" + dc.toString());
				}
				out.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			sb = new StringBuilder();
			sb.append("NumViosTimesInterestingness,");
			for(int i = 0 ; i < Config.numTopks; i++)
			{
				int topk = Config.grak * (i+1);
				PR pr = getPRTopk(topk,dcPath,dcs);
				//PR pr = new PR(0,0);
				if( i != Config.numTopks -1)
					sb.append(pr.precision +  "," + pr.recall + ",");
				else
					sb.append(pr.precision +  "," + pr.recall );
					
			}
			xu.println(sb);
			
			
			//Rank the totalDCs according to their mdl
			//System.out.println("Ranking according to mdl");
			Collections.sort(dcs, new Comparator<NewDenialConstraint>()
					{
						public int compare(DenialConstraint arg0,
								DenialConstraint arg1) {
							if(arg0.mdl > arg1.mdl)
								return -1;
							else if(arg0.mdl < arg1.mdl)
								return 1;
							else
								return 0;
								
						}
				
					});
			
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_Succ"));
			try {
				PrintWriter out = new PrintWriter(new FileWriter(dcPath));
				out.println("The set of Denial Constraints:");
				for(DenialConstraint dc: dcs)
				{
					out.println( dc.mdl+ ":" + dc.toString());
				}
				out.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			sb = new StringBuilder();
			sb.append("Succ,");
			for(int i = 0 ; i < Config.numTopks; i++)
			{
				int topk = Config.grak * (i+1);
				PR pr = getPRTopk(topk,dcPath,dcs);
				//PR pr = new PR(0,0);
				if( i != Config.numTopks -1)
					sb.append(pr.precision +  "," + pr.recall + ",");
				else
					sb.append(pr.precision +  "," + pr.recall );
					
			}
			xu.println(sb);
			
			//Rank the totalDCs according to their coverage
			//System.out.println("Ranking according to coverage");
			Collections.sort(dcs, new Comparator<NewDenialConstraint>()
					{
						public int compare(DenialConstraint arg0,
								DenialConstraint arg1) {
							if(arg0.coverage > arg1.coverage)
								return -1;
							else if(arg0.coverage < arg1.coverage)
								return 1;
							else
								return 0;
								
						}
				
					});
			
			dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_Coverage"));
			try {
				PrintWriter out = new PrintWriter(new FileWriter(dcPath));
				out.println("The set of Denial Constraints:");
				for(DenialConstraint dc: dcs)
				{
					out.println(dc.coverage+ ":" + dc.toString());
				}
				out.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			sb = new StringBuilder();
			sb.append("Coverage,");
			for(int i = 0 ; i < Config.numTopks; i++)
			{
				int topk = Config.grak * (i+1);
				PR pr = getPRTopk(topk,dcPath,dcs);
				//PR pr = new PR(0,0);
				if( i != Config.numTopks -1)
					sb.append(pr.precision +  "," + pr.recall + ",");
				else
					sb.append(pr.precision +  "," + pr.recall );
					
			}
			xu.println(sb);
			
			
			double[] as = new double[]{0,0.2,0.4,0.6,0.8,1.0};
			for(final double a: as)
			{
				//System.out.println("Ranking according to a*coverage+1-a*mdl : " + a);
				Collections.sort(dcs, new Comparator<NewDenialConstraint>()
						{
							public int compare(DenialConstraint arg0,
									DenialConstraint arg1) {
								if(scoringExp(arg0,a) > scoringExp(arg1,a))
									return -1;
								else if(scoringExp(arg0,a) < scoringExp(arg1,a))
									return 1;
								else
									return 0;
									
							}
					
						});
				dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("Rank_Inter_" + a));
				try {
					PrintWriter out = new PrintWriter(new FileWriter(dcPath));
					out.println("The set of Denial Constraints:");
					for(DenialConstraint dc: dcs)
					{
						out.println("Interesting_" + scoringExp(dc,a)+ ":" + dc.toString());
					}
					out.close();
					
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				sb = new StringBuilder();
				sb.append("Inter_" + a + "_,");
				for(int i = 0 ; i < Config.numTopks; i++)
				{
					int topk = Config.grak * (i+1);
					PR pr = getPRTopk(topk,dcPath,dcs);
					//PR pr = new PR(0,0);
					if( i != Config.numTopks -1)
						sb.append(pr.precision +  "," + pr.recall + ",");
					else
						sb.append(pr.precision +  "," + pr.recall );
						
				}
				xu.println(sb);
			}
			
			xu.close();
		}*/
		
		private double numViosTimesInter(NewDenialConstraint dc)
		{
			double numVios = dc.numVios;
			double maxVios = this.getMaxVios(new HashSet<NewPredicate>(dc.getPredicates()),dc.getPredicates().iterator().next());
			double ratio = 1 - numVios / maxVios;
			
			return ratio * (0.5 * dc.coverage + 0.5 * dc.mdl);
		}
		
		private double scoringExp(NewDenialConstraint dc, double a)
		{
			assert( a >= 0 && a <= 1);
			return a * dc.coverage + (1-a) * dc.mdl;
		}
		
		/**
		 * 
		 * @param dc
		 * @return
		 */
		public double scoring(NewDenialConstraint dc)
		{
			double result = 0;

			if(Config.sc == 0)
			{
				//double wei = (double)1.0 / 2;

				dc.mdl = mdl(dc);
				dc.coverage = coverage(dc);
				result = 0.5 * dc.mdl + 0.5 *  dc.coverage;

				//	wei * coherenceScore(dc)

			}
			else if(Config.sc == 1)
			{
				double wei = (double)1.0 / 1;
				result = wei * mdl(dc);
			}
			else if(Config.sc == 2)
			{
				double wei = (double)1.0 / 1;
				result = wei *  coverage(dc);

			}
			/*else if(Config.sc == 3)
			{
				double wei = (double)1.0 / 1;
				result =wei *  coverage(dc);
			}*/

			//double perVios = perVios(dc) * originalTable.getNumRows();
			//assert(perVios < 1);
			//result = result * (1 - perVios);
			return result ;
		}

	/*public double scoring(NewDenialConstraint dc)
	{
		double result = 0;

		if(Config.sc == 0)
		{
			//double wei = (double)1.0 / 2;

			dc.mdl = mdl(dc);
			dc.coverage = coverage(dc);
			result = 0.5 * dc.mdl + 0.5 *  dc.coverage;

			//	wei * coherenceScore(dc)

		}
		else if(Config.sc == 1)
		{
			double wei = (double)1.0 / 1;
			result = wei * mdl(dc);
		}
		else if(Config.sc == 2)
		{
			double wei = (double)1.0 / 1;
			result = wei *  coverage(dc);

		}
			else if(Config.sc == 3)
			{
				double wei = (double)1.0 / 1;
				result =wei *  coverage(dc);
			}

		//double perVios = perVios(dc) * originalTable.getNumRows();
		//assert(perVios < 1);
		//result = result * (1 - perVios);
		return result ;
	}*/
		
		/**
		 * The lenght of a DC should also affect the interestingness of a DC
		 * @param dc
		 * @return
		 */
		private double length(DenialConstraint dc)
		{
			return (double)1.0 / Math.sqrt(dc.getPredicates().size());
		}
		
		/**
		 * Coherence of a DC is the minimal of all coherence of 
		 * @param dc
		 * @return
		 */
		private double coherenceScore(NewDenialConstraint dc)
		{
			double result = 0;
			for(NewPredicate pre: dc.getPredicates())
			{
				result += pre.coherence();
				/*if(pre.coherence() < result)
				{
					result = pre.coherence();
				}*/
			}
			//return result;
			return result / dc.getPredicates().size();
		}
		/**
		 * Percentage of evidences, of which all subsets are counted in a weighted manner
		 * @param
		 * @return
		 */
		private double coverage(NewDenialConstraint dc)
		{
			
			
			ArrayList<NewPredicate> pres = dc.getPredicates();
			double count = 0;
			
			long numVios = 0;
			for(List<Boolean> info: tupleWiseInfo.keySet())
			{
				Set<NewPredicate> temp = new HashSet<NewPredicate>(pres);

				Set<NewPredicate> toRemove = new HashSet<>();
				for (NewPredicate p : temp) {
					if (!info.get(presMap.indexOf(p)))
						toRemove.add(p);
				}
				temp.removeAll(toRemove);
				
				//this is a NE
				if(temp.size() == pres.size())
				{
					//count = count - originalTable.getNumRows();
					numVios += tupleWiseInfo.get(info);
					continue;
				}
				
				//This is a temp.size()-PE
				
				
				double weight = (double) (temp.size() + 1) / pres.size();
				
				if(pres.size() == 1)
				{
					weight = 1;
				}
				
				count += weight * tupleWiseInfo.get(info);
				
			}
			assert(count >= 0);
			assert(totalEvi>=0);
			double perVios = (double)numVios / totalEvi; 
			
			dc.numVios = numVios;
			//return  (1-perVios) * count / totalEvi;
		
			
			return count / (totalEvi - numVios);
		}
		
		
		/**
		 * 
		 * @param pres
		 * @return
		 */
		private double coverageUpper(Set<NewPredicate> pres, NewPredicate currentPres)
		{
			
			assert(pres.size() <= Config.dfsLevel);
			double sumWeight = 0;
			long allEvis = originalTable.getNumRows() * (originalTable.getNumRows()-1);
			long thisEvis = 0;
			
			int count = 0;
			for(List<Boolean> info: tupleWiseInfo.keySet())
			{
				count++;
				if(count > 100)
				{
					break;
				}
				
				long negK = 0;
				for(NewPredicate pre: pres)
				{
					if(info.get(presMap.indexOf(pre)))
						negK++;
				}
				if(info.get(presMap.indexOf(super.getReverseNewPredicate(currentPres))))
					negK++;
				double maxWeight = 1;
				if(negK >= 2)
				{
					maxWeight = 1 - ((double)(negK-1)) / Config.dfsLevel;
				}
				
				
				sumWeight += maxWeight * tupleWiseInfo.get(info);
				thisEvis += tupleWiseInfo.get(info);
			}
			return (sumWeight + (allEvis - thisEvis)) / allEvis;
			//return sumWeight / thisEvis;
		}
		private double coverageUpper2(Set<NewPredicate> pres, NewPredicate currentPres)
		{
			
			assert(pres.size() <= Config.dfsLevel);
			double sumWeight = 0;
			long allEvis = originalTable.getNumRows() * (originalTable.getNumRows()-1);
			long thisEvis = 0;
			
			int count = 0;
			Map<Set<NewPredicate>,Long> myInfo = eviModPre.get(super.getReverseNewPredicate(currentPres));
			for(Set<NewPredicate> info: myInfo.keySet())
			{
				count++;
				if(count > 1000)
				{
					//System.out.println("we break");
					break;
				}
				
				long negK = 1;
				for(NewPredicate pre: pres)
				{
					if(info.contains(pre))
						negK++;
				}
				double maxWeight = 1;
				if(negK >= 2)
				{
					maxWeight = 1 - ((double)(negK-1)) / Config.dfsLevel;
				}
				sumWeight += maxWeight * myInfo.get(info);
				thisEvis += myInfo.get(info);
			}
			return (sumWeight + (allEvis - thisEvis)) / allEvis;
			//return sumWeight / thisEvis;
		}
		private long getnumVios(NewDenialConstraint dc)
		{ArrayList<NewPredicate> pres = dc.getPredicates();
			long numVios = 0;
			for(List<Boolean> info: tupleWiseInfo.keySet())
			{
				Set<NewPredicate> temp = new HashSet<NewPredicate>(pres);

				Set<NewPredicate> toRemove = new HashSet<>();
				for (NewPredicate p : temp) {
					if (!info.get(presMap.indexOf(p)))
						toRemove.add(p);
				}
				temp.removeAll(toRemove);
				
				//this is a NE
				if(temp.size() == pres.size())
				{
					//count = count - originalTable.getNumRows();
					numVios += tupleWiseInfo.get(info);
					continue;
				}
				
				//This is a temp.size()-PE
				
				
				
				
			}
			return numVios;
		}
	
		
		/**
		 * Percentage of evidences, of which all subsets are counted in a weighted manner
		 * @param
		 * @return
		 */
		private double mdl(NewDenialConstraint dc)
		{
			ArrayList<NewPredicate> pres = dc.getPredicates();
			int count = 0;
			Set<Integer> colsCovered = new HashSet<Integer>();
			Set<Integer> rowsCovered = new HashSet<Integer>();
			Set<Integer> opsCovered = new HashSet<>();
			for(NewPredicate pre: pres)
			{
				int[] rows = pre.getRows();
				for(int i: rows)
				{
					if(!rowsCovered.contains(i) && i > 0)
					{
						count++;
						rowsCovered.add(i);
					}
						
				}
				int[] cols = pre.getCols();
				for(int i: cols)
				{
					if(! colsCovered.contains(i) && i > 0)
					{
						count++;
						colsCovered.add(i);
					}
						
				}
				int name  = pre.getOperator();
				if(! opsCovered.contains(name))
				{
					count++;
					opsCovered.add(name);
				}
				
			}
			
			return (double) 4.0 / count;
		}


		/**
		 * Percentage of evidences, of which all subsets are counted in a weighted manner
		 * @param pres
		 * @return
		 */
		private double mdlUpper(Set<NewPredicate> pres)
		{
			int count = 0;
			Set<Integer> colsCovered = new HashSet<Integer>();
			Set<Integer> rowsCovered = new HashSet<Integer>();
			Set<Integer> opsCovered = new HashSet<>();
			for(NewPredicate pre: pres)
			{
				int[] rows = pre.getRows();
				for(int i: rows)
				{
					if(!rowsCovered.contains(i) && i > 0)
					{
						count++;
						rowsCovered.add(i);
					}
						
				}
				int[] cols = pre.getCols();
				for(int i: cols)
				{
					if(! colsCovered.contains(i) && i > 0)
					{
						count++;
						colsCovered.add(i);
					}
						
				}
				int name  = pre.getOperator();
				if(! opsCovered.contains(name))
				{
					count++;
					opsCovered.add(name);
				}
				
			}
			
			return (double) 4.0 / count;
		}
		
		/**
		 * Percentage of violations for DC
		 * @param dc
		 * @return
		 */
		private double perVios(NewDenialConstraint dc)
		{
			Set<NewPredicate> pres = new HashSet<NewPredicate>(dc.getPredicates());
			long numVios = 0;
			for(List<Boolean> info: tupleWiseInfo.keySet())
			{
				boolean containsAll = true;
				for (NewPredicate p : pres) {
					if (!info.get(presMap.indexOf(p))) {
						containsAll = false;
						break;
					}
				}
				if(containsAll)
				{
					numVios += tupleWiseInfo.get(info);
				}
				
			}
			
			return (double) numVios / originalTable.getNumRows();
			
		}
		
		@Override
		public void writeStats()
		{
			String statFile = "Experiments/ExpReport.CSV";
			String setting = originalTable.tableName;
			//setting += Config.qua;
			/*if(wastedWork!=0)
				assert(wastedWork2 ==0);
			if(wastedWork2!=0)
				assert(wastedWork==0);*/

			long averageDCSize = 0;
			long minDCSize = Long.MAX_VALUE;
			long maxDCSize = 0;
			for (NewDenialConstraint dc : totalDCs) {
				long dcSize = dc.getPredicates().size();
				if (dcSize > maxDCSize)
					maxDCSize = dcSize;
				if (dcSize < minDCSize)
					minDCSize = dcSize;
				averageDCSize += dcSize;
			}
			
			try {
				PrintWriter out = new PrintWriter(new FileWriter(statFile,true));
				
				String line = setting + "," 
						+ originalTable.getNumRows() + "," 
						+ originalTable.getNumCols() + "," 
						+ runningTime + ","
						+ presMap.size() + ","
						+ timeDFS/(numMinimalDCs+1) + "," //search time per DC in ms
 						+ (wastedWork+wastedWork2) + ","
						+ numMinimalDCs + ","
						+ this.timeInitTuplePair + ","
						+ this.timeDFS / 1000 + ","
						+ minDCSize + ","
						+ maxDCSize + ","
						+ averageDCSize + ","; //DFS time in s
				
				
				/*StringBuilder sb = new StringBuilder();
				for(int i = 0 ; i < Config.numTopks; i++)
				{
					int topk = Config.grak * (i+1);
					PR pr = getPRTopk(topk,new String((new StringBuilder(originalTable.getCurrentFolder())).append("ALLDCS")));
					//PR pr = new PR(0,0);
					if( i != Config.numTopks -1)
						sb.append(pr.precision +  "," + pr.recall + ",");
					else
						sb.append(pr.precision +  "," + pr.recall );
						
				}*/
				/*for(int i = 0 ; i < Config.numTopks; i++)
				{
					int Randomk = Config.grak * (i+1);
					//PR pr = getPRRandomk(Randomk);
					PR pr = new PR(0,0);
					sb.append(pr.precision + "," + pr.recall + ",");
				}*/
				//PR pr =  getPRTopk(Integer.MAX_VALUE);
				
				//sb.append(pr.precision + "," + pr.recall);
				
				
				//line += sb;
				
				
				
				out.println(line);
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
}
