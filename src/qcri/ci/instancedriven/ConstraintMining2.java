package qcri.ci.instancedriven;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import qcri.ci.ConstraintDiscovery;
import qcri.ci.generaldatastructure.constraints.DenialConstraint;
import qcri.ci.generaldatastructure.constraints.Predicate;
import qcri.ci.generaldatastructure.db.Tuple;
import qcri.ci.utils.Config;
import qcri.ci.utils.FileUtil;

public class ConstraintMining2 extends ConstraintDiscovery{

		long totalEvi;
		Map<Set<Predicate>,Long> tupleWiseInfo;
		//Map<Set<Predicate>,Set<TP>> tupleWiseInfoTP;
		
		//The followings are parameters
		int initParallel;
		int mvcParallel;
		int dynamicOrdering;
		
		
		//The following are program running stats
		public long timeInitTuplePair = 0;
		public long timeGetOrdering = 0;
		public long timeDFS = 0;
		public long timeDFSPruning = 0;
		public long timePostProcessing = 0;
		
		
		public ConstraintMining2(String inputDBPath, int initParallel, int mvcParallel, int dynamicOrdering, int numRows) {
			super(inputDBPath, numRows);
			System.out.println("METHOD: DFS SEARCH: initParallel " + initParallel + " mvcParallel " + mvcParallel + " dynamicOrdering: " + dynamicOrdering);
			this.initParallel = initParallel;
			this.mvcParallel = mvcParallel;
			this.dynamicOrdering = dynamicOrdering;
		}
		
	
		public long getMaxVios()
		{
			//int aa =  (int) (totalEvi * Config.noiseTolerance * Config.noiseTolerance);
			//return (int) Math.sqrt(aa);
			return  ((long)(originalTable.getNumRows()  * Config.noiseTolerance) )
					*  originalTable.getNumRows();
		}
		
		public long getMaxVios(Predicate pre)
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
		public long getMaxVios(Set<Predicate> pres, Predicate pre)
		{
			boolean twoTuple = false;
			for(Predicate temp: pres)
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
		
		
		Map<Predicate,Map<Set<Predicate>,Long>> eviModPre = new HashMap<Predicate,Map<Set<Predicate>,Long>>();
		Map<Predicate,ArrayList<DenialConstraint>> pre2DCs = new HashMap<Predicate,ArrayList<DenialConstraint>>();
		Set<Predicate> special = new HashSet<Predicate>();
		private void calculateEviModulePre(ArrayList<DenialConstraint> dcs )
		{
			//Step 1: the basics
			for(Predicate curPre: allVarPre)
			{
				Map<Set<Predicate>,Long> curPreInfoSet = new HashMap<Set<Predicate>,Long>();
				for(Set<Predicate> info: tupleWiseInfo.keySet())
				{
					if(!info.contains(curPre))
						continue;
					Set<Predicate> temp = new HashSet<Predicate>(info);
					temp.remove(curPre);
					curPreInfoSet.put(temp, tupleWiseInfo.get(info));
				}
				
				eviModPre.put(curPre, curPreInfoSet);
				pre2DCs.put(curPre, new ArrayList<DenialConstraint>());
				
				
				//System.out.println(curPre.toString() +":" + eviModPre.get(curPre).size() );
				
			}
			
			//Step 2: Optimization technique
			for(Predicate curPre: allVarPre)
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
						/*ArrayList<Predicate> tempPres = new ArrayList<Predicate>();
						tempPres.add(curPre);
						DenialConstraint tempDC = new DenialConstraint(2,tempPres,this);
						if(coverage(tempDC) > 0.8)
						{
							
						}
						else
						{
							continue;
						}*/
					}
					else
					{
						if(eviModPre.get(curPre).size() != 0)
							continue;
					}
				
					
					//if(curPre.getName().equals("GTE") || curPre.getName().equals("LTE")
							//||(curPre.getName().equals("IQ")&& !originalTable.getColumnMapping().positionToType(curPre.getCols()[0]).equals("String"))
							//)
					//	continue;
						
					//Add a DC for this predicate
					ArrayList<Predicate> temp = new ArrayList<Predicate>();
					temp.add(curPre);
					DenialConstraint dc = new DenialConstraint(2, temp,this);
					/*if(consPres.size() != 0)
					{
						for(Predicate consPre: consPres)
							dc.addPreciate(consPre);
					}*/
					//dcs.add(dc);
					pre2DCs.get(curPre).add(dc);
					super.removeTriviality(pre2DCs.get(curPre));
					super.removeSubset(pre2DCs.get(curPre));
					dcs.addAll(pre2DCs.get(curPre));
					
						
					
					//Get Rid of the unnecessary DFS search
					eviModPre.remove(curPre);
					Predicate reverse = super.getReversePredicate(curPre);
					eviModPre.remove(reverse);
					for(Predicate reverseImp: super.getImpliedPredicates(reverse))
					{
						eviModPre.remove(reverseImp);
						
						//This is further pruning, to be verified!
						eviModPre.remove(super.getReversePredicate(reverseImp));	
						special.add(super.getReversePredicate(reverseImp));
						
					}
					
					special.add(reverse);
					special.addAll(super.getImpliedPredicates(reverse));
					
					special.add(curPre);
					
				}
			}
			System.out.println("The size of the allVarPre is: "  + allVarPre.size());
			System.out.println("The size of special predicates is: " + special.size());
			System.out.println("The size of the keys in eviModPre is: " + eviModPre.size());
			
		}
		
		@Override
		protected void discoverInternal(ArrayList<DenialConstraint> dcs,Map<Set<Predicate>,Long> tuplewiseinfo)
		{
			
			//Step 1: Initialization of tuple pair wise information
			System.out.println( (System.currentTimeMillis() - startingTime )/ 1000 + 
					": Done reading DB to memory");
			long time1 = System.currentTimeMillis();

			this.tupleWiseInfo = tuplewiseinfo;
			//this.tupleWiseInfoTP = tuplewiseinfoTP;
			totalEvi = 0;
			for(Set<Predicate> temp: tupleWiseInfo.keySet())
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
						ArrayList<Predicate> presInDC = new ArrayList<Predicate>();
						for(Predicate pre: allVarPre)
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
						dcs.add(new DenialConstraint(2,presInDC,this));
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
				
				discoverInternal_Opt2(dcs);
			}
			else
			{
				//Step 2: Calculate the evidence module each predicate
				this.calculateEviModulePre(dcs);
				
				
				//Step 3: Do a DFS search for each predicate
				if(this.mvcParallel == 3)
					discoverInternal_Opt1(dcs);
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
		
		
			postprocess(dcs);
			
			
		}
		
		
		
	
		int wastedWork = 0;
		int wastedWork2 = 0;
		int numImpliedDCsBeforeAdding = 0;
		private void discoverInternal_Opt1( ArrayList<DenialConstraint> dcs)
		{
			System.out.println("We are in DFS search Opitimized mode : subset pruning across DFS search trees");
			Set<Predicate> done = new HashSet<Predicate>();			
			done.addAll(eviModPre.keySet());
			//for(Predicate curPre: eviModPre.keySet())
			for(Predicate curPre: allVarPre)
			{
				if(!eviModPre.containsKey(curPre))
					continue;
				
				//if(curPre.getName().equals("GTE") || curPre.getName().equals("LTE")
				//		//||(curPre.getName().equals("IQ")&& !originalTable.getColumnMapping().positionToType(curPre.getCols()[0]).equals("String"))
				//		)
				//	continue;
				
				done.remove(curPre);
				
				Map<Set<Predicate>,Long> curPreInfoSet = eviModPre.get(curPre);
				Set<Set<Predicate>> allMVC = new HashSet<Set<Predicate>>();
				
				if(curPreInfoSet.size()!=0)
					getAllMVCDFS(allMVC,curPre, curPreInfoSet,done,dcs);
				else
					allMVC.add(new HashSet<Predicate>());
		
				for(Set<Predicate> mvc: allMVC)
				{
					//System.out.println(curPre.toString() + "MVC: " + mvc.iterator().next().toString());
					//Reverse the predicate 
					Set<Predicate> reversedMVC = new HashSet<Predicate>();
					for(Predicate pre: mvc)
					{
						Predicate aaa = super.getReversePredicate(pre);
						reversedMVC.add(aaa);
					}
						
					reversedMVC.add(curPre);
					
					DenialConstraint dc = new DenialConstraint(2, new ArrayList<Predicate>(reversedMVC),this);
					/*if(consPres.size() != 0)
					{
						for(Predicate consPre: consPres)
							dc.addPreciate(consPre);
					}*/
					
					//dcs.add(dc);
					//Test implication before adding
					//if(!super.linearImplication(new HashSet<DenialConstraint>(dcs),  dc))
					 pre2DCs.get(curPre).add(dc);
					
				}
				
				//super.removeTriviality(pre2DCs.get(curPre));
				int aa = super.removeSubset(pre2DCs.get(curPre));
				wastedWork += aa;
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
		private ArrayList<Predicate> orderTaxonomyTree(Set<Predicate> keysets)
		{
			ArrayList<Predicate> result = new ArrayList<Predicate>();
			return result;
		}
		
		//Do one shot DFS search
		private void discoverInternal_Opt2( ArrayList<DenialConstraint> dcs)
		{
			//Cannot find DCs containing predicate from done
			Set<Predicate> done = new HashSet<Predicate>();	
			done.addAll(special);
			/*for(Predicate curPre: eviModPre.keySet())
			{
				
				if(curPre.getName().equals("GTE") || curPre.getName().equals("LTE") 
						//||(curPre.getName().equals("IQ")&& !originalTable.getColumnMapping().positionToType(curPre.getCols()[0]).equals("String"))
						)
					done.add(curPre);
			}*/
			
			//for(Predicate curPre: allVarPre)
			//{
				//if(curPre.getName().equals("GTE") || curPre.getName().equals("LTE")
						//||(curPre.getName().equals("IQ")&& !originalTable.getColumnMapping().positionToType(curPre.getCols()[0]).equals("String"))
					//	)
					//done.add(curPre);
			//}
			
			Set<Set<Predicate>> allMVC = new HashSet<Set<Predicate>>();
			if(tupleWiseInfo.size()!=0)
				getAllMVCDFS(allMVC,null, tupleWiseInfo,done,dcs);
			else
				allMVC.add(new HashSet<Predicate>());
	
			for(Set<Predicate> mvc: allMVC)
			{
				//Reverse the predicate 
				Set<Predicate> reversedMVC = new HashSet<Predicate>();
				for(Predicate pre: mvc)
				{
					Predicate aaa = super.getReversePredicate(pre);
					reversedMVC.add(aaa);
				}
					
				//reversedMVC.add(curPre);
				
				DenialConstraint dc = new DenialConstraint(2, new ArrayList<Predicate>(reversedMVC),this);
				/*if(consPres.size() != 0)
				{
					for(Predicate consPre: consPres)
						dc.addPreciate(consPre);
				}*/
				
					dcs.add(dc);
				//pre2DCs.get(curPre).add(dc);
			}
			System.out.println((System.currentTimeMillis() - startingTime )/1000 + ": Before subset pruning");
			int aa = super.removeSubset(dcs);
			wastedWork += aa;
		}
		
		/**
		 * This should be a DFS search 
		 * @param
		 * @return
		 */
		protected void getAllMVCDFS(Set<Set<Predicate>> allMVCs, Predicate currentPre, Map<Set<Predicate>,Long> toBeCovered,Set<Predicate> myDone,
				ArrayList<DenialConstraint> dcs)
		{
			//Get the ordering of predicate, based on their coverage
			Set<Predicate> curMVC = new HashSet<Predicate>();
			ArrayList<Predicate> candidates = new ArrayList<Predicate>();
			for(Predicate temp: allVarPre)
			{
				for(Set<Predicate> key: toBeCovered.keySet())
					if(key.contains(temp))
					{
						Predicate reverse = super.getReversePredicate(temp);
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
			ArrayList<Predicate> ordering = getOrdering2(toBeCovered,candidates,curMVC, currentPre);
			//Map<Set<Predicate>,Long> alreadyCovered2curMVCOverlapping = new HashMap<Set<Predicate>,Long>();
			getAllMVCDFSRecursive(allMVCs,currentPre,toBeCovered,ordering,curMVC,myDone,dcs,null,toBeCovered);
		}
		int countPruningInDFS1 = 0;
		int countPruningInDFS2 = 0;
		int countPruningTrivial = 0;
		
		
		Set<DenialConstraint> dcsClosure = new HashSet<DenialConstraint>();
		
		private boolean pruningInDFS(Set<Set<Predicate>> allMVCs, Set<Predicate> curMVC,Predicate lastMVC,Predicate currentPre)
		{
			//Triviality Pruning
			/*Predicate reverse = super.getReversePredicate(lastMVC);
			Set<Predicate> revImplied = super.getImpliedPredicates(reverse);
			//revImplied.add(super.getReversePredicate(lastMVC));
			if(reverse == super.getReversePredicate(currentPre) 
					 || revImplied.contains(super.getReversePredicate(currentPre))
					)
			{
				countPruningTrivial++;
				return true;
			}
			
			for(Predicate temp: curMVC)
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
			for(Set<Predicate> tempMVC: allMVCs)
			{
				if(curMVC.containsAll(tempMVC))
				{
					countPruningInDFS1++;
					return true;
				}
				
			}
			//Transitive pruning
			/*Set<Predicate> tempCurMVC = new HashSet<Predicate>(curMVC);
			for(Predicate temp: curMVC)
			{
				tempCurMVC.remove(temp);
				Predicate reversedTemp = super.getReversePredicate(temp);
				tempCurMVC.add(reversedTemp);
				for(Set<Predicate> tempMVC: allMVCs)
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
			
			//Transitive Pruning for Order Predicate
			for(Predicate temp: curMVC)
			{
				tempCurMVC.remove(temp);
				Set<Predicate> tempImplied = super.getImpliedPredicates(temp);
				Set<Predicate> tempImpliedReversed = new HashSet<Predicate>();
				for(Predicate pp: tempImplied)
					tempImpliedReversed.add(super.getReversePredicate(pp));
				tempCurMVC.addAll(tempImpliedReversed);
				for(Set<Predicate> tempMVC: allMVCs)
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
		private boolean pruningInDFS2( Set<Predicate> curMVC, ArrayList<DenialConstraint> dcs, Predicate currentPre)
		{
			Set<Predicate> reversedMVC = new HashSet<Predicate>();
			for(Predicate pre: curMVC)
				reversedMVC.add(super.getReversePredicate(pre));
			for(DenialConstraint dc: dcs)
			{
				if(reversedMVC.containsAll(dc.getPredicates()))
				{
					 countPruningInDFS3++;
					 return true;
				}
			}
			
			
			return false;
		}
		
		
		
		private boolean pruningUsingDCClosure(Set<Predicate> curMVC,  Predicate currentPre)
		{
			ArrayList<Predicate> reversedMVC = new ArrayList<Predicate>();
			for(Predicate pre: curMVC)
				reversedMVC.add(super.getReversePredicate(pre));
			if(currentPre!=null)
				reversedMVC.add(currentPre);
			
			for(DenialConstraint dc: dcsClosure)
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
		protected void getAllMVCDFSRecursive(Set<Set<Predicate>> allMVCs,  Predicate currentPre,Map<Set<Predicate>,Long> toBeCovered,
				ArrayList<Predicate> ordering,Set<Predicate> curMVC,Set<Predicate> myDone, ArrayList<DenialConstraint> dcs,Predicate lastMVC,
				Map<Set<Predicate>,Long> OriginalToBeCovered)
		{
			
			
			//If the size of the branch gets too long, return
			/*if(Config.dfsLevel != -1)
			{
				if(curMVC.size() + 1 > Config.dfsLevel)
					return;
			}*/
			
			
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
					
				Set<Predicate> newMVC = new HashSet<Predicate>(curMVC);
				
				allMVCs.add(newMVC);
				
				//Create a new DC, and add it to the closure
				ArrayList<Predicate> reversedMVC = new ArrayList<Predicate>();
				for(Predicate pre: newMVC)
				{
					Predicate aaa = super.getReversePredicate(pre);
					reversedMVC.add(aaa);
				}
				if(currentPre!=null)	
					reversedMVC.add(currentPre);
				
				DenialConstraint dc = new DenialConstraint(2, reversedMVC,this);
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
					//If the remaining vios > the # evid, last Predicate cover, 
					// then need at least two predicates
					
				}
				
				
			}
			
			
			ArrayList<Predicate> remaining = new ArrayList<Predicate>(ordering);
			for(int i = 0 ; i < ordering.size(); i++)
			{
				
				Predicate curPre = ordering.get(i);
				remaining.remove(curPre);
				
				
				Map<Set<Predicate>,Long> nextToBeCovered = new HashMap<Set<Predicate>,Long>();
				
				for(Set<Predicate> temp2: toBeCovered.keySet())
				{
					if(!temp2.contains(curPre))
					{
						nextToBeCovered.put(temp2, toBeCovered.get(temp2));	
						
					}
				}
				
				
				//Copy MVC each time vs only keep one MVC copy
				curMVC.add(curPre);
				ArrayList<Predicate> nextOrdering = getOrdering2(nextToBeCovered,remaining, curMVC, currentPre);
				getAllMVCDFSRecursive(allMVCs,currentPre,nextToBeCovered,nextOrdering,
						curMVC,myDone,dcs,curPre,OriginalToBeCovered);
				curMVC.remove(curPre);
				
				
			}
		}
		
		
		private ArrayList<Predicate> getOrdering2(Map<Set<Predicate>,Long> toBeCovered,ArrayList<Predicate> candidate, Set<Predicate> curMVC, Predicate currentPre)
		{
			if(this.dynamicOrdering == 0)
			{
				ArrayList<Predicate> result = new ArrayList<Predicate>();
				for(Predicate pre: candidate)
				{
					//Get rid of trivial DC at this point
					boolean tagTrivial = false;
					Predicate reverse = super.getReversePredicate(pre);
					Set<Predicate> revImplied = super.getImpliedPredicates(reverse);
					//revImplied.add(super.getReversePredicate(lastMVC));
					if(reverse == super.getReversePredicate(currentPre) 
							 || revImplied.contains(super.getReversePredicate(currentPre))
							)
					{
						tagTrivial = true;
					}
					
					for(Predicate temp: curMVC)
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
					for(Set<Predicate> temp: toBeCovered.keySet())
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
				ArrayList<Predicate> result = new ArrayList<Predicate>();
				ArrayList<Long> coverage = new ArrayList<Long>();
				for(Predicate pre: candidate)
				{
					//Get rid of trivial DC at this point
					boolean tagTrivial = false;
					Predicate reverse = super.getReversePredicate(pre);
					Set<Predicate> revImplied = super.getImpliedPredicates(reverse);
					//revImplied.add(super.getReversePredicate(lastMVC));
					if(reverse == super.getReversePredicate(currentPre) 
							 || revImplied.contains(super.getReversePredicate(currentPre))
							)
					{
						tagTrivial = true;
					}
					
					for(Predicate temp: curMVC)
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
					for(Set<Predicate> temp: toBeCovered.keySet())
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
		private boolean minimalityTest(Set<Predicate> curMVC, Map<Set<Predicate>,Long> OriginalToBeCovered,Predicate currentPre)
		{
			if(Config.noiseTolerance != 0)
			{
				Set<Predicate> tempMVC = new HashSet<Predicate>(curMVC);
				for(Predicate leftOut: curMVC)
				{
					tempMVC.remove(leftOut);
					//Test whether temMVC is a cover or not
					long numVios = 0;
					for(Set<Predicate> key: OriginalToBeCovered.keySet())
					{
						boolean thisKeyCovered = false;
						for(Predicate pre: tempMVC)
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
				Set<Predicate> tempMVC = new HashSet<Predicate>(curMVC);
				for(Predicate leftOut: curMVC)
				{
					tempMVC.remove(leftOut);
					//Test whether temMVC is a cover or not
					boolean isCover = true;
					for(Set<Predicate> key: OriginalToBeCovered.keySet())
					{
						boolean thisKeyCovered = false;
						for(Predicate pre: tempMVC)
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
		protected void postprocess(ArrayList<DenialConstraint> dcs)
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
			for(DenialConstraint dc: dcs)
			{
				dc.interestingness = scoring(dc);
				if(dc.interestingness <  0)
				{
					System.out.println(dc.toString());
				}
			}
			 
			//Randomly shuffle the list
			Collections.shuffle(dcs);
			
			//Rank the totalDCs according to their interestingness score
			Collections.sort(dcs, new Comparator<DenialConstraint>()
					{
						public int compare(DenialConstraint arg0,
								DenialConstraint arg1) {
							if(arg0.interestingness > arg1.interestingness)
								return -1;
							else if(arg0.interestingness < arg1.interestingness)
								return 1;
							else
								return 0;
								
						}
				
					});
			
			
			
			
			
			myOwnWriteDC2File("dcs_BeforeImp",dcs);
			//int countImpli = super.minimalCover(dcs);	
			int countImpli = super.minimalCoverAccordingtoInterestingess(dcs);
			System.out.println((System.currentTimeMillis() - startingTime)/1000 + ": We have applied Implication for "  + countImpli + " times before writing to files" );
			
			
			//Remove Symmetric DCs
			int symReduction = this.removeSymmetryDCs(dcs);
			System.out.println("Symmetrica DC Reduction:  " + symReduction);
			
			myOwnWriteDC2File("dcs_AfterImp",dcs);
			
			
			ranking(dcs);
			
			/*if(!Config.testScoringFunctionOnly)
			{
				String srFile = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_BeforeImp"));
				String dtFile = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_All"));
				FileUtil.copyfile(srFile, dtFile);
			}*/
		}
		
		private void myOwnWriteDC2File(String fileName,ArrayList<DenialConstraint> dcs)
		{
			String aa = fileName;
			
			String dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append(aa));
		
			try {
				PrintWriter out = new PrintWriter(new FileWriter(dcPath));
				out.println("The set of Denial Constraints:");
				for(DenialConstraint dc: dcs)
				{
					out.println(dc.interestingness + ":" + dc.toString());
				}
				out.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
	
				
		}
		
		public void  ranking(ArrayList<DenialConstraint> dcs)
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
				for(DenialConstraint dc: dcs)
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
			Collections.sort(dcs, new Comparator<DenialConstraint>()
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
			Collections.sort(dcs, new Comparator<DenialConstraint>()
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
			Collections.sort(dcs, new Comparator<DenialConstraint>()
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
			Collections.sort(dcs, new Comparator<DenialConstraint>()
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
				Collections.sort(dcs, new Comparator<DenialConstraint>()
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
		}
		
		private double numViosTimesInter(DenialConstraint dc)
		{
			double numVios = dc.numVios;
			double maxVios = this.getMaxVios(new HashSet<Predicate>(dc.getPredicates()),dc.getPredicates().iterator().next());
			double ratio = 1 - numVios / maxVios;
			
			return ratio * (0.5 * dc.coverage + 0.5 * dc.mdl);
		}
		
		private double scoringExp(DenialConstraint dc, double a)
		{
			assert( a >= 0 && a <= 1);
			return a * dc.coverage + (1-a) * dc.mdl;
		}
		
		/**
		 * 
		 * @param dc
		 * @return
		 */
		public double scoring(DenialConstraint dc)
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
		private double coherenceScore(DenialConstraint dc)
		{
			double result = 0;
			for(Predicate pre: dc.getPredicates())
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
		private double coverage(DenialConstraint dc)
		{
			
			
			ArrayList<Predicate> pres = dc.getPredicates();
			double count = 0;
			
			long numVios = 0;
			for(Set<Predicate> info: tupleWiseInfo.keySet())
			{
				Set<Predicate> temp = new HashSet<Predicate>(pres);
				
				temp.retainAll(info);
				
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
		private double coverageUpper(Set<Predicate> pres, Predicate currentPres)
		{
			
			assert(pres.size() <= Config.dfsLevel);
			double sumWeight = 0;
			long allEvis = originalTable.getNumRows() * (originalTable.getNumRows()-1);
			long thisEvis = 0;
			
			int count = 0;
			for(Set<Predicate> info: tupleWiseInfo.keySet())
			{
				count++;
				if(count > 100)
				{
					break;
				}
				
				long negK = 0;
				for(Predicate pre: pres)
				{
					if(info.contains(pre))
						negK++;
				}
				if(info.contains(super.getReversePredicate(currentPres)))
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
		private double coverageUpper2(Set<Predicate> pres, Predicate currentPres)
		{
			
			assert(pres.size() <= Config.dfsLevel);
			double sumWeight = 0;
			long allEvis = originalTable.getNumRows() * (originalTable.getNumRows()-1);
			long thisEvis = 0;
			
			int count = 0;
			Map<Set<Predicate>,Long> myInfo = eviModPre.get(super.getReversePredicate(currentPres));
			for(Set<Predicate> info: myInfo.keySet())
			{
				count++;
				if(count > 1000)
				{
					//System.out.println("we break");
					break;
				}
				
				long negK = 1;
				for(Predicate pre: pres)
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
		private long getnumVios(DenialConstraint dc)
		{ArrayList<Predicate> pres = dc.getPredicates();
			long numVios = 0;
			for(Set<Predicate> info: tupleWiseInfo.keySet())
			{
				Set<Predicate> temp = new HashSet<Predicate>(pres);
				
				temp.retainAll(info);
				
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
		private double mdl(DenialConstraint dc)
		{
			ArrayList<Predicate> pres = dc.getPredicates();
			int count = 0;
			Set<Integer> colsCovered = new HashSet<Integer>();
			Set<Integer> rowsCovered = new HashSet<Integer>();
			Set<String> opsCovered = new HashSet<String>();
			for(Predicate pre: pres)
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
				String name  = pre.getName();
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
		private double mdlUpper(Set<Predicate> pres)
		{
			int count = 0;
			Set<Integer> colsCovered = new HashSet<Integer>();
			Set<Integer> rowsCovered = new HashSet<Integer>();
			Set<String> opsCovered = new HashSet<String>();
			for(Predicate pre: pres)
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
				String name  = pre.getName();
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
		private double perVios(DenialConstraint dc)
		{
			Set<Predicate> pres = new HashSet<Predicate>(dc.getPredicates());
			long numVios = 0;
			for(Set<Predicate> info: tupleWiseInfo.keySet())
			{
				if(info.containsAll(pres))
				{
					numVios += tupleWiseInfo.get(info);
				}
				
			}
			
			return (double) numVios / originalTable.getNumRows();
			
		}
		
		@Override
		public void writeStats()
		{
			String statFile = "Experiments/ExpReport.csv";
			String setting = originalTable.tableName;
			//setting += Config.qua;
			/*if(wastedWork!=0)
				assert(wastedWork2 ==0);
			if(wastedWork2!=0)
				assert(wastedWork==0);*/
			
			try {
				PrintWriter out = new PrintWriter(new FileWriter(statFile,true));
				
				String line = setting + "," 
						+ originalTable.getNumRows() + "," 
						+ originalTable.getNumCols() + "," 
						+ runningTime + ","
						+ allVarPre.size() + ","
						+ timeDFS/(numMinimalDCs+1) + "," //search time per DC in ms
 						+ (wastedWork+wastedWork2) + ","
						+ numMinimalDCs + ","
						+ this.timeInitTuplePair + ","
						+ this.timeDFS / 1000 + ","; //DFS time in s
				
				
				StringBuilder sb = new StringBuilder();
				for(int i = 0 ; i < Config.numTopks; i++)
				{
					int topk = Config.grak * (i+1);
					PR pr = getPRTopk(topk,new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcs_AfterImp")));
					//PR pr = new PR(0,0);
					if( i != Config.numTopks -1)
						sb.append(pr.precision +  "," + pr.recall + ",");
					else
						sb.append(pr.precision +  "," + pr.recall );
						
				}
				/*for(int i = 0 ; i < Config.numTopks; i++)
				{
					int Randomk = Config.grak * (i+1);
					//PR pr = getPRRandomk(Randomk);
					PR pr = new PR(0,0);
					sb.append(pr.precision + "," + pr.recall + ",");
				}*/
				//PR pr =  getPRTopk(Integer.MAX_VALUE);
				
				//sb.append(pr.precision + "," + pr.recall);
				
				
				line += sb;
				
				
				
				out.println(line);
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
}
