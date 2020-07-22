package ca.pfv.spmf.frequentpatterns.MSApriori_optimized;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ca.pfv.spmf.frequentpatterns.apriori_optimized.Itemset;

/**
 * This is an implementation of the MSApriori algorithm as described by :
 * 
 *    Bing Liu et al. (1999). Mining Association Rules with Multiple Minimum Supports, Proceedings of KDD 1999.
 * 
 * This implementation was made by Azadeh Soltani  based on the Apriori implementation by Philippe Fournier-Viger
 * 
 * Copyright (c) 2008-2012 Azadeh Soltani, Philippe Fournier-Viger
 * 
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 * 
 * SPMF is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SPMF. If not, see <http://www.gnu.org/licenses/>.
 */
public class AlgoMSApriori_saveToFile {

	protected int k; // level
	// az
	int MIS[];

	// stats
	protected long startTimestamp;
	protected long endTimestamp;
	private int itemsetCount;

	private int LSRelative;

	private List<Integer[]> database = null;
	
	// pfv
	// the  comparator that is used to compare the item ordering
	final Comparator<Integer> itemComparator;

	// write to file
	BufferedWriter writer = null;
	private double maxMemory;

	public AlgoMSApriori_saveToFile() {
		itemComparator = new Comparator<Integer>() {
			public int compare(Integer o1, Integer o2) {
				// pfv
				int compare  = MIS[o1] - MIS[o2];
				if(compare ==0){ // if the same MIS, we check the lexical ordering!
					return (o1 - o2);
				}
				return compare;
			}
		};
	}

	// --------------------------------------------------------------------------------------------
	public void runAlgorithm(String input, String output,
			double beta, double LS) throws IOException { // pfv
		startTimestamp = System.currentTimeMillis();
		writer = new BufferedWriter(new FileWriter(output));
		itemsetCount = 0;
		maxMemory = 0;
		// az
		int maxItemID = -1; // pfv

		int transactionCount = 0;
		// map to count the support of each item
		Map<Integer, Integer> mapItemCount = new HashMap<Integer, Integer>(); 
		
		 // the database in memory (intially empty)
		database = new ArrayList<Integer[]>();
		
		// scan the database to load it into memory and count the support of
		// each single item at the same time
		BufferedReader reader = new BufferedReader(new FileReader(input));
		String line;
		while (((line = reader.readLine()) != null)) { // for each transaction
			String[] lineSplited = line.split(" ");

			Integer transaction[] = new Integer[lineSplited.length];

			for (int i = 0; i < lineSplited.length; i++) { // for each item in
															// the
				// transaction
				// increase the support count of the item
				Integer item = Integer.parseInt(lineSplited[i]);
				transaction[i] = item;
				Integer count = mapItemCount.get(item);
				if (count == null) {
					mapItemCount.put(item, 1);
					
					// pfv
					if (item > maxItemID) {
						maxItemID = item;
					}
				} else {
					mapItemCount.put(item, ++count);
				}
			}

			database.add(transaction);
			transactionCount++;
		}
		reader.close();
		
		// az *********************************
		MIS = new int[maxItemID + 1];

		this.LSRelative = (int) Math.ceil(LS * transactionCount);   // pfv

		k = 1;

		// all frequent items are added to the set of candidate
		List<Integer> M = new ArrayList<Integer>();
		for (Entry<Integer, Integer> entry : mapItemCount.entrySet()) {
			M.add(entry.getKey());
			// calculate the MIS value
			MIS[entry.getKey()] = (int) (beta * entry.getValue());
			if (MIS[entry.getKey()] < LSRelative){
				MIS[entry.getKey()] = LSRelative;  // pfv	
			}
			if (entry.getValue() >= MIS[entry.getKey()]) {
				saveItemsetToFile(entry.getKey(), entry.getValue());
			}
		}
		// sort the list of items by MIS order
		Collections.sort(M, itemComparator); //pfv
		
//		for(int i=0; i< MIS.length; i++){
//			System.out.println(i + " " + MIS[i]);
//		}

		// if no frequent item was found, we stop there!
		if (itemsetCount == 0) {
			return;
		}

		// create the set F
		List<Integer> F = new ArrayList<Integer>();
		double minMIS = -1;                                     // pfv   
		int i;
		for (i = 0; i < M.size(); i++) {
			Integer item = M.get(i);
			if (mapItemCount.get(item) >= MIS[item]) {
				F.add(item);
				minMIS = MIS[item];
				break;
			}// if
		}// for
		for (i++; i < M.size(); i++) {
			Integer item = M.get(i);
			if (mapItemCount.get(item) >= minMIS){
				F.add(item);
			}
		}// forj

//		mapItemCount = null;

		// end az ***********************************
		
		// sort the database  by  MIS order
		for (Integer[] transaction : database) {   //pfv
			Arrays.sort(transaction, itemComparator);  //pfv
		}
		
		List<Itemset> level = null;
		k = 2;
		// While the level is not empty
		do {
			checkMemory();
			
			// Generate candidates of size K
			List<Itemset> candidatesK;

			if (k == 2) {
				candidatesK = generateCandidate2(F, mapItemCount);
			} else {
				candidatesK = generateCandidateSizeK(level);
			}

			// We scan the database one time to calculate the support
			// of each candidates and keep those with higher suport.
			for (Integer[] transaction : database) {
								
				loopCand: for (Itemset candidate : candidatesK) {
					int pos = 0;
					for (int item : transaction) {
						if (item == candidate.items[pos]) {
							pos++;
							if (pos == candidate.items.length) {
								candidate.count++;
								continue loopCand;
							}
						} else if (itemComparator.compare(item, candidate.items[pos]) > 0){   // pfv 
							continue loopCand;
						}
					}
				}
			}

			// We build the level k+1 with all the candidates that have
			// a support higher than MIS[0]
			level = new ArrayList<Itemset>();
			for (Itemset candidate : candidatesK) {
				// az
				if (candidate.getAbsoluteSupport() >= MIS[candidate.items[0]]) {
					level.add(candidate);
					saveItemsetToFile(candidate);
				}
			}
			k++;
		} while (level.isEmpty() == false);

		endTimestamp = System.currentTimeMillis();
		checkMemory();

		writer.close();
	}
	
	/**
	 * inputMIS[itemIndex] = the minimum support of the item
	 * @param input
	 * @param output
	 * @param inputMIS
	 * @throws IOException
	 */
	public void runAlgorithm(String input, String output, double[] inputMIS) throws IOException { // pfv
		startTimestamp = System.currentTimeMillis();
		writer = new BufferedWriter(new FileWriter(output));
		itemsetCount = 0;
		maxMemory = 0;
		// az
		int maxItemID = -1; // pfv

		int transactionCount = 0;
		// map to count the support of each item
		Map<Integer, Integer> mapItemCount = new HashMap<Integer, Integer>(); 
		
		 // the database in memory (intially empty)
		database = new ArrayList<Integer[]>();
		System.out.println("Inside Apriori: Starting reading file into memory" );
		// scan the database to load it into memory and count the support of
		// each single item at the same time
		BufferedReader reader = new BufferedReader(new FileReader(input));
		String line;
		while (((line = reader.readLine()) != null)) { // for each transaction
			String[] lineSplited = line.split(" ");

			Integer transaction[] = new Integer[lineSplited.length];

			for (int i = 0; i < lineSplited.length; i++) { // for each item in
															// the
				// transaction
				// increase the support count of the item
				Integer item = Integer.parseInt(lineSplited[i]);
				transaction[i] = item;
				Integer count = mapItemCount.get(item);
				if (count == null) {
					mapItemCount.put(item, 1);
					
					// pfv
					if (item > maxItemID) {
						maxItemID = item;
					}
				} else {
					mapItemCount.put(item, ++count);
				}
			}

			database.add(transaction);
			transactionCount++;
		}
		reader.close();
		System.out.println("Inside Apriori: Done reading file into memory" );
		// az *********************************
		MIS = new int[maxItemID + 1];

		//this.LSRelative = (int) Math.ceil(LS * transactionCount);   // pfv

		k = 1;

		// all frequent items are added to the set of candidate
		List<Integer> M = new ArrayList<Integer>();
		for (Entry<Integer, Integer> entry : mapItemCount.entrySet()) {
			M.add(entry.getKey());
			// calculate the MIS value, XU
			//System.out.println("The item is: " + entry.getKey());
			MIS[entry.getKey()] = (int) (inputMIS[entry.getKey()] * database.size());
			if (entry.getValue() >= MIS[entry.getKey()]) {
				saveItemsetToFile(entry.getKey(), entry.getValue());
			}
		}
		// sort the list of items by MIS order
		Collections.sort(M, itemComparator); //pfv
		
//		for(int i=0; i< MIS.length; i++){
//			System.out.println(i + " " + MIS[i]);
//		}

		// if no frequent item was found, we stop there!
		if (itemsetCount == 0) {
			return;
		}

		// create the set F
		List<Integer> F = new ArrayList<Integer>();
		double minMIS = -1;                                     // pfv   
		int i;
		for (i = 0; i < M.size(); i++) {
			Integer item = M.get(i);
			if (mapItemCount.get(item) >= MIS[item]) {
				F.add(item);
				minMIS = MIS[item];
				break;
			}// if
		}// for
		for (i++; i < M.size(); i++) {
			Integer item = M.get(i);
			if (mapItemCount.get(item) >= minMIS){
				F.add(item);
			}
		}// forj

//		mapItemCount = null;

		// end az ***********************************
		
		// sort the database  by  MIS order
		for (Integer[] transaction : database) {   //pfv
			Arrays.sort(transaction, itemComparator);  //pfv
		}
		
		List<Itemset> level = null;
		k = 2;
		// While the level is not empty
		do {
			checkMemory();
			
			// Generate candidates of size K
			List<Itemset> candidatesK;

			if (k == 2) {
				candidatesK = generateCandidate2(F, mapItemCount);
			} else {
				candidatesK = generateCandidateSizeK(level);
			}

			// We scan the database one time to calculate the support
			// of each candidates and keep those with higher suport.
			for (Integer[] transaction : database) {
								
				loopCand: for (Itemset candidate : candidatesK) {
					int pos = 0;
					for (int item : transaction) {
						if (item == candidate.items[pos]) {
							pos++;
							if (pos == candidate.items.length) {
								candidate.count++;
								continue loopCand;
							}
						} else if (itemComparator.compare(item, candidate.items[pos]) > 0){   // pfv 
							continue loopCand;
						}
					}
				}
			}

			// We build the level k+1 with all the candidates that have
			// a support higher than MIS[0]
			level = new ArrayList<Itemset>();
			for (Itemset candidate : candidatesK) {
				// az
				if (candidate.getAbsoluteSupport() >= MIS[candidate.items[0]]) {
					level.add(candidate);
					saveItemsetToFile(candidate);
				}
				//Xu
				else
				{
					this.kNonFrequentAllK_1FrequentItemsets.add(candidate);
				}
			}
			k++;
			System.out.println("Inside Apriori: Done Level " + k );
		} while (level.isEmpty() == false);

		endTimestamp = System.currentTimeMillis();
		checkMemory();

		writer.close();
	}

	// ------------------------------------------------------------------------------

	private List<Itemset> generateCandidate2(List<Integer> frequent1, Map<Integer, Integer> mapItemCount) {
		List<Itemset> candidates = new ArrayList<Itemset>();

		// For each itemset I1 and I2 of level k-1
		for (int i = 0; i < frequent1.size(); i++) {
			Integer item1 = frequent1.get(i);
//			if(mapItemCount.get(item1) < MIS[item1]){  // pfv
//				continue;
//			}
			
			for (int j = i + 1; j < frequent1.size(); j++) {
				Integer item2 = frequent1.get(j);
//				if(mapItemCount.get(item2) < MIS[item2]){   // pfv
//					continue;
//				}

				// Create a new candidate by combining itemset1 and itemset2
				candidates.add(new Itemset(new int[] { item1, item2 }));
			}
		}
		
		mapItemCount = null; // don't need it anymore
		
		return candidates;
	}

	// --------------------------------------------------------------------------------------------
	protected List<Itemset> generateCandidateSizeK(List<Itemset> levelK_1) {
		List<Itemset> candidates = new ArrayList<Itemset>();

		// For each itemset I1 and I2 of level k-1
		loop1: for (int i = 0; i < levelK_1.size(); i++) {
			int[] itemset1 = levelK_1.get(i).items;
			loop2: for (int j = i + 1; j < levelK_1.size(); j++) {
				int[] itemset2 = levelK_1.get(j).items;

				// we compare items of itemset1 and itemset2.
				// If they have all the same k-1 items and the last item of
				// itemset1 is smaller than the last item of itemset2, we will combine them to generate a
				// candidate

				// az******************************
				for (int k = 0; k < itemset1.length; k++) {
					// if they are the last items
					if (k == itemset1.length - 1) {
						// the one from itemset1 should be smaller (lexical
						// order)
						// and different from the one of itemset2
						if(itemComparator.compare(itemset1[k], itemset2[k]) > 0){  // pfv  
							continue loop1;
						}
					}
					// if they are not the last items, and
					else if (itemset1[k] != itemset2[k]){
						if(itemComparator.compare(itemset1[k], itemset2[k]) < 0){  // pfv
							continue loop2; // we continue searching
						} else if (itemComparator.compare(itemset1[k], itemset2[k]) > 0) { // pfv
							continue loop1; // we stop searching: because of MIS order
						}
					}
				}
				// end az******************************
				// Create a new candidate by combining itemset1 and itemset2
				int newItemset[] = new int[itemset1.length + 1];
				System.arraycopy(itemset1, 0, newItemset, 0, itemset1.length);
				newItemset[itemset1.length] = itemset2[itemset2.length - 1];

				// The candidate is tested to see if its subsets of size k-1 are
				// included in
				// level k-1 (they are frequent).
				if (allSubsetsOfSizeK_1AreFrequent(newItemset, levelK_1)) {
					candidates.add(new Itemset(newItemset));
				}
			}
		}
		return candidates;
	}

	// --------------------------------------------------------------------------------------------
	protected boolean allSubsetsOfSizeK_1AreFrequent(int[] c, List<Itemset> levelK_1) {
		// generate all subsets by always each item from the candidate, one by
		// one
		for (int posRemoved = 0; posRemoved < c.length; posRemoved++) {
			// az ******************************
			// if it does not contain first item of candidate and
			// MIS(c[0])!=MIS(c[1]) there is no need to check
			
			if ((posRemoved == 0) && MIS[c[0]] != MIS[c[1]]) {
				continue;
			}
			// end az******************************
			
			// the binary search
			// perform a binary search to check if the subset appears in level
			// k-1.
			int first = 0;
			int last = levelK_1.size() - 1;
			
			boolean found = false;
			
			 // the binary search
	        while( first <= last )
	        {
	        	int middle = ( first + last ) / 2;

	            if(sameAs(levelK_1.get(middle), c, posRemoved)  < 0 ){
	            	first = middle + 1;  //  the itemset compared is larger than the subset according to the lexical order
	            }
	            else if(sameAs(levelK_1.get(middle), c, posRemoved)  > 0 ){
	            	last = middle - 1; //  the itemset compared is smaller than the subset  is smaller according to the lexical order
	            }
	            else{
	            	found = true; //  we have found it so we stop
	                break;
	            }
	        }

	        if (found == false) { // if we did not find it, that means that
									// candidate is not a frequent itemset
									// because
				// at least one of its subsets does not appear in level k-1.
				return false;
			}
		}
		return true;
	}

	// --------------------------------------------------------------------------------------------
	private int sameAs(Itemset itemset, int[] candidate, int posRemoved) {
		int j = 0;
		for (int i = 0; i < itemset.items.length; i++) {
			if (j == posRemoved) {
				j++;
			}
			// az---------------------------------------------------
			if (itemset.items[i] == candidate[j]) {
				j++;
			}else{
				return itemComparator.compare(itemset.items[i], candidate[j]);  // pfv
			}
		}
		return 0;
	}

	// --------------------------------------------------------------------------------------------
	public void saveItemsetToFile(Itemset itemset) throws IOException {
		writer.write(itemset.toString() + " Support: "	+ itemset.getAbsoluteSupport());
		writer.newLine();
		itemsetCount++;
		
		//Xu: 
				//System.out.println("Xu1");
				frequentItemsets.add(itemset);
	}

	// --------------------------------------------------------------------------------------------
	public void saveItemsetToFile(Integer item, Integer support)
			throws IOException {
		writer.write(item + " supp: " + support);
		writer.newLine();
		itemsetCount++;
		
		//Xu:
				int[] items = new int[1];
				items[0] = item;
				Itemset itemset = new Itemset(items);
				frequentItemsets.add(itemset);
	}

	// --------------------------------------------------------------------------------------------
	private void checkMemory() {
		double currentMemory = ((double) ( (Runtime.getRuntime()
				.totalMemory() / 1024) / 1024))
				- ((double) (Runtime.getRuntime().freeMemory() / 1024) / 1024);
		if (currentMemory > maxMemory) {
			maxMemory = currentMemory;
		}
	}

	// --------------------------------------------------------------------------------------------
	public void printStats() {
		System.out.println("=============  MSAPRIORI - STATS =============");
		System.out.println(" The algorithm stopped at level " + (k - 1)
				+ ", because there is no candidate");
		System.out.println(" Frequent itemsets count : " + itemsetCount);
		System.out.println(" Maximum memory usage : " + maxMemory + " mb");
		System.out.println(" Total time ~ " + (endTimestamp - startTimestamp)
				+ " ms");
		System.out
				.println("===================================================");
	}
	
	
	////////
	//Xu
	private ArrayList<Itemset> frequentItemsets = new ArrayList<Itemset>();
	private ArrayList<Itemset> kNonFrequentAllK_1FrequentItemsets = new ArrayList<Itemset>();
	
	
	public ArrayList<Itemset> getFrequentItemSets()
	{
		return frequentItemsets;
	}
	public ArrayList<Itemset> getKNonFrequentAllK_1FrequentItemsets()
	{
		return this.kNonFrequentAllK_1FrequentItemsets;
	}
}
