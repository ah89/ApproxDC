package ca.pfv.spmf.frequentpatterns.fpgrowth_saveToFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Xu
import ca.pfv.spmf.frequentpatterns.apriori_optimized.Itemset;


/**
 * This is an implementation of the FPGROWTH algorithm (Han et al., 2004) 
 * based on the description in the book of Han & Kamber.
 * 
 * This is the more optimized version that saves the result to a file.
 *
 * Copyright (c) 2008-2012 Philippe Fournier-Viger
 * 
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SPMF is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SPMF.  If not, see <http://www.gnu.org/licenses/>.
 */
public class AlgoFPGrowth {

	private long startTimestamp; // for stats
	private long endTime; // for stats
	public int relativeMinsupp;
	private int transactionCount =0; // for stats
	
	private int itemsetCount;
	BufferedWriter writer = null; 
	

	public AlgoFPGrowth() {
		
	}

	public void runAlgorithm(String input, String output, double minsupp) throws FileNotFoundException, IOException {
		startTimestamp = System.currentTimeMillis();
		itemsetCount =0;
		writer = new BufferedWriter(new FileWriter(output)); 
		
		// (1) PREPROCESSING: Initial database scan to determine the frequency of each item
		final Map<Integer, Integer> mapSupport = new HashMap<Integer, Integer>();
		scanDatabaseToDetermineFrequencyOfSingleItems(input, mapSupport);
		
		this.relativeMinsupp = (int) Math.ceil(minsupp * transactionCount);
		
		// (2) Scan the database again to build the initial FP-Tree
		// Before inserting a transaction in the FPTree, we sort the items
		// by descending order of support.  We ignore items that
		// do not have the minimum support.
		FPTree tree = new FPTree();
		
		BufferedReader reader = new BufferedReader(new FileReader(input));
		String line;
		while( ((line = reader.readLine())!= null)){ // for each transaction
			String[] lineSplited = line.split(" ");
			List<Integer> transaction = new ArrayList<Integer>();
			for(String itemString : lineSplited){  // for each item in the transaction
				Integer item = Integer.parseInt(itemString);
				// only add items that have the minimum support
				if(mapSupport.get(item) >= relativeMinsupp){
					transaction.add(item);	
				}
			}
			// sort item in the transaction by descending order of support
			Collections.sort(transaction, new Comparator<Integer>(){
				public int compare(Integer item1, Integer item2){
					int compare = mapSupport.get(item2) - mapSupport.get(item1);
					if(compare ==0){ // if the same frequency, we check the lexical ordering!
						return (item1 - item2);
					}
					return compare;
				}
			});
			// add the sorted transaction to the fptree.
			tree.addTransaction(transaction);
		}
		reader.close();
		
		// We create the header table for the tree
		tree.createHeaderList(mapSupport);
		
		// (5) We start to mine the FP-Tree by calling the recursive method.
		// Initially, the prefix alpha is empty.
		int[] prefixAlpha = new int[0];
		fpgrowth(tree, prefixAlpha, transactionCount, mapSupport);
		
		writer.close();
		endTime= System.currentTimeMillis();
	}

	private void scanDatabaseToDetermineFrequencyOfSingleItems(String input,
			final Map<Integer, Integer> mapSupport)
			throws FileNotFoundException, IOException {
		BufferedReader reader = new BufferedReader(new FileReader(input));
		String line;
		while( ((line = reader.readLine())!= null)){ // for each transaction
			String[] lineSplited = line.split(" ");
			for(String itemString : lineSplited){  // for each item in the transaction
				// increase the support count of the item
				Integer item = Integer.parseInt(itemString);
				Integer count = mapSupport.get(item);
				if(count == null){
					mapSupport.put(item, 1);
				}else{
					mapSupport.put(item, ++count);
				}
			}
			transactionCount++;
		}
		reader.close();
	}


	/**
	 * This method mines pattern from a Prefix-Tree recursively
	 * @param tree  The Prefix Tree
	 * @param prefix  The current prefix "alpha"
	 * @param mapSupport The frequency of each item in the prefix tree.
	 * @throws IOException 
	 */
	private void fpgrowth(FPTree tree, int[] prefixAlpha, int prefixSupport, Map<Integer, Integer> mapSupport) throws IOException {
		// We need to check if there is a single path in the prefix tree or not.
		// So first we check if there is only one item in the header table
		if(tree.headerList.size() == 1){
			FPNode node = tree.mapItemNodes.get(tree.headerList.get(0));
			// We need to check if this item has some node links.
			if(node.nodeLink == null){ 
				// That means that there is a single path, so we 
				// add all combinations of this path, concatenated with the prefix "alpha", to the set of patterns found.
				addAllCombinationsForPathAndPrefix(node, prefixAlpha); // CORRECT?
			}else{
				// There is more than one path
				fpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport);
			}
		}else{ // There is more than one path
			fpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport);
		}
	}
	
	/**
	 * Mine an FP-Tree having more than one path.
	 * @param tree  the FP-tree
	 * @param prefix  the current prefix, named "alpha"
	 * @param mapSupport the frequency of items in the FP-Tree
	 * @throws IOException 
	 */
	private void fpgrowthMoreThanOnePath(FPTree tree, int [] prefixAlpha, int prefixSupport, Map<Integer, Integer> mapSupport) throws IOException {
		// We process each frequent item in the header table list of the tree in reverse order.
		for(int i= tree.headerList.size()-1; i>=0; i--){
			Integer item = tree.headerList.get(i);
			
			int support = mapSupport.get(item);
			// if the item is not frequent, we skip it
			if(support <  relativeMinsupp){
				continue;
			}
			// Create Beta by concatening Alpha with the current item
			// and add it to the list of frequent patterns
			int [] beta = new int[prefixAlpha.length+1];
			System.arraycopy(prefixAlpha, 0, beta, 0, prefixAlpha.length);
			beta[prefixAlpha.length] = item;
			
			int betaSupport = (prefixSupport < support) ? prefixSupport: support;
			writeItemsetToFile(beta, betaSupport);
			
			// === Construct beta's conditional pattern base ===
			// It is a subdatabase which consists of the set of prefix paths
			// in the FP-tree co-occuring with the suffix pattern.
			List<List<FPNode>> prefixPaths = new ArrayList<List<FPNode>>();
			FPNode path = tree.mapItemNodes.get(item);
			while(path != null){
				// if the path is not just the root node
				if(path.parent.itemID != -1){
					// create the prefixpath
					List<FPNode> prefixPath = new ArrayList<FPNode>();
					// add this node.
					prefixPath.add(path);   // NOTE: we add it just to keep its support,
					// actually it should not be part of the prefixPath
					
					//Recursively add all the parents of this node.
					FPNode parent = path.parent;
					while(parent.itemID != -1){
						prefixPath.add(parent);
						parent = parent.parent;
					}
					prefixPaths.add(prefixPath);
				}
				// We will look for the next prefixpath
				path = path.nodeLink;
			}
			
			// (A) Calculate the frequency of each item in the prefixpath
			Map<Integer, Integer> mapSupportBeta = new HashMap<Integer, Integer>();
			// for each prefixpath
			for(List<FPNode> prefixPath : prefixPaths){
				// the support of the prefixpath is the support of its first node.
				int pathCount = prefixPath.get(0).counter;  
				for(int j=1; j<prefixPath.size(); j++){  // for each node, except the first one, we count the frequency
					FPNode node = prefixPath.get(j);
					if(mapSupportBeta.get(node.itemID) == null){
						mapSupportBeta.put(node.itemID, pathCount);
					}else{
						mapSupportBeta.put(node.itemID, mapSupportBeta.get(node.itemID) + pathCount);
					}
				}
			}
			
			// (B) Construct beta's conditional FP-Tree
			FPTree treeBeta = new FPTree();
			// add each prefixpath in the FP-tree
			for(List<FPNode> prefixPath : prefixPaths){
				treeBeta.addPrefixPath(prefixPath, mapSupportBeta, relativeMinsupp); 
			}  
			treeBeta.createHeaderList(mapSupportBeta); 
			
			// Mine recursively the Beta tree.
			if(treeBeta.root.childs.size() > 0){
				fpgrowth(treeBeta, beta, betaSupport, mapSupportBeta);
			}
		}
		
	}

	/**
	 * This method is for adding recursively all combinations of nodes in a path, concatenated with a given prefix,
	 * to the set of patterns found.
	 * @param nodeLink the first node of the path
	 * @param prefix  the prefix
	 * @param minsupportForNode the support of this path.
	 * @throws IOException 
	 */
	private void addAllCombinationsForPathAndPrefix(FPNode node, int[] prefix) throws IOException {
		// We add the node to the prefix

		int [] itemset = new int[prefix.length+1];
		System.arraycopy(prefix, 0, itemset, 0, prefix.length);
		itemset[prefix.length] = node.itemID;

		writeItemsetToFile(itemset, node.counter);
		
		// recursive call if there is a node link
		if(node.nodeLink != null){
			addAllCombinationsForPathAndPrefix(node.nodeLink, prefix);
			addAllCombinationsForPathAndPrefix(node.nodeLink, itemset);
		}
	}
	

	/**
	 * Write a frequent itemset that is found to the output file.
	 */
	private void writeItemsetToFile(int [] itemset, int support) throws IOException {
		itemsetCount++;
		StringBuffer buffer = new StringBuffer();
		// WRITE ITEMS
		for(int i=0; i< itemset.length; i++){
			buffer.append(itemset[i]);
			if(i != itemset.length-1){
				buffer.append(' ');
			}
		}
		buffer.append(':');
		// WRITE SUPPORT
		buffer.append(support);
		writer.write(buffer.toString());
		writer.newLine();
		
		
		//Xu: 
		//System.out.println("Xu1");
		Itemset itemset0 = new Itemset(itemset);
		itemset0.count = support;
		frequentItemsets.add(itemset0);
		
	}

	public void printStats() {
		System.out
				.println("=============  FP-GROWTH - STATS =============");
		long temps = endTime - startTimestamp;
		System.out.println(" Transactions count from database : " + transactionCount);
		System.out.println(" Frequent itemsets count : " + itemsetCount); 
		System.out.println(" Total time ~ " + temps + " ms");
		System.out
				.println("===================================================");
	}
	
	
	////////
	//Xu
	private ArrayList<Itemset> frequentItemsets = new ArrayList<Itemset>();
	private ArrayList<Itemset> kNonFrequentAllK_1FrequentItemsets = new ArrayList<Itemset>();
	
	
	public ArrayList<Itemset> getFrequentItemSets()
	{
		// sort frequent itemset in the order of their length
		Collections.sort(frequentItemsets, new Comparator<Itemset>(){
			public int compare(Itemset item1, Itemset item2){
				int compare = item1.items.length - item2.items.length;
				return compare;
			}
		});
		return frequentItemsets;
	}
	public ArrayList<Itemset> getKNonFrequentAllK_1FrequentItemsets()
	{
		//XU TODO: need to calculate
		for(int i = frequentItemsets.size() - 1 ; i >=0; i--)
		{
			
		}
		
		
		return this.kNonFrequentAllK_1FrequentItemsets;
	}
}
