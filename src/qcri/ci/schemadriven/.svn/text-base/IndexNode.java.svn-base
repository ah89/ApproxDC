package qcri.ci.schemadriven;

import qcri.ci.generaldatastructure.db.*;
import java.util.*;

public class IndexNode {

	Tuple tuple;
	Set<IndexNode> nexts = new HashSet<IndexNode>();
	
	
	/**
	 * This is to create a dummy node
	 */
	public IndexNode()
	{
		tuple = null;
	}
	
	public IndexNode(Tuple arg)
	{
		this.tuple = arg;
	}
	
	
	
	public static IndexNode joinIndexNode(IndexNode in1, IndexNode in2)
	{
		IndexNode in = new IndexNode();
		assert(in1.tuple == null);
		assert(in2.tuple == null);
		joinRecursive(in1,in2,in);
		
		//Clean up
		Set<IndexNode> toBeRemoved = new HashSet<IndexNode>();
		for(IndexNode next: in.nexts)
		{
			if(next.isEmpty())
			{
				toBeRemoved.add(next);
			}
		}
		in.nexts.removeAll(toBeRemoved);
		
		
		return in;
	}
	
	private static void joinRecursive(IndexNode in1, IndexNode in2, IndexNode in)
	{
		Set<Tuple> nextTuples = new HashSet<Tuple>();
		for(IndexNode temp: in1.nexts)
		{
			nextTuples.add(temp.tuple);
		}
		for(IndexNode temp: in2.nexts)
		{
			nextTuples.add(temp.tuple);
		}
		
		for(Tuple nextTuple: nextTuples)
		{	
			
			IndexNode location1 = in1.findIndexNode(nextTuple);
			IndexNode location2 = in2.findIndexNode(nextTuple);
			if(location1 == null || location2 == null)
				continue;
			
			IndexNode newNode = new IndexNode(nextTuple);
			in.nexts.add(newNode);
			
			joinRecursive(location1, location2,newNode);
		}
		
	}
	
	
	//From the current indexNode, find the index Node containing the tuple specified
	private IndexNode findIndexNode(Tuple arg)
	{
		if(tuple == arg)
			return this;
		
		Queue<IndexNode> queue = new LinkedList<IndexNode>();
		queue.addAll(nexts);
		
		
		while(!queue.isEmpty())
		{
			IndexNode cur = queue.remove();
			if(cur.tuple == arg)
				return cur;
			
			queue.addAll(cur.nexts);
			
		}
		
		return null;
		
	}
	

	
	public boolean isEmpty()
	{
		if(nexts.size() == 0)
			return true;
		else
			return false;
	}
	
	/**
	 * Print the current indexNode
	 */
	public void print()
	{
		Queue<IndexNode> queue = new LinkedList<IndexNode>();
		queue.addAll(nexts);
		
		while(!queue.isEmpty())
		{
			IndexNode cur = queue.remove();
			System.out.print(cur.tuple.tid + ":");
			for(IndexNode next: cur.nexts)
				System.out.print(next.tuple.tid + ",");
			System.out.println();
			queue.addAll(cur.nexts);
			
		}

		//Print this node as a graph
		
		
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		if(tuple == null)
			sb.append("curTuple: null" + "\n");
		else		
			sb.append("curTuple: " + tuple.tid + "\n");
		Queue<IndexNode> queue = new LinkedList<IndexNode>();
		queue.addAll(nexts);
		
		while(!queue.isEmpty())
		{
			IndexNode cur = queue.remove();
			//System.out.print(cur.tuple.tid + ":");
			sb.append(cur.tuple.tid + ":");
			for(IndexNode next: cur.nexts)
				//System.out.print(next.tuple.tid + ",");
				sb.append(next.tuple.tid + ",");
			sb.append("\n");	
			System.out.println();
			queue.addAll(cur.nexts);
			
		}
		return new String(sb);
	}
}
