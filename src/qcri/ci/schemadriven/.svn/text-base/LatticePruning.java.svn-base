package qcri.ci.schemadriven;

import java.util.*;

import qcri.ci.*;
import qcri.ci.generaldatastructure.constraints.*;
import qcri.ci.generaldatastructure.db.*;

public class LatticePruning extends ConstraintDiscovery{

	Map<Predicate, Set<IndexNode>> pre2In = new HashMap<Predicate,Set<IndexNode>>();
	
	public LatticePruning(String inputDBPath, int numRows) {
		super(inputDBPath, numRows);
		
		for(Predicate predicate: allVarPre)
		{
			Set<IndexNode> in =  buildIndexNode(predicate);
			if(in != null)
			{
				pre2In.put(predicate, in);
				System.out.println("Predicate: " + predicate.toString());
			}
					
		}
	
	
		Predicate p1 = null, p2 = null , p3 = null; // A> , B> , C>
		for(Predicate predicate: pre2In.keySet())
		{
			
			/*t1.A<t2.A
			t1.A>t2.A
			t1.B>t2.B
			t1.C>t2.C
			t1.B<t2.B
			t1.C<t2.C*/
			if(predicate.toString().equals("t1.A>t2.A"))
				p1 = predicate;
			else if(predicate.toString().equals("t1.B>t2.B"))
				p2 = predicate;
			else if(predicate.toString().equals("t1.C<t2.C"))
				p3 = predicate;
			
		}
		System.out.println("index node for " + p1.toString() + " is:");
		for(IndexNode temp: pre2In.get(p1))
		{
			temp.print();
			System.out.println("--");
		}
		System.out.println("index node for " + p2.toString() + " is:");
		for(IndexNode temp: pre2In.get(p2))
		{
			temp.print();
			System.out.println("--");
		}
		
		
		System.out.println("Join result for: " + p1.toString() + p2.toString());
		Set<IndexNode> in12 = joinTwoIndexNodes(pre2In.get(p1), pre2In.get(p2));
		for(IndexNode temp: in12)
		{
			temp.print();
			System.out.println("--");
		}
		
		System.out.println("Join result for: " + p1.toString() + p2.toString() + p3.toString());
		Set<IndexNode> in123 = joinTwoIndexNodes(pre2In.get(p3), in12);
		
		for(IndexNode temp: in123)
		{
			temp.print();
			System.out.println("--");
		}
		
	}
	
	public Set<IndexNode> joinTwoIndexNodes(Set<IndexNode> left, Set<IndexNode> right)
	{
		Set<IndexNode> result = new HashSet<IndexNode>();
		for(IndexNode temp1: left)
			for(IndexNode temp2: right)
			{
				IndexNode temp = IndexNode.joinIndexNode(temp1, temp2);
				result.add(temp);
			}
		return result;
	}
	
	public Set<IndexNode> buildIndexNode(Predicate arg)
	{
		
		//Sort all the tuples according to the attributes
		//arg is of the form t1.A >/< t2.A
		if(arg.isSecondCons())
			return null;
		if(arg.getCols()[0] != arg.getCols()[1]) // same column
			return null;
		if(arg.getRows()[0] == 0 || arg.getRows()[1] == 0)
			return null;
		
		
		int column = arg.getCols()[0] -1 ;
		if(arg.getName().equals("GT"))
		{
			return buildIndexNodeSameColGT(column);
		}
		else if(arg.getName().equals("LT"))
		{
			return buildIndexNodeSameColLT(column);
		}
		else if(arg.getName().equals("EQ"))
		{
			return buildIndexNodeSameColEQ(column);
		}
		else if(arg.getName().equals("IQ"))
		{
			return buildIndexNodeSameColIQ(column);
		}
		return null;
		
		
	}
	
	private Set<IndexNode> buildIndexNodeSameColGT(int column)
	{
		IndexNode result = new IndexNode();
		final int col = column;
		List<Tuple> tuples = originalTable.getTuples();
		//Rank the totalDCs according to their interestingness score
		Collections.sort(tuples, new Comparator<Tuple>()
				{
					public int compare(Tuple arg0,
							Tuple arg1) {
						if(arg0.getCell(col).greaterThan(arg1.getCell(col)))
							return -1;
						else if(arg0.getCell(col).isSameValue(arg1.getCell(col)))
							return 0;
						else
							return 1;
							
					}
			
				});
		//Build the indexNode 
		Set<IndexNode> curParents = new HashSet<IndexNode>();
		curParents.add(result);
		for(int i = 0; i < tuples.size(); i++)
		{
			//Build an indexnode for this tuple
			IndexNode ini = new IndexNode(tuples.get(i));
			if(i == 0)
			{
				for(IndexNode pa: curParents)
					pa.nexts.add(ini);
			}
			else
			{
				//find out [i - j], sharing the same value
				if(tuples.get(i).getCell(col).isSameValue(tuples.get(i-1).getCell(col)))
				{
					for(IndexNode pa: curParents)
						pa.nexts.add(ini);
					
				}
				else
				{
					//from [startIndex,endIndex] have the same value
					curParents = curParents.iterator().next().nexts;
					for(IndexNode pa: curParents)
						pa.nexts.add(ini);
				}
			}
			
		}
		Set<IndexNode> resultSet = new HashSet<IndexNode>();
		resultSet.add(result);
		return resultSet;
	}
	private Set<IndexNode> buildIndexNodeSameColLT(int column)
	{
		IndexNode result = new IndexNode();
		final int col = column;
		List<Tuple> tuples = originalTable.getTuples();
		//Rank the totalDCs according to their interestingness score
		Collections.sort(tuples, new Comparator<Tuple>()
				{
					public int compare(Tuple arg0,
							Tuple arg1) {
						if(arg0.getCell(col).greaterThan(arg1.getCell(col)))
							return 1;
						else if(arg0.getCell(col).isSameValue(arg1.getCell(col)))
							return 0;
						else
							return -1;
							
					}
			
				});
		//Build the indexNode 
		Set<IndexNode> curParents = new HashSet<IndexNode>();
		curParents.add(result);
		for(int i = 0; i < tuples.size(); i++)
		{
			IndexNode ini = new IndexNode(tuples.get(i));
			if(i == 0)
			{
				for(IndexNode pa: curParents)
					pa.nexts.add(ini);
			}
			else
			{
				//find out [i - j], sharing the same value
				if(tuples.get(i).getCell(col).isSameValue(tuples.get(i-1).getCell(col)))
				{
					for(IndexNode pa: curParents)
						pa.nexts.add(ini);
					
				}
				else
				{
					//from [startIndex,endIndex] have the same value
					curParents = curParents.iterator().next().nexts;
					for(IndexNode pa: curParents)
						pa.nexts.add(ini);
				}
			}
			
		}
		Set<IndexNode> resultSet = new HashSet<IndexNode>();
		resultSet.add(result);
		return resultSet;
	}
	
	private Set<IndexNode> buildIndexNodeSameColEQ(int column)
	{
		//Group the tuples according to the value
		final int col = column;
		List<Tuple> tuples = originalTable.getTuples();
		Map<String,Set<Tuple>> s2pars = new HashMap<String,Set<Tuple>>();
		for(Tuple tuple: tuples)
		{
			
			String value = tuple.getCell(col).getValue();
			if(!s2pars.containsKey(value))
			{
				Set<Tuple> a = new HashSet<Tuple>();
				a.add(tuple);
				s2pars.put(value, a);
			}
			else
			{
				Set<Tuple> a = s2pars.get(value);
				a.add(tuple);
				s2pars.put(value, a);
			}			
			
		}
		Set<IndexNode> resultSet = new HashSet<IndexNode>();
		for(Set<Tuple> partition: s2pars.values())
		{
			//Stripped partition
			if(partition.size() == 1)
				continue;
			
			
			//Create two IndexNode for it
			Tuple[] temp = partition.toArray(new Tuple[0]);
			IndexNode in1 = new IndexNode();
			IndexNode cur1 = in1;
			for(int i = 0; i < temp.length; i++)
			{
				cur1.nexts.add(new IndexNode(temp[i]));
				cur1 = cur1.nexts.iterator().next();
			}
			
			
			IndexNode in2 = new IndexNode();
			IndexNode cur2 = in2;
			for(int i = temp.length -1 ; i >= 0 ; i--)
			{
				cur2.nexts.add(new IndexNode(temp[i]));
				cur2 = cur2.nexts.iterator().next();
			}
			
			resultSet.add(in1);
			resultSet.add(in2);
		}
		return resultSet;
	}
	private Set<IndexNode> buildIndexNodeSameColIQ(int column)
	{
		//Group the tuples according to the value
		final int col = column;
		List<Tuple> tuples = originalTable.getTuples();
		Map<String,Set<Tuple>> s2pars = new HashMap<String,Set<Tuple>>();
		for(Tuple tuple: tuples)
		{
			
			String value = tuple.getCell(col).getValue();
			if(!s2pars.containsKey(value))
			{
				Set<Tuple> a = new HashSet<Tuple>();
				a.add(tuple);
				s2pars.put(value, a);
			}
			else
			{
				Set<Tuple> a = s2pars.get(value);
				a.add(tuple);
				s2pars.put(value, a);
			}			
			
		}
		Set<IndexNode> resultSet = new HashSet<IndexNode>();
		List<Set<Tuple>> partitions = new ArrayList<Set<Tuple>>();
		for(Set<Tuple> partition: s2pars.values())
		{
			partitions.add(partition);
		}
		
		/*IndexNode in1 = new IndexNode();
		Set<Set<IndexNode>> cur1 = new HashSet<Set<IndexNode>>();
		Set<IndexNode> temp1 = new HashSet<IndexNode>();
		temp1.add(in1);
		cur1.add(temp1);
		for(int i = 0; i < partitions.size(); i++)
		{
			Set<Set<IndexNode>> tempCur = new HashSet<Set<IndexNode>>();
			for(Set<IndexNode> pointer: cur1)
			{
				for(IndexNode aa: pointer)
				{
					for(Tuple tupleTemp: partitions.get(i))
					{
						aa.nexts.add(new IndexNode(tupleTemp));
					}
					
				}
				tempCur.add(pointer.iterator().next().nexts);
			}
			cur1.clear();
			cur1 = tempCur;
		}
		
		IndexNode in2 = new IndexNode();
		Set<Set<IndexNode>> cur2 = new HashSet<Set<IndexNode>>();
		Set<IndexNode> temp2 = new HashSet<IndexNode>();
		temp2.add(in2);
		cur2.add(temp2);
		for(int i = partitions.size() - 1 ; i >= 0 ; i--)
		{
			Set<Set<IndexNode>> tempCur = new HashSet<Set<IndexNode>>();
			for(Set<IndexNode> pointer: cur2)
			{
				for(IndexNode aa: pointer)
				{
					for(Tuple tupleTemp: partitions.get(i))
					{
						aa.nexts.add(new IndexNode(tupleTemp));
					}
					
				}
				tempCur.add(pointer.iterator().next().nexts);
			}
			cur2.clear();
			cur2 = tempCur;
		}*/
		IndexNode in1 = new IndexNode();
		Set<IndexNode> cur1 = new HashSet<IndexNode>();
		cur1.add(in1);
		for(int i = 0; i < partitions.size(); i++)
		{
			Map<Tuple,IndexNode> tempMap = new HashMap<Tuple,IndexNode>();
			for(Tuple tupleTemp: partitions.get(i))
			{
				tempMap.put(tupleTemp, new IndexNode(tupleTemp));
			}
			for(IndexNode aa: cur1)
			{
				for(Tuple tupleTemp: partitions.get(i))
				{
					aa.nexts.add(tempMap.get(tupleTemp));
				}
				
			}
			cur1 = cur1.iterator().next().nexts;
		}
		IndexNode in2 = new IndexNode();
		Set<IndexNode> cur2 = new HashSet<IndexNode>();
		cur2.add(in2);
		for(int i = partitions.size() - 1 ; i >= 0 ; i--)
		{
			Map<Tuple,IndexNode> tempMap = new HashMap<Tuple,IndexNode>();
			for(Tuple tupleTemp: partitions.get(i))
			{
				tempMap.put(tupleTemp, new IndexNode(tupleTemp));
			}
			for(IndexNode aa: cur2)
			{
				for(Tuple tupleTemp: partitions.get(i))
				{
					aa.nexts.add(tempMap.get(tupleTemp));
				}
				
			}
			cur2 = cur2.iterator().next().nexts;
		}
		
		resultSet.add(in1);
		resultSet.add(in2);
		
		
		
	
		return resultSet;
	}
}
