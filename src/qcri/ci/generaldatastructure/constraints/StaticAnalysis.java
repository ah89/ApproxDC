package qcri.ci.generaldatastructure.constraints;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import qcri.ci.FASTDC;

public class StaticAnalysis {
	
	
	private FASTDC fastdc;
	
	public StaticAnalysis(FASTDC f)
	{
		fastdc = f;
	}
	
	
	public boolean linearImplication(Collection<DenialConstraint> dcs, DenialConstraint conse)
	{

		if(conse.isTrivial())
			return true;
		
	
		Set<Predicate> premise = new HashSet<Predicate>(conse.getPredicates());
		Set<Predicate> closure = linearGetClosure(dcs,premise);
		if(closure.contains(null))
			return true;
		else
			return false;
	}
	
	private Set<Predicate> linearGetClosure(Collection<DenialConstraint> dcs, Set<Predicate> premise)
	{
		//Init of the result
		Set<Predicate> result = new HashSet<Predicate>(premise);
		for(Predicate temp: premise)
		{
			result.addAll(fastdc.getImpliedPredicates(temp));
		}
		//allImplied(result);
			
		
		//A list of candidate DCs, n-1 predicates are already in result
		List<DenialConstraint> canDCs = new ArrayList<DenialConstraint>();
		
		//From predicate, to a set of DCs, that contain the key predicate
		Map<Predicate,Set<DenialConstraint>> p2dcs = new HashMap<Predicate,Set<DenialConstraint>>();
		//From DC, to a set of predicates, that are not yet included in the closure
		Map<DenialConstraint,Set<Predicate>> dc2ps = new HashMap<DenialConstraint,Set<Predicate>>();
		
		
		//for(Predicate pre: allPres)
			//p2dcs.put(pre, new HashSet<DenialConstraint>());
		//Init two maps
		for(DenialConstraint dc: dcs)
		{
			for(Predicate pre: dc.getPredicates())
			{
				if(p2dcs.containsKey(pre))
				{
					Set<DenialConstraint> value = p2dcs.get(pre);
					value.add(dc);
					p2dcs.put(pre, value);
				}
				else
				{
					Set<DenialConstraint> value = new HashSet<DenialConstraint>();
					value.add(dc);
					p2dcs.put(pre, value);
				}
				
				
			}
			dc2ps.put(dc, new HashSet<Predicate>());
		}
		for(Predicate pre: p2dcs.keySet())
		{
			
			if(result.contains(pre))
				continue;
			
			
			for(DenialConstraint tempDC: p2dcs.get(pre))
			{
				Set<Predicate> value = dc2ps.get(tempDC);
				value.add(pre);
				dc2ps.put(tempDC, value);
			}
		}
		//Init the candidate list
		for(DenialConstraint dc: dcs)
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
			DenialConstraint canDC = canDCs.remove(0);
			
			Set<Predicate> tailSet = dc2ps.get(canDC);
			
			//Why can be 0?
			if(tailSet.size() == 0)
				continue;
			
			Predicate tail = tailSet.iterator().next();
			
			Predicate reverseTail = fastdc.getReversePredicate(tail);
			
			
			Set<Predicate> toBeProcessed = new HashSet<Predicate>();
			toBeProcessed.add(reverseTail);
			toBeProcessed.addAll(fastdc.getImpliedPredicates(reverseTail));
			toBeProcessed.removeAll(result);
			

			for(Predicate reverseTailImp: toBeProcessed)
			{
				if(p2dcs.containsKey(reverseTailImp))
				{
					Set<DenialConstraint> covered = p2dcs.get(reverseTailImp);
					Set<DenialConstraint> toBeRemoved = new HashSet<DenialConstraint>();
					for(DenialConstraint tempDC: covered)
					{
						Set<Predicate> value = dc2ps.get(tempDC);
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
	public int initMinimalDC(List<DenialConstraint> dcs)
	{
		
		int result = 0;
		for(DenialConstraint dc: dcs)
		{
			
			boolean temp = dc.initMostSuc();
			if(temp)
			{
				System.out.println(dc.toString() + " is affected!");
				result++;
			}
				
		}
		return result;
	}
	
	/**
	 * Remove from the set, the set of trivial DCs
	 * @param dcs
	 * @return the number of trivial DCs
	 */
	public int removeTriviality(List<DenialConstraint> dcs)
	{
		Set<DenialConstraint> toBeRemoved = new HashSet<DenialConstraint>();
		for(DenialConstraint dc: dcs)
		{
			if(dc.isTrivial())
				toBeRemoved.add(dc);
		}
		dcs.removeAll(toBeRemoved);
		return toBeRemoved.size();
	}
	
	
	/**
	 * Remove from the set, the set of DCs that is a superset of some other DC in the set
	 * @param dcs
	 * @return the number of DCs removed
	 */
	public int removeSubset(List<DenialConstraint> dcs)
	{
		
		Set<DenialConstraint> toBeRemoved = new HashSet<DenialConstraint>();
		int result  = 0;
		boolean changed = true;
		while(changed)
		{
			changed = false;
			for(DenialConstraint dc1: dcs)
			{
				for(DenialConstraint dc2: dcs)
				{
					if(dc1 == dc2)
						continue;
					
					if(dc2.getPredicates().containsAll(dc1.getPredicates()))
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
	}
	
	/**
	 * 
	 * @param dcs
	 * @return
	 */
	public int minimalCover(List<DenialConstraint> dcs)
	{
		int result = 0;
		
		
		int sizeBefore = -1;
		while(sizeBefore != dcs.size())
		{
			sizeBefore = dcs.size();
			for(int i = 0 ; i < dcs.size(); i++)
			{
				Set<DenialConstraint> ante = new HashSet<DenialConstraint>(dcs);
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
		
		return result;
	}
	
	public int minimalCoverLinearly(List<DenialConstraint> dcs)
	{
		int result = 0;
		Set<DenialConstraint> toBeRemoved = new HashSet<DenialConstraint>();
		Set<DenialConstraint> tempDCs = new HashSet<DenialConstraint>(dcs);
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
	
}
