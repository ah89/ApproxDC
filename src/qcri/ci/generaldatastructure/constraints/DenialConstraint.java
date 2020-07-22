package qcri.ci.generaldatastructure.constraints;

import java.util.*;

import qcri.ci.ConstraintDiscovery;
import qcri.ci.ConstraintDiscovery3;
import qcri.ci.instancedriven.SingleTupleConstantDCMining;

public class DenialConstraint {

	private int numRows;
	
	
	ArrayList<Predicate> predicates = new ArrayList<Predicate>(); //This should be most succint form of DC
	
	//Set<Predicate> predicatesFullForm = new HashSet<Predicate>();//The full set of predicates that are implied by the set of predicates
	
	
	private ConstraintDiscovery cd;
	private ConstraintDiscovery3 cd3;
	
	private SingleTupleConstantDCMining cd_single;
	
	
	
	/*public DenialConstraint(int numRows, ConstraintDiscovery cd)
	{
		this.numRows = numRows;
		this.cd = cd;
	}*/
	
	public DenialConstraint(int numRows, ArrayList<Predicate> pres)
	{
		this.numRows = numRows;
		this.predicates = pres;
		//initMostSuc();
		
	}
	public DenialConstraint(int numRows, ArrayList<Predicate> pres, ConstraintDiscovery cd)
	{
		this.numRows = numRows;
		this.predicates = pres;
		this.cd = cd;
		
		//initMostSuc();
		
	}


	public DenialConstraint(int numRows, ArrayList<Predicate> pres, ConstraintDiscovery3 cd) {
		this.numRows = numRows;
		this.predicates = pres;
		this.cd3 = cd;

		//initMostSuc();
	}

	public DenialConstraint(int numRows, ArrayList<Predicate> pres, SingleTupleConstantDCMining cd_single) {
		this.numRows = numRows;
		this.predicates = pres;
		this.cd_single = cd_single;

		//initMostSuc();
	}


	

		/**
	 * Get the most succinct form of DC
	 */
	public boolean initMostSuc()
	{
		if(cd == null)
			return false;
		int size1 = predicates.size();
		
		//P1 implied P2...
		boolean changed = true;
		while(changed)
		{
			changed = false;
			for(Predicate pre: predicates)
			{
				Set<Predicate> implied = cd.getImpliedPredicates(pre); //doesn't contain itself
				changed = predicates.removeAll(implied);
				if(changed)
				{
					break;
				}
					
				
			}
		}
		int size2 = predicates.size();
		assert(size1 == size2);
		for(int i = 0 ; i < predicates.size();  i++)
		{
			changed = false;
			int j = 0;
			for(j = 0 ; j < predicates.size(); j++)
			{
				int sizeBefore = predicates.size();
				Predicate p1 = predicates.get(i);
				Predicate p2 = predicates.get(j);
				Set<Predicate> toBeRemoved = new HashSet<Predicate>();
				toBeRemoved.add(p1);
				toBeRemoved.add(p2);
				if(p1.equalExceptOp(p2))
				{
					if(   (p1.name.equals("LTE")&&p2.name.equals("GTE") )
					   || (p1.name.equals("GTE")&&p2.name.equals("LTE") )
					  )
					{
						for(Predicate pre: cd.getAllVarPredicates())
						{
							if(pre.equalExceptOp(p1) && pre.name.equals("EQ"))
							{
								predicates.removeAll(toBeRemoved);
								predicates.add(pre);
								break;
							}
						}
					}
					else if(   (p1.name.equals("IQ")&&p2.name.equals("GTE") )
							   || (p1.name.equals("GTE")&&p2.name.equals("IQ") )
							  )
							{
								for(Predicate pre: cd.getAllVarPredicates())
								{
									if(pre.equalExceptOp(p1) && pre.name.equals("GT"))
									{
										predicates.removeAll(toBeRemoved);
										predicates.add(pre);
										break;
									}
								}
							}
					else if(   (p1.name.equals("LTE")&&p2.name.equals("IQ") )
							   || (p1.name.equals("IQ")&&p2.name.equals("LTE") )
							  )
							{
								for(Predicate pre: cd.getAllVarPredicates())
								{
									if(pre.equalExceptOp(p1) && pre.name.equals("LT"))
									{
										predicates.removeAll(toBeRemoved);
										predicates.add(pre);
										break;
									}
								}
							}
				}
				if(predicates.size() != sizeBefore)
				{
					changed = true;
					break;
				}
			}
			if(changed)
			{
				i = -1;
			}
			
		}
			
		int size3 = predicates.size();
		
		if(size3 < size1)
			return true;
		else
			return false;
	}
	
	
	
	/**
	 * This method is only used by adding constant predicate to the DC
	 * @param pre
	 * @return
	 */
	public boolean addPreciate(Predicate pre)
	{
		return predicates.add(pre);
	}
	public boolean addPredicates(Set<Predicate> consPres)
	{
		return predicates.addAll(consPres);
	}
	public boolean removePredicate(Predicate pre)
	{
		return predicates.remove(pre);
	}
	public boolean removePredicates(Set<Predicate> pres)
	{
		return predicates.removeAll(pres);
	}
	public ArrayList<Predicate> getPredicates()
	{
		return predicates;
	}
	

	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("not(");
		/*for(Predicate pre: predicates)
		{
			sb.append(pre.toString());
			sb.append("&");
			
		}*/
		/*if(cd != null)
		{
			for(Predicate pre: cd.consPres)
			{
				if(predicates.contains(pre))
				{
					sb.append(pre.toString());
					sb.append("&");
				}
			}
			
			
			for(Predicate pre: cd.allVarPre)
			{
				if(predicates.contains(pre))
				{
					sb.append(pre.toString());
					sb.append("&");
				}
			}
		}
		
		
		if(cd_single !=null)
		{
			
			for(Predicate pre: cd_single.consPres)
			{
				if(predicates.contains(pre))
				{
					sb.append(pre.toString());
					sb.append("&");
				}
			}
		}
		
		if(cd == null && cd_single == null)
		{
			for(Predicate pre: predicates)
			{
				sb.append(pre.toString());
				sb.append("&");
			}
		}
		*/
		
		for(Predicate pre: predicates)
		{
			sb.append(pre.toString());
			sb.append("&");
		}
		
		
		sb.setCharAt(sb.length()-1, ')');
		//sb.append(")");
		return new String(sb);
	}
	
	/**
	 * The following methods are concerned with the quality of the constraint
	 */
	
	/**
	 * To test whether this DC is trivial
	 * @return
	 */
	public boolean isTrivial()
	{
		for(Predicate p1: predicates)
		{
			for(Predicate p2: predicates)
			{
				if(p1.contradict(p2)) 
				{
					return true;
				}
					
			}
		}
		/*for(Predicate p1: predicates)
		{
			Set<Predicate> contra = cd.getContraPredicates(p1);
			if(contra.retainAll(predicates));
			if(contra.size() > 0)
				return true;
		}*/
		return false;
	}
	/**
	 * To test whether this DC consists of all IQ predicates
	 * @return
	 */
	public boolean allIQ()
	{
		for(Predicate p: predicates)
		{
			if(!p.getName().equals("IQ"))
			{
				return false;
			}
				
		}
		return true;
	}
	
	public boolean allEQ()
	{
		for(Predicate p: predicates)
		{
			if(!p.getName().equals("EQ"))
			{
				return false;
			}
				
		}
		return true;
	}
	/**
	 * 
	 * @deprecated
	 * To test whether the current dc is conflicting with the passed-in dc
	 * The test is going to be so much easier if the predicates in dc are ordered
	 * @param dc
	 * @param preOrdering
	 * @return
	 */
	public boolean isConflicting(DenialConstraint dc,ArrayList<Predicate> preOrdering)
	{
		Map<Predicate,Integer> orderPre = new HashMap<Predicate,Integer>();
		for(int i = 0 ; i < preOrdering.size(); i++)
		{
			orderPre.put(preOrdering.get(i), i);
		}
		
		ArrayList<Predicate> pres1 = predicates;
		ArrayList<Predicate> pres2 = dc.getPredicates();
		if(pres1.size() != pres2.size())
			return false;
		
		Set<Predicate> result = new HashSet<Predicate>();
		int i = 0 ; 
		int j = 0 ;
		while(i < pres1.size() && j < pres2.size())
		{
			if(orderPre.get(pres1.get(i)) <= orderPre.get(pres2.get(j)))
			{
				result.add(pres1.get(i));
				i++;
			}
			else
			{
				result.add(pres2.get(j));
				j++;
			}
				
		}
		if(i < pres1.size())
		{
			while(i < pres1.size())
			{
				result.add(pres1.get(i));
				i++;
			}
				
		}
		else
		{
			while(j < pres2.size())
			{
				result.add(pres2.get(j));
				j++;
			}
		}
		if(result.size() != (pres2.size() + 1))
			return false;
		
		Set<Predicate> temp1 = new HashSet<Predicate>(result);
		Set<Predicate> temp2 = new HashSet<Predicate>(result);	
		temp1.removeAll(pres1);
		temp2.removeAll(pres2);
		assert(temp1.size() == 1);
		assert(temp2.size() == 1);
		if(temp1.iterator().next().contradict(temp2.iterator().next()))
			return true;
		else
			return false;
	}
	
	
	public double interestingness = -1;
	public double numVios = -1;
	public double coverage = -1;
	public double mdl = -1;
	
	

	
}
