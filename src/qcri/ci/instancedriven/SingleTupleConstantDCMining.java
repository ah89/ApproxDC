package qcri.ci.instancedriven;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import ca.pfv.spmf.frequentpatterns.apriori_optimized.Itemset;
import ca.pfv.spmf.frequentpatterns.fpgrowth_saveToFile.AlgoFPGrowth;

import qcri.ci.ConstraintDiscovery;
import qcri.ci.generaldatastructure.constraints.*;
import qcri.ci.generaldatastructure.db.*;
import qcri.ci.utils.CombinationGenerator;
import qcri.ci.utils.Config;

public class SingleTupleConstantDCMining{


	private Table table;
	public ArrayList<Predicate> consPres;
	
	
	public SingleTupleConstantDCMining(Table table,ArrayList<Predicate> consPres)
	{
		this.table = table;
		this.consPres = consPres;
		
	}
	public ArrayList<DenialConstraint> getSingleTupleDCs()
	{
		ArrayList<DenialConstraint> dcs = new ArrayList<DenialConstraint>();
		
		
		return dcs;
	}
	
	
	
	
}
