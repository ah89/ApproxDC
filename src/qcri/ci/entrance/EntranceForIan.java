package qcri.ci.entrance;

import qcri.ci.ConstraintDiscovery;
import qcri.ci.instancedriven.ConstraintMining2;
import qcri.ci.utils.Config;

public class EntranceForIan {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//This is a set of configurable parameters
		
		//The data file Path, the name of the file must be inputDB
		//Example: C://xxx//xxx//inputDB
		String dataset = args[0]; 
		
		//Approximate level, the maximum # violations is: n^2 * Config.noiseTolerance
		//Suggested Value: [0,0.00001]
		//Default can set to 0
		Config.noiseTolerance = Double.valueOf(args[1]); 
		
		
		
		//The minimal frequency level for a constant predicate to be declared valid
		//Smaller value means more constant DCs, 1 means that no constant DCs at all
		//Suggested Value: [0.02,1]
		Config.kfre = Double.valueOf(args[2]);	
		
		ConstraintDiscovery cd = new ConstraintMining2(dataset,1,3,1,Integer.MAX_VALUE);
		cd.discoverEXCHKS();
		
		cd.initHeavyWork(Config.howInit); 
		cd.discover();
		
		
		
	}

}
