package qcri.ci.entrance;

import java.util.ArrayList;

import qcri.ci.ConstraintDiscovery;
import qcri.ci.experiment.ExpUtils;
import qcri.ci.instancedriven.ConstraintMining2;
import qcri.ci.utils.Config;

public class ClusterEntrance {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		ConstraintDiscovery cd;
		
		
		int numTuples = Integer.MAX_VALUE;
		Config.enableCrossColumn = false;
		System.out.println("Cross column: " + Config.enableCrossColumn);
		Config.howInit = 2;
		
		
		cd = new ConstraintMining2("Experiments/TaxGenerator/inputDB",1,3,1,numTuples);
		cd.initHeavyWork(Config.howInit); 
		cd.discoverEXCHKS();
		cd.discover();
	}

}
