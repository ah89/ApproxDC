package qcri.ci.schemadriven;

import qcri.ci.ConstraintDiscovery;
import qcri.ci.instancedriven.ConstraintMining2;
import qcri.ci.utils.Config;

public class TestLatticePruning {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String dataset = "Test";
		
		ConstraintDiscovery cd;
		Config.enableCrossColumn = false;
		
		
		cd = new LatticePruning("Experiments/" + dataset + "/" + "inputDB",10);
		
		
	}

}
