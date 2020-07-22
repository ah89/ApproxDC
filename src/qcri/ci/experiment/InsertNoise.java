package qcri.ci.experiment;

import java.io.IOException;

import qcri.ci.generaldatastructure.db.Table;
import qcri.ci.utils.Config;

public class InsertNoise {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		String dataset = "TaxGenerator";
		int numTuples = 10000;
		
		Config.noiseLevel = 0.01;
		insertNoise(dataset,numTuples,0.5);
		
		
	}
	//Randomly insert noise into the tax data generator
	public static void insertNoise(String dataset, int numTuples, double type) throws IOException
	{
		
		String srcFile = "Experiments/" + dataset + "/" + "inputDB";
		String desFile = "Experiments/" + dataset + "/" + "inputDBNoise";
		
		
		Table table = new Table(srcFile,numTuples);
		table.insertNoise(type);
		
		table.dump2File(desFile);
	}

}
