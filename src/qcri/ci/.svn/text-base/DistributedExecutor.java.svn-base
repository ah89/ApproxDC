package qcri.ci;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import qcri.ci.generaldatastructure.constraints.Predicate;
import qcri.ci.generaldatastructure.db.Table;
import qcri.ci.generaldatastructure.db.Tuple;

public class DistributedExecutor{
	/*public static final String DATA_DIRECTORY = "/mnt/nfs/shared/data/";

	public static final String DATA_SUFFIX = ".csv";

	public static final String DATA_NAME = "Data";
	
	public static final String PREDICATE_SUFFIX = ".pred";
	
	public static final String SERVER_PATH = "/mnt/nfs/shared/Distributed/runServer.sh";
	
	public static final String CLIENT_PATH = "/mnt/nfs/shared/Distributed/runClientInBatch.py";
	
	public static final String CLEAR_ALL_CLIENT = "/mnt/nfs/shared/Distributed/stopClientInBatch.py";*/
	
	
	
   /* public static final String DATA_DIRECTORY = "/home/ubuntu/data";

    public static final String DATA_SUFFIX = ".csv";

    public static final String DATA_NAME = "TaxGenerator";
    
    public static final String PREDICATE_SUFFIX = ".pred";
    
    public static final String SERVER_PATH = "/home/ubuntu/Distributed/runServer.sh";
    
    public static final String CLIENT_PATH = "/home/ubuntu/Distributed/runClientInBatch.py";
    
    public static final String CLEAR_ALL_CLIENT = "/home/ubuntu/Distributed/stopClientInBatch.py";*/
	
	public static final String DATA_DIRECTORY = "/home/ubuntu/data/";

    public static final String DATA_SUFFIX = ".csv";

    public static final String DATA_NAME = "TaxGenerator";
    
    public static final String PREDICATE_SUFFIX = ".pred";
    
    public static final String SERVER_PATH = "/home/ubuntu/Distributed/runServer.sh";
    
    public static final String CLIENT_PATH = "/home/ubuntu/Distributed/runClientInBatch.py";
    
    public static final String CLEAR_ALL_CLIENT = "/home/ubuntu/Distributed/stopClientInBatch.py";



	public List<String> retValue = new ArrayList<String>();
	
	public  void dumpDataFiles(String tableName, List<Tuple> tuples,
			String header) throws IOException {
		FileWriter fileWriter = new FileWriter(DATA_DIRECTORY + DATA_NAME
				+ DATA_SUFFIX);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		try {
			bufferedWriter.write(header);
			bufferedWriter.write("\n");
			for (Tuple tuple : tuples) {
				bufferedWriter.write(tuple.toString());
				bufferedWriter.write("\n");
			}
		} finally {
			bufferedWriter.close();
		}
	}

	public  void dumpPredicate(String tableName, List<Predicate> predicates)
			throws IOException {
		FileWriter fileWriter = new FileWriter(DATA_DIRECTORY + DATA_NAME
				+ PREDICATE_SUFFIX);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		try {
			for (Predicate predicate : predicates) {
				bufferedWriter.write(predicate.toString());
				bufferedWriter.write("\n");
			}
		} finally {
			bufferedWriter.close();
		}
	}
	
	public  void simpleRunProcess(String[] args) throws IOException{
		System.out.println(args[0]+" is kicked off");
		Process child = Runtime.getRuntime().exec(args);
		BufferedReader br = new BufferedReader(new InputStreamReader(child.getInputStream()));
		String output = "";
		while ((output = br.readLine())!=null){
				System.out.println(output);
		}
		BufferedReader er = new BufferedReader(new InputStreamReader(child.getErrorStream()));
		while ((output = er.readLine())!=null){
				System.out.println(output);
		}
		System.out.println(args[0]+" is finished");

	}
	
	public  void runProcessandCatchOutput(String[] args) throws IOException{
		System.out.println(args[0]+" is kicked off");
		Process child = Runtime.getRuntime().exec(args);
		BufferedReader br = new BufferedReader(new InputStreamReader(child.getInputStream()));
		String output = "";
		BufferedReader er = new BufferedReader(new InputStreamReader(child.getErrorStream()));
		List<String> outputs = new ArrayList<String>();
		while ((output = br.readLine())!=null){
			if (output.startsWith("##")){
				System.out.println(output);
				continue;
			}
			retValue.add(output);
		}
		while ((output = er.readLine())!=null){
			System.out.println(output);
		}
		System.out.println(args[0]+" is finished");
	}
	
	public static String toPredicateString(ArrayList<Predicate> predicateList){
		String retValue = "";
		for (int i = 0; i < predicateList.size()-1; i++){
			retValue+=predicateList.get(i).toString()+":";
		}
		if (predicateList.size() != 0)
			retValue+=predicateList.get(predicateList.size()-1).toString();
		return retValue;
	}
	
	public  void execute(String workPath,Table currentTable, ArrayList<Predicate> predicateList, String outputPath) throws IOException, InterruptedException{
		final Table tmp = currentTable;
		String tableName = currentTable.getTableName();
		dumpDataFiles(tableName, currentTable.getTuples(),
				currentTable.getSchema());
		dumpPredicate(tableName, predicateList);	
		String[] clearargs = {CLEAR_ALL_CLIENT};
		simpleRunProcess(clearargs);
		Thread serverThread = new Thread(new Runnable()
		{
			public void run() {
				String[] args = { SERVER_PATH,(tmp.getNumRows()+1)+"" };
				try {
					runProcessandCatchOutput(args);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		serverThread.start();
		Thread.sleep(3000);
		String[] clientargs = { CLIENT_PATH };
		simpleRunProcess(clientargs);
		serverThread.join();
		System.out.println("writing to"+outputPath);
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));
		BufferedWriter copyWriter = new BufferedWriter(new FileWriter(outputPath+"~"));
		writer.write(toPredicateString(predicateList));
		writer.write("\n");
		copyWriter.write(toPredicateString(predicateList));
		copyWriter.write("\n");
		for (String str : retValue){
			writer.write(str);
			writer.write("\n");
			copyWriter.write(str);
			copyWriter.write("\n");
		}
		writer.close();
		copyWriter.close();
	}
}