package qcri.ci.utils;

import java.io.*;

public class FileUtil{
	public static void copyfile(String srFile, String dtFile){
		try{
			File f1 = new File(srFile);
			File f2 = new File(dtFile);
			InputStream in = new FileInputStream(f1);
			
			//For Append the file.
//			OutputStream out = new FileOutputStream(f2,true);

			//For Overwrite the file.
			OutputStream out = new FileOutputStream(f2);

			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf)) > 0){
				out.write(buf, 0, len);
			}
			in.close();
			out.close();
			//System.out.println("File copied.");
		}
		catch(FileNotFoundException ex){
			System.out.println(ex.getMessage() + " in the specified directory.");
			System.exit(0);
		}
		catch(IOException e){
			System.out.println(e.getMessage());			
		}
	}
	
	
	/**
	 * Split the inputDB file to numFiles, copy the head of inputDB to each file
	 * @param input
	 * @param numFiles
	 */
	public static void split(String input, int numFiles)
	{
		int numTuples = 0;
		String head = null;
		boolean isHead = true;
		try {
			BufferedReader br1 = new BufferedReader(new FileReader(input));
			String line = null;
			while((line = br1.readLine()) != null)
			{
				if(isHead)
				{
					head = new String(line);
					isHead = false;
					continue;
				}
				numTuples++;
			}
			br1.close();
			
		} 
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			
			e.printStackTrace();
		}
		
		PrintWriter[] outs = new PrintWriter[numFiles];
		for(int i = 0 ; i < numFiles; i++)
		{
			try {
				outs[i] = new PrintWriter(new FileWriter(input.replace("inputDB.csv", "inputDB" + (i+1) + ".csv")));
				outs[i].println(head);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		int tuplesEachFile = numTuples/numFiles	;
		int temp = 0;
		int i = 0;
		try {
			BufferedReader br1 = new BufferedReader(new FileReader(input));
			String line = null;
			isHead = true;
			while((line = br1.readLine()) != null)
			{
				if(isHead)
				{
					head = new String(line);
					isHead = false;
					continue;
				}
				outs[i].println(line);
				temp ++;
				if(temp == tuplesEachFile && i	!= (numFiles - 1))
				{
					i++;
					temp = 0;
					
				}
			}
			br1.close();
			for(int j = 0 ;j < numFiles;j++)
				outs[j].close();
			
		} 
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			
			e.printStackTrace();
		}
		
		
	}


	/**
	 * 
	 * @param input
	 * @param numFiles
	 */
	public static void combine(String input, int numFiles)
	{
		try {
			PrintWriter out = new PrintWriter(new FileWriter(input.replace("inputDB.csv", "inputDB0.csv")));
			for(int i = 0 ; i < numFiles; i++)
			{
				try {
					BufferedReader br1 = new BufferedReader(new FileReader(input.replace("inputDB.csv", "outputDB" + (i+1) + ".csv")));
					String line = null;
					boolean isHead = true;
					String head = null;
					while((line = br1.readLine()) != null)
					{
						if(isHead)
						{
							head = new String(line);
							if(i == 0)
							{
								out.println(head);
							}
							isHead = false;
							continue;
						}
						out.println(line);
					}
					br1.close();
					
				} 
				catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (IOException e) {
					
					e.printStackTrace();
				}
			}
			out.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

	
	
	/**
	 * 
	 * @param file
	 * @param src
	 * @param des
	 * @param deleteFristRow
	 * @throws IOException 
	 */
	public static void replaceWithinFile(String file, char src, char des, boolean deleteFirstRow) throws IOException
	{
		File tempFile = File.createTempFile("buffer", ".tmp");
	    FileWriter fw = new FileWriter(tempFile);

	    Reader fr = new FileReader(file);
	    BufferedReader br = new BufferedReader(fr);

	    int mark = 0;
	    while(br.ready()) {
	    	if(mark == 0)
	    	{
	    		br.readLine();
	    		mark++;
	    	}
	    	else
	    		fw.write(br.readLine().replace(src,des) + "\n");
	    }

	    fw.close();
	    br.close();
	    fr.close();

	    File oldfile = new File(file);
	    oldfile.delete();
	    // Finally replace the original file.
	    tempFile.renameTo(oldfile);
	}
	
	
	/**
	 * Copy the first numTuples from the scrFile to the desFile
	 * @param srcFile
	 * @param desFile
	 */
	public static void firstKTuples(String srcFile, String desFile, int numTuples, int numAttri) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		PrintWriter out = new PrintWriter(new FileWriter(desFile));
		
		int count = -1;
		String line = null;
		while((line = br.readLine())!=null)
		{
			if(count <= numTuples)
			{
				if(numAttri == -1)
					out.println(line);
				else
				{
					String[] temp = line.split(",");
					StringBuilder sb = new StringBuilder();
					if(numAttri > temp.length)
						numAttri = temp.length;
					for(int i = 0 ; i < numAttri; i++)
					{
						sb.append(temp[i]);
						if(i != numAttri -1)
							sb.append(",");
					}
					out.println(sb);
					
				}
				count++;
			}
		}
		br.close();
		out.close();
		
	}
	
	public static void clearExpReportCSVFile(String head, String path)
	{
		PrintWriter out;
		try {
			out = new PrintWriter(new FileWriter(path));
			out.println(head);
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//String head =  "Dataset,NumOfTuples,NumOfAttrs,RunningTime";
		
	}
	
	
	public static void deleteFile(String file)
	{
		File f = new File(file);
		f.delete();
	}
}