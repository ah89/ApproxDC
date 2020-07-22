package qcri.ci.generaldatastructure.db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.*;

import com.sun.corba.se.spi.orb.StringPair;
import qcri.ci.generaldatastructure.constraints.*;
import qcri.ci.generaldatastructure.db.*;
import qcri.ci.utils.CombinationGenerator;
import qcri.ci.utils.CombinationGeneratorWithOrder;
import qcri.ci.utils.Config;
import qcri.ci.utils.IntegerPair;


import java.sql.*;

public class Table {

	public String inputDBPath;
	private ColumnMapping colMap;
	private String schema;
	
	private int numRows;
	private int numCols;
	private List<Tuple> tuples = new ArrayList<Tuple>();
	
	
	public DBProfiler dbPro;
	
	public String tableName;

	public Table(String inputDBPath, int numRows, int startingRow)
	{
		this.inputDBPath = inputDBPath;
		this.numRows = numRows;
		initFromFile(startingRow);

		//tableName = inputDBPath.split("/")[1];
		tableName = "default";
	}

	public Table(String inputDBPath, int numRows)
	{
		this.inputDBPath = inputDBPath;
		this.numRows = numRows;
		initFromFile();
		
		//tableName = inputDBPath.split("/")[1];
		tableName = "default";
	}
	public Table(Table table)
	{
		this.inputDBPath = table.inputDBPath;
		this.colMap = new ColumnMapping(table.colMap.getColumnHead());
		this.schema = table.schema;
		this.numRows = 0;
		this.numCols = table.numCols;
		this.tableName = table.tableName;
		for(Tuple tuple: table.tuples)
		{
			tuples.add(new Tuple(tuple));
		}
	}
	private boolean isInteger(String s) {
	    try { 
	        Integer.parseInt(s); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    }
	    // only got here if we didn't return false
	    return true;
	}
	private boolean isDouble(String s) {
	    try { 
	        Double.parseDouble(s);
	    } catch(NumberFormatException e) { 
	        return false; 
	    }
	    // only got here if we didn't return false
	    return true;
	}
	private void initFromFile()
	{
		try 
		{
			BufferedReader br =new BufferedReader(new FileReader(inputDBPath));
			String line = null;
			int temp = 0;
			while((line = br.readLine()) != null)
			{
				if(temp == 0)
				{
					//First line is the columns information
					colMap = new ColumnMapping(line);
					schema = line;
					String[] columns = line.split(",");
					this.numCols = columns.length;
					temp ++;
				}
				else
				{
					String[] colValues = line.split(",");
					//Length checking
					if (colValues.length != this.numCols)
					{
						//System.out.println("Input Database Error");
						continue;
					}
					boolean nullColumn = false;
					//NULl column checking
					for(String value: colValues)
					{
						if(value.equals("") || value.equals("?")
								|| value.contains("?"))
						{
							//System.out.println("NULL columns");
							nullColumn = true;
							break;
						}
					}
					if(nullColumn)
					{
						continue;
					}
					
					//Type checking
					boolean tag = true;
					for(int i = 0 ; i < numCols; i++)
					{
						String coliValue = colValues[i];
						
						int type = colMap.positionToType(i+1);
						if(type == 0 && !isInteger(coliValue))
						{
							tag = false;
							break;
						}
						else if(type == 1 && ! isDouble(coliValue))
						{
							tag = false;
							break;
						}
					}
					if(tag == false)
						continue;
					Tuple tuple = new Tuple(colValues,colMap,temp);
					tuples.add(tuple);
					temp ++;
					if(temp > numRows)
					{
						break;
					}
					
				}
			}
			br.close();
			numRows = tuples.size();
			System.out.println("NumRows:  " + numRows + " NumCols:" + numCols);
		} 
		catch (FileNotFoundException e)
		{
			
			e.printStackTrace();
		} 
		catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	private void initFromFile(int startingRow)
	{
		try
		{
			BufferedReader br =new BufferedReader(new FileReader(inputDBPath));
			String line = null;
			int temp = 0;
			while((line = br.readLine()) != null && temp<=startingRow) {
				if(temp == 0)
				{
					//First line is the columns information
					colMap = new ColumnMapping(line);
					schema = line;
					String[] columns = line.split(",");
					this.numCols = columns.length;
					temp ++;
				}
				temp++;
			}
			while((line = br.readLine()) != null)
			{
				if(temp == 0)
				{
					//First line is the columns information
					colMap = new ColumnMapping(line);
					schema = line;
					String[] columns = line.split(",");
					this.numCols = columns.length;
					temp ++;
				}
				else
				{
					String[] colValues = line.split(",");
					//Length checking
					if (colValues.length != this.numCols)
					{
						//System.out.println("Input Database Error");
						continue;
					}
					boolean nullColumn = false;
					//NULl column checking
					for(String value: colValues)
					{
						if(value.equals("") || value.equals("?")
								|| value.contains("?"))
						{
							//System.out.println("NULL columns");
							nullColumn = true;
							break;
						}
					}
					if(nullColumn)
					{
						continue;
					}

					//Type checking
					boolean tag = true;
					for(int i = 0 ; i < numCols; i++)
					{
						String coliValue = colValues[i];

						int type = colMap.positionToType(i+1);
						if(type == 0 && !isInteger(coliValue))
						{
							tag = false;
							break;
						}
						else if(type == 1 && ! isDouble(coliValue))
						{
							tag = false;
							break;
						}
					}
					if(tag == false)
						continue;
					Tuple tuple = new Tuple(colValues,colMap,temp);
					tuples.add(tuple);
					temp ++;
					if(temp > numRows+startingRow)
					{
						break;
					}

				}
			}
			br.close();
			numRows = tuples.size();
			System.out.println("NumRows:  " + numRows + " NumCols:" + numCols);
		}
		catch (FileNotFoundException e)
		{

			e.printStackTrace();
		}
		catch (IOException e) {

			e.printStackTrace();
		}
	}

	
	public void initFromFileBulkLoad(MyConnection conn)
	{
		try 
		{
			List<String[]> allColValues = new ArrayList<String[]>();
			BufferedReader br =new BufferedReader(new FileReader(inputDBPath));
			String line = null;
			int temp = 0;
			while((line = br.readLine()) != null)
			{
				if(temp == 0)
				{
					//First line is the columns information
					colMap = new ColumnMapping(line);
					schema = line;
					String[] columns = line.split(",");
					this.numCols = columns.length;
					String tableName = inputDBPath.split("/")[inputDBPath.split("/").length - 2];
					conn.createTable(tableName, colMap);
					temp ++;
				}
				else
				{
					String[] colValues = line.split(",");
					if (colValues.length != this.numCols)
					{
						System.out.println("Input Database Error");
						continue;
					}
					Tuple tuple = new Tuple(colValues,colMap,temp);
					String[] colValuesTemp = new String[colValues.length];
					for(int i = 0; i < colValues.length; i++)
					{
						if(colMap.positionToType(i+1) == 2)
							colValuesTemp[i] = "'" + colValues[i] + "'";
						else 
							colValuesTemp[i] = colValues[i];
					}
						
					tuples.add(tuple);
					String tableName = inputDBPath.split("/")[inputDBPath.split("/").length - 2];
					//conn.insertTable(tableName, colValuesTemp);
					allColValues.add(colValuesTemp);
					temp ++;
					
				}
			}
			br.close();
			numRows = tuples.size();
			conn.builInsertTable(getTableName(), allColValues);
			System.out.println("NumRows:  " + numRows);
		} 
		catch (FileNotFoundException e)
		{
			
			e.printStackTrace();
		} 
		catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	public ColumnMapping getColumnMapping()
	{
		return colMap;
	}
	public int getNumRows()
	{
		return numRows;
	}
	public int getNumCols()
	{
		return numCols;
	}
	public List<Tuple> getTuples()
	{
		return tuples;
	}
	public Tuple getTuple(int t)
	{
		return tuples.get(t);
	}
	
	public void removeTuple(Set<Tuple> toBeRemoved)
	{
		tuples.removeAll(toBeRemoved);
		numRows = tuples.size();
	}
	public void retainTuple(Set<Tuple> toBeRetained)
	{
		tuples.retainAll(toBeRetained);
		numRows = tuples.size();
	}
	public void insertTuples(Set<Tuple> toBeInserted)
	{
		tuples.addAll(toBeInserted);
		numRows = tuples.size();
	}
	/**
	 * Get cell for a row and col, both starting from 0
	 * @param row
	 * @param col
	 * @return
	 */
	public Cell getCell(int row, int col)
	{
		return tuples.get(row).getCell(col);
	}
	public void setCell(int row, int col, String newValue)
	{
		tuples.get(row).getCell(col).setValue(newValue);
	}
	public String getTableName()
	{
		return inputDBPath.split("/")[inputDBPath.split("/").length - 2];
	}
	public String getSchema()
	{
		return schema;
	}
	public String getCurrentFolder()
	{
		int index = inputDBPath.indexOf("inputDB");
		StringBuilder sb = new StringBuilder(inputDBPath.substring(0, index));
		return new String(sb);
	}
	
	

	/**
	 * @deprecated
	 * return the number of violations of this dc, actually this becomes find the intersection of different sets
	 * @param dc
	 * @return
	 */
	public int getNumVios(NewDenialConstraint
	 dc) {
		/*int countFail = 0;
		int tuplePairCount = 0;
		for(int t1 = 0 ; t1 < numRows-1; t1++)
			for(int t2 = t1+1; t2 < numRows; t2++)
			{
				NewTuple tup1 = this.getTuple(t1);
				NewTuple tup2 = this.getTuple(t2);
				tuplePairCount++;
				boolean allSatis = true;
				ArrayList<NewPredicate> predicates = dc.getPredicates();
				for(int p = 0 ; p < predicates.size(); p++)
				{
					if(!predicates.get(p).check())
					{
						allSatis = false;
						break;
					}
					
				}
				if(allSatis)
				{
					countFail++;
				}
			}*/
		return -1;
	}
		
		/*ArrayList<Predicate> predicates = dc.getPredicates();
		ArrayList<Set<Integer>> all = new ArrayList<Set<Integer>>();
		for(int i = 0 ; i < predicates.size(); i++)
			all.add(predicates.get(i).getVios());
		Set<Integer> intersect  = new HashSet<Integer>(all.get(0));
		for(int i = 1; i < all.size(); i++)
		{
			intersect.retainAll(all.get(i));
			if(intersect.size() == 0)
				return 0;
		}
		return intersect.size();
		
		return -1;
	}
	
	/**
	 * Get all the constants in a particular column
	 * @param col
	 * @return
	 */
	public Set<String> getColumnValues(int col)
	{
		Set<String> colValues = new HashSet<String>();
		for(int i = 0 ; i < numRows; i++)
		{
			String value = tuples.get(i).getCell(col).getValue();
			colValues.add(value);
		}
		return colValues;
	}
	
	
	/**
	 * Manually insert noise into this table
	 */
	public Map<Integer,Map<Integer, StringPair>> insertNoise(double noiseType)
	{
		int b;
		Map<Integer,Map<Integer, StringPair>> result = new HashMap<>();
		//collect the active domain
		Map<Integer,Set<String>> ad = new HashMap<Integer,Set<String>>();
		for(int i = 0; i < numCols; i++)
		{
			ad.put(i, getColumnValues(i));
		}
		System.out.println("Inserting noise using noise level: " + Config.noiseLevel);
		int countTypo = 0;
		int countDomainError = 0;
		Random rd = new Random();
		for(int i = 0; i < numRows; i++)
		{
			for(int j = 0 ; j < numCols; j++)
			{
				
				//do not introduce errors for those binary columns
				/*
				if(colMap.posiionToName(j+1).equals("HasChild") || 
						colMap.posiionToName(j+1).equals("MaritalStatus")
						||colMap.posiionToName(j+1).equals("State") 
						||colMap.posiionToName(j+1).equals("AreaCode")		
					)

					continue;
				*/
				double k = rd.nextDouble();
				if(rd.nextDouble() <= Config.noiseLevel )
				{
					
					String[] temp = ad.get(j).toArray(new String[0]);
					String randomValue = null;
					
					//create a typo
					if(new Random().nextDouble() <= noiseType)
					{
						countTypo++;
						if(colMap.positionToType(j+1) == 2)
						{
							String ij = getCell(i,j).toString();
							char a = ij.charAt(new Random().nextInt(ij.length()));
							randomValue = ij.replace(a, 'x');

							if (!result.containsKey(i))
								result.put(i, new HashMap<>());
							result.get(i).put(j, new StringPair(ij,randomValue));
						}
						else
						{
							//randomValue  = getCell(i,j).toString() + "0";
							
							String ij = getCell(i,j).toString();
							char a = ij.charAt(new Random().nextInt(ij.length()));
							randomValue = ij.replace(a, '8');

							if (!result.containsKey(i))
								result.put(i, new HashMap<>());
							result.get(i).put(j, new StringPair(ij,randomValue));
							
							//randomValue = temp[new Random().nextInt(temp.length)];
						}
					}
					else
					{
						countDomainError++;
						//create an active domain error
						randomValue = temp[new Random().nextInt(temp.length)];
					}
					
					
					//randomValue = temp[new Random().nextInt(temp.length)];
					if (!result.containsKey(i))
						result.put(i, new HashMap<>());
					result.get(i).put(j, new StringPair(getCell(i,j).getValue(),randomValue));

					setCell(i, j, randomValue);
				}
			}
			
		}
		System.out.println("The total number of cells changed is: typo" + countTypo);

		System.out.println("The total number of cells changed is: domain error" + countDomainError);
	
		
		return result;
	}

	/**
	 * Manually insert noise into this table
	 */
	public Map<Integer,Map<Integer, StringPair>> insertNoiseRows(double noiseType, double percent)
	{
		int b;
		Map<Integer,Map<Integer, StringPair>> result = new HashMap<>();
		//collect the active domain
		Map<Integer,Set<String>> ad = new HashMap<Integer,Set<String>>();
		for(int i = 0; i < numCols; i++)
		{
			ad.put(i, getColumnValues(i));
		}
		System.out.println("Inserting noise using noise level: " + Config.noiseLevel);
		int countTypo = 0;
		int countDomainError = 0;
		Random rd = new Random();
		for(int i = 0; i < percent*numRows; i++)
		{
			for(int j = 0 ; j < numCols; j++)
			{

				//do not introduce errors for those binary columns
				/*
				if(colMap.posiionToName(j+1).equals("HasChild") ||
						colMap.posiionToName(j+1).equals("MaritalStatus")
						||colMap.posiionToName(j+1).equals("State")
						||colMap.posiionToName(j+1).equals("AreaCode")
					)

					continue;
				*/
				double k = rd.nextDouble();
				if(rd.nextDouble() <= Config.noiseLevel )
				{

					String[] temp = ad.get(j).toArray(new String[0]);
					String randomValue = null;

					//create a typo
					if(new Random().nextDouble() <= noiseType)
					{
						countTypo++;
						if(colMap.positionToType(j+1) == 2)
						{
							String ij = getCell(i,j).toString();
							char a = ij.charAt(new Random().nextInt(ij.length()));
							randomValue = ij.replace(a, 'x');

							if (!result.containsKey(i))
								result.put(i, new HashMap<>());
							result.get(i).put(j, new StringPair(ij,randomValue));
						}
						else
						{
							//randomValue  = getCell(i,j).toString() + "0";

							String ij = getCell(i,j).toString();
							char a = ij.charAt(new Random().nextInt(ij.length()));
							randomValue = ij.replace(a, '8');

							if (!result.containsKey(i))
								result.put(i, new HashMap<>());
							result.get(i).put(j, new StringPair(ij,randomValue));

							//randomValue = temp[new Random().nextInt(temp.length)];
						}
					}
					else
					{
						countDomainError++;
						//create an active domain error
						randomValue = temp[new Random().nextInt(temp.length)];
					}


					//randomValue = temp[new Random().nextInt(temp.length)];
					if (!result.containsKey(i))
						result.put(i, new HashMap<>());
					result.get(i).put(j, new StringPair(getCell(i,j).getValue(),randomValue));

					setCell(i, j, randomValue);
				}
			}

		}
		System.out.println("The total number of cells changed is: typo" + countTypo);

		System.out.println("The total number of cells changed is: domain error" + countDomainError);


		return result;
	}

	/**
	 * Manually insert noise into this table
	 */
	public Map<Integer,Map<Integer, StringPair>> insertNoiseColumns(double noiseType,double percent)
	{
		int b;
		Map<Integer,Map<Integer, StringPair>> result = new HashMap<>();
		//collect the active domain
		Map<Integer,Set<String>> ad = new HashMap<Integer,Set<String>>();
		for(int i = 0; i < numCols; i++)
		{
			ad.put(i, getColumnValues(i));
		}
		System.out.println("Inserting noise using noise level: " + Config.noiseLevel);
		int countTypo = 0;
		int countDomainError = 0;
		Random rd = new Random();
		for(int i = 0; i < numRows; i++)
		{
			for(int j = 0 ; j < percent*numCols; j++)
			{

				//do not introduce errors for those binary columns
				/*
				if(colMap.posiionToName(j+1).equals("HasChild") ||
						colMap.posiionToName(j+1).equals("MaritalStatus")
						||colMap.posiionToName(j+1).equals("State")
						||colMap.posiionToName(j+1).equals("AreaCode")
					)

					continue;
				*/
				double k = rd.nextDouble();
				if(rd.nextDouble() <= Config.noiseLevel )
				{

					String[] temp = ad.get(j).toArray(new String[0]);
					String randomValue = null;

					//create a typo
					if(new Random().nextDouble() <= noiseType)
					{
						countTypo++;
						if(colMap.positionToType(j+1) == 2)
						{
							String ij = getCell(i,j).toString();
							char a = ij.charAt(new Random().nextInt(ij.length()));
							randomValue = ij.replace(a, 'x');

							if (!result.containsKey(i))
								result.put(i, new HashMap<>());
							result.get(i).put(j, new StringPair(ij,randomValue));
						}
						else
						{
							//randomValue  = getCell(i,j).toString() + "0";

							String ij = getCell(i,j).toString();
							char a = ij.charAt(new Random().nextInt(ij.length()));
							randomValue = ij.replace(a, '8');

							if (!result.containsKey(i))
								result.put(i, new HashMap<>());
							result.get(i).put(j, new StringPair(ij,randomValue));

							//randomValue = temp[new Random().nextInt(temp.length)];
						}
					}
					else
					{
						countDomainError++;
						//create an active domain error
						randomValue = temp[new Random().nextInt(temp.length)];
					}


					//randomValue = temp[new Random().nextInt(temp.length)];
					if (!result.containsKey(i))
						result.put(i, new HashMap<>());
					result.get(i).put(j, new StringPair(getCell(i,j).getValue(),randomValue));

					setCell(i, j, randomValue);
				}
			}

		}
		System.out.println("The total number of cells changed is: typo" + countTypo);

		System.out.println("The total number of cells changed is: domain error" + countDomainError);


		return result;
	}

	public void insertNoise(double noiseType,List<String> errors)
	{
		//collect the active domain
		Map<Integer,Set<String>> ad = new HashMap<Integer,Set<String>>();
		for(int i = 0; i < numCols; i++)
		{
			ad.put(i, getColumnValues(i));
		}
		System.out.println("Inserting noise using noise level: " + Config.noiseLevel);
		int countTypo = 0;
		int countDomainError = 0;
		
		//int numErrors = (int) (numRows * numCols * Config.noiseLevel);
		//List<String> curErrors = errors.subList(0, numErrors);
		
		for(int i = 0; i < numRows; i++)
		{
			for(int j = 0 ; j < numCols; j++)
			{
				
				//do not introduce errors for those binary columns
				/*
				if(colMap.posiionToName(j+1).equals("HasChild") || 
						colMap.posiionToName(j+1).equals("MaritalStatus")
						||colMap.posiionToName(j+1).equals("State") 
						||colMap.posiionToName(j+1).equals("AreaCode")		
					)

					continue;
				*/
				String haha = i + "," + j;
				if(errors.contains(haha) )
				{
					
					String[] temp = ad.get(j).toArray(new String[0]);
					String randomValue = null;
					
					//create a typo
					if(new Random().nextDouble() <= noiseType)
					{
						countTypo++;
						if(colMap.positionToType(j+1) == 2)
						{
							String ij = getCell(i,j).toString();
							char a = ij.charAt(new Random().nextInt(ij.length()));
							
							char rdC = (char)(new Random().nextInt(26) + 'a');
							
							randomValue = ij.replace(a, rdC);
						}
						else
						{
							//randomValue  = getCell(i,j).toString() + "0";
							
							String ij = getCell(i,j).toString();
							char a = ij.charAt(new Random().nextInt(ij.length()));
							
							char rdC = (char)('0' + new Random().nextInt(10));
							
							randomValue = ij.replace(a, rdC);
							
							//randomValue = temp[new Random().nextInt(temp.length)];
						}
					}
					else
					{
						countDomainError++;
						//create an active domain error
						randomValue = temp[new Random().nextInt(temp.length)];
					}
					
					
					//randomValue = temp[new Random().nextInt(temp.length)];
					
					System.out.println("ERROR: " + "row, col" + i + "," + colMap.posiionToName(j+1) + ": "
							+ "OLD: " + getCell(i,j).toString() 
							+ "NEW: " + randomValue) ;
					setCell(i, j, randomValue);
				}
			}
			
		}
		System.out.println("The total number of cells changed is: typo" + countTypo);

		System.out.println("The total number of cells changed is: domain error" + countDomainError);
	
		
		
	}
	/**
	 * Dump the table to the desFile
	 * @param desFile
	 */
	public void dump2File(String desFile)
	{
		PrintWriter out;
		try {
			out = new PrintWriter(new FileWriter(desFile));
			out.println(schema);
			for(int i = 0; i < numRows; i++)
			{
				String s = tuples.get(i).toString();
				out.println(s);
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
	
	
	
	
	
	
}
