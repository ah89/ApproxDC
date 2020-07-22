package qcri.ci.generaldatastructure.constraints;

import qcri.ci.generaldatastructure.db.Cell;
import qcri.ci.generaldatastructure.db.NewCell;
import qcri.ci.generaldatastructure.db.NewTable;
import qcri.ci.generaldatastructure.db.NewTuple;
import qcri.ci.generaldatastructure.db.Table;
import qcri.ci.generaldatastructure.db.Tuple;
import qcri.ci.utils.BooleanPair;
import qcri.ci.utils.OperatorMapping;

import java.util.Arrays;

/**
 * This class is for denial constraint
 * @author xchu
 *
 */
public class NewPredicate
{
	//Example: EQ(t2.Mid,t1.Eid)

	NewTable table;
	int operator;



	int numArgs;	//number of arguments of this predicate
	int[] rowNum;	//The row number for the arguments.  {2,1}
	int[] colNum;	//The column position for the argument. {2,1}

	public int getIndex() {
		return index_pre;
	}

	public void setIndex(int index) {
		this.index_pre = index;
	}

	int index_pre;

	boolean secondCons;	//if the second argument is constant or not
	String cons;	//constant of the second argument if so

	//This constructor is for 2 arguments, both of them are cells
	public NewPredicate(NewTable table,int operator, int row1, int row2, int col1, int col2)
	{
		this.table = table;
		this.operator = operator;

		numArgs = 2;
		rowNum = new int[2];
		colNum = new int[2];
		rowNum[0] = row1;
		rowNum[1] = row2;
		colNum[0] = col1;
		colNum[1] = col2;
		cons = null;
		secondCons = false;
		index_pre = 0;
		//initSatisfied();
	}
	public NewPredicate(NewTable table, int operator, int row1, int col1, String cons)
	{
		this.table = table;
		this.operator = operator;

		numArgs = 1;
		rowNum = new int[2];
		colNum = new int[2];
		rowNum[0] = row1;
		colNum[0] = col1;
		rowNum[1] = -1;
		colNum[1] = -1;
		this.cons = cons;
		secondCons = true;
		index_pre = 0;
		//initSatisfied();
	}

	public NewPredicate(NewPredicate p)
	{
		this.table = p.table;
		this.operator = p.operator;
		numArgs = p.numArgs;
		rowNum = new int[2];
		colNum = new int[2];
		rowNum[0] = p.rowNum[0];
		rowNum[1] = p.rowNum[1];
		colNum[0] = p.colNum[0];
		colNum[1] = p.colNum[1];
		cons = p.cons;
		secondCons = p.secondCons;
		index_pre = 0;
	}
	
	/**
	 * if the second argument of the predicate is a constant, or a cell
	 * @return
	 */
	public boolean isSecondCons()
	{
		return secondCons;
	}
	/**
	 * Get the constant second argument
	 * @return
	 */
	public String getCons()
	{
		return cons;
	}
	
	public int[] getCols()
	{
		return colNum;
	}
	public int[] getRows()
	{
		return rowNum;
	}
	
	public boolean check(NewTuple t1, NewTuple t2)
	{
		NewCell cell1 = t1.getCell(colNum[0]-1);
		NewCell cell2 = t2.getCell(colNum[1]-1);

		int type = cell1.getTypeId();

		if (operator == 0) {
			if (type == 0) {
				return cell1.getIntValue() == cell2.getIntValue();
			}
			if (type == 1) {
				return cell1.getDoubleValue() == cell2.getDoubleValue();
			}
			if (type == 2) {
				return cell1.getStringValue().equalsIgnoreCase(cell2.getStringValue());
			}
		}
		else if (operator == 1) {
			if (type == 0) {
				return cell1.getIntValue() != cell2.getIntValue();
			}
			if (type == 1) {
				return cell1.getDoubleValue() != cell2.getDoubleValue();
			}
			if (type == 2) {
				return !cell1.getStringValue().equalsIgnoreCase(cell2.getStringValue());
			}
		}
		else if (operator == 2) {
			if (type == 0) {
				return cell1.getIntValue() > cell2.getIntValue();
			}
			if (type == 1) {
				return cell1.getDoubleValue() > cell2.getDoubleValue();
			}
		}
		else if (operator == 3) {
			if (type == 0) {
				return cell1.getIntValue() <= cell2.getIntValue();
			}
			if (type == 1) {
				return cell1.getDoubleValue() <= cell2.getDoubleValue();
			}
		}
		else if (operator == 4) {
			if (type == 0) {
				return cell1.getIntValue() < cell2.getIntValue();
			}
			if (type == 1) {
				return cell1.getDoubleValue() < cell2.getDoubleValue();
			}
		}
		else {
			if (type == 0) {
				return cell1.getIntValue() >= cell2.getIntValue();
			}
			if (type == 1) {
				return cell1.getDoubleValue() >= cell2.getDoubleValue();
			}
		}

		return false;
	}

	public BooleanPair checkBoth(NewTuple t1, NewTuple t2)
	{
		NewCell cell1 = t1.getCell(colNum[0]-1);
		NewCell cell2 = t2.getCell(colNum[1]-1);

		int type = cell1.getTypeId();

		if (operator == 0) {
			if (type == 0) {
				boolean result = cell1.getIntValue() == cell2.getIntValue();
				return new BooleanPair(result, result);
			}
			if (type == 1) {
				boolean result = cell1.getDoubleValue() == cell2.getDoubleValue();
				return new BooleanPair(result, result);
			}
			if (type == 2) {
				boolean result = cell1.getStringValue().equalsIgnoreCase(cell2.getStringValue());
				return new BooleanPair(result, result);
			}
		}
		else if (operator == 1) {
			if (type == 0) {
				boolean result = cell1.getIntValue() != cell2.getIntValue();
				return new BooleanPair(result, result);
			}
			if (type == 1) {
				boolean result = cell1.getDoubleValue() != cell2.getDoubleValue();
				return new BooleanPair(result, result);
			}
			if (type == 2) {
				boolean result = !cell1.getStringValue().equalsIgnoreCase(cell2.getStringValue());
				return new BooleanPair(result, result);
			}
		}
		else if (operator == 2) {
			if (type == 0) {
				boolean result = cell1.getIntValue() > cell2.getIntValue();
				return new BooleanPair(result, !result);
			}
			if (type == 1) {
				boolean result = cell1.getDoubleValue() > cell2.getDoubleValue();
				return new BooleanPair(result, !result);
			}
		}
		else if (operator == 3) {
			if (type == 0) {
				boolean result1 = cell1.getIntValue() <= cell2.getIntValue();
				boolean result2 = cell2.getIntValue() <= cell1.getIntValue();
				return new BooleanPair(result1, result2);
			}
			if (type == 1) {
				boolean result1 = cell1.getDoubleValue() <= cell2.getDoubleValue();
				boolean result2 = cell2.getDoubleValue() <= cell1.getDoubleValue();
				return new BooleanPair(result1, result2);
			}
		}
		else if (operator == 4) {
			if (type == 0) {
				boolean result = cell1.getIntValue() < cell2.getIntValue();
				return new BooleanPair(result, !result);
			}
			if (type == 1) {
				boolean result = cell1.getDoubleValue() < cell2.getDoubleValue();
				return new BooleanPair(result, !result);
			}
		}
		else {
			if (type == 0) {
				boolean result1 = cell1.getIntValue() >= cell2.getIntValue();
				boolean result2 = cell2.getIntValue() >= cell1.getIntValue();
				return new BooleanPair(result1, result2);
			}
			if (type == 1) {
				boolean result1 = cell1.getDoubleValue() >= cell2.getDoubleValue();
				boolean result2 = cell2.getDoubleValue() >= cell1.getDoubleValue();
				return new BooleanPair(result1, result2);
			}
		}

		return null;
	}

	public boolean check(NewTuple t1)
	{
		assert(secondCons == true);
		NewCell[][] cells = new NewCell[2][t1.getNumCols()];
		for(int i = 0 ; i < t1.getNumCols(); i++)
		{
			if(rowNum[0] == 1)
				cells[0][i] = t1.getCell(i);
			else
				cells[1][i] = t1.getCell(i);
		}
		return check(cells);
	}

	private boolean check(NewCell[][] cells)
	{
		NewCell[] args = new NewCell[numArgs];

		if(secondCons == false)
		{
			for(int i = 0 ; i < numArgs; i++)
			{
				int row = rowNum[i];	//row 	 position in the current predicate
				int col = colNum[i];	//column position for the i th argument
				args[i] = cells[row-1][col-1];
				if(args[i].getStringValue().equals("FV"))
					return false;
			}
			if(operator == 0)
			{
				return args[0].isSameValue(args[1]);
			}
			else if(operator == 2)
			{
				return args[0].greaterThan(args[1]);
			}
			else if(operator == 1)
			{
				return !args[0].isSameValue(args[1]);
			}
			else if(operator == 4)
			{
				return (!args[0].greaterThan(args[1]) ) && (!args[0].isSameValue(args[1]));
			}
			else if(operator == 5)
			{
				return args[0].isSameValue(args[1]) || args[0].greaterThan(args[1]);
			}
			else if(operator == 3)
			{
				return !args[0].greaterThan(args[1]);
			}
			else
			{
				System.out.println("Unsupported predicate");

				System.exit(-1);
				return false;
			}
		}
		else	//Second argument is a constant
		{
			int row = rowNum[0];	//row 	 position in the current predicate
			int col = colNum[0];	//column position for the i th argument
			args[0] = cells[row-1][col-1];
			if(args[0].getStringValue().equals("FV"))
				return false;
			if(operator == 0)
			{
				return args[0].isSameValue(cons);
			}
			else if(operator == 2)
			{
				return args[0].greaterThan(cons);
			}
			else if(operator == 1)
			{
				return !args[0].isSameValue(cons);
			}
			else if(operator == 4)
			{
				return (!args[0].greaterThan(cons) ) && (!args[0].isSameValue(cons));
			}
			else if(operator == 5)
			{
				return args[0].isSameValue(cons) || args[0].greaterThan(cons);
			}
			else if(operator == 3)
			{
				return !args[0].greaterThan(cons);
			}
			else
			{
				System.out.println("Unsupported predicate");

				System.exit(-1);
				return false;
			}
		}


	}



	/*public boolean check(int tuplePairIndex)
	{
		return satisfied.contains(tuplePairIndex);
	}*/
	public int getOperator()
	{
		return operator;
	}
	
	/**
	 * Is this Cell, applaible to this predicate, the row,col passed in, is the row and col number as the denial constraints see it, NOT as the database see it
	 * @param row
	 * @param col
	 * @return
	 */
	public boolean isAppliable(int row, int col)
	{
		for(int i = 0 ; i < numArgs; i++)
		{
			if(rowNum[i] == row && colNum[i] == col)
				return true;
		}
		return false;
	}
	
	/**
	 * Get the row, and col number from the other cell from the point of view of the constraints
	 * @param row
	 * @param col
	 * @return
	 */
	public int theOtherCellRow(int row, int col)
	{
		if(rowNum[0] == row && colNum[0] == col)
			return rowNum[1];
		else 
			return rowNum[0];
	}
	/**
	 * Get the row, col number from the other cell from the point of view of the constraint
	 * @param row
	 * @param col
	 * @return
	 */
	public int theOtherCellCol(int row, int col)
	{
		if(rowNum[0] == row && colNum[0] == col)
			return colNum[1];
		else 
			return colNum[0];
	}
	
	/**
	 * Get the reverse name if the second argument is a constant
	 * @return
	 */
	public int getReverseOperator()
	{
		assert(secondCons == true);
		if(operator == 0)
			return 1;
		else if(operator == 2)
			return 3;
		else if(operator == 4)
			return 5;
		else if(operator == 1)
			return 0;
		else if(operator == 5)
			return 4;
		else if(operator == 3)
			return 2;
		else 
			return -1;
	}
	/**
	 * Get the reverse name if the second argument is not a constant
	 * @param row
	 * @param col
	 * @return
	 */
	/*public String getReverseName(int row, int col)
	{
		assert(secondCons == false);
		if(rowNum[0] == row && colNum[0] == col)
		{
			if(operator == 0)
				return "IQ";
			else if(operator == 2)
				return "LTE";
			else if(operator == 4
				return "GTE";
			else if(operator == 1)
				return "EQ";
			else if(operator == 5)
				return "LT";
			else if(operator == 3)
				return "GT";
			else if(name.equals("SIM"))
				return "IQ";
			else 
				return null;
		}
		else if(rowNum[1] == row && colNum[1] == col)
		{
			//return name;
			if(operator == 0)
				return "IQ";
			else if(operator == 2)
				return "GTE";
			else if(operator == 4
				return "LTE";
			else if(operator == 1)
				return "EQ";
			else if(operator == 5)
				return "GT";
			else if(operator == 3)
				return "LT";
			else if(name.equals("SIM"))
				return "IQ";
			else 
				return null;
		}
		else
			return null;
	}*/

	@Override
	public int hashCode() {
		int result = operator;
		result = 31 * result + numArgs;
		result = 31 * result + Arrays.hashCode(rowNum);
		result = 31 * result + Arrays.hashCode(colNum);
		return result;
	}

	/**
	 * This function tests if this predicate contradicts the passed in predicate
	 * @param p
	 * @return
	 */
	public boolean contradict(NewPredicate p)
	{
		if(secondCons != p.secondCons)
			return false;

		return contradictV(p);
		
		/*if(secondCons == true)
		{
			return contradictC(p);
		}
		else
		{
			return contradictV(p);
		}*/
		
	}
	private boolean contradictV(NewPredicate p)
	{
		if(rowNum[0]==p.rowNum[0] && rowNum[1] == p.rowNum[1]
				&& colNum[0]==p.colNum[0] && colNum[1] == p.colNum[1])
			{
				if(operator == 0 && p.operator == 1)
					return true;
				if(operator == 1 && p.operator == 0)
					return true;
				if(operator == 2 && p.operator == 3)
					return true;
				if(operator == 4 && p.operator == 5)
					return true;
				if(operator == 5 && p.operator == 4)
					return true;
				if(operator == 3 && p.operator == 2)
					return true;
				//More:
				if(operator == 0 && p.operator == 2)
					return true;
				if(operator == 0 && p.operator == 4)
					return true;
				if(operator == 2 && p.operator == 0)
					return true;
				if(operator == 2 && p.operator == 4)
					return true;
				if(operator == 4 && p.operator == 0)
					return true;
				if(operator == 4 && p.operator == 2)
					return true;
			}
			
			if(rowNum[0]==p.rowNum[1] && rowNum[1] == p.rowNum[0]
					&& colNum[0]==p.colNum[1] && colNum[1] == p.colNum[0])
				{
					System.out.println("We should never hit here in checking predicate contradictory!");
					if(operator == 0 && p.operator == 1)
						return true;
					if(operator == 1 && p.operator == 0)
						return true;
					if(operator == 2 && p.operator == 5)
						return true;
					if(operator == 4 && p.operator == 3)
						return true;
					if(operator == 5 && p.operator == 2)
						return true;
					if(operator == 3 && p.operator == 4)
						return true;
					
					//more
					
					if(operator == 0 && p.operator == 2)
						return true;
					if(operator == 0 && p.operator == 4)
						return true;
					if(operator == 2 && p.operator == 0)
						return true;
					if(operator == 2 && p.operator == 2)
						return true;
					if(operator == 4 && p.operator == 0)
						return true;
					if(operator == 4 && p.operator == 4)
						return true;
				}

			return false;
	}
	/*private boolean contradictC(NewPredicate p)
	{
		//If not the same row, and not the same column
		if(rowNum[0] != p.rowNum[0] || colNum[0] != p.colNum[0])
			return false;
		String constant1 = cons;
		String constant2 = p.cons;
		//Enumerate 36 scenarios, where two constant predicates contradicting each other
		if(operator == 0 && p.operator == 0)
		{
			if(!constant1.equals(constant2))
				return true;
		}
		else if(operator == 0 && p.operator == 1)
		{
			if(constant1.equals(constant2))
				return true;
		}
		else if(operator == 0 && p.operator == 2)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 <= v2)
				return true;
		}
		else if(operator == 0 && p.operator == 3)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 > v2)
				return true;
		}
		else if(operator == 0 && p.operator == 4
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 >= v2)
				return true;
		}
		else if(operator == 0 && p.operator == 5)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 < v2)
				return true;
		}
		else if(operator == 1 && p.operator == 0)
		{
			if(constant1.equals(constant2))
				return true;
		}
		else if(operator == 1 && p.operator == 1)
		{
			
		}
		else if(operator == 1 && p.operator == 2)
		{
			
		}
		else if(operator == 1 && p.operator == 3)
		{
			
		}
		else if(operator == 1 && p.operator == 4
		{
			
		}
		else if(operator == 1 && p.operator == 5)
		{
			
		}
		else if(operator == 2 && p.operator == 0)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 <= v1)
				return true;
		}
		else if(operator == 2 && p.operator == 1)
		{
			
		}
		else if(operator == 2 && p.operator == 2)
		{
			
		}
		else if(operator == 2 && p.operator == 3)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 <= v1)
				return true;
		}
		else if(operator == 2 && p.operator == 4
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 <= v1)
				return true;
		}
		else if(operator == 2 && p.operator == 5)
		{
			
		}
		else if(operator == 3 && p.operator == 0)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 > v1)
				return true;
		}
		else if(operator == 3 && p.operator == 1)
		{
			
		}
		else if(operator == 3 && p.operator == 2)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 >= v1)
				return true;
		}
		else if(operator == 3 && p.operator == 3)
		{
			
		}
		else if(operator == 3 && p.operator == 4
		{
			
		}
		else if(operator == 3 && p.operator == 5)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 > v1)
				return true;
		}
		else if(operator == 4 && p.operator == 0)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 >= v1)
				return true;
		}
		else if(operator == 4 && p.operator == 1)
		{
			
		}
		else if(operator == 4 && p.operator == 2)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 >= v1)
				return true;
		}
		else if(operator == 4 && p.operator == 3)
		{
			
		}
		else if(operator == 4 && p.operator == 4
		{
			
		}
		else if(operator == 4 && p.operator == 5)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 >= v1)
				return true;
		}
		else if(operator == 5 && p.operator == 0)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 < v1)
				return true;
		}
		else if(operator == 5 && p.operator == 1)
		{
			
		}
		else if(operator == 5 && p.operator == 2)
		{
			
		}
		else if(operator == 5 && p.operator == 3)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 < v1)
				return true;
		}
		else if(operator == 5 && p.operator == 4
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 <= v1)
				return true;
		}
		else if(operator == 5 && p.operator == 5)
		{
			
		}
		
			
		
		return false;
	}*/
	
	/**
	 * Test whether the current predicate implies the passed-in predicate
	 * @param p
	 * @return
	 */
	public boolean implied(NewPredicate p)
	{
		if(table != p.table)
			return false;
		if(secondCons != p.secondCons)
			return false;
		
		
		if(secondCons == true)
			return impliedC(p);
		else
			return impliedV(p);
	
	}

	private boolean impliedV(NewPredicate p)
	{
		if(rowNum[0] != p.rowNum[0])
			return false;
		if(rowNum[1] != p.rowNum[1])
			return false;
		if(colNum[0] != p.colNum[0])
			return false;
		if(colNum[1] != p.colNum[1])
			return false;
		
		
		
		if(operator == 0 && p.operator == 5)
			return true;
		if(operator == 0 && p.operator == 3)
			return true;
		
		
		if(operator == 2 && p.operator == 5)
			return true;
		if(operator == 2 && p.operator == 1)
			return true;
		if(operator == 4 && p.operator == 3)
			return true;
		if(operator == 4 && p.operator == 1)
			return true;
		
		
		
		return false;
	}
	private boolean impliedC(NewPredicate p)
	{
		//If not the same row, and not the same column
		if(rowNum[0] != p.rowNum[0] || colNum[0] != p.colNum[0])
			return false;
		String constant1 = cons;
		String constant2 = p.cons;
		if(operator == 0 && p.operator == 0)
		{
			if(constant1.equals(constant2))
				return true;
		}
		else if(operator == 0 && p.operator == 1)
		{
			if(!constant1.equals(constant2))
				return true;
		}
		else if(operator == 0 && p.operator == 2)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 > v2)
				return true;
		}
		else if(operator == 0 && p.operator == 3)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 <= v2)
				return true;
		}
		else if(operator == 0 && p.operator == 4)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 < v2)
				return true;
		}
		else if(operator == 0 && p.operator == 5)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 >= v2)
				return true;
		}
		else if(operator == 1 && p.operator == 0)
		{
			
		}
		else if(operator == 1 && p.operator == 1)
		{
			
		}
		else if(operator == 1 && p.operator == 2)
		{
			
		}
		else if(operator == 1 && p.operator == 3)
		{
			
		}
		else if(operator == 1 && p.operator == 4)
		{
			
		}
		else if(operator == 1 && p.operator == 5)
		{
			
		}
		else if(operator == 2 && p.operator == 0)
		{
			
		}
		else if(operator == 2 && p.operator == 1)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 <= v1)
				return true;
		}
		else if(operator == 2 && p.operator == 2)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 >= v2)
				return true;
		}
		else if(operator == 2 && p.operator == 3)
		{
		}
		else if(operator == 2 && p.operator == 4)
		{
		}
		else if(operator == 2 && p.operator == 5)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 >= v2)
				return true;
		}
		else if(operator == 3 && p.operator == 0)
		{
			
		}
		else if(operator == 3 && p.operator == 1)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 >  v1)
				return true;
		}
		else if(operator == 3 && p.operator == 2)
		{
			
		}
		else if(operator == 3 && p.operator == 3)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 <= v2)
				return true;
		}
		else if(operator == 3 && p.operator == 4)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 < v2)
				return true;
		}
		else if(operator == 3 && p.operator == 5)
		{
			
		}
		else if(operator == 4 && p.operator == 0)
		{
			
		}
		else if(operator == 4 && p.operator == 1)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 >= v1)
				return true;
		}
		else if(operator == 4 && p.operator == 2)
		{
			
		}
		else if(operator == 4 && p.operator == 3)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 <= v2)
				return true;
		}
		else if(operator == 4 && p.operator == 4)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v1 <= v2)
				return true;
		}
		else if(operator == 4 && p.operator == 5)
		{
			
		}
		else if(operator == 5 && p.operator == 0)
		{
			
		}
		else if(operator == 5 && p.operator == 1)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 < v1)
				return true;
		}
		else if(operator == 5 && p.operator == 2)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 < v1)
				return true;
		}
		else if(operator == 5 && p.operator == 3)
		{
			
		}
		else if(operator == 5 && p.operator == 4)
		{
			
		}
		else if(operator == 5 && p.operator == 5)
		{
			double v1 = Double.valueOf(constant1);
			double v2 = Double.valueOf(constant2);
			if(v2 <= v1)
				return true;
		}
		return false;
		
		
	}
	public String toString()
	{
		if(secondCons == false)
		{
			StringBuilder sb = new StringBuilder();
			//sb.append(name + "(");
			sb.append("t" + rowNum[0] + "." + table.getColumnMapping().posiionToName(colNum[0]));
			//sb.append(",");
			sb.append(OperatorMapping.idToOperator(operator));
			sb.append("t" + rowNum[1] + "." + table.getColumnMapping().posiionToName(colNum[1]));
			//sb.append(")");
			return new String(sb);
		}
		else
		{
			StringBuilder sb = new StringBuilder();
			//sb.append(name + "(");
			sb.append("t" + rowNum[0] + "." + table.getColumnMapping().posiionToName(colNum[0]));
			//sb.append(",");
			sb.append(OperatorMapping.idToOperator(operator));
			sb.append(cons);
			//sb.append(")");
			return new String(sb);
		}
		
	}

	protected NewPredicate clone()
	{
		NewPredicate clone = null;

		try {
			clone = (NewPredicate) super.clone();
		}
		catch(CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}

		return clone;
	}


	
	/**
	 * Determine if the predicate is the same as the passed-in operator except the operator
	 * @param p
	 * @return
	 */
	public boolean equalExceptOp(NewPredicate p)
	{
		NewPredicate op = (NewPredicate)p;
		if(table != op.table)
			return false;
		if(secondCons != op.secondCons)
			return false;
		/*if(!name.equals(op.name))
			return false;*/
		if(rowNum[0] != op.rowNum[0])
			return false;
		if(rowNum[1] != op.rowNum[1])
			return false;
		if(colNum[0] != op.colNum[0])
			return false;
		if(colNum[1] != op.colNum[1])
			return false;
		if(secondCons && (!cons.equals(op.cons)))
			return false;
		return true;
	}
	
	
	@Override
	public boolean equals(Object o)
	{
		NewPredicate op = (NewPredicate)o;
		if(operator != op.operator)
			return false;
		if(rowNum[0] != op.rowNum[0])
			return false;
		if(rowNum[1] != op.rowNum[1])
			return false;
		if(colNum[0] != op.colNum[0])
			return false;
		if(colNum[1] != op.colNum[1])
			return false;
		return true;
	}
	
	
	public double coherence()
	{
		if(secondCons || rowNum[0] == rowNum[1])
			return 1;
		else if(colNum[0] == colNum[1])
			return 0.8;
		else
			return 0.5;
	}
}