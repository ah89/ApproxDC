package qcri.ci.generaldatastructure.db;

import qcri.ci.utils.Similarity;

public class NewCell {

	//private String type;	//The data type of the cell
	private int typeId; // 0- int, 1-double, 2-string
	private String stringValue;	//The value of the cell stored in string form
	private int intValue;
	private double doubleValue;

	public NewCell(int typeId, String value)
	{
		//this.type = type;
		this.typeId = typeId;
		this.stringValue = value;
	}
	public NewCell(int typeId, int value)
	{
		//this.type = type;
		this.typeId = typeId;
		this.intValue = value;
	}
	public NewCell(int typeId, double value)
	{
		//this.type = type;
		this.typeId = typeId;
		this.doubleValue = value;
	}
	public NewCell(NewCell cell) {
		//this.type = cell.type;
		this.stringValue = cell.stringValue;
		this.intValue = cell.intValue;
		this.doubleValue = cell.doubleValue;
	}

	public boolean isSameValue(String val)
	{
		return stringValue.equals(val);

	}

	public boolean isSameValue(NewCell cell)
	{
		assert(typeId == cell.getTypeId());
		if (typeId == 0)
			return this.intValue == cell.getIntValue();
		if (typeId == 1)
			return this.doubleValue == cell.getDoubleValue();
		else
			return this.stringValue == cell.getStringValue();
	}

	public boolean greaterThan(NewCell cell)
	{
		assert(typeId == cell.getTypeId());
		if (typeId == 0)
			return this.intValue > cell.getIntValue();
		if (typeId == 1)
			return this.doubleValue > cell.getDoubleValue();

		return false;
	}

	public boolean greaterThan(String cons)
	{
		if (typeId == 0)
			return this.intValue > Integer.valueOf(cons);
		if (typeId == 1)
			return this.doubleValue > Double.valueOf(cons);

		return false;
	}
	/*public String getType()
	{
		return type;
	}*/
	public int getTypeId() {return typeId;}

	public int getIntValue() {return intValue;}
	public double getDoubleValue() {return doubleValue;}
	public String getStringValue() {return stringValue;}

	public void setStringValue(String newValue)
	{
		this.stringValue = newValue;
	}
	public void setIntValue(int newValue)
	{
		this.intValue = newValue;
	}
	public void setDpubleValue(double newValue)
	{
		this.doubleValue = newValue;
	}

	/*public boolean isSameValue(NewCell cell)
	{
		assert(type.equalsIgnoreCase(cell.getType()));
		return value.equals(cell.getValue());
	}
	public boolean isSameValue(String val)
	{
		return value.equals(val);
		
	}*/
	
	/**
	 * if two cells are similar according to some similarity functions, return true, else return false
	 * @param cell
	 * @return
	 */
	/*public boolean isSimilarValue(NewCell cell)
	{
		assert(type.equalsIgnoreCase(cell.getType()));

		String type1 = type;
		String type2 = cell.getType();
		if(!type1.equals(type2))
		{
			System.out.println("these two cells are not of the same type, so aren't comparable");
			return false;
		}
		else
		{
			if(type2.equalsIgnoreCase("String"))
			{
				return Similarity.levenshteinDistance(value,cell.getValue());
			}
			else
			{
				System.out.println("These two cells must be of String type");
				return false;
			}
		}
	}*/
	
	/**
	 * if this cell is similar to the val, return true, else return false
	 * @param
	 * @return
	 */
	/*public boolean isSimilarValue(String val)
	{
		String type1 = type;
		if(type1.equalsIgnoreCase("String"))
		{
			return Similarity.levenshteinDistance(value, val);
		}
		else
		{
			System.out.println("These two cells must be of String type");
			return false;
		}
	}*
	
	/**
	 * check if the current cell is greater than the argument cell
	 * @param cell
	 * @return
	 */
	/*public boolean greaterThan(NewCell cell)
	{
		assert(type.equalsIgnoreCase(cell.getType()));
		String type1 = type;
		if(type1.equalsIgnoreCase("Integer"))
		{
			int value1 = Integer.valueOf(value);
			int value2 = Integer.valueOf(cell.getValue());
			return (value1 > value2);
		}
		else if(type1.equalsIgnoreCase("String"))
		{
			String value1 = value;
			String value2 = cell.getValue();
			int com = value1.compareTo(value2);
			return (com > 0);
		}
		else if(type1.equalsIgnoreCase("Double"))
		{
			double value1 = Double.valueOf(value);
			double value2 = Double.valueOf(cell.getValue());
			return (value1 > value2);
		}
		else
		{
			System.out.println("Unsupported Type For now");
			return false;
		}
	}*/
	
	/*public boolean greaterThan(String val)
	{
		String type1 = type;
		if(type1.equalsIgnoreCase("Integer"))
		{
			int value1 = Integer.valueOf(value);
			int value2 = Integer.valueOf(val);
			return (value1 > value2);
		}
		else if(type1.equalsIgnoreCase("String"))
		{
			String value1 = value;
			String value2 = val;
			int com = value1.compareTo(value2);
			return (com > 0);
		}
		else if(type1.equalsIgnoreCase("Double"))
		{
			double value1 = Double.valueOf(value);
			double value2 = Double.valueOf(val);
			return (value1 > value2);
		}
		else
		{
			System.out.println("Unsupported Type For now");
			return false;
		}
	}*/
	
	public String toString()
	{
		return stringValue;
	}
	
	
}
