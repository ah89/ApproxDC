package qcri.ci.generaldatastructure.db;

import java.util.ArrayList;
import java.util.List;

public class NewTuple {

	private ColumnMapping cm;
	private List<NewCell> cells = new ArrayList<>();

	public int tid;

	public NewTuple(String[] values,ColumnMapping cm,int tid)
	{
		this.cm = cm;
		this.tid = tid;
		for(int i = 0 ; i < values.length; i++)
		{
			int type = cm.positionToType(i+1);
			NewCell cell = null;
			if (type == 0)
				cell = new NewCell(0,Integer.valueOf(values[i]));
			else if ((type == 1))
				cell = new NewCell(1, Double.valueOf(values[i]));
			else
				cell = new NewCell(2, values[i]);
			cells.add(cell);
		}
	}

	public NewTuple(NewTuple tuple)
	{
		this.cm = tuple.cm;
		for(NewCell cell: tuple.cells)
		{
			this.cells.add(new NewCell(cell));
		}
		this.tid = tuple.tid;
	}
	public List<NewCell> getTuple()
	{
		return cells;
	}
	
	public int getNumCols()
	{
		return cells.size();
	}
	public NewCell getCell(int col)
	{
		return cells.get(col);
	}
	
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		//sb.append("t" + tid + ":");
		for(int i = 0 ; i < cells.size(); i++)
		{
			sb.append(cells.get(i).toString());
			if(i != cells.size()-1)
				sb.append(",");
		}
		return new String(sb);
	}
	
}
