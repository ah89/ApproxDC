package qcri.ci;

import java.util.ArrayList;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

public class TestJavaCollection {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		int num = 10000;
		
		jdk(num);
		trove(num);
	}
	
	public static void trove(int num)
	{
		long s1 = System.currentTimeMillis();
		TIntList list = new TIntArrayList();
		for(int i = 0 ; i < num; i++)
		{
			list.add(i);
		}
		for(int i = 0 ; i < num; i++)
		{
			list.remove(i);
		}
		long s2 = System.currentTimeMillis();
		System.out.println("In trove: " + (s2-s1));
	}
	public static void jdk(int num)
	{
		long s1 = System.currentTimeMillis();
		ArrayList<Integer> list = new ArrayList<Integer>();
		for(int i = 0  ; i < num; i++)
		{
			list.add(i);
		}
		for(int i = 0 ; i < num; i++)
		{
			list.remove(new Integer(i));
		}
		long s2 = System.currentTimeMillis();
		System.out.println("In jdk: " + (s2-s1));
	}

}
