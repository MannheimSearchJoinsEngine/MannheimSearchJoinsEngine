package de.mannheim.uni.utils;

public class ByteCount {

	public static int kB(int n)
	{
		return n * 1000;
	}
	public static int kiB(int n)
	{
		return n * 1024;
	}
	public static int mB(int n)
	{
		return n * 1000000;
	}
	public static int miB(int n)
	{
		return n * 1024 * 1024;
	}
	public static long gB(int n)
	{
		return n * 1000000000;
	}
	public static int giB(int n)
	{
		return n * 1024 *1024 * 1024;
	}
	public static long tB(int n)
	{
		return n * 1000000000000L;
	}
	public static long tiB(int n)
	{
		return n * 1024 * 1024 *1024 * 1024;
	}
	
}
