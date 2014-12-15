package de.mannheim.uni.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import de.mannheim.uni.model.TableColumn.ColumnDataType;

public class ObjectStringMap<T, U> extends HashMap<T, String> {


	/**
	 * 
	 */
	private static final long serialVersionUID = 2460725199449662814L;

	class AsStringCollection implements Collection<String>
	{

		private Collection<Object> innerCollection;
		
		public AsStringCollection(Collection<Object> innerCollection)
		{
			this.innerCollection = innerCollection;
		}
		
		public int size() {
			return innerCollection.size();
		}

		public boolean isEmpty() {
			return innerCollection.isEmpty();
		}

		public boolean contains(Object o) {
			return innerCollection.contains(o);
		}

		public Iterator iterator() {
			return innerCollection.iterator();
		}

		public Object[] toArray() {
			return innerCollection.toArray();
		}

		public Object[] toArray(Object[] a) {
			return innerCollection.toArray(a);
		}

		public boolean add(String e) {
			return innerCollection.add(transformValue(e));
		}

		public boolean remove(Object o) {
			return innerCollection.remove(o);
		}

		public boolean containsAll(Collection c) {
			return innerCollection.containsAll(c);
		}

		public boolean addAll(Collection c) {
			return innerCollection.addAll(c);
		}

		public boolean removeAll(Collection c) {
			return innerCollection.retainAll(c);
		}

		public boolean retainAll(Collection c) {
			return innerCollection.retainAll(c);
		}

		public void clear() {
			innerCollection.clear();
		}
		
	}
	
	public ObjectStringMap()
	{
		super();
		this.dataType = null;
		actualMap = new HashMap<T, Object>();		
	}
	
	public ObjectStringMap(ColumnDataType dataType)
	{
		super();
		this.dataType = dataType;
		actualMap = new HashMap<T, Object>();
	}
	
	private ColumnDataType dataType;
	private HashMap<T, Object> actualMap;
	
	public ColumnDataType getDataType() {
		return dataType;
	}
	
	@Override
	public String put(T key, String value) {
		if(this.dataType==null)
			super.put(key, value);
		else
		{
			Object o = transformValue(value);
		
			actualMap.put(key, o);
		}
		
		return value;
	}
	 
	public void transformAllValues()
	{
		if(this.dataType!=null)
		{
			for(java.util.Map.Entry<T, String> e : super.entrySet())
			{
				put(e.getKey(), e.getValue());
			}
			super.clear();
		}
	}
	
	@Override
	public String get(Object key) {
		return actualMap.get(key).toString();
	}
	
	@Override
	public Collection<String> values() {
		return new AsStringCollection(actualMap.values());
	}
	
	private Object transformValue(String value)
	{
		switch (dataType) {
		case numeric:
			String s = value.replaceAll("[^0-9\\.\\,\\-]", "");
			
			return Double.valueOf(s);
		case bool:
		case coordinate:
		case date:
		case link:
		case list:
		case string:
		case unit:
		case unknown:
			return value;
		default:
			return null;
		}
	}
	
}
