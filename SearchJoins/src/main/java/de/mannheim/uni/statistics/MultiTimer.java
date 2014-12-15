package de.mannheim.uni.statistics;

import java.util.List;

/**
 * Used during Multi-Threading
 * @author Oliver
 *
 */
public class MultiTimer
	extends AggregatingTimer
{

	private AggregatingTimer _object;
	
	public MultiTimer(AggregatingTimer timer) {
		_object = timer;
		start();
		
		setCurrent(_object);
	}
	
	@Override
	public List<Timer> getChildren() {
		return _object.getChildren();
	}
	
	@Override
	protected void addChild(Timer child) {
		_object.addChild(child);
	}
	
	@Override
	protected boolean hasChild(Timer child) {
		return _object.hasChild(child);
	}
	
	@Override
	public void stop() {
		setEnd();
		_object.addDuration(super.getDuration());
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return _object.getName();
	}
	
	@Override
	protected StringBuilder print(String prefix) {
		return _object.print(prefix);
	}
	
	@Override
	protected void addDuration(long duration) {
		_object.addDuration(duration);
	}
	
	@Override
	protected String formatValue() {
		 return _object.formatValue();
	}
	
	@Override
	public long getDuration() {
		return _object.getDuration();
	}
	
	@Override
	public boolean equals(Object obj) {
		return _object.equals(obj);
	}
	
	@Override
	public int hashCode() {
		return _object.hashCode();
	}
	
	@Override
	public String toString() {
		return _object.toString();
	}
}
