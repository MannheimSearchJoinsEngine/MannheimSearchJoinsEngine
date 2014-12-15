package de.mannheim.uni.statistics;

import org.apache.commons.lang.time.DurationFormatUtils;

public class AggregatingTimer
	extends Timer
{

	private long _total;
	private int _count;
	
	public AggregatingTimer(String name) {
		super(name);
	}
	
	public AggregatingTimer(String name, Timer parent) {
		super(name, parent);
	}

	protected AggregatingTimer()
	{
		
	}
	
	protected void addDuration(long duration)
	{
		_total =+ duration;
		_count++;
	}
	
	@Override
	public void stop() {
		super.stop();
		
		addDuration(getDuration());
	}
	
	@Override
	protected String formatValue() {
		if(_total==0)
		{
			return super.formatValue();
		}
		else
		{
			String value="", valueAvg="";
			
			value = DurationFormatUtils.formatDuration(_total, "HH:mm:ss.S");
			valueAvg = DurationFormatUtils.formatDuration(_total/(long)_count, "HH:mm:ss.S");
			
			return getName() + ": " + value + "(" + _count + " times; " + valueAvg + " on avg.)";
		}
	}
}
