package de.mannheim.uni.utils.concurrent;

import java.util.Date;
import java.util.Queue;
import java.util.logging.Level;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.log4j.Logger;

import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.statistics.Timer;

public class RunnableQueueSizeReporter 
	implements Runnable
{
	private Thread thread;
	private Queue q;
	private boolean stop=false;
	private Timer timer;
	private Logger logger; 
	
	public void setTimerToReport(Timer t)
	{
		timer = t;
	}
	
	public void start()
	{
		thread = new Thread(this);
		thread.start();
	}
	
	public void stop()
	{
		stop=true;
	}
	
	public RunnableQueueSizeReporter(Queue q)
	{
		this.q = q;
		
		logger = Logger.getLogger(RunnableQueueSizeReporter.class);
	}
	
	
	public void run() {
		int last = 0;
		
		while(!stop)
		{
			int change = q.size() - last;
			double changeRate = 0.0;
			
			if(last!=0)
				changeRate = (double)change / (double)last;
			
			String sign="";
			if(change>0)
				sign = "+";
			
			int stepsLeft = 0;
			
			if(q.size()>0 && change < 0)
				stepsLeft = Math.abs(q.size() / change);
			int millisLeft = stepsLeft * 10 * 1000;
			
			logger.info(new Date() + " Current queue size: " + q.size() + " (" + sign + change + " = " + changeRate*100 + "%) -> " + DurationFormatUtils.formatDuration(millisLeft, "HH:mm:ss.S") + " left");
			//System.out.println(new Date() + " Current queue size: " + q.size() + " (" + sign + String.format("0.00", change) + " = " + changeRate*100 + "%) -> " + DurationFormatUtils.formatDuration(millisLeft, "HH:mm:ss.S") + " left");
			if(timer!=null)
				logger.info(timer.toString());
				//System.out.println(timer.toString());
			
			last = q.size();
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
