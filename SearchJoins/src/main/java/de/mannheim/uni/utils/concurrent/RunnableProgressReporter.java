package de.mannheim.uni.utils.concurrent;

import java.util.Date;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;

import org.apache.log4j.Logger;

import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.statistics.Timer;

public class RunnableProgressReporter
	implements Runnable
{

	private ThreadPoolExecutor pool;
	private Thread thread;
	private Timer timer;
	private Task userTask;
	private boolean stop;
	private Logger logger;
	
	public RunnableProgressReporter()
	{
		
		logger = Logger.getLogger(RunnableProgressReporter.class);
	}
	
	public Task getUserTask() {
		return userTask;
	}
	
	public void setUserTask(Task userTask) {
		this.userTask = userTask;
	}
	
	public ThreadPoolExecutor getPool() {
		return pool;
	}

	public void setPool(ThreadPoolExecutor pool) {
		this.pool = pool;
	}

	public void setTimer(Timer t)
	{
		timer = t;
	}
	
	public void run() {
		try
		{
			long tasks = pool.getTaskCount();
			long done = pool.getCompletedTaskCount();
			long left = tasks - done;
			while(!stop)
			{
				tasks = pool.getTaskCount();
				done = pool.getCompletedTaskCount();
				left = tasks - done;
				logger.info(new Date() + ": " + done + " of " + tasks + " tasks completed (" + pool.getActiveCount() + "/" + pool.getPoolSize() + " active threads).");
				//System.out.println(new Date() + ": " + done + " of " + tasks + " tasks completed (" + pool.getActiveCount() + "/" + pool.getPoolSize() + " active threads).");
				if(userTask!=null)
					userTask.execute();
				if(timer!=null)
					logger.info(timer.toString());
					//System.out.println(timer.toString());
				
				Thread.sleep(10000);
			}
		}
		catch(Exception e)
		{
			
		}
	}
	
	public void start()
	{
		stop = false;
		thread = new Thread(this);
		//thread.run();
		thread.start();
	}
	
	public void stop()
	{
		stop = true;
	}

}
