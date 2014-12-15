package de.mannheim.uni.utils.concurrent;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import de.mannheim.uni.utils.concurrent.Parallel.Consumer;

public abstract class Producer<T> {

	public abstract void execute();
	
	private ThreadPoolExecutor pool;
	private Consumer<T> consumer;
	
	public void setPool(ThreadPoolExecutor pool)
	{
		this.pool = pool;
	}
	
	public void setConsumer(Consumer<T> consumer)
	{
		this.consumer = consumer;
	}
	
	protected void produce(final T value)
	{
		pool.execute(new Runnable() {
			
			public void run() {
				consumer.execute(value);
			}
		});
	}
	
}
