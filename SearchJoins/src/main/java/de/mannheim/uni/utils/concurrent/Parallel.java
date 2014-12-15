package de.mannheim.uni.utils.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.mannheim.uni.statistics.Timer;

public class Parallel<T> {

	public interface Consumer<T> {
		void execute(T parameter);
	}

	private static int defaultNumProcessors = Runtime.getRuntime()
			.availableProcessors();
	private int overrideNumProcessors = 0;
	private static Parallel<?> currentTask = null;
	private static ConcurrentHashMap<Object, Lock> locks = new ConcurrentHashMap<Object, Lock>();

	public static void lock(Object obj) {
		Lock l = null;

		synchronized (locks) {
			if (locks.containsKey(obj))
				l = locks.get(obj);
			else {
				l = new ReentrantLock();
				locks.put(obj, l);
			}

		}

		l.lock();
	}

	public static void unlock(Object obj) {
		synchronized (locks) {
			Lock l = null;

			if (locks.containsKey(obj))
				l = locks.remove(obj);
			else
				return;

			l.unlock();
		}
	}

	public Parallel() {

	}

	public Parallel(int numProcessors) {
		overrideNumProcessors = numProcessors;
	}

	public static void SetDefaultNumProcessors(int numProcessors) {
		defaultNumProcessors = numProcessors;
	}

	private int getNumProcessors() {
		if (overrideNumProcessors > 0)
			return overrideNumProcessors;
		else
			return defaultNumProcessors;
	}

	private static int getNumProcessors(Parallel<?> obj) {
		// if this is a nested parallel process, only use 1 thread ...
		if (currentTask == null || currentTask == obj)
			return obj.getNumProcessors();
		else
			return 1;
	}

	private static boolean startParallelProcess(Parallel<?> obj) {
		if (currentTask == null) {
			currentTask = obj;
			return true;
		} else
			return false;
	}

	private static void endParallelProcess(Parallel<?> obj) {
		if (currentTask == obj)
			currentTask = null;
	}

	public static Thread run(final Task task) {
		Runnable r = new Runnable() {

			public void run() {
				task.execute();
			}
		};

		Thread t = new Thread(r, "Parallel.run thread");

		t.start();

		return t;
	}

	public static void forLoop(int from, int to,
			final Consumer<Integer> loopBody) throws InterruptedException {
		List<Integer> lst = new LinkedList<Integer>();

		for (int i = from; i < to; i++)
			lst.add(i);

		new Parallel<Integer>().foreach(lst, loopBody);
	}

	public void foreach(Iterable<T> items, final Consumer<T> loopBody)
			throws InterruptedException {
		foreach(items, loopBody, null);
	}

	public void foreach(Iterable<T> items, final Consumer<T> body,
			Timer timerToReport) throws InterruptedException {
		boolean isOuter = startParallelProcess(this);

		Iterator<T> it = items.iterator();

		ThreadPoolExecutor pool = new ThreadPoolExecutor(
				getNumProcessors(this), getNumProcessors(this), 0,
				TimeUnit.SECONDS,
				new java.util.concurrent.LinkedBlockingQueue<Runnable>(),
				new ThreadFactory() {

					public Thread newThread(Runnable r) {
						return new Thread(r, "Parallel.foreach thread");
					}
				});

		while (it.hasNext()) {
			final T value = it.next();

			Runnable r = new Runnable() {

				public void run() {
					body.execute(value);
				}
			};

			pool.execute(r);
		}

		RunnableProgressReporter p = new RunnableProgressReporter();
		p.setPool(pool);
		p.setTimer(timerToReport);
		p.start();

		pool.shutdown();

		pool.awaitTermination(1, TimeUnit.DAYS);
		p.stop();

		endParallelProcess(this);
	}

	public void producerConsumer(final Producer<T> producer,
			final Consumer<T> consumer) {
		boolean isOuter = startParallelProcess(this);
		// final Timer tim = new Timer("Parallel.producerConsumer");

		ThreadPoolExecutor pool = new ThreadPoolExecutor(
				getNumProcessors(this), getNumProcessors(this), 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
				new ThreadFactory() {

					public Thread newThread(Runnable r) {
						return new Thread(r, "Parallel.producerConsumer thread");
					}
				});

		producer.setConsumer(consumer);
		producer.setPool(pool);

		RunnableProgressReporter rpr = new RunnableProgressReporter();
		rpr.setPool(pool);
		// p.setTimer(timerToReport);
		if (isOuter)
			rpr.start();

		// start the producer thread
		Thread p = run(new Task() {

			@Override
			public void execute() {
				// Timer tp = new Timer("Producer", tim);
				producer.execute();
				// tp.stop();
			}
		});

		// wait for the producer thread to finish
		try {
			p.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (isOuter)
			System.out.println("Producer finished.");

		// wait for the consumer threads to finish

		pool.shutdown();

		try {
			pool.awaitTermination(10, TimeUnit.MINUTES);
			pool.shutdownNow();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			pool.shutdownNow();
		}
		rpr.stop();

		endParallelProcess(this);
	}

}
