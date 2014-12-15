package de.mannheim.uni.utils.aws;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class LocalQueueManager 
	extends QueueManager
{

	private Queue<String> queue;
	
	public LocalQueueManager(List<String> queueItems) {
		super("", "", "", "");
		queue = new LinkedList<String>(queueItems);
	}

	@Override
	public String nextFile() {
		return queue.poll();
	}
	
	@Override
	public void setFileProcessed() throws Exception {
	}
	
	@Override
	public long GetInflightSize() {
		return 0;
	}
	
	@Override
	public void SendBatch(ArrayList<String> data, long firstId) {
		for(String s : data)
			queue.add(s);
	}
	
	@Override
	public long GetQueueSize() {
		return queue.size();
	}
}
