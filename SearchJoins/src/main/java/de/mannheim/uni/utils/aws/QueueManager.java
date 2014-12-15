package de.mannheim.uni.utils.aws;

import java.util.ArrayList;
import java.util.logging.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

public class QueueManager {
	public static final int BATCH_SIZE = 10;
	private static Logger log = Logger.getLogger("QueueManager.class");
	public String queueName;
	public String queueEndpoint;
	private String queueUrl = null;
	private AmazonSQSClient sqs;
	private Message currentMessage;
	private String awsKey;
	private String awsSecret;
	
	public QueueManager(String awsKey, String awsSecret, String queueEndpoint, String queueName)
	{
		this.awsKey = awsKey;
		this.awsSecret = awsSecret;
		this.queueEndpoint = queueEndpoint;
		this.queueName = queueName;
	}
	
	protected com.amazonaws.auth.AWSCredentials getCredentials()
	{
		return new com.amazonaws.auth.AWSCredentials() {
			
			public String getAWSSecretKey() {
				return awsSecret;
			}
			
			public String getAWSAccessKeyId() {
				return awsKey;
			}
		};
	}
	
	protected AmazonSQS getQueue() {
		if (sqs == null) {
			
			sqs = new AmazonSQSClient(getCredentials());
			sqs.setEndpoint(queueEndpoint);
		}
		return sqs;
	}
	
	protected String getQueueUrl()
	{
		if (queueUrl == null) {
			if (queueName == null || queueName.trim().equals("")) {
				log.warning("No job queue given");
				return "";
			}
			try {
				GetQueueUrlResult res = getQueue().getQueueUrl(
						new GetQueueUrlRequest(queueName));
				queueUrl = res.getQueueUrl();
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				log.warning("Error requesting job queue");
				return "";
			}
		}
		
		return queueUrl;
	}
	
	public String nextFile()
	{
		// receive task message from queue
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
				getQueueUrl())
				.withAttributeNames("ApproximateReceiveCount");
		receiveMessageRequest.setMaxNumberOfMessages(1);
		ReceiveMessageResult queueRes = getQueue().receiveMessage(
				receiveMessageRequest);
		if (queueRes.getMessages().size() < 1) {
			log.warning("Queue is empty");
			return null;
		}
		currentMessage = queueRes.getMessages().get(0);
		
		System.out.println(Thread.currentThread().getName() + ": Message received: " + currentMessage.getBody());
		
		return currentMessage.getBody();
	}
	
	public void setFileProcessed() throws Exception
	{
		try
		{
			System.out.println("Deleting message " + currentMessage);
			getQueue().deleteMessage(
				new DeleteMessageRequest(getQueueUrl(), currentMessage
						.getReceiptHandle()));
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			throw ex;
		}
	}
	
	public void SendBatch(ArrayList<String> data, long firstId)
	{
		SendMessageBatchRequest smbr = new SendMessageBatchRequest(
				getQueueUrl());
		smbr.setEntries(new ArrayList<SendMessageBatchRequestEntry>());
		
		long id = firstId;
		for(String value : data)
		{
			SendMessageBatchRequestEntry smbre = new SendMessageBatchRequestEntry();
			smbre.setMessageBody(value);
			smbre.setId("task_" + id);
			smbr.getEntries().add(smbre);
			id++;
		}
		
		getQueue().sendMessageBatch(smbr);
	}
	
	public long GetQueueSize()
	{
		// get queue attributes
		GetQueueAttributesResult res = getQueue().getQueueAttributes(
				new GetQueueAttributesRequest(getQueueUrl())
						.withAttributeNames("All"));
		Long queueSize = Long.parseLong(res.getAttributes().get(
				"ApproximateNumberOfMessages"));
		
		return queueSize;
	}
	
	public long GetInflightSize()
	{
		GetQueueAttributesResult res = getQueue().getQueueAttributes(
				new GetQueueAttributesRequest(getQueueUrl())
						.withAttributeNames("All"));
		
		Long inflightSize = Long.parseLong(res.getAttributes().get(
				"ApproximateNumberOfMessagesNotVisible"));
		
		return inflightSize;
	}
}
