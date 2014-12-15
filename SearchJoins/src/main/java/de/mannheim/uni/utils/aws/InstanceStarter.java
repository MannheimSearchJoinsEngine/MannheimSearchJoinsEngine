package de.mannheim.uni.utils.aws;


/*
 * 
 * This is a modified copy of org.fuberlin.wbsg.ccrdf.Master
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.martiansoftware.jsap.JSAPException;


public class InstanceStarter  {
	private static Logger log = Logger.getLogger("InstanceStarter.class");
	private RestS3Service s3;
	String awsAccessKey;
	String awsSecretKey;
	private String jarDeployBucket;
	private String jarDeployKey;

	// command line parameters, different actions
	public static void main(String[] args) throws JSAPException {
		/* set up command line parameters */
		Options options = new Options();

		options.addOption(OptionBuilder.hasArgs(13).create("start"));
		options.addOption(OptionBuilder.hasArgs(3).create("monitor_cpu"));
		options.addOption(OptionBuilder.hasArgs(5).create("monitor"));
		options.addOption(OptionBuilder.hasArgs(3).create("shutdown"));
		options.addOption(OptionBuilder.hasArgs(5).create("queue"));
		
		/* parse command line parameters */
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);

			// start
			String[] io = cmd.getOptionValues("start");
			if (io != null) {
				int instanceCount = Integer.parseInt(io[2]);
				double priceLimit = Double.parseDouble(io[3]);
				String instanceType = io[4];
				String ec2ami = io[5];
				String ec2keypair = io[6];
				String ec2endpoint = io[7];
				String javamemory = io[8];
				String queueName = io[9];
				String queueEndpoint = io[10];
				String outputBucket = io[11];
				// JAR file must be placed in output-bucket
				String jarKey = io[12];

				new InstanceStarter(io[0], io[1]).createInstances(instanceCount, priceLimit, 
						instanceType, ec2ami, ec2keypair, ec2endpoint, javamemory, queueName,
						queueEndpoint, outputBucket, jarKey);
				
				System.out.println("done.");
				System.exit(0);
			}
			
			// monitor_cpu
			io = cmd.getOptionValues("monitor_cpu");
			if (io != null) {
				new InstanceStarter(io[0], io[1]).monitorCPUUsage(io[2]);
				System.exit(0);
			}

			// monitor
			io = cmd.getOptionValues("monitor");
			if (io != null) {
				new InstanceStarter(io[0], io[1]).monitorQueue(io[2], io[3], io[4]);
			}
			
			// shutdown
			io = cmd.getOptionValues("shutdown");
			if (io != null) {
				System.out.print("Cancelling spot request and shutting down all worker instances in EC2...");
				new InstanceStarter(io[0], io[1]).shutdownInstances(io[2]);
				System.out.println("done.");
				System.exit(0);
			}
			
			// queue
			io = cmd.getOptionValues("queue");
			if (io != null) {
				new InstanceStarter(io[0], io[1]).queue(io[2], io[3], io[4]);
				System.out.println("done.");
				System.exit(0);
			}			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}

		System.out.println("Usage: InstanceStarter -start <AWS key> <AWS secret key> <Instance Count> <Price Limit> <Instance type> <ami> <keypair> <ec2 endpoint> <java memory> <queue name> <queue endpoint> <output bucket> <jar file key>");
		System.out.println("Usage: InstanceStarter -monitor_cpu <AWS key> <AWS secret key> <ec2 endpoint>");
		System.out.println("Usage: InstanceStarter -monitor <AWS key> <AWS secret key> <Queue Name> <Queue endpoint>");
		System.out.println("Usage: InstanceStarter -shutdown <AWS key> <AWS secret key> <ec2 endpoint>");
		System.out.println("Usage: InstanceStarter -queue <AWS key> <AWS secret key> <ec2 endpoint> <Queue Name> <queue endpoint>");
	}

	public InstanceStarter(String awsAccessKey, String awsSecretKey)
	{
		this.awsAccessKey = awsAccessKey;
		this.awsSecretKey = awsSecretKey;
	}
	
	protected RestS3Service getStorage() {
		if (s3 == null) {
			try {
				s3 = new RestS3Service(new AWSCredentials(awsAccessKey, awsSecretKey));
			} catch (S3ServiceException e1) {
				log.warning("Unable to connect to S3");
			}
		}
		return s3;
	}

	private String getJarUrl() {
		return "https://s3.amazonaws.com/" + jarDeployBucket + "/" + jarDeployKey;
	}

	/**
	 * NOTICE: This is a startup shell script for EC2 instances. It installs
	 * Java, downloads the Extractor JAR from S3 and launches it.
	 * 
	 * This is designed to work on the Ubuntu AMI
	 * "Ubuntu 11.10 Oneiric instance-store" from www.alestic.com see
	 * http://alestic.com/2009/06/ec2-user-data-scripts
	 * */

	private com.amazonaws.auth.AWSCredentials getAwsCredentials()
	{
		return new com.amazonaws.auth.AWSCredentials() {
			public String getAWSAccessKeyId() {
				return awsAccessKey;
			}

			public String getAWSSecretKey() {
				return awsSecretKey;
			}
		};
	}
	
	private String getStartupScript(String javamemory, String queueName, String queueEndpoint, String outputBucket)
	{
		// TODO change startup script to run our programm
		return "#!/bin/bash \n echo 1 > /proc/sys/vm/overcommit_memory \n aptitude update \n aptitude -y install openjdk-6-jre-headless htop \n wget -O /tmp/start.jar \""
				+ getJarUrl()
				+ "\" \n mkdir /tmp/output \n"
				+ "sudo java -Xmx"
				+ javamemory.trim()
				+ " -cp /tmp/start.jar de.mannheim.uni.webtables.TableStatisticsExtractor "
				+ " > /tmp/start.log 2>&1 & \n";
	}
	
	public void createInstances(int count, double priceLimitDollars, String ec2instancetype, 
			String ec2ami, String ec2keypair, String ec2endpoint, String javamemory, 
			String queueName, String queueEndpoint, String outputBucket, String jarKey) {
		this.jarDeployBucket = outputBucket;
		this.jarDeployKey = jarKey;
		
		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(ec2endpoint);

		log.info("Requesting " + count + " instances of type "
				+ ec2instancetype + " with price limit of "
				+ priceLimitDollars + " US$");
		log.info("Startup script:\n" + getStartupScript(javamemory, queueName, queueEndpoint, outputBucket));

		try {
			// our bid
			RequestSpotInstancesRequest runInstancesRequest = new RequestSpotInstancesRequest()
					.withSpotPrice(Double.toString(priceLimitDollars))
					.withInstanceCount(count).withType("persistent");

			// increase volume size
			// BlockDeviceMapping mapping = new BlockDeviceMapping()
			// .withDeviceName("/dev/sda1").withEbs(
			// new EbsBlockDevice().withVolumeSize(Integer
			// .parseInt(getOrCry("ec2disksize"))));

			// what we want
			LaunchSpecification workerSpec = new LaunchSpecification()
					.withInstanceType(ec2instancetype)
					.withImageId(ec2ami)
					.withKeyName(ec2keypair)
					// .withBlockDeviceMappings(mapping)
					.withUserData(
							new String(Base64.encodeBase64(getStartupScript(javamemory, queueName, queueEndpoint, outputBucket)
									.getBytes())));

			runInstancesRequest.setLaunchSpecification(workerSpec);

			// place the request
			ec2.requestSpotInstances(runInstancesRequest);
			log.info("Request placed, now use 'monitor' to check how many instances are running. Use 'shutdown' to cancel the request and terminate the corresponding instances.");
		} catch (Exception e) {
			e.printStackTrace();
			log.warning("Failed to start instances - ");
		}
	}

	public void monitorCPUUsage(String ec2endpoint) {
		AmazonCloudWatchClient cloudClient = new AmazonCloudWatchClient(
				getAwsCredentials());
		GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
		Calendar cal = Calendar.getInstance();
		request.setEndTime(cal.getTime());
		cal.add(Calendar.MINUTE, -5);
		request.setStartTime(cal.getTime());
		request.setNamespace("AWS/EC2");
		List<String> statistics = new ArrayList<String>();
		statistics.add("Maximium");
		statistics.add("Average");
		request.setStatistics(statistics);
		request.setMetricName("CPUUtilization");
		request.setPeriod(300);
		
		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(ec2endpoint);
		DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();
		DescribeSpotInstanceRequestsResult describeResult = ec2
				.describeSpotInstanceRequests(describeRequest);
		List<SpotInstanceRequest> describeResponses = describeResult
				.getSpotInstanceRequests();
		
		for(SpotInstanceRequest instance : describeResponses)
		{
			List<Dimension> dimensions = new ArrayList<Dimension>();
			
			Dimension dimension = new Dimension();
			dimension.setName("InstanceId");
			dimension.setValue(instance.getInstanceId());
			dimensions.add(dimension);		
			
			request.setDimensions(dimensions);
			
			GetMetricStatisticsResult result = cloudClient
					.getMetricStatistics(request);
			List<Datapoint> dataPoints = result.getDatapoints();
			for (Datapoint dataPoint : dataPoints) {
				System.out.println(dataPoint.getAverage());
			}
		}
		
		/*Dimension dimension = new Dimension();
		dimension.setName("InstanceId");
		dimension.setValue("i-d93fa2a4");
		List<Dimension> dimensions = new ArrayList<Dimension>();
		dimensions.add(dimension);*/


	}

	public void shutdownInstances(String ec2endpoint) {
		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(ec2endpoint);

		try {
			// cancel spot request, so no new instances will be launched
			DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();
			DescribeSpotInstanceRequestsResult describeResult = ec2
					.describeSpotInstanceRequests(describeRequest);
			List<SpotInstanceRequest> describeResponses = describeResult
					.getSpotInstanceRequests();
			List<String> spotRequestIds = new ArrayList<String>();
			List<String> instanceIds = new ArrayList<String>();

			for (SpotInstanceRequest describeResponse : describeResponses) {
				spotRequestIds.add(describeResponse.getSpotInstanceRequestId());
				if ("active".equals(describeResponse.getState())) {
					instanceIds.add(describeResponse.getInstanceId());
				}
			}
			ec2.cancelSpotInstanceRequests(new CancelSpotInstanceRequestsRequest()
					.withSpotInstanceRequestIds(spotRequestIds));
			log.info("Cancelled spot request");

			if (instanceIds.size() > 0) {
				ec2.terminateInstances(new TerminateInstancesRequest(
						instanceIds));
				log.info("Shut down " + instanceIds.size() + " instances");
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.warning("Failed to shutdown instances - ");
		}
	}

	public void deploy(File jarFile) {
		String deployBucket = jarDeployBucket;
		String deployFilename = jarDeployKey;

		try {
			getStorage().getOrCreateBucket(deployBucket);
			AccessControlList bucketAcl = getStorage().getBucketAcl(
					deployBucket);
			bucketAcl.grantPermission(GroupGrantee.ALL_USERS,
					Permission.PERMISSION_READ);

			S3Object statFileObject = new S3Object(jarFile);
			statFileObject.setKey(deployFilename);
			statFileObject.setAcl(bucketAcl);

			getStorage().putObject(deployBucket, statFileObject);

			log.info("File " + jarFile + " now accessible at " + getJarUrl());
		} catch (Exception e) {
			log.warning("Failed to deploy or set permissions in bucket  "
					+ deployBucket + ", key " + deployFilename);
		}
	}
	
	
	public void queue(String queueName, String queueEndpoint, String fileName) throws IOException {
		QueueManager qm = new QueueManager(awsAccessKey, awsSecretKey, queueEndpoint, queueName);
		
		ArrayList<String> data = new ArrayList<String>();
		
		long objectsQueuedTotal = 0;
		long objectsQueuedSegment=0;
			
		List<String> messages = new ArrayList<String>();
		
		BufferedReader r = new BufferedReader(new FileReader(fileName));
		String line;
		
		while((line = r.readLine()) != null)
			messages.add(line);
		
		r.close();
		
		data.clear();
		try
		{
			//for (S3Object object : getStorage().listObjects(bucket,
					//prefix, null))
			for(String s : messages)
			{
				//data.add(object.getKey());
				if(!s.startsWith("stats"))
				{
					data.add(s);
					objectsQueuedSegment++;
					objectsQueuedTotal++;
					
					if (data.size() == QueueManager.BATCH_SIZE) {
						qm.SendBatch(data, objectsQueuedTotal-data.size());
						data.clear();
					}
				}
			}
			
			// send the rest
			if (data.size() > 0) {
				qm.SendBatch(data, objectsQueuedTotal-data.size());
				data.clear();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		log.info("Queued " + objectsQueuedTotal + " objects.");
	}
	
	private class DateSizeRecord {
		Date recordTime;
		Long queueSize;

		public DateSizeRecord(Date time, Long size) {
			this.recordTime = time;
			this.queueSize = size;
		}
	}
	
	public void monitorQueue(String ec2endpoint, String queueName, String queueEndpoint) {
		System.out
				.println("Monitoring job queue, extraction rate and running instances.");
		System.out.println();

		List<DateSizeRecord> sizeLog = new ArrayList<DateSizeRecord>();
		DecimalFormat twoDForm = new DecimalFormat("#.##");

		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(ec2endpoint);
		
		QueueManager qm = new QueueManager(awsAccessKey, awsSecretKey, queueEndpoint, queueName);

		while (true) {
			try {
				DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();
				DescribeSpotInstanceRequestsResult describeResult = ec2
						.describeSpotInstanceRequests(describeRequest);
				List<SpotInstanceRequest> describeResponses = describeResult
						.getSpotInstanceRequests();

				int requestedInstances = 0;
				int runningInstances = 0;
				for (SpotInstanceRequest describeResponse : describeResponses) {
					if ("active".equals(describeResponse.getState())) {
						runningInstances++;
						requestedInstances++;
					}
					if ("open".equals(describeResponse.getState())) {
						requestedInstances++;
					}
				}

				// get queue attributes
				Long queueSize = qm.GetQueueSize();
				Long inflightSize = qm.GetInflightSize();

				// add the new value to the tail, now remove too old stuff from
				// the
				// head
				DateSizeRecord nowRecord = new DateSizeRecord(Calendar
						.getInstance().getTime(), queueSize + inflightSize);
				sizeLog.add(nowRecord);

				int windowSizeSec = 120;

				// remove outdated entries
				for (DateSizeRecord rec : new ArrayList<DateSizeRecord>(sizeLog)) {
					if (nowRecord.recordTime.getTime()
							- rec.recordTime.getTime() > windowSizeSec * 1000) {
						sizeLog.remove(rec);
					}
				}
				// now the first entry is the first data point, and the entry
				// just
				// added the last;
				DateSizeRecord compareRecord = sizeLog.get(0);
				double timeDiffSec = (nowRecord.recordTime.getTime() - compareRecord.recordTime
						.getTime()) / 1000;
				long sizeDiff = compareRecord.queueSize - nowRecord.queueSize;

				double rate = sizeDiff / timeDiffSec;

				System.out.print('\r');

				if (rate > 0) {
					System.out.print("Q: " + queueSize + " (" + inflightSize
							+ "), R: " + twoDForm.format(rate * 60)
							+ " m/min, ETA: "
							+ twoDForm.format((queueSize / rate) / 3600)
							+ " h, N: " + runningInstances + "/"
							+ requestedInstances + "          ");
				} else {
					System.out.print("Q: " + queueSize + " (" + inflightSize
							+ "), N: " + runningInstances + "/"
							+ requestedInstances
							+ "                          	");
				}

			} catch (AmazonServiceException e) {
				System.out.print("\r! // ");
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// who cares if we get interrupted here
			}
		}
	}
}
