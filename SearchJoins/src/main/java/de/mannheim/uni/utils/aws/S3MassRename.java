package de.mannheim.uni.utils.aws;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class S3MassRename implements Runnable {

	private S3Helper s3;
	private String bucket;
	private String oldKey;
	private String newKey;
	
	public static void main(String[] args)
	{
		if(args.length!=4)
		{
			System.out.println("Usage: S3MassRename srcList bucket access-key secret-key");
			return;
		}

		String fileList, bucket, awsKey, awsSecret;
		fileList = args[0];
		bucket = args[1];
		awsKey = args[2];
		awsSecret = args[3];		
		
		try {
			BufferedReader r = new BufferedReader(new FileReader(fileList));
			
			String line;

			long id = 0;

			ThreadPoolExecutor p = new ThreadPoolExecutor(4, 16, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
			
			long startTime = System.currentTimeMillis();
			long todo=0;
			while((line = r.readLine()) != null)
			{
				String suffix = line.substring(line.lastIndexOf("."));
				String newKey = line.substring(0,line.lastIndexOf("/")) + "/" + (id++) + suffix;
				
				S3MassRename s3r = new S3MassRename(awsKey, awsSecret, bucket, line, newKey);
				
				p.submit(s3r);
				todo++;
			}
			
			while(todo>0)
			{
				long total = p.getTaskCount();
				long finished = p.getCompletedTaskCount();
				float runtime = (float)(System.currentTimeMillis() - startTime) / 1000.0f;
				System.out
						.printf("Runtime: %.2fs --> Total: %d, Done: %d, %ss / item, Finished in: %.2fs \n",
								runtime, total, finished,
								String.format("%.2f", ((float) runtime) / finished),
								((float) runtime / finished) * (total - finished));
				
				todo = total - finished;
				
				Thread.sleep(10000);
			}
			
			p.shutdown();
			
			r.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public S3MassRename(String accessKey, String secretKey, String bucket, String oldKey, String newKey)
	{
		s3 = new S3Helper(accessKey, secretKey);
		this.oldKey = oldKey;
		this.newKey = newKey;
		this.bucket = bucket;
	}
	
	public void run()
	{
		s3.Rename(bucket, oldKey, newKey);
	}
}
