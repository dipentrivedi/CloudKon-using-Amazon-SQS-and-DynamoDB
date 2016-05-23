/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web
 * Services developer account, and be signed up to use Amazon SQS. For more
 * information on Amazon SQS, see http://aws.amazon.com/sqs.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (/home/dipen/.aws/credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 */
public class Client extends Thread{

	/**
	 * @param args
	 */
		static Queue<String> reqQueue = new LinkedList<String>();
		static Queue<Integer> resQueue = new LinkedList<Integer>();
		
		public void run(){	
			int i=0;
			while (!reqQueue.isEmpty()) {
				 try {
					Thread.sleep(Integer.parseInt(reqQueue.poll()));
					System.out.println("i---->"+i++);
					resQueue.add(0);
				} catch (NumberFormatException | InterruptedException e) {
					// TODO Auto-generated catch block
					resQueue.add(1);
					System.exit(0);
				}
			}
		}
		
		static AmazonDynamoDBClient dynamoDB;
		static AmazonSQS sqs ;
		
		private static void init() throws Exception {
	        /*
	         * The ProfileCredentialsProvider will return your [default]
	         * credential profile by reading from the credentials file located at
	         * (/home/dipen/.aws/credentials).
	         */
	        AWSCredentials credentials = null;
	        try {
	            credentials = new ProfileCredentialsProvider("default").getCredentials();
	        } catch (Exception e) {
	            throw new AmazonClientException(
	                    "Cannot load the credentials from the credential profiles file. " +
	                    "Please make sure that your credentials file is at the correct " +
	                    "location (/home/dipen/.aws/credentials), and is in valid format.",
	                    e);
	        }
	        dynamoDB = new AmazonDynamoDBClient(credentials);
	        sqs = new AmazonSQSClient(credentials);
	        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
	        dynamoDB.setRegion(usEast1);
	        sqs.setRegion(usEast1);        
	    }
		
		
		public static void main(String[] args) throws Exception {
			// TODO Auto-generated method stub
			
			///*-------------------------------------------*///
			
			Scanner sc = new Scanner(System.in);
			
			String command = sc.nextLine();
			String[] commandArr=new String[15];
			
			
				commandArr = command.split("\\s+");
				if("Client".equals(commandArr[0]) && "-s".equals(commandArr[1])){
					if("LOCAL".equals(commandArr[2])){
						//Local
						System.out.println("Local Strart");
						runLocalClient(commandArr);
						
					}
					else{
						//Remote
						runRemoteClient(commandArr);
					}
				}
				else{
					System.out.println("Command not found!!!");
				}
			
		
		}
		private static void runLocalClient(String[] commandArr) throws IOException {
			// TODO Auto-generated method stub

			///*-------------------Client -s LOCAL -t 1 -w /home/dipen/A4/1worker/10------------------------*///
			
			
			String sourceFilePath=commandArr[6];

			String line=null;
			StringTokenizer st;
			
			//first Args[0] No. of Threads
			int nThreads=Integer.parseInt(commandArr[4]);;//Integer.parseInt(args[0]);
			
			//second Args[1] FilePath
			String filePath=sourceFilePath;//+args[1];

			BufferedReader fileReader=new BufferedReader(new FileReader(filePath));
			
			while((line=fileReader.readLine())!=null){
				st=new StringTokenizer(line," ");
				st.nextToken();
				reqQueue.add(st.nextToken());
			}
			
			System.out.println("Request Queue Size:"+reqQueue.size());
			int[] threads=new int[]{nThreads};
			double startTime=0;
			for(int j=0;j<threads.length;j++){
				
				Client []t1=new Client[threads[j]];

				startTime=System.currentTimeMillis();
				
				for(int i=0;i<threads[j];i++){
					t1[i]=new Client();
					System.out.println("before thread start");
					t1[i].start();
				}
				
				System.out.println("thread start done");	
				
				for(int k=0;k<threads[j];k++){
					System.out.println("before thread join");
					
					try {
						t1[k].join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}	
				double endTime=System.currentTimeMillis();
				double TotalTime=endTime-startTime;
				
				System.out.println("Total Time :"+TotalTime);
				int sumResQueue=0;//new int[resQueue.size()];
				System.out.println("Size of Response Queue :"+resQueue.size());
				while(!resQueue.isEmpty()){
					sumResQueue=sumResQueue+resQueue.poll();
				}
				
				if(sumResQueue>0){
					System.out.println("Error in Task");
				}
				else {
					System.out.println("Task Done Successfully");
				}
			
			fileReader.close();
			
		}
		
		
		 private static void runRemoteClient(String[] commandArr) throws Exception {
				// TODO Auto-generated method stub
				/*
		         * The ProfileCredentialsProvider will return your [default]
		         * credential profile by reading from the credentials file located at
		         * (/home/dipen/.aws/credentials).
		         */
			 	
			///*-------------------Client -s RequestQueue -t 1 -w /home/dipen/A4/1worker/10------------------------*///
				
			 	init();
			 	//DynamoDB Create Table
		    	
		    	DynamoDB dynamo = new DynamoDB(dynamoDB);
		        Table tab = null;
		        try {
		            String tableName = "Task-Table";

		            // Create table if it does not exist yet
		            if (Tables.doesTableExist(dynamoDB, tableName)) {
		                System.out.println("Table " + tableName + " is already ACTIVE");
		            } else {

		            	CreateTableRequest createTableRequest =new CreateTableRequest().
		            			withTableName(tableName).
		            			withKeySchema(Arrays.asList(
		                            new KeySchemaElement("TaskId", KeyType.HASH),  //Partition key
		                            new KeySchemaElement("Job", KeyType.RANGE))).
		            			withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)).
		            			withAttributeDefinitions(new AttributeDefinition("TaskId", ScalarAttributeType.S),
		                                new AttributeDefinition("Job", ScalarAttributeType.S));
		            	tab=dynamo.createTable(createTableRequest);
		            	tab.waitForActive();
		            	System.out.println("Waiting for " + tableName + " to become ACTIVE...");
		            }
		        } catch (AmazonServiceException ase) {
		            System.out.println("Caught an AmazonServiceException, which means your request made it "
		                    + "to AWS, but was rejected with an error response for some reason.");
		            System.out.println("Error Message:    " + ase.getMessage());
		            System.out.println("HTTP Status Code: " + ase.getStatusCode());
		            System.out.println("AWS Error Code:   " + ase.getErrorCode());
		            System.out.println("Error Type:       " + ase.getErrorType());
		            System.out.println("Request ID:       " + ase.getRequestId());
		        } catch (AmazonClientException ace) {
		            System.out.println("Caught an AmazonClientException, which means the client encountered "
		                    + "a serious internal problem while trying to communicate with AWS, "
		                    + "such as not being able to access the network.");
		            System.out.println("Error Message: " + ace.getMessage());
		        }
		        
		        System.out.println("===========================================");
		        System.out.println("Getting Started with Amazon SQS");
		        System.out.println("===========================================\n");

		        try {
		            // Create a queue
		        	//Request Queue
		            System.out.println("Creating a new SQS queue called "+commandArr[2]);
		            CreateQueueRequest createQueueRequest = new CreateQueueRequest(commandArr[2]);
		            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
		            
		            //Response Queue
		            CreateQueueRequest createQueueRequest1 = new CreateQueueRequest("ResponseQueue");
		            String myQueueUrl1 = sqs.createQueue(createQueueRequest1).getQueueUrl();
		            System.out.println(myQueueUrl);
		            
		            // List queues
		            System.out.println("Listing all queues in your account.\n");
		            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
		                System.out.println("  QueueUrl: " + queueUrl);
		            }
		           
		            System.out.println();

		            // Send a message
		            System.out.println("Sending a message to "+commandArr[2]);
		  
		            //fetching the data from the file
		            String sourceFilePath=commandArr[6];
		    		
		    		BufferedReader fileReader=new BufferedReader(new FileReader(sourceFilePath));
		    		
		    		String line=null;
		    		int sendCount=0;
		    		while((line=fileReader.readLine())!=null){
		    			sendCount++;
		    			sqs.sendMessage(new SendMessageRequest(myQueueUrl, line));	
		    		}
		    		fileReader.close();
		    		
		    		// Receive messages
		    		while(sendCount==0){
		    		try{
		    		System.out.println("Receiving messages from Response Queue.\n");
		           
		    		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl1);
		            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		            System.out.println("Receving Messages size :"+messages.size());
		            
		            for (Message message : messages) {	
		            	System.out.println( "Message Received Succesfully. Id:"+message.getMessageId());
		            }
		       		}catch(Exception e){
		       			System.out.println("Response Queue Errro:"+e);
		       		}
		    	}
		   } catch (AmazonServiceException ase) {
		            System.out.println("Caught an AmazonServiceException, which means your request made it " +
		                    "to Amazon SQS, but was rejected with an error response for some reason.");
		            System.out.println("Error Message:    " + ase.getMessage());
		            System.out.println("HTTP Status Code: " + ase.getStatusCode());
		            System.out.println("AWS Error Code:   " + ase.getErrorCode());
		            System.out.println("Error Type:       " + ase.getErrorType());
		            System.out.println("Request ID:       " + ase.getRequestId());
		        } catch (AmazonClientException ace) {
		            System.out.println("Caught an AmazonClientException, which means the client encountered " +
		                    "a serious internal problem while trying to communicate with SQS, such as not " +
		                    "being able to access the network.");
		            System.out.println("Error Message: " + ace.getMessage());
		        }
		    
			}
  
}
