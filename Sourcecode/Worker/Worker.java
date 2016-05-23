import java.util.List;
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
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Worker {

	/**
	 * @param args
	 */
	static AmazonDynamoDBClient dynamoDB;
	static AmazonSQS sqs ;
	
	
/*	private static void init() throws Exception {
        
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/home/dipen/.aws/credentials).
         
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
                
    }*/

           	public static void main(String[] args) throws Exception {
           		// TODO Auto-generated method stub
           		 /*
                    * The ProfileCredentialsProvider will return your [default]
                    * credential profile by reading from the credentials file located at
                    * (/home/dipen/.aws/credentials).
                    */
           		
           	///*-----------------Worker -s ResponseQueue –t 1--------------------------*///
    			
    			Scanner sc = new Scanner(System.in);
    			
    			String command = sc.nextLine();
    			String[] commandArr=new String[15];
    			
    		
    				commandArr = command.split("\\s+");
    				for(int i=0;i<commandArr.length;i++){
    					System.out.println(commandArr[i]);
    				}
    				if("Worker".equals(commandArr[0]) && "-s".equals(commandArr[1])){
    					if("LOCAL".equals(commandArr[2])){
    						//Local
    						System.out.println("Local Strart");
    						//runLocalClient(commandArr);
    						
    					}
    					else{
    						//Remote
    						System.out.println("Remote Worker Strart");
        					
    						runRemoteWorker(commandArr);
    					}
    				}
    				else{
    					System.out.println("Command not found!!!");
    				}
    		
    		
            
           	}

           	
           	private static void runRemoteWorker(String[] commandArr) {
		// TODO Auto-generated method stub
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
        		
        		//init();
             //   GetQueueUrlResult myQueueUrls = sqs.getQueueUrl("MyQueue");
              
                //Create Queue
                
                //Request Queue
                CreateQueueRequest createQueueRequest = new CreateQueueRequest("RequestQueue");
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
                
               int i=1;
              
               while(i>-1){
               try {	
                
        		// Receive messages
                System.out.println("Receiving messages from MyQueue.\n");
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
                System.out.println("Receving Messages size :"+messages.size());
                
                for (Message message : messages) {
                
               
               
                /*    System.out.println("  Message");
                    System.out.println("    MessageId:     " + message.getMessageId());
                    System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                    System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                    System.out.println("    Body:          " + message.getBody());
                */    
                	        	try {
        				System.out.println("-------------------------------------------");
        			 	
        				/*---------check in DynamoDB------------*/
        				
        			   boolean checkDynamo=checkInDynamoDb(message);
        		       System.out.println("checkDynamo value:"+checkDynamo);
        				if (checkDynamo){
        					//Message exist in DynamoDB
        					//Delete Message
        					System.out.println("Msg Deleted:----------->"+i);
        			        i++;
        					System.out.println("Deleting a message.\n");
        			        String messageReceiptHandle = messages.get(0).getReceiptHandle();
        			        sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
        			        
        				}
        				else{
        					//Message Does not exist in DynamoDB
        					//Message Added in DynamoDB in while checkInDynamoDB
        					sqs.sendMessage(new SendMessageRequest(myQueueUrl1, "0"));	

        				}
        				//Thread.sleep(Integer.parseInt());
                		
        				System.out.println("-------------------------------------------");
                		
        			} catch (NumberFormatException e ) {
        				// TODO Auto-generated catch block
        				System.out.println(e);
        			}catch (NullPointerException e){
        				System.out.println(e);
        			}
                 
                    for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                        System.out.println("  Attribute");
                        System.out.println("    Name:  " + entry.getKey());
                        System.out.println("    Value: " + entry.getValue());
                    }
                }
                System.out.println();

                //Sleep thread
            //    Thread.sleep(10000);
               
              

         /*       // Delete a queue
                System.out.println("Deleting the test queue.\n");
                sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
        */  } catch (AmazonServiceException ase) {
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
            } catch (IndexOutOfBoundsException e){
            	System.out.println("No more Messages in Queue");
            	System.exit(0);
            }
            }
		
	}


			private static boolean checkInDynamoDb(Message message) {
           		 DynamoDB dynamo = new DynamoDB(dynamoDB);
           		 String tableName = "Task-Table";
           		 Table table = new Table(dynamoDB, tableName);
           		 Item item1 ;
           		 String taskBody=message.getBody();
           		
           		 StringTokenizer st;
           		 st=new StringTokenizer(message.getBody()," ");
                	 st.nextToken();

           		 System.out.println("MessageBody:"+taskBody);//messageId;
           		 String taskId=message.getMessageId();
                   try {
                     	            // Create table if it does not exist yet
                       if (Tables.doesTableExist(dynamoDB, tableName)) {
                           System.out.println("Table " + tableName + " is already ACTIVE");
                       
                       
           			   GetItemSpec spec = new GetItemSpec().withPrimaryKey("TaskId", taskId, "Job", taskBody);
                          System.out.println("Attempting to read the item...");
                          Item outcome = table.getItem(spec);
                          
                   
                          if(outcome==null){
                       	   item1= new Item().withPrimaryKey("TaskId", taskId,"Job",taskBody);//.with("Job","done");
                       	   System.out.println("Task added in DynamoDb :"+item1); 
                       	   table.putItem(item1);
                       	   
                       	   //Run Sleep Task
                       	   Thread.sleep(Integer.parseInt( st.nextToken()));
                       	   return false;
                          }
                          else{
                       	// Delete a message
                       	   System.out.println("entering else");
                             return true;
                          }
           	
           		// TODO Auto-generated method stub
           		
                       } else {
                       	System.out.println("Table Does not Exist");
                       	return true;
                       }
                   	}catch(Exception e){

                    	   System.out.println(e);
                   		
                   		return true;
                       }
             	   
           			
                   }
           }

