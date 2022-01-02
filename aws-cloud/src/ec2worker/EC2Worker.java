package ec2worker;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class EC2Worker {
	
	S3Client s3Client;
	SqsClient sqsClient;
	
	EC2Worker(S3Client s3Client,SqsClient sqsClient){
		this.s3Client = s3Client;
		this.sqsClient = sqsClient;
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		Region region = Region.US_EAST_1;
		SqsClient sqsClient = SqsClient.builder().region(region).build();
		S3Client s3Client = S3Client.builder().region(region).build();
		EC2Worker ec2Worker = new EC2Worker(s3Client,sqsClient);
		//Create an AWS SQS queue named Inbox to receive messages from clients
		String Inbox = "Inbox";
		//ec2Worker.createQueue(Inbox);
		String Outbox ="Outbox";
		//ec2Worker.createQueue(Outbox);
		List<Message> messages;
		String message ="";
		String bucketName="boutaina3667" ;
		String fileName ;
		String outputPath = "C:\\Users\\HP\\Desktop\\EMSE\\data.csv";
		String resultFile="C:\\Users\\HP\\Desktop\\EMSE\\result.txt";
		boolean isDeleted=false;
		int totalNumberOfSales;
		
		do {
			messages = ec2Worker.retreiveMsg(Inbox);
			if(!messages.isEmpty()) {
				 message = messages.get(0).body();
				 ec2Worker.deleteMsg(sqsClient, Inbox, messages);
				 isDeleted=true;
				 String parts[] = message.split(" ");
				 ec2Worker.getObjectBytes(s3Client, bucketName, "data.txt", outputPath);
				 totalNumberOfSales=ec2Worker.count(outputPath);
				 try {
				      FileWriter myWriter = new FileWriter(resultFile);
				      myWriter.write("total number od sales "+totalNumberOfSales);
				      myWriter.close();
				      System.out.println("Successfully wrote to the file.");
				    } catch (IOException e) {
				      System.out.println("An error occurred.");
				      e.printStackTrace();
				    }
				 ec2Worker.putS3Object(bucketName, "result.txt",resultFile);
				 ec2Worker.sendMessage("data.csv result.txt", Outbox);
				System.out.println(message);
				
			}
			else {
				System.out.println("inbox is empty");
			}
			Thread.sleep(5000);
		
			
			
		} while (messages.isEmpty() || isDeleted);
			
		

	}
	
	
	public void createQueue(String queueName) {
		CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
	            .queueName(queueName)
	            .build();

	        sqsClient.createQueue(createQueueRequest);
	}
	
	public List<Message> retreiveMsg(String queueName) {
		try {
			GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
		                .queueName(queueName)
		                .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
		            
            
            // Receive messages from the queue
			ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(5)
                .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            
           
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
	}
	public void deleteMsg(SqsClient sqsClient, String queueName,  List<Message> messages) {
		try {
			
			GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
	                .queueName(queueName)
	                .build();

			String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
			
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
                sqsClient.deleteMessage(deleteMessageRequest);
            }
            
            System.out.println("messages deleted succesfully");

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
	}
	
	public void sendMessage(String msg, String queueName) {
		try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(msg)
                .delaySeconds(5)
                .build();

            sqsClient.sendMessage(sendMsgRequest);
            System.out.println("msg succesfully sent");

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
	}
	
	public void getObjectBytes (S3Client s3, String bucketName, String keyName, String path ) {

        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file
            File myFile = new File(path );
            OutputStream os = new FileOutputStream(myFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (S3Exception e) {
          System.err.println(e.awsErrorDetails().errorMessage());
           System.exit(1);
        }
    }
	
	public String putS3Object(String bucketName,String objectKey,String objectPath) {
		try {
		
			Map<String, String> metadata = new HashMap<>();
			metadata.put("x-amz-meta-myVal", "test");
			
			PutObjectRequest putOb = PutObjectRequest.builder()
			.bucket(bucketName)
			.key(objectKey)
			.metadata(metadata)
			.build();
			
			PutObjectResponse response = s3Client.putObject(putOb,
			RequestBody.fromBytes(getObjectFile(objectPath)));
			
			return response.eTag();
		
			} catch (S3Exception e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
		return "";
	}
private static byte[] getObjectFile(String filePath) {
		
		FileInputStream fileInputStream = null;
		byte[] bytesArray = null;
		
		try {
			File file = new File(filePath);
			bytesArray = new byte[(int) file.length()];
			fileInputStream = new FileInputStream(file);
			fileInputStream.read(bytesArray);
			
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (fileInputStream != null) {
					try {
						fileInputStream.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}	
		}
		return bytesArray;
	}

public int count(String filename) throws IOException {
    InputStream is = new BufferedInputStream(new FileInputStream(filename));
    try {
    byte[] c = new byte[1024];
    int count = 0;
    int readChars = 0;
    boolean empty = true;
    while ((readChars = is.read(c)) != -1) {
        empty = false;
        for (int i = 0; i < readChars; ++i) {
            if (c[i] == '\n') {
                ++count;
            }
        }
    }
    return (count == 0 && !empty) ? 1 : count;
    } finally {
    is.close();
   }
}
	



}
