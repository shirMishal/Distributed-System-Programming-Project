package manager;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class InputTask implements Runnable{


    private AWSCredentialsProvider credentialsProvider;
    private AmazonS3 s3;
    private AmazonSQS sqs;
    private AmazonEC2 ec2;
    private Bucket bucketName;
    private Resources resources;
    private String localAppToManagerURL;
    private String workerInputsSQSURL;
    private String awsID;
    private String awsSecretKey;

    public InputTask(String awsID, String awsSecretKey){
        credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        bucketName = s3.listBuckets().get(0);
        resources = Resources.getInstance();
        GetQueueUrlResult result = sqs.getQueueUrl("localAppToManager");
        localAppToManagerURL = result.getQueueUrl();
        result = sqs.getQueueUrl("workerInputs");
        workerInputsSQSURL = result.getQueueUrl();
        this.awsID = awsID;
        this.awsSecretKey = awsSecretKey;
    }

    @Override
    public void run() {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for(int i = 0; i < 5; i++){
            Runnable parseInput = new ParseInputTask(i, awsID, awsSecretKey);
            executor.execute(parseInput);
        }
        executor.shutdown();
        while(true){
            Message firstMessage = getOneMessage(localAppToManagerURL);
            if (firstMessage != null) {
                String[] body = firstMessage.getBody().split(" ");
                if(body[0].equals("Terminate")){
                    terminate();
                }
            }
        }
    }

    private void terminate() {
        sqs.sendMessage(workerInputsSQSURL, "Terminate");
        this.terminate();
    }

    public Message getOneMessage (String url){
        ReceiveMessageResult inputMessage = sqs.receiveMessage(new ReceiveMessageRequest(url)
                .withMessageAttributeNames("Name"));
        List<Message> messages = inputMessage.getMessages();
        if (messages.isEmpty()) {
            return null;
        } else {
            Message firstMessage = messages.get(0);
            return firstMessage;
        }
    }
}
