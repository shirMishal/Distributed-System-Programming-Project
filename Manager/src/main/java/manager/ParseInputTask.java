package manager;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.util.*;

public class ParseInputTask implements Runnable{


    private AWSCredentialsProvider credentialsProvider;
    private AmazonS3 s3;
    private AmazonSQS sqs;
    private AmazonEC2 ec2;
    private Bucket bucketName;
    private Resources resources;
    private String localAppToManagerURL;
    private String workerInputsSQSURL;
    private int threadID;
    private String awsID;
    private String awsSecretKey;

    public ParseInputTask(int threadID, String awsID, String awsSecretKey){
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
        this.threadID = threadID;
        this.awsID = awsID;
        this.awsSecretKey = awsSecretKey;
    }

    @Override
    public void run() {
        while (true) {
            Message message = getOneMessage(localAppToManagerURL);
            if (message != null) {
                //assuming termination will only happen after all inputs are processed
                String[] body = message.getBody().split(" ");
                String key = body[0];
                String workerPerTasks = body[1];
                //creating outputSQS for an input file - the name of the sqs is the input ID
                CreateQueueResult ID = sqs.createQueue(key);

                File localFile = new File("localFilename" + threadID);
                s3.getObject(new GetObjectRequest(bucketName.getName(), key), localFile);
                JsonBooks parsedInput = (JsonBooks) deserializeObject(localFile.getPath());

                int amountOfWorkersToOpen = parsedInput.getBooks().size() / Integer.parseInt(workerPerTasks);
                resources.addWorkers(amountOfWorkersToOpen, awsID, awsSecretKey);

                //downloading the file from s3 and splitting it to the workersSQS

                // create a counter for amount of messages that are done, sits in hash map and can got to s3 instead
                int amountToFinish = parsedInput.getBooks().size();
                resources.addInput(key, amountToFinish);

                for (int i = 0; i < amountToFinish; i++) {
                    String toUpload = "toUpload" + threadID;
                    serializeJason(toUpload, parsedInput.getBooks().get(i));
                    PutObjectResult putResult = s3.putObject(bucketName.getName(), key + "*" + i, new File(toUpload));
                    Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                    messageAttributes.put("Name", new MessageAttributeValue()
                            .withDataType("String")
                            .withStringValue(key));
                    SendMessageRequest sendMessageRequest = new SendMessageRequest();
                    sendMessageRequest.withMessageBody(key + "*" + i);
                    sendMessageRequest.withQueueUrl(workerInputsSQSURL);
                    sendMessageRequest.withMessageAttributes(messageAttributes);
                    sqs.sendMessage(sendMessageRequest);
                }
                s3.deleteObject(bucketName.getName(), key);


                sqs.deleteMessage(localAppToManagerURL, message.getReceiptHandle());
                terminate();
            }
        }
    }

    private void terminate() {
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

    public Object deserializeObject(String obj) {
        try {

            FileInputStream fis = new FileInputStream(obj);
            ObjectInputStream ois = new ObjectInputStream(fis);
            Object result = ois.readObject();
            ois.close();
            fis.close();
            return result;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void serializeJason(String fileName, Object obj) {
        FileOutputStream fileOut;
        ObjectOutputStream out;
        try {
            fileOut = new FileOutputStream(new File(fileName));
            out = new ObjectOutputStream(fileOut);
            out.writeObject(obj);
            out.close();
            fileOut.close();

        } catch (IOException i) {
            i.printStackTrace();
        }
    }
}
