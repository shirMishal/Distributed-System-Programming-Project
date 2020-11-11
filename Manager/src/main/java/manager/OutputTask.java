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

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class OutputTask implements Runnable{
    private AWSCredentialsProvider credentialsProvider;
    private AmazonS3 s3;
    private AmazonSQS sqs;
    private AmazonEC2 ec2;
    private String workerOutputsSQSURL;
    private String managerToLocalAppURL;
    private Bucket bucketName;
    private Resources resources;



    public OutputTask() {
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
        GetQueueUrlResult result = sqs.getQueueUrl("workerOutputs");
        workerOutputsSQSURL = result.getQueueUrl();
        result = sqs.getQueueUrl("managerToLocalApp");
        managerToLocalAppURL = result.getQueueUrl();
    }


    @Override
    public void run() {
        while(true){
            Message firstMessage = getOneMessage(workerOutputsSQSURL);
            if (firstMessage != null) {
                String[] body = firstMessage.getBody().split(" ");
                if(body[0].equals("Terminate")){
                    terminate();
                }
                uploadOutputToSQS(firstMessage);
            }
        }
    }

    private void terminate() {
        ListQueuesResult result = sqs.listQueues();
        List<String> sqsList = result.getQueueUrls();
        for (String curr : sqsList){
            if(!curr.equals(managerToLocalAppURL)){
                sqs.deleteQueue(curr);
            }
        }
        //terminating all ec2 and self last
        String managerID = "";
        boolean done = false;
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        while (!done) {
            DescribeInstancesResult response = ec2.describeInstances(request);
            for (Reservation reservation : response.getReservations()) {
                for (Instance instance : reservation.getInstances()) {
                    List<Tag> tags = instance.getTags();
                    String status = instance.getState().getName();
                    for (Tag t : tags) {
                        if (t.getKey().equals("Name") && t.getValue().equals("Manager") && (status.equals("pending") || status.equals("running"))) {
                            managerID = instance.getInstanceId();
                        }
                        else if (status.equals("running") || status.equals("pending")){
                            String currID = instance.getInstanceId();
                            ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(currID));
                        }
                    }
                }
                request.setNextToken(response.getNextToken());
                if (response.getNextToken() == null) {
                    done = true;
                }
            }
        }
        sqs.sendMessage(managerToLocalAppURL, "Terminate");
        ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(managerID));
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

    public void uploadOutputToSQS(Message output){
        String ID = output.getMessageAttributes().get("Name").getStringValue();
        String body = output.getBody();
        GetQueueUrlResult url = sqs.getQueueUrl(ID);
        sqs.sendMessage(url.getQueueUrl(), body);
        int nowValue = resources.getAmountOfReviewsLeft(ID);
        resources.addInput(ID, nowValue - 1);
        if(nowValue - 1 == 0){
            ReceiveMessageResult inputMessage = sqs.receiveMessage(url.getQueueUrl());
            List<Message> messages = inputMessage.getMessages();
            List<HtmlBook> outputsToBeUploaded = new ArrayList<>();
            while (!messages.isEmpty()){
                for (Message curr: messages) {
                    File localFile = new File("localFilename");
                    s3.getObject(new GetObjectRequest(bucketName.getName(), curr.getBody()), localFile);
                    List<HtmlBook> htmlBooksToAdd = (List<HtmlBook>) deserializeObject(localFile.getPath());
                    for(HtmlBook htmlBookToAdd : htmlBooksToAdd){
                        outputsToBeUploaded.add(htmlBookToAdd);
                    }
                    String messageReceiptHandle = curr.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(url.getQueueUrl(), messageReceiptHandle));
                    s3.deleteObject(bucketName.getName(), curr.getBody());
                }
                inputMessage = sqs.receiveMessage(url.getQueueUrl());
                messages = inputMessage.getMessages();
            }
            String toUpload = "toUpload";
            serializeJason(toUpload, outputsToBeUploaded);
            PutObjectResult putResult = s3.putObject(bucketName.getName(),"output"+ID, new File(toUpload));
            sqs.sendMessage(managerToLocalAppURL, ID);
            sqs.deleteQueue(new DeleteQueueRequest(url.getQueueUrl()));
        }
        sqs.deleteMessage(workerOutputsSQSURL, output.getReceiptHandle());
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

    public static void serializeJason(String fileName, Object obj) {
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
