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
import com.amazonaws.services.sqs.model.*;

import org.apache.log4j.BasicConfigurator;

public class Manager {

    // Global Variables:
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 s3;
    public static AmazonSQS sqs;
    public static AmazonEC2 ec2;
    public static String localAppToManagerURL;
    public static String managerToLocalAppURL;
    public static String workerOutputsSQSURL;
    public static String workerInputsSQSURL;
    public static Bucket bucketName;
    public static Resources resources;
    private static String awsID;
    private static String awsSecretKey;



    public static void main(String[] args) {
        BasicConfigurator.configure();
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

        //initializing "must have" sqses
        sqs.createQueue("localAppToManager"); // SQS to get data from Local apps
        GetQueueUrlResult result = sqs.getQueueUrl("localAppToManager");
        localAppToManagerURL = result.getQueueUrl();
        SetQueueAttributesRequest visibilityRequest = new SetQueueAttributesRequest()
                .withQueueUrl(localAppToManagerURL)
                .addAttributesEntry(QueueAttributeName.VisibilityTimeout
                        .toString(),  Integer.toString(1200));
        sqs.setQueueAttributes(visibilityRequest);
        sqs.createQueue("managerToLocalApp"); // SQS to get data to Local apps
        result = sqs.getQueueUrl("managerToLocalApp");
        managerToLocalAppURL = result.getQueueUrl();
        sqs.createQueue("workerInputs"); // SQS to send data to Local apps
        result = sqs.getQueueUrl("workerInputs");
        workerInputsSQSURL = result.getQueueUrl();
        visibilityRequest = new SetQueueAttributesRequest()
                .withQueueUrl(workerInputsSQSURL)
                .addAttributesEntry(QueueAttributeName.VisibilityTimeout
                        .toString(),  Integer.toString(1200));
        sqs.setQueueAttributes(visibilityRequest);
        sqs.createQueue("workerOutputs"); // SQS for worker's ouputs
        result = sqs.getQueueUrl("workerOutputs");
        workerOutputsSQSURL = result.getQueueUrl();
        bucketName = s3.listBuckets().get(0);
        resources = Resources.getInstance();
        awsID = args[0];
        awsSecretKey = args[1];


        InputTask input = new InputTask(awsID, awsSecretKey);
        OutputTask output = new OutputTask();
        Thread inputThread = new Thread(input);
        Thread outputThread = new Thread(output);

        inputThread.start();
        outputThread.start();

    }

}
