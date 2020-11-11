package manager;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import org.apache.commons.codec.binary.Base64;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Resources {

    private static ConcurrentHashMap<String, Integer> inputIDToDoneReviewsCounter;
    private static AtomicInteger numOfWorkers;
    private AWSCredentialsProvider credentialsProvider;
    private AmazonEC2 ec2;

    private static class ResourceHolder {
        private static Resources instance = new Resources();
    }

    private Resources() {
        inputIDToDoneReviewsCounter = new ConcurrentHashMap<>();
        numOfWorkers = new AtomicInteger(0);
        credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
    }

    public static Resources getInstance() {
        return ResourceHolder.instance;
    }

    public synchronized void addInput(String id, Integer amount){
        inputIDToDoneReviewsCounter.put(id, amount);
    }

    public synchronized Integer getAmountOfReviewsLeft(String id){
        return inputIDToDoneReviewsCounter.get(id);
    }

    public synchronized void addWorkers(int amountToOpen, String awsID, String awsSecretKey){
        amountToOpen = amountToOpen - numOfWorkers.get();
        if ( (amountToOpen > 0) && (amountToOpen + numOfWorkers.get() + 1 > 32)) {
            amountToOpen = 31 - numOfWorkers.get(); // we take down 1 for the manager instance
        }
        Tag workerTag = new Tag();
        workerTag.setKey("Name");
        workerTag.setValue("Worker");
        while (amountToOpen > 0) {
            RunInstancesRequest run_request = new RunInstancesRequest()
                    .withImageId("ami-0196929316245b60d") //The default image includes AWS command line tools ...Java.
                    .withInstanceType(InstanceType.T2Large)
                    .withMaxCount(1)
                    .withMinCount(1)
                    .withKeyName("worker")
                    .withUserData(getUserDataScript(awsID, awsSecretKey));
            RunInstancesResult run_response = ec2.runInstances(run_request);
            Instance instance = run_response.getReservation().getInstances().get(0);
            CreateTagsRequest tag_request_worker = new CreateTagsRequest()
                    .withTags(workerTag)
                    .withResources(instance.getInstanceId());
            CreateTagsResult tag_response_manager = ec2.createTags(tag_request_worker);
            numOfWorkers.incrementAndGet();
            amountToOpen--;
        }
    }


    private static String getUserDataScript(String awsID, String awsSecretKey){
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#!/bin/bash");
        lines.add("cd /home/ec2-user");
        lines.add("aws configure set aws_access_key_id " + awsID );
        lines.add("aws configure set aws_secret_access_key " + awsSecretKey );
        lines.add("aws configure set default.region us-east-1" );
        lines.add("aws s3 cp s3://akiavjhk4qo25zrc3ro4bbbbbbbbbb/worker.jar worker.jar");
        lines.add("java -cp .:worker.jar:stanford-corenlp-3.9.2.jar:stanford-corenlp-3.9.2-models.jar:ejml-0.23.jar:jollyday-0.5.1.jar dsps_assignment1.Worker");
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
    }

    private static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }
}
