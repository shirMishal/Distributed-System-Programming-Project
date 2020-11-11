package local_app;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.google.gson.Gson;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class LocalApp {
    public static Boolean shouldSendTerminate;
    public static int numOfWorkersPerTask;
    public static String[ ] json_inputs ; //get only jsons...
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonEC2 ec2;
    public static AmazonS3 s3;
    public static AmazonSQS sqs;
    public static String localAppToManager; //use to send messages to manager
    public static String managerToLocalApp;//use to get messages from manager
    public static List<String> inputs;
    public static Bucket bucket;
    private static String awsID;
    private static String awsSecretKey;


    public static void main(String[] args) {

        BasicConfigurator.configure();

        credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        localAppToManager = null;//after manager initialize, updates with manager queues
        managerToLocalApp = null;
        inputs = new ArrayList<>();
        json_inputs = new String[2];
        String pathForSerialized = args[0];

        int endOfArgs = args.length-1;
        if(args[endOfArgs].equals("Terminate")){
            shouldSendTerminate = true;
            endOfArgs = endOfArgs -1;
        }
        else{
            shouldSendTerminate = false;
        }
        numOfWorkersPerTask = Integer.parseInt(args[endOfArgs]);
        System.out.println("should terminate: " +shouldSendTerminate);
        System.out.println("numOf workers: "+ numOfWorkersPerTask);
        awsSecretKey = args[endOfArgs-1];
        awsID = args[endOfArgs-2];
        for(int i = 1; i <= endOfArgs - 3; i++){
            json_inputs[i] = args[i];
            System.out.println(json_inputs[i]);
        }

        //create bucket
        String uniqueBucketName = (credentialsProvider.getCredentials().getAWSAccessKeyId()).toLowerCase();

        if (s3.listBuckets().isEmpty()){
            try {
                bucket = s3.createBucket(uniqueBucketName);
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
            }
        }
        else{
            bucket = s3.listBuckets().get(0);

        }

        //check if manager exist, if not creates one
        initialize_manager();
        //updates sqs strings to communicate with manager

        while (! update_manager_queues()) {
            System.out.println("busy wait for manager ques to be open");
        }

        //create file to s3 from jsons and upload
        for (int i = 0; i < json_inputs.length; i++){
            JsonBooks inputBooks = new JsonBooks();
            parseJason(json_inputs[i], inputBooks);
            //serialization to inputBooks
            String path = pathForSerialized + Integer.toString((i+1));
            serializeJason( path , inputBooks);
            JsonBooks parsedInput = (JsonBooks) deserializeObject(path);
            parsedInput.toString();
            //upload to bucket
            uploadToS3(path, uniqueBucketName);
            inputs.add(Paths.get(path).getFileName().toString());
            sqs.sendMessage(localAppToManager, Paths.get(path).getFileName().toString() + " " + numOfWorkersPerTask);
        }
        HashMap<String, List<HtmlBook>> outputs = new HashMap<>();
        while (true){
            ReceiveMessageResult inputMessage = sqs.receiveMessage(managerToLocalApp);
            List<Message> messages = inputMessage.getMessages();
            if(!messages.isEmpty()){
                for (Message curr: messages) {
                    if(curr.getBody().equals("Terminate")){
                        sqs.deleteQueue(managerToLocalApp);
                        System.exit(0);
                    }
                    else if(inputs.contains(curr.getBody())){
                        File localFile = new File("localFilename");
                        s3.getObject(new GetObjectRequest(bucket.getName(), "output" + curr.getBody()), localFile);
                        List<HtmlBook> htmlBooksToAdd = (List<HtmlBook>) deserializeObject(localFile.getPath());
                        outputs.put(curr.getBody(), htmlBooksToAdd);
                        sqs.deleteMessage(managerToLocalApp, curr.getReceiptHandle());
                        s3.deleteObject(bucket.getName(), "output" + curr.getBody());
                        createHtmlOutput(curr, htmlBooksToAdd);
                        inputs.remove(curr.getBody());
                        if(inputs.isEmpty()){
                            if (shouldSendTerminate) {
                                sqs.sendMessage(localAppToManager, "Terminate");
                            }
                            else{
                                System.exit(0);
                            }
                        }
                    }
                }
            }
        }

    }

    private static void createHtmlOutput(Message curr, List<HtmlBook> htmlBooksToAdd) {
        List<String> htmlTxtLines = new ArrayList<>();
        htmlTxtLines.add("<!DOCTYPE html>\n");
        htmlTxtLines.add("<html>\n");
        htmlTxtLines.add("<head>\n");
        htmlTxtLines.add("<title>Output" + curr.getBody() + "</title>");
        htmlTxtLines.add("</head>\n");
        htmlTxtLines.add("<body>\n");
        for (HtmlBook book : htmlBooksToAdd){
            String color; String isSarcastic; String entities = "[";
            if (book.getColor() == 0){color = "DarkRed";}
            else if(book.getColor() == 1){color = "Red";}
            else if(book.getColor() == 2){color = "Black";}
            else if(book.getColor() == 3){color = "LightGreen";}
            else{color = "DarkGreen";}
            htmlTxtLines.add("<p>" + "<a href=" + book.getUrl() + " style=\"color:" + color + ";\">" + book.getUrl() + "</a>");
            for (String entity: book.getEntities()){
                entities = entities.concat(entity + "-");
            }
            String withAllEntities = entities.substring(0, entities.length()-1);
            withAllEntities = withAllEntities.concat("]");
            if (book.isSarcastic()){isSarcastic = "Sarcastic";}
            else{isSarcastic = "Not Sarcastic";}
            htmlTxtLines.add(withAllEntities);
            htmlTxtLines.add(isSarcastic + "</p>\n");
        }
        htmlTxtLines.add("</body>\n");
        htmlTxtLines.add("</html>");
        try {
            File htmlFile = new File("Output" + curr.getBody() + ".html");
            Path out = Paths.get(htmlFile.getPath());
            Files.write(out, htmlTxtLines, Charset.defaultCharset());
        }catch (Exception e){System.out.println("Damn it!"); System.exit(1);}
    }

    private static void parseJason(String inputName, JsonBooks inputBooks) {

        Gson gson = new Gson();
        try (BufferedReader br = new BufferedReader(new FileReader(inputName))) {
            String line;
            while ((line = br.readLine()) != null) {
                Book book = gson.fromJson(line, Book.class);
                inputBooks.addBook(book);
                System.out.println(book.toString());
                // process the line.
            }
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    private static void uploadToS3 (String file_path, String uniqueBucketName){
        String key_name = Paths.get(file_path).getFileName().toString();
        System.out.format("Uploading %s to S3 bucket %s...\n", file_path, bucket);
        try {
            s3.putObject(uniqueBucketName, key_name, new File(file_path));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    private static void initialize_manager(){
        if (! is_manager_exist()) {
            //create manager node
            try {
                RunInstancesRequest run_request = new RunInstancesRequest()
                        .withImageId("ami-0cf1c17ac894d0c3b")
                        .withInstanceType(InstanceType.T2Xlarge)
                        .withMaxCount(1)
                        .withMinCount(1)
                        .withKeyName("worker")
                        .withUserData(getUserDataScript(awsID, awsSecretKey));

                RunInstancesResult run_response = ec2.runInstances(run_request);
                List<Instance> instances = run_response.getReservation().getInstances();
                String reservation_id = run_response.getReservation().getInstances().get(0).getInstanceId();
                System.out.println("Launch instances: " + instances);

                Instance instance = instances.get(0);

                Tag managerTag = new Tag();
                managerTag.setKey("Name");
                managerTag.setValue("Manager");

                CreateTagsRequest tag_request_manager = new CreateTagsRequest()
                        .withTags(managerTag)
                        .withResources(instance.getInstanceId());
                CreateTagsResult tag_response_manager = ec2.createTags(tag_request_manager);

            } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Response Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
            }
        }
    }

    //instance-state-name - The state of the instance pending | running -> do not create manager
    // create manager :  shutting-down | terminated | stopping | stopped
    private static boolean is_manager_exist() {
        boolean done = false;
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        while (!done) {
            DescribeInstancesResult response = ec2.describeInstances(request);
            System.out.println("response: "+response.toString());
            List<Reservation> reservations = response.getReservations();
            if (!(reservations == null || reservations.isEmpty())){
                for (Reservation reservation : reservations) {
                    System.out.println("reservation: " +reservation.toString());
                    List<Instance> instances = reservation.getInstances();
                    if (!(instances == null || instances.isEmpty())){
                        for (Instance instance : instances) {
                            System.out.println("instance: "+instance.toString());
                            String status = instance.getState().getName();
                            List<Tag> tags = instance.getTags();
                            for (Tag t : tags) {
                                if (t.getKey().equals("Name") && t.getValue().equals("Manager") && (status.equals("pending") || status.equals("running"))) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            request.setNextToken(response.getNextToken());
            if (response.getNextToken() == null) {
                done = true;
            }
        }
        return false;
    }

    private static String getUserDataScript(String awsID, String awsSecretKey){
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#!/bin/bash");
        lines.add("cd /home/ec2-user");
        lines.add("aws configure set aws_access_key_id " + awsID );
        lines.add("aws configure set aws_secret_access_key " + awsSecretKey );
        lines.add("aws configure set default.region us-east-1" );
        lines.add("aws s3 cp s3://akiavjhk4qo25zrc3ro4bbbbbbbbbb/manager.jar manager.jar");
        lines.add("java -jar manager.jar " + awsID + " " + awsSecretKey);
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
    }

    static String join(Collection<String> s, String delimiter) {
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

    private static boolean update_manager_queues() {
        //Listing all queues in your account.\n");
        boolean isLocalAppToManager = false;
        boolean isManagerToLocalApp = false;
        String name_prefix = "";
        ListQueuesResult lq_result = sqs.listQueues(new ListQueuesRequest(name_prefix));
        System.out.println("Queue URLs with prefix: " + name_prefix);
        for (String url : lq_result.getQueueUrls()) {
            if (url.equals(sqs.getQueueUrl("localAppToManager").getQueueUrl())) {
                localAppToManager = sqs.getQueueUrl("localAppToManager").getQueueUrl();
                isLocalAppToManager = true;
            }
            else if  (url.equals(sqs.getQueueUrl("managerToLocalApp").getQueueUrl())) {
                managerToLocalApp = sqs.getQueueUrl("managerToLocalApp").getQueueUrl();
                isManagerToLocalApp = true;
            }
        }
        return (isLocalAppToManager  && isManagerToLocalApp);
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

    public static Object deserializeObject(String obj) {
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
}
