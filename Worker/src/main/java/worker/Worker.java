package worker;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.log4j.BasicConfigurator;
import java.io.*;
import java.util.*;

public class Worker {

    public static Book book;
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonSQS sqs;
    public static AmazonS3 s3;
    public static String workerInputsSQSURL;
    public static String workerOutputsSQSURL;
    public static Bucket bucketName;


    public static void main(String[] args){
        BasicConfigurator.configure();
        credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        GetQueueUrlResult result = sqs.getQueueUrl("workerInputs");
        workerInputsSQSURL = result.getQueueUrl();
        result = sqs.getQueueUrl("workerOutputs");
        workerOutputsSQSURL = result.getQueueUrl();
        bucketName = s3.listBuckets().get(0);

        while (true){
            Message curr = getOneMessage(workerInputsSQSURL);
            if(curr != null){
                if(curr.getBody().equals("Terminate")){
                    SendMessageRequest sendMessageRequest = new SendMessageRequest();
                    sendMessageRequest.withMessageBody("Terminate");
                    sendMessageRequest.withQueueUrl(workerOutputsSQSURL);
                    //sendMessageRequest.withDelaySeconds(900); //longest time can use dely
                    sqs.sendMessage(sendMessageRequest);
                }
                else {
                    File localFile = new File("localFilename");
                    s3.getObject(new GetObjectRequest(bucketName.getName(), curr.getBody()), localFile);
                    book = (Book) deserializeObject(localFile.getPath());

                    int[] sentiments = sentimentAnalysis();
                    List<List<String>> allReviewsEntities = extracts_entities();
                    List<HtmlBook> output = new ArrayList<>();
                    for(int i = 0; i < book.getReviews().length; i++){
                        boolean isSarcastic;
                        if(sentiments[i] ==  book.getReviews()[i].getRating()){
                            isSarcastic = false;
                        }else {isSarcastic = true;}
                        HtmlBook outputToAdd = new HtmlBook(sentiments[i], book.getReviews()[i].getLink(), allReviewsEntities.get(i), isSarcastic);
                        output.add(outputToAdd);
                    }
                    String toUpload = "toUpload";
                    serializeJason(toUpload, output);
                    PutObjectResult putResult = s3.putObject(bucketName.getName(),curr.getBody()+"!",new File(toUpload));
                    s3.deleteObject(bucketName.getName(), curr.getBody());

                    Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                    messageAttributes.put("Name", new MessageAttributeValue()
                            .withDataType("String")
                            .withStringValue(curr.getMessageAttributes().get("Name").getStringValue()));
                    SendMessageRequest sendMessageRequest = new SendMessageRequest();
                    sendMessageRequest.withMessageBody(curr.getBody()+"!");
                    sendMessageRequest.withQueueUrl(workerOutputsSQSURL);
                    sendMessageRequest.withMessageAttributes(messageAttributes);
                    sqs.sendMessage(sendMessageRequest);
                    sqs.deleteMessage(workerInputsSQSURL, curr.getReceiptHandle());
                }
            }
        }

    }

    public static Message getOneMessage (String url){
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

    public static int[] sentimentAnalysis() {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, parse, sentiment");
        //props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP sentimentPipeline = new StanfordCoreNLP(props);

        int[] output = new int[book.getReviews().length];
        for (int i = 0; i < book.getReviews().length; i++) {
            output[i] = findSentiment((book.getReviews()[i].getReviewTitle()) + " " + (book.getReviews()[i].getReviewText()), sentimentPipeline);
        }
        return output;
    }

    private static int findSentiment(String review, StanfordCoreNLP sentimentPipeline) {
        int mainSentiment = 0;
        if (review != null && review.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(review);

            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }

    public static List<List<String>> extracts_entities() {
        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        StanfordCoreNLP NERPipeline = new StanfordCoreNLP(props);
        List<List<String>> allReviews = new ArrayList<>();
        for (int i = 0; i < book.getReviews().length; i++) {
            allReviews.add(printEntities((book.getReviews()[i].getReviewTitle()) + " " + (book.getReviews()[i].getReviewText()), NERPipeline));

        }
        return allReviews;
    }

    public static List<String> printEntities(String review, StanfordCoreNLP NERPipeline) {
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        //nimrod's code
        List<String> outputEntities = new ArrayList<>();

        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);

                //did this instead of printing
                String toAdd = word + ":" + ne;
                outputEntities.add(toAdd);
                //System.out.println("\t-" + word + ":" + ne);
            }
        }
        return outputEntities;
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
