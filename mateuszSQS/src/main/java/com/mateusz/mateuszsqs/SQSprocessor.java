/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mateusz.mateuszsqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import javax.imageio.ImageIO;

/**
 *
 * @author Mateusz
 */
public class SQSprocessor {
    
private static AWSCredentials credentials = null;
    private final static String sqsURL = "https://sqs.us-west-2.amazonaws.com/983680736795/WozniakSQS";
    private final static String bucketName = "lab4-weeia";

    public static void main(String[] args) throws AmazonClientException{
        System.out.println("Start process SQS");
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            System.out.println("Error");
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        final Thread mainThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                    AmazonSQS sqs = new AmazonSQSClient(credentials);
                    Region usWest2 = Region.getRegion(Regions.US_WEST_2);
                    sqs.setRegion(usWest2);
                    
                    AmazonS3 s3 = new AmazonS3Client(credentials);
                    s3.setRegion(usWest2);
                    
                    List<String> filesList = SQSConfig.getMessages(sqs, sqsURL);
                    
                    for (String file : filesList) {
                        String[] files = file.split(",");
                        if (!file.equals("missing parameter: fileNames"))
                            for (int i=0; i<files.length; i++) {
                                try {
                                    SQSConfig.processFile(files[i], s3, bucketName);
                                } catch (IOException e) {
                                    System.out.println("Error");
                                    e.printStackTrace();
                                }
                            }

                    }                   
                    System.out.println("\nWaiting for messages.........\n");
            }
        });
        mainThread.start();
    }
    
}
