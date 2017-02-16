/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mateusz.mateuszsqs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
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
import java.util.Map;
import javax.imageio.ImageIO;

/**
 *
 * @author Mateusz
 */
public class SQSConfig {
    
     public static List<String> getMessages(AmazonSQS sqs, String sqsURL) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsURL);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
        List<String> filesToProcess = new ArrayList<String>();
        for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Map.Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue().getStringValue());
                filesToProcess.add(entry.getValue().getStringValue());
            }
            System.out.println("Deleting a message.\n");
            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(sqsURL, messageReceiptHandle));
        }

        return filesToProcess;
    }
     
      public static void processFile(String key, AmazonS3 s3, String bucketName) throws IOException {
        System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        System.out.println("Deleting an object\n");
        s3.deleteObject(bucketName, key);
        System.out.println("Processing...");
        System.out.println("Uploading a new object to S3 from a file\n");
        InputStream changedStream = procesIamge(object.getObjectContent());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(changedStream.available());
        metadata.setLastModified(new Date(System.currentTimeMillis()));      
        s3.putObject(new PutObjectRequest(bucketName, key, changedStream, metadata));
    }
      
      private static InputStream procesIamge(InputStream input) {
        BufferedImage inputFile = null;
        try {
            inputFile = ImageIO.read(input);
        } catch (IOException e) {
            System.out.println("Error");
            e.printStackTrace();
        }
        try {
            ImageIO.write(inputFile, "png", new File("tmp.png"));
        } catch (IOException e) {
            System.out.println("Error");
            e.printStackTrace();
        }
        for (int x = 0; x < inputFile.getWidth(); x++) {
            for (int y = 0; y < inputFile.getHeight(); y++) {
                int rgba = inputFile.getRGB(x, y);
                Color col = new Color(rgba, true);
                int nCol = (col.getRed()+col.getGreen()+col.getBlue())/3;
                col = new Color(nCol, nCol, nCol);
                inputFile.setRGB(x, y, col.getRGB());
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ImageIO.write(inputFile, "png", baos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        InputStream is = new ByteArrayInputStream(baos.toByteArray());
        return is;
    }
}
