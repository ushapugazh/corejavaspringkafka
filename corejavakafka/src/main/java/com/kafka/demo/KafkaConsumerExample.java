package com.kafka.demo;


import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerExample {
	public static void main(String[] args) 
	{
        String bootstrapServers = "localhost:9092";
        String groupId = "my-group";
        String topic = "my-topic";

        // Set consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create Kafka consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(topic));

            // Poll for messages
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) 
                {
                    System.out.printf("Received message: key = %s, value = %s, partition = %s, offset = %s%n",
                            record.key(), record.value(), record.partition(), record.offset());
                   
                    emailconfiguration();
        	       
                }
            }
        }
    }
	private static void emailconfiguration() {
		final String fromEmail = "pugazhenthiusha@gmail.com"; //requires valid gmail id
		final String password = ""; // correct password for gmail id
		final String toEmail = "ushapugazhenthi1425@gmail.com"; // can be any email id

		System.out.println("SSLEmail Start");
		Properties props = new Properties();
		props.put("mail.smtp.host", "smtp.gmail.com"); //SMTP Host
		props.put("mail.smtp.socketFactory.port", "465"); //SSL Port
		props.put("mail.smtp.socketFactory.class",
		        "javax.net.ssl.SSLSocketFactory"); //SSL Factory Class
		props.put("mail.smtp.auth", "true"); //Enabling SMTP Authentication
		props.put("mail.smtp.port", "465"); //SMTP Port

		Authenticator auth = new Authenticator() {
		    //override the getPasswordAuthentication method
		    protected PasswordAuthentication getPasswordAuthentication() {
		        return new PasswordAuthentication(fromEmail, password);
		    }
		};

		Session session = Session.getDefaultInstance(props, auth);
		System.out.println("Session created");
		sendEmail(session, toEmail,"Test Email from Gmail Via Java Program", "Test Email Hello Usha from Gmail Via Java Program ");
	}
	public static void sendEmail(Session session, String toEmail, String subject, String body){
	    try
	    {
	        MimeMessage msg = new MimeMessage(session);
	        //set message headers
	        msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
	        msg.addHeader("format", "flowed");
	        msg.addHeader("Content-Transfer-Encoding", "8bit");

	        msg.setFrom(new InternetAddress("pugazhenthiusha@gmail.com", "NoReply-JD"));
	        msg.setReplyTo(InternetAddress.parse("pugazhenthiusha@gmail.com", false));
	        msg.setSubject(subject, "UTF-8");
	        msg.setText(body, "UTF-8");
	        msg.setSentDate(new Date());

	        msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail, false));
	        System.out.println("Message is ready");
	        Transport.send(msg);

	        System.out.println("EMail Sent Successfully!!");
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	    }
	}
}
