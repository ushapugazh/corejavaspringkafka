package com.kafka.demo;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class ListKafkaTopics {
	 public static void main(String[] args) {
	        // Configure the Kafka client
	        Properties props = new Properties();
	        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker address
	        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

	        try (AdminClient adminClient = AdminClient.create(props)) {
	            // Get the list of topics
	            ListTopicsResult listTopics = adminClient.listTopics();
	            Set<String> topicNames = listTopics.names().get();

	            // Print the topic names
	            System.out.println("List of Kafka topics:");
	            for (String topic : topicNames) {
	                System.out.println(topic);
	            }

	        } catch (InterruptedException | ExecutionException e) {
	            e.printStackTrace();
	        }
	    }
}
