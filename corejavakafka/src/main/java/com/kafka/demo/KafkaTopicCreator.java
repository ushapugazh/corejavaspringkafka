package com.kafka.demo;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaTopicCreator {
	  public static void main(String[] args) {
	        String bootstrapServers = "localhost:9092"; // Replace with your Kafka brokers
	        String topicName = "my-topic";
	        int numPartitions = 1;
	        short replicationFactor = 1;

	        Properties properties = new Properties();
	        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

	        try (AdminClient adminClient = AdminClient.create(properties)) {
	            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
	            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
	            result.all().get(); // Wait for the topic creation to complete

	            System.out.println("Topic '" + topicName + "' created successfully.");
	        } catch (InterruptedException | ExecutionException e) {
	            System.err.println("Error creating topic: " + e.getMessage());
	        }
	    }
	}