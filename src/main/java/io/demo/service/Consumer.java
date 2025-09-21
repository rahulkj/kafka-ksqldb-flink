package io.demo.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import io.demo.model.UserOrder;

@Service
public class Consumer {

	private final Logger logger = LoggerFactory.getLogger(Consumer.class);
	
	@Value("${spring.kafka.topics.useroder-topic}")
	private String topic; 

	@KafkaListener(id = "demo-consumer", topics = "#{'${spring.kafka.topics.useroder-topic}'}", groupId = "#{'${spring.kafka.consumer.group-id}'}", autoStartup = "true")
	public void listen(List<UserOrder> userOrders) {
		for (UserOrder userOrder : userOrders) {			
			logger.info("\n\n Consumed event from topic %s: \n\n".formatted(userOrder));
		}
	}
}