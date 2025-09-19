package io.demo.service;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import io.demo.model.Order;
import io.demo.model.User;

@Service
public class Producer {

	private static final Logger logger = LoggerFactory.getLogger(Producer.class);
	private static final String USERS_TOPIC = "users";
	private static final String ORDERS_TOPIC = "orders";

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	public void sendMessage(String topic, String key, Object value, boolean isCallbackRequired) {

		CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, value);
		if (isCallbackRequired) {
			future.whenComplete((result, ex) -> {
				if (ex == null) {
					System.out.println(result.getRecordMetadata().topic() + " " + result.getRecordMetadata().partition()
							+ " " + result.getRecordMetadata().offset());
					logger.info(String.format("\n\n Produced event to topic %s: key = %-10s value = %s \n\n", topic,
							value));
				} else {
					ex.printStackTrace();
				}
			});
		}
	}

	public void publishOrder(int records) {

		for (int i = 0; i < records; i++) {

			User user = new User();
			Random random = new Random();

			int minAge = 18;
			int maxAge = 65;

			int ageRange = maxAge - minAge + 1;

			Integer ageRandom = random.nextInt(ageRange) + minAge;
			
			user.setAge(ageRandom);

			user.setUserName("USER-" + random.nextInt(100));

			user.setOrderNumber("ORDER-" + random.nextInt(1000));

			sendMessage(USERS_TOPIC, user.getUserName().toString(), user, false);

			Order order = new Order();

			order.setOrderNumber(user.getOrderNumber());
			order.setOrderSize(random.nextInt(10));

			Integer productRandom = random.nextInt(10);

			if (productRandom < 1) {
				order.setProductName("BELT");
			} else if (productRandom < 3) {
				order.setProductName("SHOE");
			} else if (productRandom < 5) {
				order.setProductName("SOCKS");
			} else if (productRandom < 7) {
				order.setProductName("JACKET");
			} else if (productRandom < 9) {
				order.setProductName("HAT");
			} else {
				order.setProductName("SHIRT");
			}

			sendMessage(ORDERS_TOPIC, order.getOrderNumber().toString(), order, false);
		}
	}
}
