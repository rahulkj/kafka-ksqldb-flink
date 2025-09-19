package io.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.demo.service.Producer;

@RestController
public class Produce {
	
	@Autowired
	private Producer producer;
	
	@PostMapping("/generate")
	public ResponseEntity<String> createOrder(@RequestParam(name = "records", defaultValue = "10") int records) {
		producer.publishOrder(records);
		return ResponseEntity.ok("Orders processed successfully");
	}

}
