package com.sliit.payment_service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final List<Map<String,Object>> payments = Collections.synchronizedList(new ArrayList<>());
    private int idCounter = 1;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PaymentController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @GetMapping
    public List<Map<String,Object>> getPayments() {
        return payments;
    }

    @PostMapping("/process")
    public ResponseEntity<Map<String,Object>> processPayment(@RequestBody Map<String,Object> payment) {
        payment.put("id", idCounter++);
        payment.put("status", "SUCCESS");
        payments.add(payment);
        publishProcessedEvent(payment);
        return ResponseEntity.status(201).body(payment);
    }

    @KafkaListener(topics = "orders.created", groupId = "payment-service")
    public void onOrderCreated(String message) {
        try {
            Map<String, Object> order = objectMapper.readValue(message, new TypeReference<>() {});
            Map<String, Object> payment = new LinkedHashMap<>();
            payment.put("id", idCounter++);
            payment.put("orderId", order.get("id"));
            payment.put("amount", order.getOrDefault("amount", 0));
            payment.put("method", "AUTO");
            payment.put("status", "SUCCESS");
            payments.add(payment);
            publishProcessedEvent(payment);
        } catch (Exception ignored) {
            // Ignore malformed events to keep the consumer running.
        }
    }

    private void publishProcessedEvent(Map<String, Object> payment) {
        try {
            kafkaTemplate.send("payments.processed", objectMapper.writeValueAsString(payment));
        } catch (Exception ignored) {
            // Ignore transient publishing failures for this demo service.
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getPayment(@PathVariable int id) {
        return payments.stream()
                .filter(p -> p.get("id").equals(id))
                .findFirst()
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}