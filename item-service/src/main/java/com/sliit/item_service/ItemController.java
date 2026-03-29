package com.sliit.item_service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/items")
public class ItemController {

    private final List<String> items = Collections.synchronizedList(new ArrayList<>(List.of("Book", "Laptop", "Phone")));
    private final List<Map<String, Object>> paymentEvents = Collections.synchronizedList(new ArrayList<>());
    private final ObjectMapper objectMapper;

    public ItemController(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @GetMapping
    public List<String> getItems() {
        return items;
    }

    @PostMapping
    public ResponseEntity<String> addItem(@RequestBody String item) {
        items.add(item);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body("Item added: " + item);
    }

    @GetMapping("/{id}")
    public ResponseEntity<String> getItem(@PathVariable int id) {
        if (id < 0 || id >= items.size())
            return ResponseEntity.notFound().build();
        return ResponseEntity.ok(items.get(id));
    }

    @GetMapping("/events/payments")
    public List<Map<String, Object>> getPaymentEvents() {
        return paymentEvents;
    }

    @KafkaListener(topics = "payments.processed", groupId = "item-service")
    public void onPaymentProcessed(String message) {
        try {
            Map<String, Object> payment = objectMapper.readValue(message, new TypeReference<>() {});
            paymentEvents.add(payment);
        } catch (Exception ignored) {
            // Ignore malformed events to keep the consumer running.
        }
    }
}