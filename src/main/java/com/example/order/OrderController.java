package com.example.order.controller;

import com.example.order.dto.OrderRequest;
import com.example.order.dto.OrderResponse;
import com.example.order.service.OrderService;
import io.micrometer.core.annotation.Timed;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/orders")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Order Service", description = "API for managing orders")
public class OrderController {

    private final OrderService service;

    @GetMapping("/{id}")
    @Timed(value = "order.get.by.id", description = "Time taken to get order by ID")
    @Operation(summary = "Get order by ID")
    @ApiResponse(responseCode = "200", description = "Successfully retrieved")
    @ApiResponse(responseCode = "404", description = "Order not found")
    public ResponseEntity<OrderResponse> getById(
            @Parameter(description = "Order ID")
            @PathVariable UUID id) {
        log.info("GET /orders/{}", id);
        return ResponseEntity.ok(service.getById(id));
    }

    @GetMapping
    @Operation(summary = "Get all orders")
    public ResponseEntity<List<OrderResponse>> getAll() {
        log.info("GET /orders");
        return ResponseEntity.ok(service.getAll());
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create new order")
    @ApiResponse(responseCode = "201", description = "Order created successfully")
    @ApiResponse(responseCode = "400", description = "Invalid input")
    public ResponseEntity<OrderResponse> create(
            @Valid @RequestBody OrderRequest request) {
        log.info("POST /orders");
        OrderResponse response = service.create(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update existing order")
    @ApiResponse(responseCode = "200", description = "Order updated successfully")
    @ApiResponse(responseCode = "404", description = "Order not found")
    public ResponseEntity<OrderResponse> update(
            @PathVariable UUID id,
            @Valid @RequestBody OrderRequest request) {
        log.info("PUT /orders/{}", id);
        return ResponseEntity.ok(service.update(id, request));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "Delete order")
    @ApiResponse(responseCode = "204", description = "Order deleted successfully")
    @ApiResponse(responseCode = "404", description = "Order not found")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        log.info("DELETE /orders/{}", id);
        service.delete(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/search")
    @Operation(summary = "Search orders")
    public ResponseEntity<List<OrderResponse>> search(
            @RequestParam String query) {
        log.info("GET /orders/search?query={}", query);
        return ResponseEntity.ok(service.search(query));
    }
}