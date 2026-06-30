package com.example.order.service.impl;

import com.example.order.entity.OrderEntity;
import com.example.order.repository.OrderRepository;
import com.example.order.dto.OrderRequest;
import com.example.order.dto.OrderResponse;
import com.example.order.service.OrderService;
import com.example.order.mapper.OrderMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class OrderServiceImpl implements OrderService {

    private final OrderRepository repository;
    private final OrderMapper mapper;
    //

    @Override
    @Cacheable(value = "orders", key = "#id", unless = "#result == null")
    @CircuitBreaker(name = "orderService", fallbackMethod = "getByIdFallback")
    @Retry(name = "orderService")
    @RateLimiter(name = "orderService")
    public OrderResponse getById(UUID id) {
        log.info("Fetching order with id: {}", id);
        return repository.findById(id)
                .map(mapper::toResponse)
                .orElseThrow(() -> new RuntimeException("Order not found with id: " + id));
    }

    @Override
    @Transactional
    public OrderResponse create(OrderRequest request) {
        log.info("Creating new order");
        OrderEntity entity = mapper.toEntity(request);
        entity = repository.save(entity);
        log.info("Created order with id: {}", entity.getId());
        return mapper.toResponse(entity);
    }

    @Override
    @Transactional
    @CircuitBreaker(name = "orderService")
    @Retry(name = "orderService")
    public OrderResponse update(UUID id, OrderRequest request) {
        log.info("Updating order with id: {}", id);
        OrderEntity entity = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Order not found with id: " + id));

        mapper.updateEntity(request, entity);
        entity = repository.save(entity);
        log.info("Updated order with id: {}", id);
        return mapper.toResponse(entity);
    }

    @Override
    @Transactional
    @CacheEvict(value = "orders", key = "#id")
    public void delete(UUID id) {
        log.info("Deleting order with id: {}", id);
        if (!repository.existsById(id)) {
            throw new RuntimeException("Order not found with id: " + id);
        }
        repository.deleteById(id);
        log.info("Deleted order with id: {}", id);
    }

    @Override
    public List<OrderResponse> getAll() {
        log.info("Fetching all orders");
        return repository.findAll().stream()
                .map(mapper::toResponse)
                .collect(Collectors.toList());
    }

    @Override
    public List<OrderResponse> search(String query) {
        log.info("Searching orders with query: {}", query);
        return repository.searchByTerm(query).stream()
                .map(mapper::toResponse)
                .collect(Collectors.toList());
    }

    /**
     * Fallback method for getById when Circuit Breaker or Rate Limiter is triggered
     */
    public OrderResponse getByIdFallback(UUID id, Throwable t) {
        log.warn("Fallback triggered for order with id: {}. Reason: {}", id, t.getMessage());
        //
        return OrderResponse.builder()
                .id(id)
                .build();
    }
}