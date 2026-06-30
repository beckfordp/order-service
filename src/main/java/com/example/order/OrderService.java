package com.example.order.service;

import com.example.order.dto.OrderRequest;
import com.example.order.dto.OrderResponse;
import java.util.List;
import java.util.UUID;

public interface OrderService {

    OrderResponse getById(UUID id);

    OrderResponse create(OrderRequest request);

    OrderResponse update(UUID id, OrderRequest request);

    void delete(UUID id);

    List<OrderResponse> getAll();

    List<OrderResponse> search(String query);
}
