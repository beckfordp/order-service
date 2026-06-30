package com.example.order.service.impl;

import com.example.order.dto.OrderRequest;
import com.example.order.dto.OrderResponse;
import com.example.order.entity.OrderEntity;
import com.example.order.mapper.OrderMapper;
import com.example.order.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceImplTest {

    @Mock
    private OrderRepository repository;

    @Mock
    private OrderMapper mapper;

    @InjectMocks
    private OrderServiceImpl service;

    private final UUID TEST_ID = UUID.randomUUID();

    @Test
    void getById_returnsResponse() {
        OrderEntity entity = new OrderEntity();
        OrderResponse response = OrderResponse.builder().id(TEST_ID).build();
        when(repository.findById(TEST_ID)).thenReturn(Optional.of(entity));
        when(mapper.toResponse(entity)).thenReturn(response);

        OrderResponse result = service.getById(TEST_ID);
        assertThat(result.getId()).isEqualTo(TEST_ID);
    }

    @Test
    void create_savesAndReturnsResponse() {
        OrderRequest request = new OrderRequest();
        OrderEntity entity = new OrderEntity();
        OrderResponse response = OrderResponse.builder().id(TEST_ID).build();
        when(mapper.toEntity(request)).thenReturn(entity);
        when(repository.save(entity)).thenReturn(entity);
        when(mapper.toResponse(entity)).thenReturn(response);

        OrderResponse result = service.create(request);
        assertThat(result).isNotNull();
        verify(repository).save(any());
    }

    @Test
    void update_updatesAndReturnsResponse() {
        OrderRequest request = new OrderRequest();
        OrderEntity entity = new OrderEntity();
        OrderResponse response = OrderResponse.builder().id(TEST_ID).build();
        when(repository.findById(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(entity)).thenReturn(entity);
        when(mapper.toResponse(entity)).thenReturn(response);

        OrderResponse result = service.update(TEST_ID, request);
        assertThat(result).isNotNull();
        verify(mapper).updateEntity(eq(request), eq(entity));
    }

    @Test
    void delete_removesEntity() {
        when(repository.existsById(TEST_ID)).thenReturn(true);
        service.delete(TEST_ID);
        verify(repository).deleteById(TEST_ID);
    }

    @Test
    void getAll_returnsAllResponses() {
        when(repository.findAll()).thenReturn(List.of(new OrderEntity()));
        when(mapper.toResponse(any())).thenReturn(OrderResponse.builder().build());

        List<OrderResponse> result = service.getAll();
        assertThat(result).hasSize(1);
    }
}
