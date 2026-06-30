package com.example.order.controller;

import com.example.order.dto.OrderRequest;
import com.example.order.dto.OrderResponse;
import com.example.order.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderService service;

    private final UUID TEST_ID = UUID.randomUUID();

    @Test
    void getById_returnsOk() throws Exception {
        OrderResponse response = OrderResponse.builder().id(TEST_ID).build();
        when(service.getById(TEST_ID)).thenReturn(response);

        mockMvc.perform(get("/v1/orders/{id}", TEST_ID))
                .andExpect(status().isOk());
    }

    @Test
    void getAll_returnsOkList() throws Exception {
        when(service.getAll()).thenReturn(List.of());
        mockMvc.perform(get("/v1/orders"))
                .andExpect(status().isOk());
    }

    @Test
    void create_returnsCreated() throws Exception {
        OrderRequest request = OrderRequest.builder()
                .orderDescription("test-value")
                .deliveryAdress("test-value")
                .isActive(true)
                .build();
        OrderResponse response = OrderResponse.builder().id(TEST_ID).build();
        when(service.create(any())).thenReturn(response);

        mockMvc.perform(post("/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated());
    }

    @Test
    void update_returnsOk() throws Exception {
        OrderRequest request = OrderRequest.builder()
                .orderDescription("test-value")
                .deliveryAdress("test-value")
                .isActive(true)
                .build();
        OrderResponse response = OrderResponse.builder().id(TEST_ID).build();
        when(service.update(eq(TEST_ID), any())).thenReturn(response);

        mockMvc.perform(put("/v1/orders/{id}", TEST_ID)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk());
    }

    @Test
    void delete_returnsNoContent() throws Exception {
        doNothing().when(service).delete(TEST_ID);
        mockMvc.perform(delete("/v1/orders/{id}", TEST_ID))
                .andExpect(status().isNoContent());
    }
}
