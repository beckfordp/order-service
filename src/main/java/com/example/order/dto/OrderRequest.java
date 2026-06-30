package com.example.order.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequest {

    @NotBlank(message = "orderDescription is required")
    private String orderDescription;

    @NotBlank(message = "deliveryAdress is required")
    private String deliveryAdress;

    @NotNull(message = "isActive is required")
    private Boolean isActive;
}
