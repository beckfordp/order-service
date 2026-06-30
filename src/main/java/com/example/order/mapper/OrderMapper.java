package com.example.order.mapper;

import com.example.order.dto.OrderRequest;
import com.example.order.dto.OrderResponse;
import com.example.order.entity.OrderEntity;
import org.mapstruct.Mapper;
import org.mapstruct.MappingTarget;

@Mapper(componentModel = "spring")
public interface OrderMapper {

    OrderResponse toResponse(OrderEntity entity);

    OrderEntity toEntity(OrderRequest request);

    void updateEntity(OrderRequest request, @MappingTarget OrderEntity entity);
}
