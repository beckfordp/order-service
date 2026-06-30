package com.example.order.repository;

import com.example.order.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {

    @Query("SELECT e FROM OrderEntity e WHERE " +
           "LOWER(CAST(e.orderDescription AS string)) LIKE LOWER(CONCAT('%', :term, '%'))")
    List<OrderEntity> searchByTerm(@Param("term") String term);
}
