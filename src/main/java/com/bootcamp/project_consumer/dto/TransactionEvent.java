package com.bootcamp.project_consumer.dto;

import java.util.List;

import com.bootcamp.project_consumer.entity.TransactionType;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TransactionEvent {

    private TransactionType type;
    private Long supplierId;
    private String customerName;
    private List<TransactionDetailRequest> details;
}