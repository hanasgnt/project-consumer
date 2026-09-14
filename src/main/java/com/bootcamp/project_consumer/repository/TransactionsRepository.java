package com.bootcamp.project_consumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.bootcamp.project_consumer.entity.Transactions;

public interface TransactionsRepository extends JpaRepository<Transactions, Long> {

}
