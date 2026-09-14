package com.bootcamp.project_consumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.bootcamp.project_consumer.entity.StockLogs;

public interface StockLogsRepository extends JpaRepository<StockLogs, Long> {

}
