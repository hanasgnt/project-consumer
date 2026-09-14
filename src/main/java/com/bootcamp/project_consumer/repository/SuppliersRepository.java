package com.bootcamp.project_consumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.bootcamp.project_consumer.entity.Suppliers;

public interface SuppliersRepository extends JpaRepository<Suppliers, Long> {

}
