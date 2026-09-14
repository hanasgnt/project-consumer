package com.bootcamp.project_consumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.bootcamp.project_consumer.entity.Products;

public interface ProductsRepository extends JpaRepository<Products, Long> {

}
