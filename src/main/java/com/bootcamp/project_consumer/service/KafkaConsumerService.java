package com.bootcamp.project_consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.bootcamp.project_consumer.dto.TransactionDetailRequest;
import com.bootcamp.project_consumer.dto.TransactionEvent;
import com.bootcamp.project_consumer.entity.Products;
import com.bootcamp.project_consumer.entity.StockLogs;
import com.bootcamp.project_consumer.entity.Suppliers;
import com.bootcamp.project_consumer.entity.TransactionDetails;
import com.bootcamp.project_consumer.entity.Transactions;
import com.bootcamp.project_consumer.repository.ProductsRepository;
import com.bootcamp.project_consumer.repository.StockLogsRepository;
import com.bootcamp.project_consumer.repository.SuppliersRepository;
import com.bootcamp.project_consumer.repository.TransactionsRepository;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    // Harus sama persis dengan key cache Redis yang dipakai producer:
    // @Cacheable(value = "products", key = "'all'") -> tersimpan sebagai
    // "products::all"
    private static final String PRODUCTS_CACHE_KEY = "products::all";

    private final TransactionsRepository transactionsRepository;
    private final ProductsRepository productsRepository;
    private final SuppliersRepository suppliersRepository;
    private final StockLogsRepository stockLogsRepository;
    private final StringRedisTemplate redisTemplate;

    public KafkaConsumerService(
            TransactionsRepository transactionsRepository,
            ProductsRepository productsRepository,
            SuppliersRepository suppliersRepository,
            StockLogsRepository stockLogsRepository,
            StringRedisTemplate redisTemplate) {

        this.transactionsRepository = transactionsRepository;
        this.productsRepository = productsRepository;
        this.suppliersRepository = suppliersRepository;
        this.stockLogsRepository = stockLogsRepository;
        this.redisTemplate = redisTemplate;
    }

    @Transactional
    @KafkaListener(topics = "transactions", groupId = "transaction-consumer-group")
    public void consumeTransaction(TransactionEvent event) {

        logger.info(
                "Transaction received from Kafka. Type: {}, Supplier ID: {}, Customer: {}",
                event.getType(),
                event.getSupplierId(),
                event.getCustomerName());

        Transactions transaction = new Transactions();
        transaction.setType(event.getType());
        transaction.setCustomerName(event.getCustomerName());

        if (event.getSupplierId() != null) {
            Suppliers supplier = suppliersRepository
                    .findById(event.getSupplierId())
                    .orElseThrow(() -> new RuntimeException(
                            "Supplier not found: " + event.getSupplierId()));

            transaction.setSupplier(supplier);
        }

        for (TransactionDetailRequest detailRequest : event.getDetails()) {
            Products product = productsRepository
                    .findById(detailRequest.getProductId())
                    .orElseThrow(() -> new RuntimeException(
                            "Product not found: " + detailRequest.getProductId()));

            TransactionDetails detail = new TransactionDetails();
            detail.setTransaction(transaction);
            detail.setProduct(product);
            detail.setQuantity(detailRequest.getQuantity());
            detail.setUnitPrice(product.getUnitPrice());
            transaction.getDetails().add(detail);

            updateStock(product, event, detailRequest);
        }

        transactionsRepository.save(transaction);

        logger.info(
                "Transaction saved successfully. Transaction type: {}",
                event.getType());

        evictProductsCache();
    }

    private void evictProductsCache() {
        try {
            Boolean deleted = redisTemplate.delete(PRODUCTS_CACHE_KEY);

            logger.info(
                    "Products cache evicted after stock update. Key: {}, Existed: {}",
                    PRODUCTS_CACHE_KEY,
                    deleted);
        } catch (Exception exception) {
            logger.error(
                    "Failed to evict products cache after stock update. Key: {}",
                    PRODUCTS_CACHE_KEY,
                    exception);
        }
    }

    private void updateStock(
            Products product,
            TransactionEvent event,
            TransactionDetailRequest detailRequest) {

        int currentStock = product.getUnitsInStock() == null
                ? 0
                : product.getUnitsInStock();

        int quantity = detailRequest.getQuantity();

        int newStock;

        if (event.getType().name().equals("IN")) {
            newStock = currentStock + quantity;
        } else {
            newStock = currentStock - quantity;
        }

        if (newStock < 0) {
            logger.warn(
                    "Insufficient stock. Product: {}, Current Stock: {}, Requested: {}",
                    product.getProductName(),
                    currentStock,
                    quantity);

            throw new RuntimeException(
                    "Insufficient stock for product: " + product.getProductName());
        }

        product.setUnitsInStock((short) newStock);

        productsRepository.save(product);

        StockLogs stockLog = new StockLogs();
        stockLog.setProduct(product);
        stockLog.setQuantity(quantity);
        stockLog.setType(event.getType().name());

        stockLogsRepository.save(stockLog);

        logger.info(
                "Stock updated. Product: {}, Old Stock: {}, Quantity: {}, New Stock: {}",
                product.getProductName(),
                currentStock,
                quantity,
                newStock);
    }
}