package br.com.microservices.choreography.inventoryservice.core.service;

import br.com.microservices.choreography.inventoryservice.config.exception.ValidationException;
import br.com.microservices.choreography.inventoryservice.core.dto.Event;
import br.com.microservices.choreography.inventoryservice.core.dto.History;
import br.com.microservices.choreography.inventoryservice.core.dto.Order;
import br.com.microservices.choreography.inventoryservice.core.dto.OrderProduct;
import br.com.microservices.choreography.inventoryservice.core.model.Inventory;
import br.com.microservices.choreography.inventoryservice.core.model.OrderInventory;
import br.com.microservices.choreography.inventoryservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.inventoryservice.core.repository.InventoryRepository;
import br.com.microservices.choreography.inventoryservice.core.repository.OrderInventoryRepository;
import br.com.microservices.choreography.inventoryservice.core.saga.SagaExecutionController;
import br.com.microservices.choreography.inventoryservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static br.com.microservices.choreography.inventoryservice.core.enums.ESagaStatus.*;

@Service
@Slf4j
@AllArgsConstructor
public class InventoryService {

    private static final String CURRENT_SOURCE = "INVENTORY_SERVICE";

    private final SagaExecutionController sagaExecutionController;
    private final InventoryRepository inventoryRepository;
    private final OrderInventoryRepository orderInventoryRepository;

    public void updateInventory(Event event){
        try{
            checkCurrentValidation(event);
            createOrderInventory(event);
            updateInventory(event.getPayload());
            handleSuccess(event);
        }catch (Exception e){
            log.error("Error trying to update inventory: ", e);
            handleFailCurrentNotExecuted(event, e.getMessage());
        }
        sagaExecutionController.handleSaga(event);
    }

    private void createOrderInventory(Event event) {
        event.getPayload().getProducts().forEach(product -> {
            var inventory = findInventoryByProductCode(product.getProduct().getCode());
            var orderInventory = createOrderInventory(event, product, inventory);
            orderInventoryRepository.save(orderInventory);
        });
    }

    private OrderInventory createOrderInventory(Event event, OrderProduct orderProduct, Inventory inventory){

        return OrderInventory.builder()
                .inventory(inventory)
                .oldQuantity(inventory.getAvailable())
                .orderQuantity(orderProduct.getQuantity())
                .newQuantity(inventory.getAvailable() - orderProduct.getQuantity())
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .build();

    }

    private Inventory findInventoryByProductCode(String productCode){
        return inventoryRepository.findByProductCode(productCode)
                .orElseThrow( () -> new ValidationException("Inventory not found by informed product"));
    }

    private void checkCurrentValidation(Event event) {
        if (orderInventoryRepository.existsByOrderIdAndTransactionId(event.getOrderId(), event.getTransactionId()))
            throw new ValidationException("There is another transactionId for this validation");
    }

    private void updateInventory(Order order){
        order.getProducts().forEach( product -> {

            var inventory = findInventoryByProductCode(product.getProduct().getCode());
            checkInventory(inventory.getAvailable(), product.getQuantity());
            inventory.setAvailable(inventory.getAvailable() - product.getQuantity());
            inventoryRepository.save(inventory);
        });

    }

    private void checkInventory(int available, int orderQuantity){
        if(orderQuantity > available)
            throw new ValidationException("Product is out of stock.");
    }

    private void handleSuccess(Event event) {

        event.setStatus(SUCCESS);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Inventory updated successfully");

    }

    private void addHistory(Event event, String message){
        var history = History.builder()
                .status(event.getStatus())
                .source(event.getSource())
                .message(message)
                .createdAt(LocalDateTime.now())
                .build();
        event.addToHistory(history);
    }

    private void handleFailCurrentNotExecuted (Event event, String message){
        event.setStatus(ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Fail to update inventory: ".concat(message));
    }

    public void rollbackInventory(Event event){
        event.setStatus(FAIL);
        event.setSource(CURRENT_SOURCE);
        try{
            returnInventoryToPreviousValues(event);
            addHistory(event, "Rollback executed for inventory.");
        }catch (ValidationException e){
            addHistory(event, "Rollback not executed for inventory: ".concat(e.getMessage()));
        }
        sagaExecutionController.handleSaga(event);
    }

    private void returnInventoryToPreviousValues(Event event) {
        orderInventoryRepository
                .findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
                .forEach(orderInventory -> {
                    var inventory = orderInventory.getInventory();
                    inventory.setAvailable(orderInventory.getOldQuantity());
                    inventoryRepository.save(inventory);
                    log.info("Restored inventory for order {} from {} to {}",
                            event.getPayload().getId(), orderInventory.getNewQuantity(), inventory.getAvailable());
                });
    }

}
