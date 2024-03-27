package br.com.microservices.choreography.productvalidationservice.core.service;

import br.com.microservices.choreography.productvalidationservice.core.dto.Event;
import br.com.microservices.choreography.productvalidationservice.core.dto.History;
import br.com.microservices.choreography.productvalidationservice.core.dto.OrderProduct;
import br.com.microservices.choreography.productvalidationservice.core.enums.ESagaStatus;
import br.com.microservices.choreography.productvalidationservice.config.exception.ValidationException;
import br.com.microservices.choreography.productvalidationservice.core.model.Validation;
import br.com.microservices.choreography.productvalidationservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.productvalidationservice.core.repository.ProductRepository;
import br.com.microservices.choreography.productvalidationservice.core.repository.ValidationRepository;
import br.com.microservices.choreography.productvalidationservice.core.saga.SagaExecutionController;
import br.com.microservices.choreography.productvalidationservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static org.springframework.util.ObjectUtils.isEmpty;

@Service
@Slf4j
@AllArgsConstructor
public class ProductValidationService {

    private static final String CURRENT_SOURCE = "PRODUCT_VALIDATION_SERVICE";
    private final SagaExecutionController sagaExecutionController;
    private final ProductRepository productRepository;
    private final ValidationRepository validationRepository;

    public void validateExistingProducts(Event event){
        try{
            checkCurrentValidation(event);
            createValidation(event, true);
            handleSuccess(event);
        }catch (Exception e){
            log.error("Error trying to validation products: ", e);
            handleFailCurrentNotExecuted(event, e.getMessage());
        }
        sagaExecutionController.handleSaga(event);
    }

    private void validaProductsInformed(Event event) {
        if(isEmpty(event.getPayload()) || isEmpty(event.getPayload().getProducts()))
            throw new ValidationException("Product list is empty");

        if(isEmpty(event.getId()) || isEmpty(event.getTransactionId()))
            throw new ValidationException("OrderId and TransactionId must be informed");

    }

    private void checkCurrentValidation(Event event) {
        validaProductsInformed(event);
        if (validationRepository.existsByOrderIdAndTransactionId(event.getOrderId(), event.getTransactionId()))
            throw new ValidationException("There is another transactionId for this validation");

        event.getPayload().getProducts().forEach(product -> {
            validateProductInformed(product);
            validateExistingProduct(product.getProduct().getCode());
        });

    }

    private void validateProductInformed(OrderProduct orderProduct){
        if(isEmpty(orderProduct.getProduct()) || isEmpty(orderProduct.getProduct().getCode()))
            throw new ValidationException("Product must be informed");
    }

    private void validateExistingProduct(String code){
        if(!productRepository.existsByCode(code))
            throw new ValidationException("Product does not exist in database.");
    }

    private void createValidation(Event event, boolean success){
        var validation = Validation
                .builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .success(success)
                .build();
        validationRepository.save(validation);
    }

    private void handleSuccess(Event event) {

        event.setStatus(ESagaStatus.SUCCESS);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Products are validated successfully");

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
        event.setStatus(ESagaStatus.ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Fail to validate products: ".concat(message));
    }

    public void rollbackEvent(Event event){
        changeValidationToFail(event);
        event.setStatus(ESagaStatus.FAIL);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Rollback executed on product validation.");
        sagaExecutionController.handleSaga(event);
    }

    private void changeValidationToFail(Event event) {
        validationRepository.findByOrderIdAndTransactionId(event.getPayload().getId(),
                event.getTransactionId()).ifPresentOrElse(validation -> {
                    validation.setSuccess(false);
                    validationRepository.save(validation);
                    }, () -> createValidation(event, false));
    }

}
