package br.com.microservices.choreography.inventoryservice.core.saga;

import br.com.microservices.choreography.inventoryservice.core.dto.Event;
import br.com.microservices.choreography.inventoryservice.core.producer.KafkaProducer;
import br.com.microservices.choreography.inventoryservice.core.utils.JsonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static java.lang.String.format;

@Component
@Slf4j
@RequiredArgsConstructor
public class SagaExecutionController {

    private static final String SAGA_LOG_ID = "ORDER ID: %s | TRANSACTION ID %s | EVENT ID: %s";


    private final JsonUtil jsonUtil;
    private final KafkaProducer producer;

    @Value("${spring.kafka.topic.notify-ending}")
    private String notifyEndingTopic;
    @Value("${spring.kafka.topic.payment-fail}")
    private String paymentFailTopic;
    @Value("${spring.kafka.topic.inventory-fail}")
    private String inventoryFailTopic;

    public void handleSaga(Event event){

        switch (event.getStatus()){
            case SUCCESS -> handleSuccess(event);
            case FAIL -> handleFail(event);
            case ROLLBACK_PENDING -> handleRollbackPending(event);
        }
    }

    private void handleSuccess(Event event){
        log.info("### CURRENT SAGA: {} | SUCCESS | NEXT TOPIC {} | {}",
                event.getSource(), notifyEndingTopic, createSagaId(event));
        sendEvent(event, notifyEndingTopic);
    }

    private void handleFail(Event event){
        log.info("### CURRENT SAGA: {} | SENDING TO ROLLBACK PREVIOUS SERVICE | NEXT TOPIC {} | {}",
                event.getSource(), paymentFailTopic, createSagaId(event));
        sendEvent(event, paymentFailTopic);
    }

    private void handleRollbackPending(Event event){
        log.info("### CURRENT SAGA: {} | SENDING TO ROLLBACK CURRENT SERVICE | NEXT TOPIC {} | {}",
                event.getSource(), inventoryFailTopic, createSagaId(event));
        sendEvent(event, inventoryFailTopic);
    }

    private void sendEvent(Event event, String topic){
        producer.sendEvent(jsonUtil.toJson(event), topic);
    }

    private String createSagaId(Event event){
        return format(SAGA_LOG_ID,
                event.getPayload().getId(), event.getTransactionId(), event.getId());
    }

}
