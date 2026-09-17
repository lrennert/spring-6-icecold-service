package guru.springframework.spring6icecoldservice.listeners;

import guru.springframework.spring6icecoldservice.config.KafkaConfig;
import guru.springframework.spring6restmvcapi.events.DrinkPreparedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class DrinkPreparedKafkaConsumer {

    AtomicInteger messageCounter = new AtomicInteger(0);

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = KafkaConfig.DRINK_PREPARED_TOPIC)
    public void receive(DrinkPreparedEvent drinkPreparedEvent) {
        System.out.println("Received Messages: " + drinkPreparedEvent);
        messageCounter.incrementAndGet();
    }
}
