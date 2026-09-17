package guru.springframework.spring6icecoldservice.listeners;

import guru.springframework.spring6icecoldservice.config.KafkaConfig;
import guru.springframework.spring6restmvcapi.events.DrinkRequestEvent;
import guru.springframework.spring6restmvcapi.model.BeerDTO;
import guru.springframework.spring6restmvcapi.model.BeerOrderLineDTO;
import guru.springframework.spring6restmvcapi.model.BeerStyle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(controlledShutdown = true, topics = {KafkaConfig.DRINK_PREPARED_TOPIC}, partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DrinkRequestListenerTest {

    @Autowired
    DrinkRequestListener sut;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    DrinkPreparedKafkaConsumer drinkPreparedKafkaConsumer;

    @BeforeEach
    void setUp() {
        kafkaListenerEndpointRegistry.getAllListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, 1));
    }

    @Test
    void testDrinkRequestListener() {
        DrinkRequestEvent drinkRequestEvent = DrinkRequestEvent.builder()
                .beerOrderLineDTO(createBeerOrderLineDTO())
                .build();

        sut.listenDrinkRequest(drinkRequestEvent);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(1, drinkPreparedKafkaConsumer.messageCounter.get()));
    }

    private BeerOrderLineDTO createBeerOrderLineDTO() {
        return BeerOrderLineDTO.builder()
                .beer(BeerDTO.builder()
                        .id(UUID.randomUUID())
                        .beerStyle(BeerStyle.IPA)
                        .beerName("Test Beer")
                        .build())
                .build();
    }
}