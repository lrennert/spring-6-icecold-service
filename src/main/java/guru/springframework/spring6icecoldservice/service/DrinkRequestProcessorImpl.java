package guru.springframework.spring6icecoldservice.service;

import guru.springframework.spring6restmvcapi.events.DrinkRequestEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DrinkRequestProcessorImpl implements DrinkRequestProcessor {

    @Override
    public void processDrinkRequest(DrinkRequestEvent drinkRequestEvent) {
        log.info("Processing drink request...");
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            log.error("Drink request processing interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
}
