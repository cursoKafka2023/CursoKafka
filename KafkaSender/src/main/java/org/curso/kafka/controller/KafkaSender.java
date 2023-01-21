package org.curso.kafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/KafkaSender")
public class KafkaSender {

    private static final Logger log = LoggerFactory.getLogger(KafkaSender.class);

    @RequestMapping(value = "/sendToKafka", method = RequestMethod.GET)
    public void sendToKafka() {
        log.info("Sending message");
    }

}
