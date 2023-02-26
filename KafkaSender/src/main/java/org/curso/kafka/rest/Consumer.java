package org.curso.kafka.rest;

import com.google.gson.Gson;
import io.swagger.annotations.ApiParam;
import org.curso.kafka.service.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/KafkaConsumer")
public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    @RequestMapping(value = "/consume", method = RequestMethod.POST)
    public ResponseEntity<String> consume(@RequestParam String bootstrapServers,
                                          @RequestParam String topic,
                                          @ApiParam(required = false) @RequestParam(required = false) String groupId,
                                          @ApiParam(required = false) @RequestBody String properties) {
        log.info("Read messages... ");
        String result = new KafkaConsumer(getKafkaProperties(properties), groupId, bootstrapServers, topic).pollTopic();
        log.info(result);
        return ResponseEntity.ok(result);
    }

    private Properties getKafkaProperties(String propertiesString) {
        Gson gson = new Gson();
        Map<String, String> map = gson.fromJson(propertiesString, Map.class);
        Properties properties = new Properties();
        map.keySet().forEach(k -> properties.put(k, map.get(k)));
        return properties;
    }
}
