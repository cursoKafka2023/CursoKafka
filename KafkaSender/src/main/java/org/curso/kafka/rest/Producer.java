package org.curso.kafka.rest;

import com.google.gson.Gson;
import io.swagger.annotations.ApiParam;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.curso.kafka.service.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

@RestController
@RequestMapping("/KafkaProducer")
public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    @RequestMapping(value = "/sendToKafka", method = RequestMethod.POST)
    public void sendToKafka(@RequestParam String bootstrapServers,
                            @RequestParam String topic,
                            @ApiParam(value = "KEY_SERIALIZER", allowableValues = "Long, String, GenericRecord") @RequestParam String keySerializer,
                            @ApiParam(required = false) @RequestParam(required = false) String key,
                            @ApiParam(value = "VALUE_SERIALIZER", allowableValues = "Long, String, GenericRecord") @RequestParam String valueSerializer,
                            @RequestParam String value,
                            @ApiParam(required = false) @RequestBody String properties,
                            @ApiParam(required = false) @RequestParam String headers,
                            @ApiParam(required = false) @RequestParam(required = false, defaultValue = "") String partition) {
        log.info("Sending message ");
        try {
            new KafkaProducer(bootstrapServers, keySerializer, valueSerializer, getKafkaProperties(properties))
                    .sendMessage(topic, getPartition(partition), key, value, getKafkaHeaders(headers), new Date().getTime());
        } catch (NumberFormatException e) {
            log.error("{} is not number", partition);
        }
    }

    private Properties getKafkaProperties(String propertiesString) {
        Gson gson = new Gson();
        Map<String, String> map = gson.fromJson(propertiesString, Map.class);
        Properties properties = new Properties();
        map.keySet().forEach(k -> properties.put(k, map.get(k)));
        return properties;
    }

    private Integer getPartition(String partition) {
        try {
            return Integer.valueOf(partition);
        } catch (Exception e) {
            return null;
        }
    }

    private Iterable<Header> getKafkaHeaders(String headerString) {
        Gson gson = new Gson();
        Map<String, String> map = gson.fromJson(headerString, Map.class);
        Headers headers = new RecordHeaders();
        map.keySet().forEach(k -> headers.add(new RecordHeader(k, map.get(k).getBytes(StandardCharsets.UTF_8))));
        return headers;

    }

}
