package org.curso.kafka.controller;

import com.google.gson.Gson;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiParam;
import io.swagger.models.auth.In;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.curso.kafka.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

@RestController
@RequestMapping("/KafkaSender")
public class KafkaSender {

    private static final Logger log = LoggerFactory.getLogger(KafkaSender.class);

    @RequestMapping(value = "/sendToKafka", method = RequestMethod.POST)
    public void sendToKafka(@RequestParam String bootstrapServers, @RequestParam String topic,
                            @ApiParam(value = "KEY_SERIALIZER", allowableValues = "Long, String, GenericRecord") @RequestParam String keySerializer,
                            @ApiParam(required = false) @RequestParam(required=false) String key,
                            @ApiParam(value = "VALUE_SERIALIZER", allowableValues = "Long, String, GenericRecord") @RequestParam String valueSerializer,
                            @RequestParam String value, @ApiParam(required = false) @RequestBody Object headers, @ApiParam(required = false) @RequestParam(required=false) Integer partition) {
        log.info("Sending message ");
        try {
            new KafkaService(bootstrapServers, keySerializer, valueSerializer)
                    .sendMessage(topic, partition, key, value, getKafkaHeaders(headers), new Date().getTime());
        }catch (NumberFormatException e){
            log.error("{} is not number", partition);
        }
    }

    private Iterable<Header> getKafkaHeaders(Object headerString) {
        Gson gson = new Gson();
        Map<String, String> map = gson.fromJson(headerString.toString(), Map.class);
        Headers headers = new RecordHeaders();
        map.keySet().forEach(k -> headers.add(new RecordHeader(k, map.get(k).getBytes(StandardCharsets.UTF_8))));
        return headers;

    }

}
