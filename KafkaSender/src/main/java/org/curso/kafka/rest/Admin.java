package org.curso.kafka.rest;

import com.google.gson.Gson;
import io.swagger.annotations.ApiParam;
import io.swagger.models.auth.In;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.curso.kafka.service.KafkaAdmin;
import org.curso.kafka.service.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

@RestController
@RequestMapping("/KafkaAdmin")
public class Admin {

    private static final Logger log = LoggerFactory.getLogger(Admin.class);

    @RequestMapping(value = "/createTopic", method = RequestMethod.POST)
    public void createTopic(@RequestParam String bootstrapServers,
                            @RequestParam String topic,
                            @ApiParam(required = false) @RequestParam String partitions,
                            @ApiParam(required = false) @RequestParam(required = false, defaultValue = "1") String replicationFactor,
                            @ApiParam(required = false) @RequestParam(required = false, defaultValue = "") String partition) {
        log.info("Sending message ");
        try {
            new KafkaAdmin(bootstrapServers).createTopic(topic, getPartition(partitions), getReplication(replicationFactor));
        } catch (NumberFormatException e) {
            log.error("{} is not number", partition);
        }
    }

    private short getReplication(String replicationFactor) {
        return Short.parseShort(replicationFactor);
    }

    private Integer getPartition(String partitions) {
        return Integer.valueOf(partitions);
    }


}
