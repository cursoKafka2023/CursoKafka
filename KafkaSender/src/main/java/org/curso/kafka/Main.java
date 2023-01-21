package org.curso.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

@SpringBootApplication
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        ConfigurableApplicationContext app = SpringApplication.run(Main.class, args);
        Environment env = app.getEnvironment();
        log.info("\n------------------------------------------------------------------------------\n" +
                        "\t\t - App name: {}\n " +
                        "\t\t - Url: http://localhost:{}/swagger-ui.html#/sender\n" +
                        "\t\t - Kafka Course\n " +
                        "------------------------------------------------------------------------------",
                env.getProperty("app.name"), env.getProperty("server.port"));
    }
}