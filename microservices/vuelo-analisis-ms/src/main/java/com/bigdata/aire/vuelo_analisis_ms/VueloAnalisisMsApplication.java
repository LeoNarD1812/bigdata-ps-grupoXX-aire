
package com.bigdata.aire.vuelo_analisis_ms;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class VueloAnalisisMsApplication {

    public static void main(String[] args) {
        SpringApplication.run(
            VueloAnalisisMsApplication.class,
            args
        );
    }
}
