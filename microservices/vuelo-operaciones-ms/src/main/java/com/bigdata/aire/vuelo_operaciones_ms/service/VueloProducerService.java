
package com.bigdata.aire.vuelo_operaciones_ms.service;

import com.bigdata.aire.vuelo_operaciones_ms.model.VueloEvento;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.TimeUnit;

@Service
public class VueloProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.operaciones}")
    private String topic;

    public VueloProducerService(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {

        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publicar(VueloEvento evento) throws Exception {

        String json = objectMapper.writeValueAsString(evento);

        var resultado = kafkaTemplate.send(
            topic,
            evento.flightId(),
            json
        ).get(10, TimeUnit.SECONDS);

        System.out.println(
            "EVENTO PUBLICADO EN KAFKA"
            + " | Vuelo: " + evento.flightId()
            + " | Particion: "
            + resultado.getRecordMetadata().partition()
            + " | Offset: "
            + resultado.getRecordMetadata().offset()
        );
    }
}
