
package com.bigdata.aire.vuelo_analisis_ms.service;

import com.bigdata.aire.vuelo_analisis_ms.model.VueloEvento;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class VueloAnalisisConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.salida}")
    private String topicSalida;

    private static final Set<String> ESTADOS_VALIDOS =
        Set.of("PROGRAMADO", "EN_VUELO", "ATERRIZADO");

    public VueloAnalisisConsumer(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {

        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "${app.kafka.topic.entrada}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void procesar(String mensaje) throws Exception {

        VueloEvento evento =
            objectMapper.readValue(mensaje, VueloEvento.class);

        if (evento.eventId() == null ||
            evento.flightId() == null ||
            evento.flightId().isBlank() ||
            evento.airportCode() == null ||
            !evento.airportCode().matches("[A-Z]{4}") ||
            evento.schemaVersion() != 1 ||
            !"vuelo.estado_actualizado".equals(evento.eventType()) ||
            evento.eventTime() == null ||
            evento.source() == null ||
            !ESTADOS_VALIDOS.contains(evento.status())) {

            System.out.println("EVENTO RECHAZADO");
            return;
        }

        String clasificacion = switch (evento.status()) {
            case "PROGRAMADO" -> "PENDIENTE";
            case "EN_VUELO" -> "OPERACION_ACTIVA";
            case "ATERRIZADO" -> "OPERACION_FINALIZADA";
            default -> throw new IllegalStateException();
        };

        Map<String, Object> resultado = Map.of(
            "eventId", UUID.randomUUID().toString(),
            "originalEventId", evento.eventId(),
            "eventType", "vuelo.analizado",
            "schemaVersion", 1,
            "flightId", evento.flightId(),
            "airportCode", evento.airportCode(),
            "status", evento.status(),
            "classification", clasificacion,
            "processedAt", Instant.now().toString(),
            "source", "vuelo-analisis-ms"
        );

        String json = objectMapper.writeValueAsString(resultado);

        var confirmacion = kafkaTemplate.send(
            topicSalida,
            evento.flightId(),
            json
        ).get(10, TimeUnit.SECONDS);

        System.out.println(
            "EVENTO ANALIZADO"
            + " | Vuelo: " + evento.flightId()
            + " | Clasificacion: " + clasificacion
            + " | Particion: "
            + confirmacion.getRecordMetadata().partition()
            + " | Offset: "
            + confirmacion.getRecordMetadata().offset()
        );
    }
}
