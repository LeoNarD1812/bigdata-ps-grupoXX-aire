
package com.bigdata.aire.vuelo_operaciones_ms.controller;

import com.bigdata.aire.vuelo_operaciones_ms.dto.OperacionRequest;
import com.bigdata.aire.vuelo_operaciones_ms.model.VueloEvento;
import com.bigdata.aire.vuelo_operaciones_ms.service.VueloProducerService;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/operaciones")
public class VueloController {

    private final VueloProducerService producer;

    public VueloController(VueloProducerService producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<VueloEvento> crearOperacion(
            @Valid @RequestBody OperacionRequest request
    ) throws Exception {

        VueloEvento evento = new VueloEvento(
            UUID.randomUUID().toString(),
            "vuelo.estado_actualizado",
            1,
            request.flightId(),
            request.airportCode(),
            request.status(),
            Instant.now().toString(),
            "operaciones"
        );

        producer.publicar(evento);

        return ResponseEntity.ok(evento);
    }
}
