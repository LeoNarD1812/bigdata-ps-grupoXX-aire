
package com.bigdata.aire.vuelo_analisis_ms.model;

public record VueloEvento(
    String eventId,
    String eventType,
    int schemaVersion,
    String flightId,
    String airportCode,
    String status,
    String eventTime,
    String source
) {}
