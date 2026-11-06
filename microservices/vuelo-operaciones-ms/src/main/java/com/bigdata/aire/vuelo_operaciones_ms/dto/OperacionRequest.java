
package com.bigdata.aire.vuelo_operaciones_ms.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record OperacionRequest(

    @NotBlank
    String flightId,

    @NotBlank
    @Pattern(regexp = "[A-Z]{4}")
    String airportCode,

    @NotBlank
    @Pattern(
        regexp = "PROGRAMADO|EN_VUELO|ATERRIZADO"
    )
    String status

) {}
