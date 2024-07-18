package com.coda.web.exceptions;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class ErrorResponse {

    /**
     * HTTP status code.
     */
    private final HttpStatus status;

    /**
     * Error message.
     */
    private final String message;

    /**
     * Constructor.
     * @param status HTTP status code.
     * @param message Error message.
     */
    public ErrorResponse(HttpStatus status, String message) {
        this.status = status;
        this.message = message;
    }
}
