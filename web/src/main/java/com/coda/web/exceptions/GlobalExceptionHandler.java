package com.coda.web.exceptions;

import com.coda.core.exceptions.ReadFromDbExceptions;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ReadFromDbExceptions.class)
    public ResponseEntity<ErrorResponse> handleReadFromDbExceptions(final ReadFromDbExceptions ex,
                                                                    final WebRequest request) {
        HttpStatus status = switch (ex.getErrorType()) {
            case INVALID_DB_CREDENTIALS -> HttpStatus.UNAUTHORIZED;
            case DB_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case READ_FROM_DB_EXCEPTIONS -> HttpStatus.INTERNAL_SERVER_ERROR;
            case DB_NOT_SUPPORTED -> HttpStatus.NOT_IMPLEMENTED;
            default -> HttpStatus.BAD_REQUEST;
        };

        ErrorResponse errorResponse = new ErrorResponse(status, ex.getMessage());
        return new ResponseEntity<>(errorResponse, status);
    }
}
