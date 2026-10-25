package com.skdebela.lakehouse.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
class ApiErrors {

    static class NotFound extends RuntimeException {
        NotFound(String message) { super(message); }
    }

    static class BadRequest extends RuntimeException {
        BadRequest(String message) { super(message); }
    }

    @ExceptionHandler
    ProblemDetail notFound(NotFound e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler
    ProblemDetail badRequest(BadRequest e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

}
