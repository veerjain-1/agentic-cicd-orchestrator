package com.cicd.orchestrator.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.NoSuchElementException;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class GlobalExceptionHandlerTest {

    private MockMvc mockMvc;

    @RestController
    static class ExceptionTestController {

        @GetMapping("/test/illegal-argument")
        public void throwIllegalArgument() {
            throw new IllegalArgumentException("Invalid branch name specified.");
        }

        @GetMapping("/test/illegal-state")
        public void throwIllegalState() {
            throw new IllegalStateException("Pipeline is not in awaiting approval state.");
        }

        @GetMapping("/test/not-found")
        public void throwNotFound() {
            throw new NoSuchElementException("Pipeline with ID pipe-999 not found.");
        }

        @GetMapping("/test/security")
        public void throwSecurity() {
            throw new SecurityException("Webhook HMAC signature mismatch.");
        }

        @GetMapping("/test/unexpected")
        public void throwUnexpected() {
            throw new RuntimeException("Unexpected runtime failure in downstream worker.");
        }
    }

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders
                .standaloneSetup(new ExceptionTestController())
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void testHandleIllegalArgumentExceptionReturns400() throws Exception {
        mockMvc.perform(get("/test/illegal-argument")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value("Invalid branch name specified."))
                .andExpect(jsonPath("$.timestamp").isNotEmpty());
    }

    @Test
    void testHandleIllegalStateExceptionReturns409() throws Exception {
        mockMvc.perform(get("/test/illegal-state")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.error").value("Conflict"))
                .andExpect(jsonPath("$.message").value("Pipeline is not in awaiting approval state."));
    }

    @Test
    void testHandleNoSuchElementExceptionReturns404() throws Exception {
        mockMvc.perform(get("/test/not-found")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.error").value("Not Found"))
                .andExpect(jsonPath("$.message").value("Pipeline with ID pipe-999 not found."));
    }

    @Test
    void testHandleSecurityExceptionReturns401() throws Exception {
        mockMvc.perform(get("/test/security")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"))
                .andExpect(jsonPath("$.message").value("Webhook HMAC signature mismatch."));
    }

    @Test
    void testHandleGenericExceptionReturns500() throws Exception {
        mockMvc.perform(get("/test/unexpected")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(jsonPath("$.error").value("Internal Server Error"))
                .andExpect(jsonPath("$.message").value("An unexpected error occurred: Unexpected runtime failure in downstream worker."));
    }
}
