package com.cicd.orchestrator.controller;

import com.cicd.orchestrator.model.CommitEvent;
import com.cicd.orchestrator.service.GitHubWebhookService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(WebhookController.class)
class WebhookControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private GitHubWebhookService gitHubWebhookService;

    @Test
    void testHandlePushEventSuccess() throws Exception {
        CommitEvent event = new CommitEvent();
        event.setRepo("veerjain-1/test-repo");
        event.setBranch("main");
        event.setCommitSha("abcdef123456");
        event.setAuthor("veerjain");

        when(gitHubWebhookService.processWebhook(eq("push"), any(), anyString())).thenReturn(event);

        mockMvc.perform(post("/api/v1/webhooks/github")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-GitHub-Event", "push")
                        .header("X-Hub-Signature-256", "sha256=mockedvalidhash")
                        .content("{\"dummy\": \"payload\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("TRIGGERED"))
                .andExpect(jsonPath("$.repo").value("veerjain-1/test-repo"))
                .andExpect(jsonPath("$.branch").value("main"))
                .andExpect(jsonPath("$.commitSha").value("abcdef123456"))
                .andExpect(jsonPath("$.author").value("veerjain"));
    }

    @Test
    void testHandlePingEventAcknowledged() throws Exception {
        when(gitHubWebhookService.processWebhook(eq("ping"), any(), anyString())).thenReturn(null);

        mockMvc.perform(post("/api/v1/webhooks/github")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-GitHub-Event", "ping")
                        .content("{\"zen\": \"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("IGNORED"))
                .andExpect(jsonPath("$.event").value("ping"));
    }

    @Test
    void testHandleInvalidSignatureReturns401() throws Exception {
        when(gitHubWebhookService.processWebhook(anyString(), any(), anyString()))
                .thenThrow(new SecurityException("Invalid HMAC signature"));

        mockMvc.perform(post("/api/v1/webhooks/github")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-GitHub-Event", "push")
                        .header("X-Hub-Signature-256", "sha256=tampered")
                        .content("{\"dummy\": \"payload\"}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value("UNAUTHORIZED"))
                .andExpect(jsonPath("$.error").value("Invalid HMAC signature"));
    }
}
