package com.cicd.orchestrator.controller;

import com.cicd.orchestrator.model.PipelineState;
import com.cicd.orchestrator.service.PipelineExecutionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(PipelineController.class)
public class PipelineControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PipelineExecutionService executionService;

    @Test
    public void testTriggerPipelineReturnsAccepted() throws Exception {
        mockMvc.perform(post("/api/v1/pipelines/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {
                          "repo": "veerjain-1/agentguard",
                          "branch": "main",
                          "commitSha": "9f680d5"
                        }
                        """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.pipelineId").isNotEmpty());

        verify(executionService, times(1)).executePipeline(any(PipelineState.class));
    }

    @Test
    public void testGetStatusFound() throws Exception {
        PipelineState state = new PipelineState("pipe-123", "test/repo", "main", "abc1234");
        state.status(PipelineState.PipelineStatus.COMPLETED);

        when(executionService.getPipelineStatus("pipe-123")).thenReturn(state);

        mockMvc.perform(get("/api/v1/pipelines/pipe-123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.pipelineId").value("pipe-123"))
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    @Test
    public void testGetStatusNotFound() throws Exception {
        when(executionService.getPipelineStatus("missing-id")).thenReturn(null);

        mockMvc.perform(get("/api/v1/pipelines/missing-id"))
                .andExpect(status().isNotFound());
    }

    @Test
    public void testCancelPipelineSuccess() throws Exception {
        when(executionService.cancelPipeline("pipe-999")).thenReturn(true);

        mockMvc.perform(delete("/api/v1/pipelines/pipe-999"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }
}
