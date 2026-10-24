package com.cicd.orchestrator.controller;

import com.cicd.orchestrator.model.PipelineState;
import com.cicd.orchestrator.service.PipelineExecutionService;
import com.cicd.orchestrator.service.RetryExecutorService;
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

    @MockBean
    private RetryExecutorService retryExecutorService;

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

    @Test
    public void testGetAllPipelines() throws Exception {
        PipelineState state1 = new PipelineState("p1", "repo1", "main", "sha1");
        PipelineState state2 = new PipelineState("p2", "repo2", "dev", "sha2");

        when(executionService.getAllPipelines()).thenReturn(java.util.List.of(state1, state2));

        mockMvc.perform(get("/api/v1/pipelines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].pipelineId").value("p1"))
                .andExpect(jsonPath("$[1].pipelineId").value("p2"));
    }

    @Test
    public void testGetReportFound() throws Exception {
        java.util.Map<String, Object> report = java.util.Map.of(
                "pipelineId", "p100",
                "repo", "veerjain-1/agentguard",
                "status", "COMPLETED",
                "hasPrComment", true
        );

        when(executionService.getPipelineReport("p100")).thenReturn(report);

        mockMvc.perform(get("/api/v1/pipelines/p100/report"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.pipelineId").value("p100"))
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.hasPrComment").value(true));
    }

    @Test
    public void testGetPrCommentFound() throws Exception {
        PipelineState state = new PipelineState("p200", "repo", "main", "sha");
        state.setPrCommentBody("### 🛡️ AI Security & Quality Audit Report\nPassed.");

        when(executionService.getPipelineStatus("p200")).thenReturn(state);

        mockMvc.perform(get("/api/v1/pipelines/p200/pr-comment"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("AI Security & Quality Audit Report")));
    }

    @Test
    public void testGetPrCommentNoContentWhenNull() throws Exception {
        PipelineState state = new PipelineState("p300", "repo", "main", "sha");

        when(executionService.getPipelineStatus("p300")).thenReturn(state);

        mockMvc.perform(get("/api/v1/pipelines/p300/pr-comment"))
                .andExpect(status().isNoContent());
    }

    @Test
    public void testApprovePipelineSuccess() throws Exception {
        PipelineState approvedState = new PipelineState("p400", "repo", "main", "sha");
        approvedState.status(PipelineState.PipelineStatus.APPROVED);
        approvedState.approver("veerjain");

        when(executionService.approvePipeline(eq("p400"), eq("veerjain"), eq("LGTM")))
                .thenReturn(approvedState);

        mockMvc.perform(post("/api/v1/pipelines/p400/approve")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"approver\": \"veerjain\", \"notes\": \"LGTM\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPROVED"))
                .andExpect(jsonPath("$.approver").value("veerjain"));
    }

    @Test
    public void testApprovePipelineNotFound() throws Exception {
        when(executionService.approvePipeline(eq("p-missing"), any(), any()))
                .thenReturn(null);

        mockMvc.perform(post("/api/v1/pipelines/p-missing/approve"))
                .andExpect(status().isNotFound());
    }

    @Test
    public void testApprovePipelineThrowsBadRequestWhenNotGating() throws Exception {
        when(executionService.approvePipeline(eq("p-fail"), any(), any()))
                .thenThrow(new IllegalStateException("Pipeline is not currently in GATING state"));

        mockMvc.perform(post("/api/v1/pipelines/p-fail/approve"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Pipeline is not currently in GATING state"));
    }

    @Test
    public void testRejectPipelineSuccess() throws Exception {
        PipelineState rejectedState = new PipelineState("p500", "repo", "main", "sha");
        rejectedState.status(PipelineState.PipelineStatus.BLOCKED);
        rejectedState.approver("sec-officer");

        when(executionService.rejectPipeline(eq("p500"), eq("sec-officer"), eq("Vulnerabilities detected")))
                .thenReturn(rejectedState);

        mockMvc.perform(post("/api/v1/pipelines/p500/reject")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"approver\": \"sec-officer\", \"reason\": \"Vulnerabilities detected\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("BLOCKED"))
                .andExpect(jsonPath("$.approver").value("sec-officer"));
    }

    @Test
    public void testGetRollbackRecordNone() throws Exception {
        when(executionService.getRollbackRecord("pipe-none")).thenReturn(null);

        mockMvc.perform(get("/api/v1/pipelines/pipe-none/rollback"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rollback").value("none"))
                .andExpect(jsonPath("$.pipelineId").value("pipe-none"));
    }

    @Test
    public void testGetRetryHistory() throws Exception {
        when(retryExecutorService.getRetryHistory("pipe-retries")).thenReturn(java.util.List.of());

        mockMvc.perform(get("/api/v1/pipelines/pipe-retries/retries"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }
}
