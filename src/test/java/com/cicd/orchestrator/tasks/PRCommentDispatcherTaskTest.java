package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.service.GitHubCommentDispatcherService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PRCommentDispatcherTaskTest {

    @Mock
    private GitHubCommentDispatcherService commentDispatcherService;

    private PRCommentDispatcherTask task;

    @BeforeEach
    void setUp() {
        task = new PRCommentDispatcherTask(commentDispatcherService);
    }

    @Test
    void testExecuteWithPrCommentAndPrNumberDispatchesComment() {
        when(commentDispatcherService.dispatchPrComment(eq("veerjain-1/test-repo"), eq(101), anyString()))
                .thenReturn(true);

        Map<String, Object> context = new HashMap<>();
        context.put("repo", "veerjain-1/test-repo");
        context.put("prNumber", 101);
        context.put("prCommentBody", "### Review Summary\nLGTM!");

        boolean result = task.execute(context);

        assertTrue(result);
        assertEquals(true, context.get("prCommentDispatched"));
        verify(commentDispatcherService).dispatchPrComment(eq("veerjain-1/test-repo"), eq(101), eq("### Review Summary\nLGTM!"));
    }

    @Test
    void testExecuteWithoutPrNumberSkipsGracefully() {
        Map<String, Object> context = new HashMap<>();
        context.put("repo", "veerjain-1/test-repo");
        context.put("prCommentBody", "### Review Summary\nLGTM!");

        boolean result = task.execute(context);

        assertTrue(result);
        verify(commentDispatcherService, never()).dispatchPrComment(any(), any(), any());
    }

    @Test
    void testExecuteWithoutCommentBodySkipsGracefully() {
        Map<String, Object> context = new HashMap<>();
        context.put("repo", "veerjain-1/test-repo");
        context.put("prNumber", 101);

        boolean result = task.execute(context);

        assertTrue(result);
        verify(commentDispatcherService, never()).dispatchPrComment(any(), any(), any());
    }
}
