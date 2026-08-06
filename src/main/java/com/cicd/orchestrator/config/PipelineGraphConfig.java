package com.cicd.orchestrator.config;

import com.cicd.orchestrator.model.PipelineState;
import com.cicd.orchestrator.nodes.*;
import org.bsc.langgraph4j.StateGraph;
import org.bsc.langgraph4j.action.EdgeAction;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.bsc.langgraph4j.StateGraph.END;

@Configuration
public class PipelineGraphConfig {

    private final CommitAnalyzerNode commitAnalyzer;
    private final DependencyValidatorNode dependencyValidator;
    private final BuildOrchestratorNode buildOrchestrator;
    private final TestRunnerNode testRunner;
    private final PRCommentSummarizerNode prCommentSummarizer;
    private final DeploymentGateNode deploymentGate;

    public PipelineGraphConfig(
            CommitAnalyzerNode commitAnalyzer,
            DependencyValidatorNode dependencyValidator,
            BuildOrchestratorNode buildOrchestrator,
            TestRunnerNode testRunner,
            PRCommentSummarizerNode prCommentSummarizer,
            DeploymentGateNode deploymentGate) {
        this.commitAnalyzer = commitAnalyzer;
        this.dependencyValidator = dependencyValidator;
        this.buildOrchestrator = buildOrchestrator;
        this.testRunner = testRunner;
        this.prCommentSummarizer = prCommentSummarizer;
        this.deploymentGate = deploymentGate;
    }

    @Bean
    public StateGraph<PipelineState> pipelineGraph() throws Exception {
        return new StateGraph<>(PipelineState.class)
                .addNode("commitAnalyzer", commitAnalyzer)
                .addNode("dependencyValidator", dependencyValidator)
                .addNode("buildOrchestrator", buildOrchestrator)
                .addNode("testRunner", testRunner)
                .addNode("prCommentSummarizer", prCommentSummarizer)
                .addNode("deploymentGate", deploymentGate)
                
                // Define the flow
                .addEdge("commitAnalyzer", "dependencyValidator")
                
                // Conditional edge from dependency validator
                .addConditionalEdges("dependencyValidator", 
                    (EdgeAction<PipelineState>) state -> {
                        Map<String, Object> report = state.getDependencyReport();
                        if (report != null && "PASS".equals(report.get("status"))) {
                            return "pass";
                        }
                        return "fail";
                    },
                    Map.of("pass", "buildOrchestrator", "fail", END) // In a real app, maybe fail routes back to fix
                )
                
                .addEdge("buildOrchestrator", "testRunner")
                
                // Conditional edge from test runner
                .addConditionalEdges("testRunner",
                    (EdgeAction<PipelineState>) state -> {
                        Map<String, Object> results = state.getTestResults();
                        if (results != null && (int)results.get("failed") == 0) {
                            return "pass";
                        }
                        return "retry";
                    },
                    Map.of(
                        "pass", "prCommentSummarizer",
                        "retry", state -> {
                            if (state.canRetry()) {
                                state.incrementRetry();
                                return "buildOrchestrator"; // Loop back
                            }
                            return END;
                        }
                    )
                )
                
                .addEdge("prCommentSummarizer", "deploymentGate")
                .addEdge("deploymentGate", END)
                
                .setEntryPoint("commitAnalyzer");
    }
}
