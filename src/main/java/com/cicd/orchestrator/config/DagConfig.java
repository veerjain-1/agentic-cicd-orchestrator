package com.cicd.orchestrator.config;

import com.cicd.orchestrator.engine.DagExecutor;
import com.cicd.orchestrator.engine.TaskNode;
import com.cicd.orchestrator.service.LLMAnalysisService;
import com.cicd.orchestrator.tasks.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
public class DagConfig {

    @Bean
    public DagExecutor dagExecutor() {
        return new DagExecutor();
    }

    /**
     * Defines the autonomous CI/CD DAG topology with concurrent dependency validation
     * and recursive LLM code analysis agent inspection.
     */
    @Bean
    public List<TaskNode> standardPipelineDag(LLMAnalysisService llmAnalysisService) {
        TaskNode cloneNode = new TaskNode("clone", new CloneRepoTask());

        TaskNode validateDepsNode = new TaskNode("validate-dependencies", new DependencyValidationTask())
                .dependsOn("clone");

        TaskNode codeAnalysisNode = new TaskNode("code-analysis-agent", new CodeAnalysisAgentTask(llmAnalysisService))
                .dependsOn("clone");

        TaskNode buildNode = new TaskNode("build", new MavenBuildTask())
                .dependsOn("clone")
                .dependsOn("validate-dependencies");

        TaskNode testNode = new TaskNode("test", new RunTestsTask())
                .dependsOn("build");

        TaskNode publishNode = new TaskNode("publish", new PublishArtifactTask())
                .dependsOn("test")
                .dependsOn("code-analysis-agent");

        // The engine performs topological sorting and executes non-dependent nodes
        // concurrently using Java 21 Virtual Threads.
        return Arrays.asList(cloneNode, validateDepsNode, codeAnalysisNode, buildNode, testNode, publishNode);
    }
}

