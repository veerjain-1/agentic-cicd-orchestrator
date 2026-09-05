package com.cicd.orchestrator.config;

import com.cicd.orchestrator.engine.DagExecutor;
import com.cicd.orchestrator.engine.TaskNode;
import com.cicd.orchestrator.tasks.CloneRepoTask;
import com.cicd.orchestrator.tasks.MavenBuildTask;
import com.cicd.orchestrator.tasks.PublishArtifactTask;
import com.cicd.orchestrator.tasks.RunTestsTask;
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
     * Defines the standard CI/CD DAG topology.
     */
    @Bean
    public List<TaskNode> standardPipelineDag() {
        TaskNode cloneNode = new TaskNode("clone", new CloneRepoTask());
        
        TaskNode buildNode = new TaskNode("build", new MavenBuildTask())
                .dependsOn("clone");
                
        TaskNode testNode = new TaskNode("test", new RunTestsTask())
                .dependsOn("build");
                
        TaskNode publishNode = new TaskNode("publish", new PublishArtifactTask())
                .dependsOn("test");

        // The engine will perform a topological sort and execute these concurrently 
        // using Virtual Threads, respecting the dependencies defined above.
        return Arrays.asList(cloneNode, buildNode, testNode, publishNode);
    }
}
