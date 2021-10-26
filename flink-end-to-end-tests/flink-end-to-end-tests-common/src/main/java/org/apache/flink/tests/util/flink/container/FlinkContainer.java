/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util.flink.container;

import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** A Flink cluster running JM and TM on containers. */
public class FlinkContainer implements BeforeAllCallback, AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String JOB_MANAGER_HOSTNAME = "jobmanager";
    public static final String TASK_MANAGER_HOSTNAME_PREFIX = "taskmanager-";

    private final GenericContainer<?> jobManager;
    private final List<GenericContainer<?>> taskManagers;
    private final Configuration conf;

    private RestClusterClient<StandaloneClusterId> restClusterClient;
    private boolean isRunning;

    FlinkContainer(
            GenericContainer<?> jobManager,
            List<GenericContainer<?>> taskManagers,
            Configuration conf) {
        this.jobManager = jobManager;
        this.taskManagers = taskManagers;
        this.conf = conf;
    }

    public static FlinkContainerBuilder builder(int numTaskManagers) {
        return new FlinkContainerBuilder(numTaskManagers);
    }

    public void start() {
        this.jobManager.start();
        this.taskManagers.parallelStream().forEach(GenericContainer::start);
        waitForAllTaskManagerRegistered().waitUntilReady(jobManager);
        isRunning = true;
    }

    public void stop() {
        isRunning = false;
        if (restClusterClient != null) {
            restClusterClient.close();
        }
        this.taskManagers.forEach(GenericContainer::stop);
        this.jobManager.stop();
    }

    public boolean isRunning() {
        return isRunning;
    }

    public GenericContainer<?> getJobManager() {
        return this.jobManager;
    }

    public String getJobManagerHost() {
        return jobManager.getHost();
    }

    public int getJobManagerPort() {
        return jobManager.getMappedPort(this.conf.get(RestOptions.PORT));
    }

    public RestClusterClient<StandaloneClusterId> getRestClusterClient() {
        if (this.restClusterClient != null) {
            return restClusterClient;
        }
        checkState(isRunning(), "Cluster client should only be retrieved for a running cluster");
        try {
            final Configuration clientConfiguration = new Configuration();
            clientConfiguration.set(RestOptions.ADDRESS, jobManager.getHost());
            clientConfiguration.set(
                    RestOptions.PORT, jobManager.getMappedPort(conf.get(RestOptions.PORT)));
            this.restClusterClient =
                    new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create client for FlinkContainer cluster", e);
        }
        return restClusterClient;
    }

    public void restartTaskManager(RunnableWithException afterFailAction) throws Exception {
        taskManagers.forEach(GenericContainer::stop);
        afterFailAction.run();
        taskManagers.forEach(GenericContainer::start);
    }

    /**
     * Submits an SQL job to the running cluster.
     *
     * <p><b>NOTE:</b> You should not use {@code '\t'}.
     */
    public void submitSQLJob(SQLJobSubmission job) throws IOException, InterruptedException {
        checkState(isRunning(), "SQL job submission is only applicable for a running cluster");
        // Create SQL script and copy it to JobManager
        final List<String> commands = new ArrayList<>();
        Path script = Files.createTempDirectory("sql-script").resolve("script");
        Files.write(script, job.getSqlLines());
        jobManager.copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.sql");

        // Construct SQL client command
        commands.add("cat /tmp/script.sql | ");
        commands.add("flink/bin/sql-client.sh");
        for (String jar : job.getJars()) {
            commands.add("--jar");
            Path path = Paths.get(jar);
            String containerPath = "/tmp/" + path.getFileName();
            jobManager.copyFileToContainer(MountableFile.forHostPath(path), containerPath);
            commands.add(containerPath);
        }

        // Execute command in JobManager
        Container.ExecResult execResult =
                jobManager.execInContainer("bash", "-c", String.join(" ", commands));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
        }
    }

    // ------------------------ JUnit 5 lifecycle management ------------------------
    @Override
    public void beforeAll(ExtensionContext context) {
        this.start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        this.stop();
    }

    // ----------------------------- Helper functions --------------------------------

    private WaitStrategy waitForAllTaskManagerRegistered() {
        return new HttpWaitStrategy()
                .forPort(this.conf.get(RestOptions.PORT))
                .forPath(TaskManagersHeaders.URL)
                .forResponsePredicate(
                        response -> {
                            try {
                                TaskManagersInfo managersInfo =
                                        OBJECT_MAPPER.readValue(response, TaskManagersInfo.class);
                                return this.taskManagers.size()
                                        == managersInfo.getTaskManagerInfos().size();
                            } catch (JsonProcessingException e) {
                                return false;
                            }
                        });
    }
}
