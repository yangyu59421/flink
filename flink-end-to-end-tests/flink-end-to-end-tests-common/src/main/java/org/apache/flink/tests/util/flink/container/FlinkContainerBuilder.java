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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;

import org.slf4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A builder class for {@link FlinkContainer}. */
public class FlinkContainerBuilder {

    private final int numTaskManagers;
    private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();
    private final Configuration conf = new Configuration();
    private final Map<String, String> envVars = new HashMap<>();

    private Network network = Network.newNetwork();
    private Logger logger;

    public FlinkContainerBuilder(int numTaskManagers) {
        this.numTaskManagers = numTaskManagers;
    }

    public FlinkContainerBuilder withConfiguration(Configuration conf) {
        this.conf.addAll(conf);
        return this;
    }

    public FlinkContainerBuilder dependsOn(GenericContainer<?> container) {
        container.withNetwork(this.network);
        this.dependentContainers.add(container);
        return this;
    }

    public FlinkContainerBuilder withEnv(String key, String value) {
        this.envVars.put(key, value);
        return this;
    }

    public FlinkContainerBuilder withNetwork(Network network) {
        this.network = network;
        return this;
    }

    public FlinkContainerBuilder withLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    public FlinkContainer build() {
        final GenericContainer<?> jobManager = buildJobManagerContainer();
        final List<GenericContainer<?>> taskManagers = buildTaskManagerContainers();
        return new FlinkContainer(jobManager, taskManagers, conf);
    }

    private GenericContainer<?> buildJobManagerContainer() {
        // Configure JobManager
        final Configuration jobManagerConf = new Configuration();
        jobManagerConf.addAll(this.conf);
        jobManagerConf.set(JobManagerOptions.ADDRESS, FlinkContainer.JOB_MANAGER_HOSTNAME);
        // Build JobManager container
        final ImageFromDockerfile jobManagerImage;
        try {
            jobManagerImage =
                    new FlinkImageBuilder()
                            .withConfiguration(jobManagerConf)
                            .asJobManager()
                            .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build JobManager image", e);
        }
        final GenericContainer<?> jobManager = buildContainer(jobManagerImage);
        // Setup network for JobManager
        jobManager
                .withNetworkAliases(FlinkContainer.JOB_MANAGER_HOSTNAME)
                .withExposedPorts(jobManagerConf.get(RestOptions.PORT));
        // Add environment variables
        jobManager.withEnv(envVars);
        // Setup logger
        if (this.logger != null) {
            jobManager.withLogConsumer(new Slf4jLogConsumer(this.logger).withPrefix("JobManager"));
        }
        return jobManager;
    }

    private List<GenericContainer<?>> buildTaskManagerContainers() {
        List<GenericContainer<?>> taskManagers = new ArrayList<>();
        for (int i = 0; i < numTaskManagers; i++) {
            // Configure TaskManager
            final Configuration taskManagerConf = new Configuration();
            taskManagerConf.addAll(this.conf);
            taskManagerConf.set(JobManagerOptions.ADDRESS, FlinkContainer.JOB_MANAGER_HOSTNAME);
            final String taskManagerHostName = FlinkContainer.TASK_MANAGER_HOSTNAME_PREFIX + i;
            taskManagerConf.set(TaskManagerOptions.HOST, taskManagerHostName);
            // Build TaskManager container
            final ImageFromDockerfile taskManagerImage;
            try {
                taskManagerImage =
                        new FlinkImageBuilder()
                                .withConfiguration(taskManagerConf)
                                .asTaskManager()
                                .build();
            } catch (Exception e) {
                throw new RuntimeException("Failed to build TaskManager image", e);
            }
            final GenericContainer<?> taskManager = buildContainer(taskManagerImage);
            // Setup network for TaskManager
            taskManager.withNetworkAliases(taskManagerHostName);
            // Setup logger
            if (this.logger != null) {
                taskManager.withLogConsumer(
                        new Slf4jLogConsumer(this.logger).withPrefix("TaskManager-" + i));
            }
            taskManagers.add(taskManager);
        }
        return taskManagers;
    }

    private GenericContainer<?> buildContainer(ImageFromDockerfile image) {
        final GenericContainer<?> container = new GenericContainer<>(image);
        for (GenericContainer<?> dependentContainer : dependentContainers) {
            container.dependsOn(dependentContainer);
        }
        container.withNetwork(this.network);
        return container;
    }
}
