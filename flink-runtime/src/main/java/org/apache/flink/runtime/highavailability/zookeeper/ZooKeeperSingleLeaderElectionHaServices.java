/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.DefaultSingleLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.SingleLeaderElectionService;
import org.apache.flink.runtime.leaderelection.ZooKeeperSingleLeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.Executor;

/**
 * ZooKeeper HA services that only use a single leader election per process.
 *
 * <pre>
 * /flink
 *      +/cluster_id_1/leader/latch
 *      |            |       /resource_manager/connection_info
 *      |            |       /dispatcher/connection_info
 *      |            |       /rest_server/connection_info
 *      |            |       /job-id-1/connection_info
 *      |            |       /job-id-2/connection_info
 *      |            |
 *      |            |
 *      |            +jobgraphs/job-id-1
 *      |            |         /job-id-2
 *      |            +jobs/job-id-1/checkpoints/latest
 *      |                 |                    /latest-1
 *      |                 |                    /latest-2
 *      |                 |       /checkpoint_id_counter
 * </pre>
 */
public class ZooKeeperSingleLeaderElectionHaServices extends AbstractZooKeeperHaServices {

    private final Object lock = new Object();

    private final CuratorFramework leaderNamespacedCuratorFramework;

    private final FatalErrorHandler fatalErrorHandler;

    @Nullable
    @GuardedBy("lock")
    private SingleLeaderElectionService singleLeaderElectionService = null;

    public ZooKeeperSingleLeaderElectionHaServices(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Configuration config,
            Executor ioExecutor,
            BlobStoreService blobStoreService,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        super(curatorFrameworkWrapper, ioExecutor, config, blobStoreService);
        this.leaderNamespacedCuratorFramework =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getCuratorFramework(), ZooKeeperUtils.getLeaderPath());
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    protected LeaderElectionService createLeaderElectionService(String leaderName) {
        final SingleLeaderElectionService singleLeaderElectionDriver;

        synchronized (lock) {
            singleLeaderElectionDriver = getOrInitializeSingleLeaderElectionService();
        }

        return new DefaultLeaderElectionService(
                singleLeaderElectionDriver.createDriverFactory(leaderName));
    }

    @GuardedBy("lock")
    private SingleLeaderElectionService getOrInitializeSingleLeaderElectionService() {
        if (singleLeaderElectionService == null) {
            try {
                singleLeaderElectionService =
                        new DefaultSingleLeaderElectionService(
                                fatalErrorHandler,
                                "Single leader election service.",
                                new ZooKeeperSingleLeaderElectionDriverFactory(
                                        leaderNamespacedCuratorFramework));
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Could not initialize the %s",
                                DefaultSingleLeaderElectionService.class.getSimpleName()),
                        e);
            }
        }

        return singleLeaderElectionService;
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String leaderPath) {
        // Maybe use a single service for leader retrieval
        return ZooKeeperUtils.createLeaderRetrievalService(
                leaderNamespacedCuratorFramework, leaderPath, configuration);
    }

    @Override
    protected void internalClose() throws Exception {
        Exception exception = null;
        synchronized (lock) {
            if (singleLeaderElectionService != null) {
                try {
                    singleLeaderElectionService.close();
                } catch (Exception e) {
                    exception = e;
                }
                singleLeaderElectionService = null;
            }
        }

        try {
            super.internalClose();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        ExceptionUtils.tryRethrowException(exception);
    }

    @Override
    protected void internalCleanupJobData(JobID jobID) throws Exception {
        super.internalCleanupJobData(jobID);
    }

    @Override
    protected String getLeaderPathForResourceManager() {
        return ZooKeeperUtils.getResourceManagerNode();
    }

    @Override
    protected String getLeaderPathForDispatcher() {
        return ZooKeeperUtils.getDispatcherNode();
    }

    @Override
    protected String getLeaderPathForJobManager(JobID jobID) {
        return jobID.toString();
    }

    @Override
    protected String getLeaderPathForRestServer() {
        return ZooKeeperUtils.getRestServerNode();
    }
}
