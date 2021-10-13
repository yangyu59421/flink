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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.TreeCacheSelector;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionStateListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ZooKeeperSingleLeaderElectionService implements LeaderLatchListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(ZooKeeperSingleLeaderElectionService.class);

    private final Object lock = new Object();

    private final CuratorFramework curatorFramework;

    private final LeaderLatch leaderLatch;

    private final TreeCache treeCache;

    private final ConnectionStateListener listener =
            (client, newState) -> handleStateChange(newState);

    private final String leaderContenderDescription;

    private final FatalErrorHandler fatalErrorHandler;

    @GuardedBy("lock")
    private final ExecutorService leadershipOperationExecutor;

    @GuardedBy("lock")
    private final Map<String, LeaderElectionEventHandler> leaderElectionEventHandlers;

    private boolean running = true;

    @Nullable
    @GuardedBy("lock")
    private UUID currentLeaderSessionId = null;

    public ZooKeeperSingleLeaderElectionService(
            CuratorFramework curatorFramework,
            FatalErrorHandler fatalErrorHandler,
            String leaderContenderDescription)
            throws Exception {
        this.curatorFramework = curatorFramework;
        this.leaderContenderDescription = leaderContenderDescription;
        this.fatalErrorHandler = fatalErrorHandler;
        this.leadershipOperationExecutor =
                java.util.concurrent.Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                String.format(
                                        "leadershipOperation-%s", leaderContenderDescription)));
        this.leaderElectionEventHandlers = new HashMap<>();
        this.leaderLatch = new LeaderLatch(curatorFramework, ZooKeeperUtils.getLeaderLatchNode());
        this.treeCache =
                TreeCache.newBuilder(curatorFramework, "/")
                        .setCacheData(true)
                        .setCreateParentNodes(false)
                        .setSelector(new ConnectionInfoNodeSelector())
                        .setExecutor(Executors.newDirectExecutorService())
                        .build();
        treeCache
                .getListenable()
                .addListener(
                        (client, event) -> {
                            switch (event.getType()) {
                                case NODE_ADDED:
                                case NODE_REMOVED:
                                case NODE_UPDATED:
                                    handleChangedLeaderInformation(event.getData());
                            }
                        });

        leaderLatch.addListener(this);
        leaderLatch.start();
        treeCache.start();

        curatorFramework.getConnectionStateListenable().addListener(listener);
    }

    private void handleChangedLeaderInformation(ChildData childData) {
        if (leaderLatch.hasLeadership()) {
            if (childData != null) {
                final String path = childData.getPath();
                final String[] splits = path.split("/");

                Preconditions.checkState(
                        splits.length >= 2, "Expecting at least <leader_name>/connection_info");
                final String id = splits[splits.length - 2];

                final LeaderElectionEventHandler leaderElectionEventHandler;

                synchronized (lock) {
                    if (!running) {
                        return;
                    }
                    leaderElectionEventHandler = leaderElectionEventHandlers.get(id);

                    if (leaderElectionEventHandler != null) {
                        final LeaderInformation leaderInformation =
                                tryReadingLeaderInformation(childData, id);

                        leadershipOperationExecutor.execute(
                                () ->
                                        leaderElectionEventHandler.onLeaderInformationChange(
                                                leaderInformation));
                    }
                }
            }
        }
    }

    private LeaderInformation tryReadingLeaderInformation(ChildData childData, String id) {
        LeaderInformation leaderInformation;
        try {
            leaderInformation = ZooKeeperUtils.readLeaderInformation(childData.getData());

            LOG.debug("Leader information for {} has changed to {}.", id, leaderInformation);
        } catch (IOException | ClassNotFoundException e) {
            LOG.debug(
                    "Could not read leader information for {}. Rewriting the information.", id, e);
            leaderInformation = LeaderInformation.empty();
        }

        return leaderInformation;
    }

    public void close() throws Exception {
        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;

            LOG.info("Closing {}.", this);

            curatorFramework.getConnectionStateListenable().removeListener(listener);
            ExecutorUtils.gracefulShutdown(10L, TimeUnit.SECONDS, leadershipOperationExecutor);

            Exception exception = null;

            try {
                treeCache.close();
            } catch (Exception e) {
                exception = e;
            }

            try {
                leaderLatch.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            ExceptionUtils.tryRethrowException(exception);
        }
    }

    public LeaderElectionDriverFactory createDriverFactory(String leaderName) {
        return new LeaderElectionDriverAdapterFactory(leaderName, this);
    }

    public void writeLeaderInformation(String leaderName, LeaderInformation leaderInformation) {
        Preconditions.checkState(running);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write leader information {} for {}.", leaderInformation, leaderName);
        }
        if (!leaderLatch.hasLeadership() || leaderInformation.isEmpty()) {
            return;
        }

        final String connectionInformationPath =
                ZooKeeperUtils.generateConnectionInformationPath(leaderName);

        try {
            ZooKeeperUtils.writeLeaderInformationToZooKeeper(
                    leaderInformation,
                    curatorFramework,
                    leaderLatch::hasLeadership,
                    connectionInformationPath);
        } catch (Exception e) {
            fatalErrorHandler.onFatalError(
                    new LeaderElectionException(
                            "Could not write leader address and leader session ID to "
                                    + "ZooKeeper.",
                            e));
        }
    }

    private void handleStateChange(ConnectionState newState) {
        switch (newState) {
            case CONNECTED:
                LOG.debug("Connected to ZooKeeper quorum. Leader election can start.");
                break;
            case SUSPENDED:
                LOG.warn("Connection to ZooKeeper suspended, waiting for reconnection.");
                break;
            case RECONNECTED:
                LOG.info(
                        "Connection to ZooKeeper was reconnected. Leader election can be restarted.");
                break;
            case LOST:
                // Maybe we have to throw an exception here to terminate the JobManager
                LOG.warn(
                        "Connection to ZooKeeper lost. The contender "
                                + leaderContenderDescription
                                + " no longer participates in the leader election.");
                break;
        }
    }

    @Override
    public void isLeader() {
        final UUID newLeaderSessionId = UUID.randomUUID();
        synchronized (lock) {
            if (!running) {
                return;
            }

            currentLeaderSessionId = UUID.randomUUID();

            forEachLeaderElectionEventHandler(
                    leaderElectionEventHandler ->
                            leaderElectionEventHandler.onGrantLeadership(newLeaderSessionId));
        }
    }

    @Override
    public void notLeader() {
        synchronized (lock) {
            if (!running) {
                return;
            }

            currentLeaderSessionId = null;

            forEachLeaderElectionEventHandler(LeaderElectionEventHandler::onRevokeLeadership);
        }
    }

    @GuardedBy("lock")
    private void forEachLeaderElectionEventHandler(
            Consumer<? super LeaderElectionEventHandler> action) {

        for (LeaderElectionEventHandler leaderElectionEventHandler :
                leaderElectionEventHandlers.values()) {
            leadershipOperationExecutor.execute(() -> action.accept(leaderElectionEventHandler));
        }
    }

    public void registerLeaderElectionEventHandler(
            String id, LeaderElectionEventHandler leaderElectionEventHandler) {

        synchronized (lock) {
            Preconditions.checkState(
                    !leaderElectionEventHandlers.containsKey(id),
                    "Do not support duplicate LeaderElectionEventHandler registration under %s",
                    id);
            leaderElectionEventHandlers.put(id, leaderElectionEventHandler);

            if (currentLeaderSessionId != null) {
                leadershipOperationExecutor.execute(
                        () -> leaderElectionEventHandler.onGrantLeadership(currentLeaderSessionId));
            }
        }
    }

    public void unregisterLeaderElectionEventHandler(String id) {
        final LeaderElectionEventHandler unregisteredLeaderElectionEventHandler;
        synchronized (lock) {
            unregisteredLeaderElectionEventHandler = leaderElectionEventHandlers.remove(id);

            if (unregisteredLeaderElectionEventHandler != null) {
                leadershipOperationExecutor.execute(
                        unregisteredLeaderElectionEventHandler::onRevokeLeadership);
            }
        }
    }

    public boolean hasLeadership() {
        Preconditions.checkState(running);
        return leaderLatch.hasLeadership();
    }

    private static final class SingleLeaderElectionDriverAdapter implements LeaderElectionDriver {
        private final String leaderName;
        private final ZooKeeperSingleLeaderElectionService zooKeeperSingleLeaderElectionDriver;

        private SingleLeaderElectionDriverAdapter(
                String leaderName,
                ZooKeeperSingleLeaderElectionService zooKeeperSingleLeaderElectionDriver,
                LeaderElectionEventHandler leaderElectionEventHandler) {
            this.leaderName = leaderName;
            this.zooKeeperSingleLeaderElectionDriver = zooKeeperSingleLeaderElectionDriver;

            zooKeeperSingleLeaderElectionDriver.registerLeaderElectionEventHandler(
                    leaderName, leaderElectionEventHandler);
        }

        @Override
        public void writeLeaderInformation(LeaderInformation leaderInformation) {
            zooKeeperSingleLeaderElectionDriver.writeLeaderInformation(
                    leaderName, leaderInformation);
        }

        @Override
        public boolean hasLeadership() {
            return zooKeeperSingleLeaderElectionDriver.hasLeadership();
        }

        @Override
        public void close() throws Exception {
            zooKeeperSingleLeaderElectionDriver.unregisterLeaderElectionEventHandler(leaderName);
        }
    }

    private static final class LeaderElectionDriverAdapterFactory
            implements LeaderElectionDriverFactory {
        private final String leaderName;
        private final ZooKeeperSingleLeaderElectionService zooKeeperSingleLeaderElectionDriver;

        private LeaderElectionDriverAdapterFactory(
                String leaderName,
                ZooKeeperSingleLeaderElectionService zooKeeperSingleLeaderElectionDriver) {
            this.leaderName = leaderName;
            this.zooKeeperSingleLeaderElectionDriver = zooKeeperSingleLeaderElectionDriver;
        }

        @Override
        public LeaderElectionDriver createLeaderElectionDriver(
                LeaderElectionEventHandler leaderEventHandler,
                FatalErrorHandler fatalErrorHandler,
                String leaderContenderDescription)
                throws Exception {
            return new SingleLeaderElectionDriverAdapter(
                    leaderName, zooKeeperSingleLeaderElectionDriver, leaderEventHandler);
        }
    }

    private static class ConnectionInfoNodeSelector implements TreeCacheSelector {
        @Override
        public boolean traverseChildren(String fullPath) {
            return true;
        }

        @Override
        public boolean acceptChild(String fullPath) {
            return fullPath.endsWith(ZooKeeperUtils.CONNECTION_INFO_NODE);
        }
    }
}
