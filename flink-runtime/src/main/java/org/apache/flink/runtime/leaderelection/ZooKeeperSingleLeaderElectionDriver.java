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

import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/** ZooKeeper based {@link SingleLeaderElectionDriver} implementation. */
public class ZooKeeperSingleLeaderElectionDriver
        implements SingleLeaderElectionDriver, LeaderLatchListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(ZooKeeperSingleLeaderElectionDriver.class);

    private final CuratorFramework curatorFramework;

    private final String leaderContenderDescription;

    private final SingleLeaderElectionDriver.Listener leaderElectionListener;

    private final LeaderLatch leaderLatch;

    private final TreeCache treeCache;

    private final ConnectionStateListener listener =
            (client, newState) -> handleStateChange(newState);

    private AtomicBoolean running = new AtomicBoolean(true);

    public ZooKeeperSingleLeaderElectionDriver(
            CuratorFramework curatorFramework,
            String leaderContenderDescription,
            SingleLeaderElectionDriver.Listener leaderElectionListener)
            throws Exception {
        this.curatorFramework = curatorFramework;
        this.leaderContenderDescription = leaderContenderDescription;
        this.leaderElectionListener = leaderElectionListener;

        this.leaderLatch = new LeaderLatch(curatorFramework, ZooKeeperUtils.getLeaderLatchNode());
        this.treeCache =
                TreeCache.newBuilder(curatorFramework, "/")
                        .setCacheData(true)
                        .setCreateParentNodes(false)
                        .setSelector(
                                new ZooKeeperSingleLeaderElectionDriver
                                        .ConnectionInfoNodeSelector())
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
                                    if (event.getData() != null) {
                                        handleChangedLeaderInformation(event.getData());
                                    }
                            }
                        });

        leaderLatch.addListener(this);
        curatorFramework.getConnectionStateListenable().addListener(listener);
        leaderLatch.start();
        treeCache.start();
    }

    @Override
    public void close() throws Exception {
        if (running.compareAndSet(true, false)) {
            LOG.info("Closing {}.", this);

            curatorFramework.getConnectionStateListenable().removeListener(listener);

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

    @Override
    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }

    @Override
    public void writeLeaderInformation(String leaderName, LeaderInformation leaderInformation)
            throws Exception {
        Preconditions.checkState(running.get());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write leader information {} for {}.", leaderInformation, leaderName);
        }
        if (!leaderLatch.hasLeadership() || leaderInformation.isEmpty()) {
            return;
        }

        final String connectionInformationPath =
                ZooKeeperUtils.generateConnectionInformationPath(leaderName);

        ZooKeeperUtils.writeLeaderInformationToZooKeeper(
                leaderInformation,
                curatorFramework,
                leaderLatch::hasLeadership,
                connectionInformationPath);
    }

    @Override
    public void deleteLeaderInformation(String leaderName) throws Exception {
        ZooKeeperUtils.deleteZNode(curatorFramework, ZooKeeperUtils.makeZooKeeperPath(leaderName));
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
        leaderElectionListener.isLeader();
    }

    @Override
    public void notLeader() {
        leaderElectionListener.notLeader();
    }

    private void handleChangedLeaderInformation(ChildData childData) {
        if (running.get() && leaderLatch.hasLeadership() && isConnectionInfoNode(childData)) {

            final String path = childData.getPath();
            final String[] splits = path.split("/");

            Preconditions.checkState(
                    splits.length >= 2,
                    String.format(
                            "Expecting path consisting of <leader_name>/connection_info. Got path '%s'",
                            path));
            final String leaderName = splits[splits.length - 2];

            final LeaderInformation leaderInformation =
                    tryReadingLeaderInformation(childData, leaderName);

            leaderElectionListener.notifyLeaderInformationChange(leaderName, leaderInformation);
        }
    }

    private boolean isConnectionInfoNode(ChildData childData) {
        return childData.getPath().endsWith(ZooKeeperUtils.CONNECTION_INFO_NODE);
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

    private static class ConnectionInfoNodeSelector implements TreeCacheSelector {
        @Override
        public boolean traverseChildren(String fullPath) {
            return true;
        }

        @Override
        public boolean acceptChild(String fullPath) {
            return !fullPath.endsWith(ZooKeeperUtils.getLeaderLatchNode());
        }
    }
}
