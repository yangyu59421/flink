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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Default implementation of a {@link SingleLeaderElectionService} that allows to register multiple
 * {@link LeaderElectionEventHandler}.
 */
public class DefaultSingleLeaderElectionService
        implements SingleLeaderElectionService, SingleLeaderElectionDriver.Listener {
    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultSingleLeaderElectionService.class);

    private final Object lock = new Object();

    private final SingleLeaderElectionDriver singleLeaderElectionDriver;

    private final FatalErrorHandler fatalErrorHandler;

    @GuardedBy("lock")
    private final ExecutorService leadershipOperationExecutor;

    @GuardedBy("lock")
    private final Map<String, LeaderElectionEventHandler> leaderElectionEventHandlers;

    private boolean running = true;

    @Nullable
    @GuardedBy("lock")
    private UUID currentLeaderSessionId = null;

    public DefaultSingleLeaderElectionService(
            FatalErrorHandler fatalErrorHandler,
            String leaderContenderDescription,
            SingleLeaderElectionDriverFactory singleLeaderElectionDriverFactory)
            throws Exception {
        this.fatalErrorHandler = fatalErrorHandler;

        this.leadershipOperationExecutor =
                java.util.concurrent.Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                String.format(
                                        "leadershipOperation-%s", leaderContenderDescription)));

        leaderElectionEventHandlers = new HashMap<>();

        singleLeaderElectionDriver =
                singleLeaderElectionDriverFactory.create(leaderContenderDescription, this);
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;

            LOG.info("Closing {}.", this);

            ExecutorUtils.gracefulShutdown(10L, TimeUnit.SECONDS, leadershipOperationExecutor);

            Exception exception = null;
            try {
                singleLeaderElectionDriver.close();
            } catch (Exception e) {
                exception = e;
            }

            ExceptionUtils.tryRethrowException(exception);
        }
    }

    @Override
    public LeaderElectionDriverFactory createDriverFactory(String leaderName) {
        return new LeaderElectionDriverAdapterFactory(leaderName, this);
    }

    @Override
    public void writeLeaderInformation(String leaderName, LeaderInformation leaderInformation) {
        try {
            singleLeaderElectionDriver.writeLeaderInformation(leaderName, leaderInformation);
        } catch (Exception e) {
            fatalErrorHandler.onFatalError(
                    new FlinkException(
                            String.format(
                                    "Could not write leader information %s for leader %s.",
                                    leaderInformation, leaderName),
                            e));
        }
    }

    @Override
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

    @Override
    public void unregisterLeaderElectionEventHandler(String id) throws Exception {
        final LeaderElectionEventHandler unregisteredLeaderElectionEventHandler;
        synchronized (lock) {
            unregisteredLeaderElectionEventHandler = leaderElectionEventHandlers.remove(id);

            if (unregisteredLeaderElectionEventHandler != null) {
                leadershipOperationExecutor.execute(
                        unregisteredLeaderElectionEventHandler::onRevokeLeadership);
            }
        }

        singleLeaderElectionDriver.deleteLeaderInformation(id);
    }

    @Override
    public boolean hasLeadership(String leaderName) {
        synchronized (lock) {
            Preconditions.checkState(running);

            return leaderElectionEventHandlers.containsKey(leaderName)
                    && singleLeaderElectionDriver.hasLeadership();
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

    @Override
    public void notifyLeaderInformationChange(
            String leaderName, LeaderInformation leaderInformation) {
        synchronized (lock) {
            if (!running) {
                return;
            }

            final LeaderElectionEventHandler leaderElectionEventHandler =
                    leaderElectionEventHandlers.get(leaderName);

            if (leaderElectionEventHandler != null) {
                leadershipOperationExecutor.execute(
                        () ->
                                leaderElectionEventHandler.onLeaderInformationChange(
                                        leaderInformation));
            }
        }
    }

    @Override
    public void notifyAllKnownLeaderInformation(
            Collection<LeaderInformationWithLeaderName> leaderInformationWithNames) {
        synchronized (lock) {
            if (!running) {
                return;
            }

            final Map<String, LeaderInformation> leaderInformationByName =
                    leaderInformationWithNames.stream()
                            .collect(
                                    Collectors.toMap(
                                            LeaderInformationWithLeaderName::getLeaderName,
                                            LeaderInformationWithLeaderName::getLeaderInformation));

            for (Map.Entry<String, LeaderElectionEventHandler>
                    leaderNameLeaderElectionEventHandlerPair :
                            leaderElectionEventHandlers.entrySet()) {
                final String leaderName = leaderNameLeaderElectionEventHandlerPair.getKey();
                if (leaderInformationByName.containsKey(leaderName)) {
                    leaderNameLeaderElectionEventHandlerPair
                            .getValue()
                            .onLeaderInformationChange(leaderInformationByName.get(leaderName));
                } else {
                    leaderNameLeaderElectionEventHandlerPair
                            .getValue()
                            .onLeaderInformationChange(LeaderInformation.empty());
                }
            }
        }
    }
}
