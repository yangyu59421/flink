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

package org.apache.flink.connector.pulsar.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Committer implementation for {@link org.apache.flink.connector.pulsar.sink.PulsarSink}. */
@Internal
public class PulsarCommitter implements Committer<PulsarSinkCommittable>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarCommitter.class);

    private final DeliveryGuarantee deliveryGuarantee;

    private final Configuration configuration;

    private transient PulsarClientImpl pulsarClient;

    private final Closer closer = Closer.create();

    public PulsarCommitter(DeliveryGuarantee deliveryGuarantee, Configuration configuration) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.configuration = configuration;
    }

    @Override
    public List<PulsarSinkCommittable> commit(List<PulsarSinkCommittable> committables)
            throws IOException {
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE || committables.isEmpty()) {
            return Collections.emptyList();
        }
        final TransactionCoordinatorClientImpl tcClient = getTcClient();
        LOG.debug("commit committables, current tcClient state: {}", tcClient.getState());
        List<PulsarSinkCommittable> recoveryCommittables = new ArrayList<>();
        for (PulsarSinkCommittable committable : committables) {
            LOG.debug("commit committables {}", committable.getTxnID());
            try {
                tcClient.commit(committable.getTxnID());
            } catch (TransactionCoordinatorClientException e) {
                LOG.error("commit committables failed " + committable.getTxnID(), e);
                recoveryCommittables.add(committable);
            }
        }
        return recoveryCommittables;
    }

    private synchronized TransactionCoordinatorClientImpl getTcClient()
            throws TransactionCoordinatorClientException {
        if (this.pulsarClient != null && !this.pulsarClient.isClosed()) {
            return pulsarClient.getTcClient();
        }
        pulsarClient = (PulsarClientImpl) PulsarConfigUtils.createClient(configuration);
        closer.register(pulsarClient);
        return pulsarClient.getTcClient();
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }
}
