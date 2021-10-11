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

package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.IOException;

/** a serializer for PulsarWriter. */
public class PulsarWriterStateSerializer implements SimpleVersionedSerializer<PulsarWriterState> {
    private static final int MAGIC_NUMBER = 0x1e765c70;

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(PulsarWriterState writerState) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(writerState, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public PulsarWriterState deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        switch (version) {
            case 1:
                validateMagicNumber(in);
                return deserializeV1(in);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private void serializeV1(PulsarWriterState record, DataOutputView target) throws IOException {
        if (record.getTxnID() == null) {
            target.writeBoolean(false);
        } else {
            target.writeBoolean(true);
            target.writeLong(record.getTxnID().getMostSigBits());
            target.writeLong(record.getTxnID().getLeastSigBits());
        }
    }

    private PulsarWriterState deserializeV1(DataInputView dataInputView) throws IOException {
        TxnID transactionalId = null;
        if (dataInputView.readBoolean()) {
            long mostSigBits = dataInputView.readLong();
            long leastSigBits = dataInputView.readLong();
            transactionalId = new TxnID(mostSigBits, leastSigBits);
        }
        return new PulsarWriterState(transactionalId);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
