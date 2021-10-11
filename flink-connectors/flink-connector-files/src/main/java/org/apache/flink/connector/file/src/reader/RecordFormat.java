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

package org.apache.flink.connector.file.src.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A reader format that reads individual records from a file via {@link Path}.
 *
 * <p>This interface teams up with its superinterface together build a 2-levels API. {@link
 * StreamFormat} focuses on abstract input stream and {@link RecordFormat} pays attention to the
 * concrete FileSystem. This format is for cases where the readers need access to the file directly
 * or need to create a custom stream. For readers that can directly work on input streams, consider
 * using the superinterface {@link StreamFormat}.
 *
 * <p>Please refer the javadoc of {@link StreamFormat} for details.
 *
 * @param <T> - The type of records created by this format reader.
 */
@PublicEvolving
public interface RecordFormat<T> extends StreamFormat<T> {

    /**
     * Creates a new reader to read in this format. This method is called when a fresh reader is
     * created for a split that was assigned from the enumerator. This method may also be called on
     * recovery from a checkpoint, if the reader never stored an offset in the checkpoint (see
     * {@link #restoreReader(Configuration, Path, long, long, long)} for details.
     *
     * <p>Provide the default implementation, subclasses are therefore not forced to implement it.
     * Compare to the {@link #createReader(Configuration, FSDataInputStream, long, long)}, This
     * method put the focus on the {@link Path}. The default implementation adapts information given
     * by method arguments to {@link FSDataInputStream} and calls {@link
     * #createReader(Configuration, FSDataInputStream, long, long)}.
     *
     * <p>If the format is {@link #isSplittable() splittable}, then the {@code inputStream} is
     * positioned to the beginning of the file split, otherwise it will be at position zero.
     */
    default StreamFormat.Reader<T> createReader(
            Configuration config, Path filePath, long splitOffset, long splitLength)
            throws IOException {

        checkNotNull(filePath, "filePath");

        final FileSystem fileSystem = filePath.getFileSystem();
        final FileStatus fileStatus = fileSystem.getFileStatus(filePath);
        final FSDataInputStream inputStream = fileSystem.open(filePath);

        if (isSplittable()) {
            inputStream.seek(splitOffset);
        }

        return createReader(config, inputStream, fileStatus.getLen(), splitOffset + splitLength);
    }

    /**
     * Restores a reader from a checkpointed position. This method is called when the reader is
     * recovered from a checkpoint and the reader has previously stored an offset into the
     * checkpoint, by returning from the {@link RecordFormat.Reader#getCheckpointedPosition()} a
     * value with non-negative {@link CheckpointedPosition#getOffset() offset}. That value is
     * supplied as the {@code restoredOffset}.
     *
     * <p>If the reader never produced a {@code CheckpointedPosition} with a non-negative offset
     * before, then this method is not called, and the reader is created in the same way as a fresh
     * reader via the method {@link #createReader(Configuration, Path, long, long)} and the
     * appropriate number of records are read and discarded, to position to reader to the
     * checkpointed position.
     *
     * <p>Provide the default implementation, subclasses are therefore not forced to implement it.
     * Compare to the {@link #restoreReader(Configuration, FSDataInputStream, long, long, long)},
     * This method put the focus on the {@link Path}. For valid {@code restoredOffset}, the default
     * implementation adapts information supplied by method arguments to {@link FSDataInputStream}
     * and delegates the call to {@link #restoreReader(Configuration, FSDataInputStream, long, long,
     * long)}.
     */
    default StreamFormat.Reader<T> restoreReader(
            Configuration config,
            Path filePath,
            long restoredOffset,
            long splitOffset,
            long splitLength)
            throws IOException {

        checkArgument(
                restoredOffset >= 0,
                "This method can be only called "
                        + "when the supplied restoredOffset has a non-negative value. ");

        final FileSystem fileSystem = filePath.getFileSystem();
        final FileStatus fileStatus = fileSystem.getFileStatus(filePath);
        final FSDataInputStream inputStream = fileSystem.open(filePath);

        if (isSplittable()) {
            inputStream.seek(splitOffset);
        }

        return restoreReader(
                config,
                inputStream,
                restoredOffset,
                fileStatus.getLen(),
                splitOffset + splitLength);
    }
}
