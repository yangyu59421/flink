/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;

import org.rocksdb.CompactionStyle;
import org.rocksdb.InfoLogLevel;

import java.io.Serializable;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.rocksdb.CompactionStyle.FIFO;
import static org.rocksdb.CompactionStyle.LEVEL;
import static org.rocksdb.CompactionStyle.NONE;
import static org.rocksdb.CompactionStyle.UNIVERSAL;
import static org.rocksdb.InfoLogLevel.INFO_LEVEL;

/**
 * This class contains the configuration options for the {@link DefaultConfigurableOptionsFactory}.
 *
 * <p>If nothing specified, RocksDB's options would be configured by {@link PredefinedOptions} and
 * user-defined {@link RocksDBOptionsFactory}.
 *
 * <p>If some options has been specifically configured, a corresponding {@link
 * DefaultConfigurableOptionsFactory} would be created and applied on top of {@link
 * PredefinedOptions} except if a user-defined {@link RocksDBOptionsFactory} overrides it.
 */
public class RocksDBConfigurableOptions implements Serializable {

    // --------------------------------------------------------------------------
    // Provided configurable DBOptions within Flink
    // --------------------------------------------------------------------------

    public static final ConfigOption<Integer> MAX_BACKGROUND_THREADS =
            key("state.backend.rocksdb.thread.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of concurrent background flush and compaction jobs (per stateful operator). "
                                    + "RocksDB has default configuration as '1'.");

    public static final ConfigOption<Integer> MAX_OPEN_FILES =
            key("state.backend.rocksdb.files.open")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of open files (per stateful operator) that can be used by the DB, '-1' means no limit. "
                                    + "RocksDB has default configuration as '-1'.");

    public static final ConfigOption<MemorySize> LOG_MAX_FILE_SIZE =
            key("state.backend.rocksdb.log.max-file-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum size of RocksDB's file used for information logging. "
                                    + "If the log files becomes larger than this, a new file will be created. "
                                    + "If 0 (RocksDB default setting), all logs will be written to one log file.");

    public static final ConfigOption<Integer> LOG_FILE_NUM =
            key("state.backend.rocksdb.log.file-num")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of files RocksDB should keep for information logging (RocksDB default setting: 1000).");

    public static final ConfigOption<String> LOG_DIR =
            key("state.backend.rocksdb.log.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The directory for RocksDB's information logging files. "
                                    + "If empty (RocksDB default setting), log files will be in the same directory as data files. "
                                    + "If non-empty, this directory will be used and the data directory's absolute path will be used as the prefix of the log file name.");

    public static final ConfigOption<InfoLogLevel> LOG_LEVEL =
            key("state.backend.rocksdb.log.level")
                    .enumType(InfoLogLevel.class)
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The specified information logging level for RocksDB. "
                                                    + "If unset, Flink will use %s.",
                                            code(INFO_LEVEL.name()))
                                    .linebreak()
                                    .text(
                                            "Note: RocksDB info logs will not be written to the TaskManager logs and there "
                                                    + "is a rolling strategy of keeping %s files with %s bytes per file, and you could "
                                                    + "configure %s, %s, and %s accordingly to change this. "
                                                    + "Without a rolling strategy, long-running tasks may lead to uncontrolled "
                                                    + "disk space usage if configured with increased log levels!",
                                            text(
                                                    String.valueOf(
                                                            PredefinedOptions
                                                                    .DEFAULT_LOG_FILE_NUM)),
                                            text(
                                                    String.valueOf(
                                                            PredefinedOptions
                                                                    .DEFAULT_LOG_FILE_SIZE)),
                                            code(LOG_DIR.key()),
                                            code(LOG_MAX_FILE_SIZE.key()),
                                            code(LOG_FILE_NUM.key()))
                                    .linebreak()
                                    .text(
                                            "There is no need to modify the RocksDB log level, unless for troubleshooting RocksDB.")
                                    .build());

    // --------------------------------------------------------------------------
    // Provided configurable ColumnFamilyOptions within Flink
    // --------------------------------------------------------------------------

    public static final ConfigOption<CompactionStyle> COMPACTION_STYLE =
            key("state.backend.rocksdb.compaction.style")
                    .enumType(CompactionStyle.class)
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The specified compaction style for DB. Candidate compaction style is %s, %s, %s or %s, "
                                            + "and RocksDB choose '%s' as default style.",
                                    LEVEL.name(),
                                    FIFO.name(),
                                    UNIVERSAL.name(),
                                    NONE.name(),
                                    LEVEL.name()));

    public static final ConfigOption<Boolean> USE_DYNAMIC_LEVEL_SIZE =
            key("state.backend.rocksdb.compaction.level.use-dynamic-size")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If true, RocksDB will pick target size of each level dynamically. From an empty DB, ")
                                    .text(
                                            "RocksDB would make last level the base level, which means merging L0 data into the last level, ")
                                    .text(
                                            "until it exceeds max_bytes_for_level_base. And then repeat this process for second last level and so on. ")
                                    .text("RocksDB has default configuration as 'false'. ")
                                    .text(
                                            "For more information, please refer to %s",
                                            link(
                                                    "https://github.com/facebook/rocksdb/wiki/Leveled-Compaction#level_compaction_dynamic_level_bytes-is-true",
                                                    "RocksDB's doc."))
                                    .build());

    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE_BASE =
            key("state.backend.rocksdb.compaction.level.target-file-size-base")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The target file size for compaction, which determines a level-1 file size. "
                                    + "RocksDB has default configuration as '64MB'.");

    public static final ConfigOption<MemorySize> MAX_SIZE_LEVEL_BASE =
            key("state.backend.rocksdb.compaction.level.max-size-level-base")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The upper-bound of the total size of level base files in bytes. "
                                    + "RocksDB has default configuration as '256MB'.");

    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            key("state.backend.rocksdb.writebuffer.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The amount of data built up in memory (backed by an unsorted log on disk) "
                                    + "before converting to a sorted on-disk files. RocksDB has default writebuffer size as '64MB'.");

    public static final ConfigOption<Integer> MAX_WRITE_BUFFER_NUMBER =
            key("state.backend.rocksdb.writebuffer.count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of write buffers that are built up in memory. "
                                    + "RocksDB has default configuration as '2'.");

    public static final ConfigOption<Integer> MIN_WRITE_BUFFER_NUMBER_TO_MERGE =
            key("state.backend.rocksdb.writebuffer.number-to-merge")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The minimum number of write buffers that will be merged together before writing to storage. "
                                    + "RocksDB has default configuration as '1'.");

    public static final ConfigOption<MemorySize> BLOCK_SIZE =
            key("state.backend.rocksdb.block.blocksize")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The approximate size (in bytes) of user data packed per block. "
                                    + "RocksDB has default blocksize as '4KB'.");

    public static final ConfigOption<MemorySize> METADATA_BLOCK_SIZE =
            key("state.backend.rocksdb.block.metadata-blocksize")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Approximate size of partitioned metadata packed per block. "
                                    + "Currently applied to indexes block when partitioned index/filters option is enabled. "
                                    + "RocksDB has default metadata blocksize as '4KB'.");

    public static final ConfigOption<MemorySize> BLOCK_CACHE_SIZE =
            key("state.backend.rocksdb.block.cache-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The amount of the cache for data blocks in RocksDB. "
                                    + "RocksDB has default block-cache size as '8MB'.");

    public static final ConfigOption<MemorySize> WRITE_BATCH_SIZE =
            key("state.backend.rocksdb.write-batch-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "The max size of the consumed memory for RocksDB batch write, "
                                    + "will flush just based on item count if this config set to 0.");
}
