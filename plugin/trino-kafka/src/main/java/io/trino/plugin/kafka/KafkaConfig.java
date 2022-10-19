/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.plugin.kafka.schema.file.FileTableDescriptionSupplier;
import io.trino.spi.HostAddress;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.File;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;

@DefunctConfig("kafka.connect-timeout")
public class KafkaConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;

    private Set<HostAddress> nodes = ImmutableSet.of();
    private DataSize kafkaBufferSize = DataSize.of(64, Unit.KILOBYTE);
    private String defaultSchema = "default";
    private boolean hideInternalColumns = true;
    private int messagesPerSplit = 100_000;
    private boolean timestampUpperBoundPushDownEnabled;
    private String tableDescriptionSupplier = FileTableDescriptionSupplier.NAME;
    private List<File> resourceConfigFiles = ImmutableList.of();
    private String internalFieldPrefix = "_";

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("kafka.nodes")
    @ConfigDescription("Seed nodes for Kafka cluster. At least one must exist")
    public KafkaConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    public DataSize getKafkaBufferSize()
    {
        return kafkaBufferSize;
    }

    @Config("kafka.buffer-size")
    @ConfigDescription("Kafka message consumer buffer size")
    public KafkaConfig setKafkaBufferSize(String kafkaBufferSize)
    {
        this.kafkaBufferSize = DataSize.valueOf(kafkaBufferSize);
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("kafka.default-schema")
    @ConfigDescription("Schema name to use in the connector")
    public KafkaConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    public String getTableDescriptionSupplier()
    {
        return tableDescriptionSupplier;
    }

    @Config("kafka.table-description-supplier")
    @ConfigDescription("The table description supplier to use, default is FILE")
    public KafkaConfig setTableDescriptionSupplier(String tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = tableDescriptionSupplier;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("kafka.hide-internal-columns")
    @ConfigDescription("Whether internal columns are shown in table metadata or not. Default is no")
    public KafkaConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    private static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return stream(splitter.split(nodes))
                .map(KafkaConfig::toHostAddress)
                .collect(toImmutableSet());
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }

    @Min(1)
    public int getMessagesPerSplit()
    {
        return messagesPerSplit;
    }

    @Config("kafka.messages-per-split")
    @ConfigDescription("Count of Kafka messages to be processed by single Trino Kafka connector split")
    public KafkaConfig setMessagesPerSplit(int messagesPerSplit)
    {
        this.messagesPerSplit = messagesPerSplit;
        return this;
    }

    public boolean isTimestampUpperBoundPushDownEnabled()
    {
        return timestampUpperBoundPushDownEnabled;
    }

    @Config("kafka.timestamp-upper-bound-force-push-down-enabled")
    @ConfigDescription("timestamp upper bound force pushing down enabled")
    public KafkaConfig setTimestampUpperBoundPushDownEnabled(boolean timestampUpperBoundPushDownEnabled)
    {
        this.timestampUpperBoundPushDownEnabled = timestampUpperBoundPushDownEnabled;
        return this;
    }

    @NotNull
    public List<@FileExists File> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("kafka.config.resources")
    @ConfigDescription("Optional config files")
    public KafkaConfig setResourceConfigFiles(List<String> files)
    {
        this.resourceConfigFiles = files.stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    @NotEmpty
    public String getInternalFieldPrefix()
    {
        return internalFieldPrefix;
    }

    @Config("kafka.internal-column-prefix")
    @ConfigDescription("Prefix for internal columns")
    public KafkaConfig setInternalFieldPrefix(String internalFieldPrefix)
    {
        this.internalFieldPrefix = internalFieldPrefix;
        return this;
    }
}
