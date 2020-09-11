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
package io.prestosql.plugin.kafka;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.prestosql.spi.HostAddress;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.File;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

@DefunctConfig("kafka.connect-timeout")
public class KafkaConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;

    private Set<HostAddress> nodes = ImmutableSet.of();
    private DataSize kafkaBufferSize = DataSize.of(64, Unit.KILOBYTE);
    private String defaultSchema = "default";
    private Set<String> tableNames = ImmutableSet.of();
    private File tableDescriptionDir = new File("etc/kafka/");
    private boolean hideInternalColumns = true;
    private int messagesPerSplit = 100_000;

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
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("kafka.table-names")
    @ConfigDescription("Set of tables known to this connector")
    public KafkaConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
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

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kafka.table-description-dir")
    @ConfigDescription("Folder holding JSON description files for Kafka topics")
    public KafkaConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    private static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return StreamSupport.stream(splitter.split(nodes).spliterator(), false)
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
    @ConfigDescription("Count of Kafka messages to be processed by single Presto Kafka connector split")
    public KafkaConfig setMessagesPerSplit(int messagesPerSplit)
    {
        this.messagesPerSplit = messagesPerSplit;
        return this;
    }
}
