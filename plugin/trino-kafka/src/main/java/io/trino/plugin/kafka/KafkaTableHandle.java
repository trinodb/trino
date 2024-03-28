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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.filterKeys;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public final class KafkaTableHandle
        implements ConnectorTableHandle, ConnectorInsertTableHandle
{
    /**
     * The schema name for this table. Is set through configuration and read
     * using {@link KafkaConfig#getDefaultSchema()}. Usually 'default'.
     */
    private final String schemaName;

    /**
     * The table name used by Trino.
     */
    private final String tableName;

    /**
     * The topic name that is read from Kafka.
     */
    private final String topicName;

    private final String keyDataFormat;
    private final String messageDataFormat;
    private final Optional<String> keyDataSchemaLocation;
    private final Optional<String> messageDataSchemaLocation;
    private final Optional<String> keySubject;
    private final Optional<String> messageSubject;
    private final List<KafkaColumnHandle> columns;
    private final TupleDomain<KafkaColumnHandle> compactEffectivePredicate;
    private final TupleDomain<ColumnHandle> constraint;
    private final Map<String, KafkaColumnHandle> predicateColumns;
    /**
     * Represents the flag indicating whether the table is right for pushdown.
     */
    private final boolean rightForPush;
    /**
     * Represents a handle to a limit value in a Kafka table.
     * The limit is an optional value indicating the maximum number of records to return in a query.
     */
    private final OptionalLong limit;

    @JsonCreator
    public KafkaTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("keyDataFormat") String keyDataFormat,
            @JsonProperty("messageDataFormat") String messageDataFormat,
            @JsonProperty("keyDataSchemaLocation") Optional<String> keyDataSchemaLocation,
            @JsonProperty("messageDataSchemaLocation") Optional<String> messageDataSchemaLocation,
            @JsonProperty("keySubject") Optional<String> keySubject,
            @JsonProperty("messageSubject") Optional<String> messageSubject,
            @JsonProperty("columns") List<KafkaColumnHandle> columns,
            @JsonProperty("compactEffectivePredicate") TupleDomain<KafkaColumnHandle> compactEffectivePredicate,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("predicateColumns") Map<String, KafkaColumnHandle> predicateColumns,
            @JsonProperty("rightForPush") boolean rightForPush,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "keyDataFormat is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.keyDataSchemaLocation = requireNonNull(keyDataSchemaLocation, "keyDataSchemaLocation is null");
        this.messageDataSchemaLocation = requireNonNull(messageDataSchemaLocation, "messageDataSchemaLocation is null");
        this.keySubject = requireNonNull(keySubject, "keySubject is null");
        this.messageSubject = requireNonNull(messageSubject, "messageSubject is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "constraint is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.predicateColumns = predicateColumns;
        this.rightForPush = rightForPush;
        this.limit = requireNonNull(limit, "constraint is null");
    }

    public KafkaTableHandle(
            String schemaName,
            String tableName,
            String topicName,
            String keyDataFormat,
            String messageDataFormat,
            Optional<String> keyDataSchemaLocation,
            Optional<String> messageDataSchemaLocation,
            Optional<String> keySubject,
            Optional<String> messageSubject,
            List<KafkaColumnHandle> columns,
            TupleDomain<ColumnHandle> constraint,
            OptionalLong limit)
    {
        this(
                schemaName,
                tableName,
                topicName,
                keyDataFormat,
                messageDataFormat,
                keyDataSchemaLocation,
                messageDataSchemaLocation,
                keySubject,
                messageSubject,
                columns,
                TupleDomain.all(),
                constraint,
                null,
                false,
                limit);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public String getKeyDataFormat()
    {
        return keyDataFormat;
    }

    @JsonProperty
    public String getMessageDataFormat()
    {
        return messageDataFormat;
    }

    @JsonProperty
    public Optional<String> getMessageDataSchemaLocation()
    {
        return messageDataSchemaLocation;
    }

    @JsonProperty
    public Optional<String> getKeyDataSchemaLocation()
    {
        return keyDataSchemaLocation;
    }

    @JsonProperty
    public Optional<String> getKeySubject()
    {
        return keySubject;
    }

    @JsonProperty
    public Optional<String> getMessageSubject()
    {
        return messageSubject;
    }

    @JsonProperty
    public List<KafkaColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public TupleDomain<KafkaColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    @JsonProperty
    public Map<String, KafkaColumnHandle> getPredicateColumns()
    {
        return predicateColumns;
    }

    @JsonProperty
    public boolean isRightForPush()
    {
        return rightForPush;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                schemaName,
                tableName,
                topicName,
                keyDataFormat,
                messageDataFormat,
                keyDataSchemaLocation,
                messageDataSchemaLocation,
                keySubject,
                messageSubject,
                columns,
                constraint,
                limit);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KafkaTableHandle other = (KafkaTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName)
                && Objects.equals(this.tableName, other.tableName)
                && Objects.equals(this.topicName, other.topicName)
                && Objects.equals(this.keyDataFormat, other.keyDataFormat)
                && Objects.equals(this.messageDataFormat, other.messageDataFormat)
                && Objects.equals(this.keyDataSchemaLocation, other.keyDataSchemaLocation)
                && Objects.equals(this.messageDataSchemaLocation, other.messageDataSchemaLocation)
                && Objects.equals(this.keySubject, other.keySubject)
                && Objects.equals(this.messageSubject, other.messageSubject)
                && Objects.equals(this.columns, other.columns)
                && Objects.equals(this.compactEffectivePredicate, other.compactEffectivePredicate)
                && Objects.equals(this.constraint, other.constraint)
                && Objects.equals(this.predicateColumns, other.predicateColumns)
                && this.rightForPush == other.rightForPush
                && Objects.equals(this.limit, other.limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("topicName", topicName)
                .add("keyDataFormat", keyDataFormat)
                .add("messageDataFormat", messageDataFormat)
                .add("keyDataSchemaLocation", keyDataSchemaLocation)
                .add("messageDataSchemaLocation", messageDataSchemaLocation)
                .add("keySubject", keySubject)
                .add("messageSubject", messageSubject)
                .add("columns", columns)
                .add("constraint", constraint)
                .add("limit", limit)
                .toString();
    }

    /**
     * Returns the list of KafkaColumnHandles that represent the remaining predicate columns.
     * The method filters out the columns that are not compatible with the Kafka connector,
     * such as columns that are not part of the supported types or columns associated with unsupported operations.
     *
     * @param internalFieldManager The KafkaInternalFieldManager used to retrieve the internal field ID of a column
     * @return The list of remaining predicate columns that can be used for filtering
     */
    public List<KafkaColumnHandle> getRemainingPredicateColumn(final KafkaInternalFieldManager internalFieldManager)
    {
        // the connector for database does not support discrete range matching(or multiple ranges) at right now
        // such as a query with the clause "where condition" like "where x in (2, 6) and (y > 7 or y = 7)"
        return constraint.getDomains()
                .orElseGet(HashMap::new)
                .entrySet().stream()
                .collect(toMap(e -> (KafkaColumnHandle) e.getKey(), e -> e.getValue()))
                .entrySet().stream()
                .filter(kafkaColumnHandleDomainEntry -> {
                    switch (internalFieldManager.getInternalFieldId(kafkaColumnHandleDomainEntry.getKey())) {
                        case PARTITION_OFFSET_FIELD -> {
                            ValueSet valueSet = kafkaColumnHandleDomainEntry.getValue().getValues();
                            if (valueSet instanceof SortedRangeSet sortedSet
                                    && sortedSet.getRanges().getRangeCount() == 1) {
                                return true;
                            }
                            return false;
                        }
                        case PARTITION_ID_FIELD -> {
                            return true;
                        }
                        default ->
                        {
                            return false;
                        }
                    }
                })
                .map(entity -> entity.getKey())
                .collect(toImmutableList());
    }

    /**
     * Returns the remaining filter for the Kafka table.
     *
     * This method filters out unnecessary predicates and returns the remaining filter as a TupleDomain of ColumnHandles.
     * The remaining filter includes only the predicate columns that are compatible with the Kafka connector, such as
     * columns that are part of the supported types and are associated with supported operations.
     *
     * @param internalFieldManager The KafkaInternalFieldManager used to retrieve the internal field ID of a column.
     * @return The remaining filter as a TupleDomain of ColumnHandles that can be used for filtering.
     */
    public TupleDomain<ColumnHandle> getRemainingFilter(final KafkaInternalFieldManager internalFieldManager)
    {
        // filter some unnecessary predicates
        return TupleDomain.<ColumnHandle>all()
                .intersect(withColumnDomains(filterKeys(constraint.getDomains().get(), not(in(getRemainingPredicateColumn(internalFieldManager))))));
    }

    /**
     * Returns the enforced predicate for the Kafka table.
     *
     * @param internalFieldManager The KafkaInternalFieldManager used to retrieve the internal field ID of a column
     * @return The enforced predicate for the Kafka table
     */
    public TupleDomain<ColumnHandle> getEnforcedPredicate(final KafkaInternalFieldManager internalFieldManager)
    {
        return TupleDomain.<ColumnHandle>all()
                .intersect(withColumnDomains(filterKeys(constraint.getDomains().get(), in(getRemainingPredicateColumn(internalFieldManager)))));
    }
}
