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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;

/**
 * Trino session properties.
 */
public class PaimonSessionProperties
{
    public static final String SCAN_TIMESTAMP = "scan_timestamp_millis";
    public static final String SCAN_SNAPSHOT = "scan_snapshot_id";
    public static final String MINIMUM_SPLIT_WEIGHT = "minimum_split_weight";
    public static final String INSERT_EXISTING_PARTITIONS_BEHAVIOR =
            "insert_existing_partitions_behavior";

    private final List<PropertyMetadata<?>> sessionProperties;

    public PaimonSessionProperties()
    {
        sessionProperties =
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(
                                longProperty(
                                        SCAN_TIMESTAMP,
                                        SCAN_TIMESTAMP_MILLIS.description().toString(),
                                        null,
                                        true))
                        .add(
                                longProperty(
                                        SCAN_SNAPSHOT,
                                        SCAN_SNAPSHOT_ID.description().toString(),
                                        null,
                                        true))
                        .add(
                                doubleProperty(
                                        MINIMUM_SPLIT_WEIGHT, "Minimum split weight", 0.05, false))
                        .add(
                                new PropertyMetadata<>(
                                        INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                                        "Behavior on insert existing partitions",
                                        VARCHAR,
                                        InsertExistingPartitionsBehavior.class,
                                        InsertExistingPartitionsBehavior.APPEND,
                                        false,
                                        value ->
                                                InsertExistingPartitionsBehavior.valueOf(
                                                        (String) value),
                                        InsertExistingPartitionsBehavior::toString))
                        .build();
    }

    public static Long getScanTimestampMillis(ConnectorSession session)
    {
        return session.getProperty(SCAN_TIMESTAMP, Long.class);
    }

    public static Long getScanSnapshotId(ConnectorSession session)
    {
        return session.getProperty(SCAN_SNAPSHOT, Long.class);
    }

    public static Double getMinimumSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_SPLIT_WEIGHT, Double.class);
    }

    public static boolean enableInsertOverwrite(ConnectorSession session)
    {
        return session.getProperty(
                INSERT_EXISTING_PARTITIONS_BEHAVIOR, InsertExistingPartitionsBehavior.class)
                == InsertExistingPartitionsBehavior.OVERWRITE;
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    /**
     * Insert existing partitions behavior.
     */
    public enum InsertExistingPartitionsBehavior
    {
        ERROR,
        APPEND,
        OVERWRITE,
    }
}
