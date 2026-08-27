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
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.apache.paimon.CoreOptions.SCAN_CREATION_TIME_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;

public class PaimonSessionProperties
{
    public static final String SCAN_TIMESTAMP = "scan_timestamp_millis";
    public static final String SCAN_SNAPSHOT = "scan_snapshot_id";
    public static final String SCAN_TAG = "scan_tag_name";
    public static final String SCAN_FILE_CREATION_TIME = "scan_file_creation_time_millis";
    public static final String SCAN_CREATION_TIME = "scan_creation_time_millis";
    public static final String MINIMUM_SPLIT_WEIGHT = "minimum_split_weight";
    public static final String INSERT_EXISTING_PARTITIONS_BEHAVIOR = "insert_existing_partitions_behavior";
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";

    private final List<PropertyMetadata<?>> sessionProperties;

    public PaimonSessionProperties()
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(longProperty(SCAN_TIMESTAMP, SCAN_TIMESTAMP_MILLIS.description().toString(), null, true))
                .add(longProperty(SCAN_SNAPSHOT, SCAN_SNAPSHOT_ID.description().toString(), null, true))
                .add(longProperty(
                        SCAN_FILE_CREATION_TIME,
                        SCAN_FILE_CREATION_TIME_MILLIS.description().toString(),
                        null,
                        true))
                .add(longProperty(
                        SCAN_CREATION_TIME,
                        SCAN_CREATION_TIME_MILLIS.description().toString(),
                        null,
                        true))
                .add(stringProperty(
                        SCAN_TAG,
                        SCAN_TAG_NAME.description().toString(),
                        null,
                        value -> {
                            if (value != null && value.isBlank()) {
                                throw new TrinoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must not be blank", SCAN_TAG));
                            }
                        },
                        true))
                .add(doubleProperty(
                        MINIMUM_SPLIT_WEIGHT,
                        "Minimum split weight",
                        0.05,
                        value -> {
                            if (!Double.isFinite(value) || value <= 0 || value > 1) {
                                throw new TrinoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be > 0 and <= 1.0: %s", MINIMUM_SPLIT_WEIGHT, value));
                            }
                        },
                        false))
                .add(new PropertyMetadata<>(
                        INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        "Behavior on insert existing partitions",
                        VARCHAR,
                        InsertExistingPartitionsBehavior.class,
                        InsertExistingPartitionsBehavior.APPEND,
                        false,
                        value -> InsertExistingPartitionsBehavior.valueOf(((String) value).strip().toUpperCase(Locale.ENGLISH)),
                        InsertExistingPartitionsBehavior::toString))
                .add(durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        new Duration(0, TimeUnit.SECONDS),
                        false))
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

    public static String getScanTagName(ConnectorSession session)
    {
        String scanTagName = session.getProperty(SCAN_TAG, String.class);
        return scanTagName == null ? null : scanTagName.strip();
    }

    public static Long getScanFileCreationTimeMillis(ConnectorSession session)
    {
        return session.getProperty(SCAN_FILE_CREATION_TIME, Long.class);
    }

    public static Long getScanCreationTimeMillis(ConnectorSession session)
    {
        return session.getProperty(SCAN_CREATION_TIME, Long.class);
    }

    public static Double getMinimumSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_SPLIT_WEIGHT, Double.class);
    }

    public static InsertExistingPartitionsBehavior getInsertExistingPartitionsBehavior(ConnectorSession session)
    {
        return session.getProperty(INSERT_EXISTING_PARTITIONS_BEHAVIOR, InsertExistingPartitionsBehavior.class);
    }

    public static boolean enableInsertOverwrite(ConnectorSession session)
    {
        return getInsertExistingPartitionsBehavior(session) == InsertExistingPartitionsBehavior.OVERWRITE;
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
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
        ERROR, APPEND, OVERWRITE,
    }
}
