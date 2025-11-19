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
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.validateMaxDataSize;
import static io.trino.plugin.hive.parquet.ParquetReaderConfig.PARQUET_READER_MAX_SMALL_FILE_THRESHOLD;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;

public class PaimonSessionProperties
        implements SessionPropertiesProvider
{
    public static final String SCAN_TIMESTAMP = "scan_timestamp_millis";
    public static final String SCAN_SNAPSHOT = "scan_snapshot_id";
    public static final String MINIMUM_SPLIT_WEIGHT = "minimum_split_weight";
    private static final String PARQUET_SMALL_FILE_THRESHOLD = "parquet_small_file_threshold";
    private static final String ORC_TINY_STRIPE_THRESHOLD = "orc_tiny_stripe_threshold";
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PaimonSessionProperties(
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            PaimonConfig paimonConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(longProperty(
                        SCAN_TIMESTAMP,
                        SCAN_TIMESTAMP_MILLIS.description().toString(),
                        null,
                        true))
                .add(longProperty(
                        SCAN_SNAPSHOT,
                        SCAN_SNAPSHOT_ID.description().toString(),
                        null,
                        true))
                .add(doubleProperty(
                        MINIMUM_SPLIT_WEIGHT,
                        "Minimum split weight",
                        0.05,
                        false))
                .add(dataSizeProperty(
                        PARQUET_SMALL_FILE_THRESHOLD,
                        "Parquet: Size below which a parquet file will be read entirely",
                        parquetReaderConfig.getSmallFileThreshold(),
                        value -> validateMaxDataSize(PARQUET_SMALL_FILE_THRESHOLD, value, DataSize.valueOf(PARQUET_READER_MAX_SMALL_FILE_THRESHOLD)),
                        false))
                .add(dataSizeProperty(
                        ORC_TINY_STRIPE_THRESHOLD,
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        orcReaderConfig.getTinyStripeThreshold(),
                        false))
                .add(booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Read only required fields from a row type",
                        paimonConfig.isProjectionPushdownEnabled(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
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

    public static DataSize getParquetSmallFileThreshold(ConnectorSession session)
    {
        return session.getProperty(PARQUET_SMALL_FILE_THRESHOLD, DataSize.class);
    }

    public static DataSize getOrcTinyStripeThreshold(ConnectorSession session)
    {
        return session.getProperty(ORC_TINY_STRIPE_THRESHOLD, DataSize.class);
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }
}
