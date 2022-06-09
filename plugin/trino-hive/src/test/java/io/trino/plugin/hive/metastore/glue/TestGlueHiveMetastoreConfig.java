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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGlueHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GlueHiveMetastoreConfig.class)
                .setPinGlueClientToCurrentRegion(false)
                .setMaxGlueConnections(30)
                .setMaxGlueErrorRetries(10)
                .setDefaultWarehouseDir(null)
                .setCatalogId(null)
                .setPartitionSegments(5)
                .setGetPartitionThreads(20)
                .setAssumeCanonicalPartitionKeys(false)
                .setReadStatisticsThreads(5)
                .setWriteStatisticsThreads(20));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.glue.pin-client-to-current-region", "true")
                .put("hive.metastore.glue.max-connections", "10")
                .put("hive.metastore.glue.max-error-retries", "20")
                .put("hive.metastore.glue.default-warehouse-dir", "/location")
                .put("hive.metastore.glue.catalogid", "0123456789")
                .put("hive.metastore.glue.partitions-segments", "10")
                .put("hive.metastore.glue.get-partition-threads", "42")
                .put("hive.metastore.glue.assume-canonical-partition-keys", "true")
                .put("hive.metastore.glue.read-statistics-threads", "42")
                .put("hive.metastore.glue.write-statistics-threads", "43")
                .buildOrThrow();

        GlueHiveMetastoreConfig expected = new GlueHiveMetastoreConfig()
                .setPinGlueClientToCurrentRegion(true)
                .setMaxGlueConnections(10)
                .setMaxGlueErrorRetries(20)
                .setDefaultWarehouseDir("/location")
                .setCatalogId("0123456789")
                .setPartitionSegments(10)
                .setGetPartitionThreads(42)
                .setAssumeCanonicalPartitionKeys(true)
                .setReadStatisticsThreads(42)
                .setWriteStatisticsThreads(43);

        assertFullMapping(properties, expected);
    }
}
