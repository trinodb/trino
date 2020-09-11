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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.prestosql.plugin.hive.HiveCompressionCodec.GZIP;
import static io.prestosql.plugin.iceberg.IcebergFileFormat.ORC;
import static io.prestosql.plugin.iceberg.IcebergFileFormat.PARQUET;

public class TestIcebergConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergConfig.class)
                .setMetastoreTransactionCacheSize(1000)
                .setFileFormat(ORC)
                .setCompressionCodec(GZIP));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("iceberg.metastore.transaction-cache.size", "999")
                .put("iceberg.file-format", "Parquet")
                .put("iceberg.compression-codec", "NONE")
                .build();

        IcebergConfig expected = new IcebergConfig()
                .setMetastoreTransactionCacheSize(999)
                .setFileFormat(PARQUET)
                .setCompressionCodec(HiveCompressionCodec.NONE);

        assertFullMapping(properties, expected);
    }
}
