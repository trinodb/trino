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
package io.prestosql.plugin.hive.metastore.file;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestFileHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileHiveMetastoreConfig.class)
                .setCatalogDirectory(null)
                .setMetastoreUser("presto")
                .setAssumeCanonicalPartitionKeys(false));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.catalog.dir", "some path")
                .put("hive.metastore.user", "some user")
                .put("hive.metastore.assume-canonical-partition-keys", "true")
                .build();

        FileHiveMetastoreConfig expected = new FileHiveMetastoreConfig()
                .setCatalogDirectory("some path")
                .setMetastoreUser("some user")
                .setAssumeCanonicalPartitionKeys(true);

        assertFullMapping(properties, expected);
    }
}
