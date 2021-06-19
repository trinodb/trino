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
package io.trino.plugin.hive.metastore.file;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig.VersionCompatibility.NOT_SUPPORTED;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig.VersionCompatibility.UNSAFE_ASSUME_COMPATIBILITY;

public class TestFileHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileHiveMetastoreConfig.class)
                .setCatalogDirectory(null)
                .setVersionCompatibility(NOT_SUPPORTED)
                .setMetastoreUser("presto"));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.catalog.dir", "some path")
                .put("hive.metastore.version-compatibility", "UNSAFE_ASSUME_COMPATIBILITY")
                .put("hive.metastore.user", "some user")
                .build();

        FileHiveMetastoreConfig expected = new FileHiveMetastoreConfig()
                .setCatalogDirectory("some path")
                .setVersionCompatibility(UNSAFE_ASSUME_COMPATIBILITY)
                .setMetastoreUser("some user");

        assertFullMapping(properties, expected);
    }
}
