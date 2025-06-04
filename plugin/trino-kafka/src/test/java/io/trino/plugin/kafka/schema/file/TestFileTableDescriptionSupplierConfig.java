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
package io.trino.plugin.kafka.schema.file;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestFileTableDescriptionSupplierConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileTableDescriptionSupplierConfig.class)
                .setTableDescriptionDir(new File("etc/kafka"))
                .setTableNames(ImmutableSet.of()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kafka.table-description-dir", "/var/lib/kafka")
                .put("kafka.table-names", "table1, table2, table3")
                .buildOrThrow();

        FileTableDescriptionSupplierConfig expected = new FileTableDescriptionSupplierConfig()
                .setTableDescriptionDir(new File("/var/lib/kafka"))
                .setTableNames(ImmutableSet.of("table1", "table2", "table3"));
        assertFullMapping(properties, expected);
    }
}
