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
package io.trino.plugin.iceberg;

import org.testng.annotations.Test;

import static io.trino.plugin.iceberg.IcebergUtil.parseVersion;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestIcebergUtil
{
    @Test
    public void testParseVersion()
    {
        assertEquals(parseVersion("00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), 0);
        assertEquals(parseVersion("99999-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), 99999);
        assertEquals(parseVersion("00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), 10);
        assertEquals(parseVersion("00011-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), 11);
        assertEquals(parseVersion("v0.metadata.json"), 0);
        assertEquals(parseVersion("v10.metadata.json"), 10);
        assertEquals(parseVersion("v99999.metadata.json"), 99999);
        assertEquals(parseVersion("v0.gz.metadata.json"), 0);
        assertEquals(parseVersion("v0.metadata.json.gz"), 0);

        assertThatThrownBy(() -> parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44/metadata/00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Not a file name: .*");
        assertThatThrownBy(() -> parseVersion("orders_5_581fad8517934af6be1857a903559d44"))
                .hasMessageMatching("Invalid metadata file name: .*");
        assertThatThrownBy(() -> parseVersion("metadata"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("00010_409702ba_4735_4645_8f14_09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v10_metadata_json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v1..gz.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v1.metadata.json.gz."))
                .hasMessageMatching("Invalid metadata file name:.*");

        assertThatThrownBy(() -> parseVersion("00003_409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("-00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v-10.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
    }
}
