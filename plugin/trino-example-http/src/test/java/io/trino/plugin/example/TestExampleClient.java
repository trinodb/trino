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
package io.trino.plugin.example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URL;

import static io.trino.plugin.example.MetadataUtil.CATALOG_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExampleClient
{
    @Test
    public void testMetadata()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestExampleClient.class, "/example-data/example-metadata.json");
        assertThat(metadataUrl)
                .describedAs("metadataUrl is null")
                .isNotNull();
        URI metadata = metadataUrl.toURI();
        ExampleClient client = new ExampleClient(new ExampleConfig().setMetadata(metadata), CATALOG_CODEC);
        assertThat(client.getSchemaNames()).isEqualTo(ImmutableSet.of("example", "tpch"));
        assertThat(client.getTableNames("example")).isEqualTo(ImmutableSet.of("numbers"));
        assertThat(client.getTableNames("tpch")).isEqualTo(ImmutableSet.of("orders", "lineitem"));

        ExampleTable table = client.getTable("example", "numbers");
        assertThat(table)
                .describedAs("table is null")
                .isNotNull();
        assertThat(table.getName()).isEqualTo("numbers");
        assertThat(table.getColumns()).isEqualTo(ImmutableList.of(new ExampleColumn("text", createUnboundedVarcharType()), new ExampleColumn("value", BIGINT)));
        assertThat(table.getSources()).isEqualTo(ImmutableList.of(metadata.resolve("numbers-1.csv"), metadata.resolve("numbers-2.csv")));
    }
}
