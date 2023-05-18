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
package io.trino.metadata;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQualifiedTablePrefix
{
    private static final JsonCodec<QualifiedTablePrefix> CODEC = jsonCodec(QualifiedTablePrefix.class);

    @Test
    public void testCatalog()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog");
        assertThat("catalog").isEqualTo(tableName.getCatalogName());

        assertThat(tableName.hasSchemaName()).isFalse();
        assertThat(tableName.hasTableName()).isFalse();
    }

    @Test
    public void testSchema()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog", "schema");

        assertThat("catalog").isEqualTo(tableName.getCatalogName());
        assertThat(tableName.hasSchemaName()).isTrue();

        assertThat("schema").isEqualTo(tableName.getSchemaName().get());
        assertThat(tableName.hasTableName()).isFalse();
    }

    @Test
    public void testTable()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog", "schema", "table");
        assertThat("catalog").isEqualTo(tableName.getCatalogName());

        assertThat(tableName.hasSchemaName()).isTrue();
        assertThat("schema").isEqualTo(tableName.getSchemaName().get());

        assertThat(tableName.hasTableName()).isTrue();
        assertThat("table").isEqualTo(tableName.getTableName().get());
    }

    @Test
    public void testNullSchema()
    {
        assertThatThrownBy(() -> new QualifiedTablePrefix("catalog", null, "table"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("schemaName is null");
    }

    @Test
    public void testRoundTrip()
    {
        QualifiedTablePrefix table = new QualifiedTablePrefix("abc", "xyz", "fgh");
        assertThat(CODEC.fromJson(CODEC.toJson(table))).isEqualTo(table);
    }
}
