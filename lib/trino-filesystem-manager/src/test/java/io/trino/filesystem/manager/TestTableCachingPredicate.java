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
package io.trino.filesystem.manager;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTableCachingPredicate
{
    @Test
    public void testWildcardMatchesAll()
    {
        TableCachingPredicate predicate = new TableCachingPredicate(ImmutableList.of("*"));

        assertThat(predicate.test(new SchemaTableName("schema1", "table1"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("schema2", "table2"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("any_schema", "any_table"))).isTrue();
    }

    @Test
    public void testSchemaWildcard()
    {
        TableCachingPredicate predicate = new TableCachingPredicate(ImmutableList.of("schema1.*"));

        assertThat(predicate.test(new SchemaTableName("schema1", "table1"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("schema1", "table2"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("schema1", "any_table"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("schema2", "table1"))).isFalse();
        assertThat(predicate.test(new SchemaTableName("other", "table1"))).isFalse();
    }

    @Test
    public void testExactTableMatch()
    {
        TableCachingPredicate predicate = new TableCachingPredicate(ImmutableList.of("schema1.table1"));

        assertThat(predicate.test(new SchemaTableName("schema1", "table1"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("schema1", "table2"))).isFalse();
        assertThat(predicate.test(new SchemaTableName("schema2", "table1"))).isFalse();
    }

    @Test
    public void testMultiplePatterns()
    {
        TableCachingPredicate predicate = new TableCachingPredicate(ImmutableList.of("schema1.table1", "schema2.*"));

        assertThat(predicate.test(new SchemaTableName("schema1", "table1"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("schema1", "table2"))).isFalse();
        assertThat(predicate.test(new SchemaTableName("schema2", "any_table"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("schema3", "table1"))).isFalse();
    }

    @Test
    public void testEmptyListMatchesNone()
    {
        TableCachingPredicate predicate = new TableCachingPredicate(ImmutableList.of());

        assertThat(predicate.test(new SchemaTableName("schema1", "table1"))).isFalse();
        assertThat(predicate.test(new SchemaTableName("any", "table"))).isFalse();
    }

    @Test
    public void testInvalidPatternWithoutSchema()
    {
        assertThatThrownBy(() -> new TableCachingPredicate(ImmutableList.of("table_only")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid schemaTableName");
    }

    @Test
    public void testInvalidPatternTooManyParts()
    {
        assertThatThrownBy(() -> new TableCachingPredicate(ImmutableList.of("catalog.schema.table")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid schemaTableName");
    }

    @Test
    public void testMixedPatternsWithGlobalWildcard()
    {
        TableCachingPredicate predicate = new TableCachingPredicate(ImmutableList.of("schema1.table1", "*"));

        assertThat(predicate.test(new SchemaTableName("schema1", "table1"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("any_schema", "any_table"))).isTrue();
    }

    @Test
    public void testFromConfig()
    {
        FileSystemConfig config = new FileSystemConfig()
                .setCacheIncludeTables(ImmutableList.of("myschema.*"));

        TableCachingPredicate predicate = new TableCachingPredicate(config);

        assertThat(predicate.test(new SchemaTableName("myschema", "table1"))).isTrue();
        assertThat(predicate.test(new SchemaTableName("other", "table1"))).isFalse();
    }

    @Test
    public void testDefaultConfigMatchesAll()
    {
        FileSystemConfig config = new FileSystemConfig();

        TableCachingPredicate predicate = new TableCachingPredicate(config);

        assertThat(predicate.test(new SchemaTableName("any_schema", "any_table"))).isTrue();
    }
}
