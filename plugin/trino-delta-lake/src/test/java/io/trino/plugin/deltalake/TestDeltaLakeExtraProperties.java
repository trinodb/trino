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
package io.trino.plugin.deltalake;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeltaLakeExtraProperties
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.allowed-extra-properties", "custom.property.one,custom.property.two,custom.property.three,custom.CaseSensitive,delta.appendOnly,delta.checkpoint.writeStatsAsJson,delta.enableChangeDataFeed,DELTA.ENABLETYPEWIDENING,DELTA.FEATURE.timestampNtz,DELTA.ROWTRACKINGSUSPENDED")
                .build();
        return queryRunner;
    }

    @Test
    void testExtraProperties()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_extra_properties_",
                "(value integer) WITH (extra_properties = MAP(ARRAY['custom.property.one', 'custom.CaseSensitive', 'delta.checkpoint.writeStatsAsJson'], ARRAY['one', 'case-sensitive', 'false']))")) {
            assertThat(getTableProperties(table.getName()))
                    .containsEntry("custom.property.one", "one")
                    .containsEntry("custom.CaseSensitive", "case-sensitive")
                    .containsEntry("delta.checkpoint.writeStatsAsJson", "false");

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES extra_properties = MAP(ARRAY['custom.property.one'], ARRAY['updated'])");
            assertThat(getTableProperties(table.getName()))
                    .containsEntry("custom.property.one", "updated")
                    .containsEntry("custom.CaseSensitive", "case-sensitive");

            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .doesNotContain("extra_properties =", "custom.property.one", "custom.CaseSensitive");
        }
    }

    @Test
    void testReplaceTableExtraProperties()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_replace_extra_properties_",
                "(value integer) WITH (extra_properties = MAP(ARRAY['custom.property.one', 'custom.property.two'], ARRAY['one', 'two']))")) {
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " (value integer) WITH (extra_properties = MAP(ARRAY['custom.property.three'], ARRAY['three']))");
            assertThat(getTableProperties(table.getName()))
                    .containsEntry("custom.property.three", "three")
                    .doesNotContainKeys("custom.property.one", "custom.property.two");

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " WITH (extra_properties = MAP(ARRAY['custom.property.one'], ARRAY['ctas'])) AS SELECT 1 value", 1);
            assertThat(getTableProperties(table.getName()))
                    .containsEntry("custom.property.one", "ctas")
                    .doesNotContainKeys("custom.property.two", "custom.property.three");
        }
    }

    @Test
    void testCreateTableAsSelectWithExtraProperties()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_ctas_extra_properties_",
                "WITH (extra_properties = MAP(ARRAY['custom.property.one', 'delta.checkpoint.writeStatsAsJson'], ARRAY['one', 'false'])) AS SELECT 1 value")) {
            assertThat(getTableProperties(table.getName()))
                    .containsEntry("custom.property.one", "one")
                    .containsEntry("delta.checkpoint.writeStatsAsJson", "false");
        }
    }

    @Test
    void testNullExtraProperty()
    {
        assertQueryFails(
                "CREATE TABLE test_null_extra_property (value integer) WITH (extra_properties = MAP(ARRAY['custom.property.one'], ARRAY[null]))",
                ".*\\QUnable to set catalog 'delta' table property 'extra_properties' to [MAP(ARRAY['custom.property.one'], ARRAY[null])]: Extra table property value cannot be null '{custom.property.one=null}'\\E");
        assertQueryFails(
                "CREATE TABLE test_ctas_null_extra_property WITH (extra_properties = MAP(ARRAY['custom.property.one'], ARRAY[null])) AS SELECT 1 value",
                ".*\\QUnable to set catalog 'delta' table property 'extra_properties' to [MAP(ARRAY['custom.property.one'], ARRAY[null])]: Extra table property value cannot be null '{custom.property.one=null}'\\E");
    }

    @Test
    void testIllegalExtraPropertyKey()
    {
        assertQueryFails(
                "CREATE TABLE test_protected_extra_property (value integer) WITH (extra_properties = MAP(ARRAY['delta.enableChangeDataFeed'], ARRAY['true']))",
                "\\QIllegal keys in extra_properties: [delta.enableChangeDataFeed]");
        assertQueryFails(
                "CREATE TABLE test_feature_extra_property (value integer) WITH (extra_properties = MAP(ARRAY['delta.appendOnly'], ARRAY['true']))",
                "\\QIllegal keys in extra_properties: [delta.appendOnly]");
        assertQueryFails(
                "CREATE TABLE test_synthetic_extra_property (value integer) WITH (extra_properties = MAP(ARRAY['DELTA.FEATURE.timestampNtz'], ARRAY['supported']))",
                "\\QIllegal keys in extra_properties: [DELTA.FEATURE.timestampNtz]");
        assertQueryFails(
                "CREATE TABLE test_feature_enablement_extra_property (value integer) WITH (extra_properties = MAP(ARRAY['DELTA.ENABLETYPEWIDENING'], ARRAY['true']))",
                "\\QIllegal keys in extra_properties: [DELTA.ENABLETYPEWIDENING]");
        assertQueryFails(
                "CREATE TABLE test_row_tracking_extra_property (value integer) WITH (extra_properties = MAP(ARRAY['DELTA.ROWTRACKINGSUSPENDED'], ARRAY['true']))",
                "\\QIllegal keys in extra_properties: [DELTA.ROWTRACKINGSUSPENDED]");
        assertQueryFails(
                "CREATE TABLE test_nested_extra_property WITH (extra_properties = MAP(ARRAY['extra_properties'], ARRAY['value'])) AS SELECT 1 value",
                "\\QIllegal keys in extra_properties: [extra_properties]");
        assertQueryFails(
                "CREATE TABLE test_comment_extra_property WITH (extra_properties = MAP(ARRAY['comment'], ARRAY['value'])) AS SELECT 1 value",
                "\\QIllegal keys in extra_properties: [comment]");
        assertQueryFails(
                "CREATE TABLE test_not_allowed_extra_property WITH (extra_properties = MAP(ARRAY['not.allowed'], ARRAY['value'])) AS SELECT 1 value",
                "\\QIllegal keys in extra_properties: [not.allowed]");
        assertQueryFails(
                "CREATE TABLE test_case_sensitive_extra_property WITH (extra_properties = MAP(ARRAY['custom.casesensitive'], ARRAY['value'])) AS SELECT 1 value",
                "\\QIllegal keys in extra_properties: [custom.casesensitive]");
    }

    @Test
    void testSetIllegalExtraPropertyKey()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_illegal_extra_property_", "(value integer)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES extra_properties = MAP(ARRAY['delta.enableChangeDataFeed'], ARRAY['true'])",
                    "\\QIllegal keys in extra_properties: [delta.enableChangeDataFeed]");
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES extra_properties = MAP(ARRAY['not.allowed'], ARRAY['value'])",
                    "\\QIllegal keys in extra_properties: [not.allowed]");
        }
    }

    @Test
    void testSetExtraPropertiesWithChangeDataFeed()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_extra_property_with_cdf_", "(value integer)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES change_data_feed_enabled = true, extra_properties = MAP(ARRAY['custom.property.one'], ARRAY['value'])");

            assertThat(getTableProperties(table.getName()))
                    .containsEntry("custom.property.one", "value")
                    .containsEntry("delta.enableChangeDataFeed", "true")
                    .containsEntry("delta.minWriterVersion", "4");
        }
    }

    private Map<String, String> getTableProperties(String tableName)
    {
        return computeActual("SELECT key, value FROM \"" + tableName + "$properties\"").getMaterializedRows().stream()
                .collect(toImmutableMap(row -> (String) row.getField(0), row -> (String) row.getField(1)));
    }
}
