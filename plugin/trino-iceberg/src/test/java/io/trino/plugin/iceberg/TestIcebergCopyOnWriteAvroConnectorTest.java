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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.TestIcebergCopyOnWriteParquetConnectorTest.enableCopyOnWrite;

/**
 * Runs the standard {@link BaseIcebergConnectorTest} suite with
 * {@code write.merge.mode=copy-on-write} set on the pre-loaded TPCH tables
 * (format version 2, Avro). This is a compatibility guard: it verifies that
 * enabling copy-on-write table properties does not break reads, statistics,
 * metadata, or schema operations. It does not by itself add row-level DML
 * copy-on-write coverage, because the inherited DML tests create their own
 * tables that default to merge-on-read. Dedicated copy-on-write DML coverage
 * lives in {@link TestIcebergCopyOnWrite}.
 */
final class TestIcebergCopyOnWriteAvroConnectorTest
        extends BaseIcebergAvroConnectorTest
{
    TestIcebergCopyOnWriteAvroConnectorTest()
    {
        super(2);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = createQueryRunnerBuilder().build();
        enableCopyOnWrite(queryRunner);
        return queryRunner;
    }

    @Override
    protected IcebergQueryRunner.Builder createQueryRunnerBuilder()
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.format-version", String.valueOf(formatVersion))
                        .put("iceberg.allowed-extra-properties", "*")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .buildOrThrow())
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES);
    }

    @Test
    @Override
    public void testIllegalExtraPropertyKey()
    {
        // With allowed-extra-properties=*, arbitrary keys are accepted.
        // Only Trino-managed keys (sorted_by, write.format.default, comment, extra_properties) are always blocked.
        assertQueryFails(
                "CREATE TABLE test_create_table_with_illegal_extra_properties (c1 integer) WITH (extra_properties = MAP(ARRAY['sorted_by'], ARRAY['id']))",
                "\\QIllegal keys in extra_properties: [sorted_by]");
        assertQueryFails(
                "CREATE TABLE test_create_table_as_with_illegal_extra_properties WITH (extra_properties = MAP(ARRAY['extra_properties'], ARRAY['some_value'])) AS SELECT 1 as c1",
                "\\QIllegal keys in extra_properties: [extra_properties]");
        assertQueryFails(
                "CREATE TABLE test_create_table_with_as_illegal_extra_properties WITH (extra_properties = MAP(ARRAY['write.format.default'], ARRAY['ORC'])) AS SELECT 1 as c1",
                "\\QIllegal keys in extra_properties: [write.format.default]");
        assertQueryFails(
                "CREATE TABLE test_create_table_with_as_illegal_extra_properties WITH (extra_properties = MAP(ARRAY['comment'], ARRAY['some comment'])) AS SELECT 1 as c1",
                "\\QIllegal keys in extra_properties: [comment]");
    }

    @Test
    @Override
    public void testSetIllegalExtraPropertyKey()
    {
        // With allowed-extra-properties=*, arbitrary keys are accepted.
        // Only Trino-managed keys are always blocked.
        try (TestTable table = newTrinoTable("test_set_illegal_table_properties", "(x int)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES extra_properties = MAP(ARRAY['sorted_by'], ARRAY['id'])",
                    "\\QIllegal keys in extra_properties: [sorted_by]");
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES extra_properties = MAP(ARRAY['comment'], ARRAY['some comment'])",
                    "\\QIllegal keys in extra_properties: [comment]");
        }
    }
}
