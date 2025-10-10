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
package io.trino.plugin.lakehouse;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.lakehouse.TableType.ICEBERG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLakehouseIcebergConnectorSmokeTest
        extends BaseLakehouseConnectorSmokeTest
{
    protected TestLakehouseIcebergConnectorSmokeTest()
    {
        super(ICEBERG);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.region (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   format = 'PARQUET',
                   format_version = 2,
                   location = \\E's3://test-bucket-.*/tpch/region-.*'\\Q,
                   type = 'ICEBERG'
                )\\E""");
    }

    @Test
    void testProcedures()
    {
        String tableName = "table_for_procedures_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s AS SELECT 2 AS age", tableName), 1);

        assertThat(query(format("CALL lakehouse.system.rollback_to_snapshot(CURRENT_SCHEMA, '%s', 1234)", tableName)))
                .failure().hasMessage("Cannot roll back to unknown snapshot id: 1234");

        assertThat(query(format("CALL lakehouse.system.register_table('ICEBERG', CURRENT_SCHEMA, '%s', 's3://bucket/table')", tableName)))
                .failure().hasMessage("Failed checking table location: s3://bucket/table");

        assertThat(query(format("CALL lakehouse.system.unregister_table('ICEBERG', CURRENT_SCHEMA, '%s')", tableName)))
                .succeeds().returnsEmptyResult();
    }
}
