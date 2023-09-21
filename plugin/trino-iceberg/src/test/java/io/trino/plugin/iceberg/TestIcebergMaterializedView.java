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

import io.trino.Session;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private Session secondIceberg;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = createIcebergQueryRunner();
        try {
            queryRunner.createCatalog("iceberg2", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg2-catalog").toString(),
                    "iceberg.hive-catalog-name", "hive"));

            secondIceberg = Session.builder(queryRunner.getDefaultSession())
                    .setCatalog("iceberg2")
                    .build();

            queryRunner.execute(secondIceberg, "CREATE SCHEMA " + secondIceberg.getSchema().orElseThrow());
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Override
    protected String getSchemaDirectory()
    {
        return getDistributedQueryRunner().getCoordinator().getBaseDataDir().resolve("iceberg_data/tpch").toString();
    }

    @Test
    public void testTwoIcebergCatalogs()
    {
        Session defaultIceberg = getSession();

        // Base table for staleness check
        String createTable = "CREATE TABLE common_base_table AS SELECT 10 value";
        assertUpdate(secondIceberg, createTable, 1); // this one will be used by MV
        assertUpdate(defaultIceberg, createTable, 1); // this one exists so that it can be mistakenly treated as the base table

        assertUpdate(defaultIceberg, """
                            CREATE MATERIALIZED VIEW iceberg.tpch.mv_on_iceberg2
                            AS SELECT sum(value) AS s FROM iceberg2.tpch.common_base_table
                """);

        // The MV is initially stale
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"common_base_table\"");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"st_")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After INSERT to the base table, the MV is still fresh, because it currently does not detect changes to tables in other catalog.
        assertUpdate(secondIceberg, "INSERT INTO common_base_table VALUES 7", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"st_")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh again
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"st_")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '17'");

        assertUpdate(secondIceberg, "DROP TABLE common_base_table");
        assertUpdate(defaultIceberg, "DROP TABLE common_base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_on_iceberg2");
    }
}
