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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseDeltaLakeCompatibility
        extends AbstractTestQueryFramework
{
    protected final URI resourcePath;

    public BaseDeltaLakeCompatibility(String resourcePath)
            throws Exception
    {
        this.resourcePath = Resources.getResource(requireNonNull(resourcePath)).toURI();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = createDeltaLakeQueryRunner(
                DELTA_CATALOG,
                ImmutableMap.of(),
                ImmutableMap.of(
                        "delta.enable-non-concurrent-writes", "true",
                        "delta.register-table-procedure.enabled", "true"));
        TpchTable.getTables().forEach(table -> {
            String tableName = table.getTableName();
            queryRunner.execute(format("CALL system.register_table('%s', '%s', '%s')",
                    TPCH_SCHEMA,
                    tableName,
                    resourcePath.resolve(tableName)));
        });
        return queryRunner;
    }

    @Test(dataProvider = "tpchTablesDataProvider")
    public void testSelectAll(String tableName)
    {
        assertThat(query("SELECT * FROM " + tableName))
                .skippingTypesCheck() // Delta Lake connector returns varchar, but TPCH connector returns varchar(n)
                .matches("SELECT * FROM tpch.tiny." + tableName);
    }

    @DataProvider
    public static Object[][] tpchTablesDataProvider()
    {
        return TpchTable.getTables().stream()
                .map(table -> new Object[] {table.getTableName()})
                .toArray(Object[][]::new);
    }
}
