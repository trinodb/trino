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
package io.trino.plugin.iceberg.catalog.rest;

import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRestCatalogInitFailure
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .disableSchemaInitializer()
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", "http://127.0.0.1:1")
                .build();
    }

    @Test
    void testProcedure()
    {
        assertThat(query("CALL iceberg.system.rollback_to_snapshot('tpch', 'nation', 1)"))
                .failure().hasErrorCode(ICEBERG_CATALOG_ERROR)
                .hasMessageContaining("Failed to initialize Iceberg REST catalog");
    }

    @Test
    void testTableFunction()
    {
        assertThat(query("SELECT * FROM TABLE(iceberg.system.table_changes('tpch', 'nation', 0, 1))"))
                .failure().hasErrorCode(ICEBERG_CATALOG_ERROR)
                .hasMessageContaining("Failed to initialize Iceberg REST catalog");
    }

    @Test
    void testSelect()
    {
        assertThat(query("SELECT * FROM iceberg.tpch.nation"))
                .failure().hasErrorCode(ICEBERG_CATALOG_ERROR)
                .hasMessageContaining("Failed to initialize Iceberg REST catalog");
    }
}
