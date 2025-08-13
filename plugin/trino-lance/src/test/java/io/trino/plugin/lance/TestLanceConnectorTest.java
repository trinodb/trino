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
package io.trino.plugin.lance;

import io.trino.Session;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.plugin.lance.LanceQueryRunner.LANCE_CATALOG;
import static io.trino.plugin.lance.catalog.namespace.DirectoryNamespace.DEFAULT_NAMESPACE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

final class TestLanceConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ADD_FIELD,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_OR_REPLACE_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_DROP_FIELD,
                 SUPPORTS_DYNAMIC_FILTER_PUSHDOWN,
                 SUPPORTS_INSERT,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path catalogDir = Files.createTempDirectory("lance-catalog");
        return LanceQueryRunner.builder(catalogDir.toString())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        // Lance only supports variable width VARCHAR
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertThat(actualColumns).isEqualTo(expectedColumns);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        // Lance only supports variable width VARCHAR
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                .isEqualTo(format(
                        """
                        CREATE TABLE %s.%s.orders (
                           orderkey bigint,
                           custkey bigint,
                           orderstatus varchar,
                           totalprice double,
                           orderdate date,
                           orderpriority varchar,
                           clerk varchar,
                           shippriority integer,
                           comment varchar
                        )\
                        """,
                        catalog,
                        schema));
    }

    @Override
    protected Session getSession() {
        // Lance only supports variable width VARCHAR
        return Session.builder(super.getSession())
                .setCatalog(LANCE_CATALOG)
                .setSchema(DEFAULT_NAMESPACE)
                .build();
    }
}
