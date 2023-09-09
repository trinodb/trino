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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kafka.schema.confluent.KafkaWithConfluentSchemaRegistryQueryRunner;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.kafka.TestingKafka;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.PART_SUPPLIER;
import static io.trino.tpch.TpchTable.REGION;
import static io.trino.tpch.TpchTable.SUPPLIER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaWithConfluentLatestConnectorTest
        extends BaseConnectorTest
{
    private TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        return KafkaWithConfluentSchemaRegistryQueryRunner.builder(testingKafka)
                .setTables(ImmutableList.of(ORDERS, LINE_ITEM, NATION, REGION, PART, PART_SUPPLIER, SUPPLIER, CUSTOMER))
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.confluent-subjects-cache-refresh-interval", "1ms")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                    SUPPORTS_AGGREGATION_PUSHDOWN,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_CREATE_TABLE,
                    SUPPORTS_CREATE_TABLE_WITH_DATA,
                    SUPPORTS_DELETE,
                    SUPPORTS_TRUNCATE,
                    SUPPORTS_INSERT,
                    SUPPORTS_UPDATE,
                    SUPPORTS_RENAME_COLUMN,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_MERGE,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW,
                    SUPPORTS_COMMENT_ON_VIEW,
                    SUPPORTS_CREATE_VIEW -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .isEqualTo(format("""
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
                                )""",
                        catalog, schema));
    }

    @Override
    public void testInsert()
    {
        // Cannot call the super method as it will insert a row and cause other tests to fail.
        throw new SkipException("Insert tests do not work without create table");
    }

    @Override
    public void testInsertNegativeDate()
    {
        // Cannot call the super method as it will insert a row and cause other tests to fail.
        throw new SkipException("Insert tests do not work without create table");
    }
}
