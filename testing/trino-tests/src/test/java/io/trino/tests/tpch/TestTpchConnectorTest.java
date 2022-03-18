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
package io.trino.tests.tpch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.Session;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.type.Type;
import io.trino.sql.planner.planprinter.IoPlanPrinter;
import io.trino.sql.planner.planprinter.IoPlanPrinter.EstimatedStatsAndCost;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.type.TypeDeserializer;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedMarker.Bound.EXACTLY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestTpchConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
                return false;

            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_INSERT:
                return false;

            case SUPPORTS_DELETE:
                return false;

            case SUPPORTS_ARRAY:
                return false;

            case SUPPORTS_ROW_TYPE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testIoExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + query);
        EstimatedStatsAndCost scanEstimate = new EstimatedStatsAndCost(15000.0, 1597294.0, 1597294.0, 0.0, 0.0);
        EstimatedStatsAndCost totalEstimate = new EstimatedStatsAndCost(15000.0, 1597294.0, 1597294.0, 0.0, 1597294.0);
        IoPlanPrinter.IoPlan.TableColumnInfo input = new IoPlanPrinter.IoPlan.TableColumnInfo(
                new CatalogSchemaTableName("tpch", "tiny", "orders"),
                ImmutableSet.of(
                        new IoPlanPrinter.ColumnConstraint(
                                "orderstatus",
                                createVarcharType(1),
                                new IoPlanPrinter.FormattedDomain(
                                        false,
                                        ImmutableSet.of(
                                                new IoPlanPrinter.FormattedRange(
                                                        new IoPlanPrinter.FormattedMarker(Optional.of("F"), EXACTLY),
                                                        new IoPlanPrinter.FormattedMarker(Optional.of("F"), EXACTLY)),
                                                new IoPlanPrinter.FormattedRange(
                                                        new IoPlanPrinter.FormattedMarker(Optional.of("O"), EXACTLY),
                                                        new IoPlanPrinter.FormattedMarker(Optional.of("O"), EXACTLY)),
                                                new IoPlanPrinter.FormattedRange(
                                                        new IoPlanPrinter.FormattedMarker(Optional.of("P"), EXACTLY),
                                                        new IoPlanPrinter.FormattedMarker(Optional.of("P"), EXACTLY)))))),
                scanEstimate);

        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(getQueryRunner().getTypeManager())));
        JsonCodec<IoPlanPrinter.IoPlan> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(IoPlanPrinter.IoPlan.class);

        assertEquals(
                codec.fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IoPlanPrinter.IoPlan(ImmutableSet.of(input), Optional.empty(), totalEstimate));
    }

    @Test
    public void testAnalyzePropertiesSystemTable()
    {
        assertQuery("SELECT COUNT(*) FROM system.metadata.analyze_properties WHERE catalog_name = 'tpch'", "SELECT 0");
    }

    @Test
    public void testAnalyze()
    {
        assertUpdate("ANALYZE orders", 15000);
        assertQueryFails("ANALYZE orders WITH (foo = 'bar')", ".* analyze property 'foo' does not exist");
    }

    @Test
    public void testPreSortedInput()
    {
        // TPCH connector produces pre-sorted data for orders and lineitem tables
        assertExplain(
                "EXPLAIN SELECT * FROM orders ORDER BY orderkey ASC NULLS FIRST LIMIT 10",
                "\\QLimitPartial[10, input pre-sorted by (orderkey)]");
        assertExplain(
                "EXPLAIN SELECT * FROM lineitem ORDER BY orderkey ASC NULLS FIRST LIMIT 10",
                "\\QLimitPartial[10, input pre-sorted by (orderkey)]");
        assertExplain(
                "EXPLAIN SELECT * FROM lineitem ORDER BY orderkey ASC NULLS FIRST, linenumber ASC NULLS FIRST LIMIT 10",
                "\\QLimitPartial[10, input pre-sorted by (orderkey, linenumber)]");
        assertExplain(
                "EXPLAIN SELECT * FROM lineitem ORDER BY orderkey ASC NULLS FIRST, linenumber LIMIT 10",
                "\\QTopNPartial[10 by (orderkey ASC NULLS FIRST, linenumber ASC NULLS LAST)]");
        assertExplain(
                "EXPLAIN SELECT * FROM lineitem ORDER BY orderkey ASC LIMIT 10",
                "\\QTopNPartial[10 by (orderkey ASC NULLS LAST)]");

        assertQuery(
                "SELECT * FROM lineitem WHERE orderkey IS NOT NULL ORDER BY orderkey ASC NULLS FIRST LIMIT 10",
                "SELECT * FROM lineitem ORDER BY orderkey ASC LIMIT 10");
    }

    @Test
    @Override
    public void testShowTables()
    {
        assertQuerySucceeds(createSession("sf1"), "SHOW TABLES");
        assertQuerySucceeds(createSession("sf1.0"), "SHOW TABLES");
        assertQuerySucceeds("SHOW TABLES FROM sf1");
        assertQuerySucceeds("SHOW TABLES FROM \"sf1.0\"");
        assertQueryFails("SHOW TABLES FROM sf0", "line 1:1: Schema 'sf0' does not exist");
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE tpch.tiny.orders (\n" +
                        "   orderkey bigint NOT NULL,\n" +
                        "   custkey bigint NOT NULL,\n" +
                        "   orderstatus varchar(1) NOT NULL,\n" +
                        "   totalprice double NOT NULL,\n" +
                        "   orderdate date NOT NULL,\n" +
                        "   orderpriority varchar(15) NOT NULL,\n" +
                        "   clerk varchar(15) NOT NULL,\n" +
                        "   shippriority integer NOT NULL,\n" +
                        "   comment varchar(79) NOT NULL\n" +
                        ")");
    }

    @Override
    public void testPredicateReflectedInExplain()
    {
        // this connector specific test is supplementary to the generic one
        super.testPredicateReflectedInExplain();

        // TPCH connector supports predicate pushdown for e.g. orderstatus
        assertExplain(
                "EXPLAIN SELECT orderkey FROM orders WHERE orderstatus = 'F'",
                "\\Q:: [[F]]");
    }

    private Session createSession(String schemaName)
    {
        return testSessionBuilder()
                .setSource("test")
                .setCatalog("tpch")
                .setSchema(schemaName)
                .build();
    }
}
