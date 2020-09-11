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
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.Session;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.EstimatedStatsAndCost;
import io.prestosql.testing.AbstractTestQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import io.prestosql.type.TypeDeserializer;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.predicate.Marker.Bound.EXACTLY;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestTpchDistributedQueries
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testIoExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + query);
        EstimatedStatsAndCost scanEstimate = new EstimatedStatsAndCost(15000.0, 1597294.0, 1597294.0, 0.0, 0.0);
        EstimatedStatsAndCost totalEstimate = new EstimatedStatsAndCost(15000.0, 1597294.0, 1597294.0, 0.0, 1597294.0);
        IoPlanPrinter.IoPlan.TableColumnInfo input = new IoPlanPrinter.IoPlan.TableColumnInfo(
                new CatalogSchemaTableName("tpch", "sf0.01", "orders"),
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
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(getQueryRunner().getMetadata())));
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
        assertQueryFails("ANALYZE orders WITH (foo = 'bar')", ".* does not support analyze property 'foo'.*");
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

    private Session createSession(String schemaName)
    {
        return testSessionBuilder()
                .setSource("test")
                .setCatalog("tpch")
                .setSchema(schemaName)
                .build();
    }
}
