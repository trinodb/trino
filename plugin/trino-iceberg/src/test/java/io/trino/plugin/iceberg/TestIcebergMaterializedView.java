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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.Page;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private Session secondIceberg;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .build();
        try {
            metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                    .getInstance(HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());

            queryRunner.createCatalog("iceberg2", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg2-catalog").toString(),
                    "iceberg.hive-catalog-name", "hive"));

            secondIceberg = Session.builder(queryRunner.getDefaultSession())
                    .setCatalog("iceberg2")
                    .build();

            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withTableFunctions(ImmutableSet.of(new SequenceTableFunction()))
                    .withFunctionProvider(Optional.of(new FunctionProvider()
                    {
                        @Override
                        public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
                        {
                            if (functionHandle instanceof SequenceTableFunctionHandle) {
                                return new SequenceTableFunctionProcessorProvider();
                            }
                            throw new IllegalArgumentException("This ConnectorTableFunctionHandle is not supported");
                        }
                    }))
                    .withTableFunctionSplitSources(functionHandle -> {
                        if (functionHandle instanceof SequenceTableFunctionHandle) {
                            return new FixedSplitSource(ImmutableList.of(new SequenceConnectorSplit()));
                        }
                        throw new IllegalArgumentException("This ConnectorTableFunctionHandle is not supported");
                    })
                    .build()));
            queryRunner.createCatalog("mock", "mock");

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
        return "local:///tpch";
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        Table table = metastore.getTable("tpch", materializedViewName).orElseThrow();
        return table.getParameters().get(METADATA_LOCATION_PROP);
    }

    @Test
    public void testMaterializedViewCreatedFromTableFunction()
    {
        String viewName = "materialized_view_for_ptf_" + randomNameSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM TABLE(mock.system.sequence_function())");

        assertFreshness(viewName, "STALE");
        assertThat(computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE name = '" + viewName + "'").getOnlyValue()).isNull();
        int result1 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();

        int result2 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result2).isNotEqualTo(result1); // differs because PTF sequence_function is called directly as mv is considered stale
        assertFreshness(viewName, "STALE");
        assertThat(computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE name = '" + viewName + "'").getOnlyValue()).isNull();

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        assertFreshness(viewName, "UNKNOWN");
        ZonedDateTime lastFreshTime = (ZonedDateTime) computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE name = '" + viewName + "'").getOnlyValue();
        assertThat(lastFreshTime).isNotNull();
        int result3 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result3).isNotEqualTo(result2);  // mv is not stale anymore so all selects until next refresh returns same result
        int result4 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        int result5 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result4).isEqualTo(result3);
        assertThat(result4).isEqualTo(result5);

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        assertThat((ZonedDateTime) computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE name = '" + viewName + "'").getOnlyValue()).isAfter(lastFreshTime);
        assertFreshness(viewName, "UNKNOWN");
        int result6 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result6).isNotEqualTo(result5);
    }

    @Test
    public void testMaterializedViewCreatedFromTableFunctionAndTable()
    {
        String sourceTableName = "source_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " (VALUE INTEGER)");
        assertUpdate("INSERT INTO " + sourceTableName + " VALUES 2", 1);
        String viewName = "materialized_view_for_ptf_adn_table_" + randomNameSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM TABLE(mock.system.sequence_function()) CROSS JOIN " + sourceTableName);

        List<MaterializedRow> materializedRows = computeActual("SELECT * FROM " + viewName).getMaterializedRows();
        assertThat(materializedRows.size()).isEqualTo(1);
        assertThat(materializedRows.get(0).getField(1)).isEqualTo(2);
        int valueFromPtf1 = (int) materializedRows.get(0).getField(0);
        assertFreshness(viewName, "STALE");
        assertThat(computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE name = '" + viewName + "'").getOnlyValue()).isNull();

        materializedRows = computeActual("SELECT * FROM " + viewName).getMaterializedRows();
        assertThat(materializedRows.size()).isEqualTo(1);
        assertThat(materializedRows.get(0).getField(1)).isEqualTo(2);
        int valueFromPtf2 = (int) materializedRows.get(0).getField(0);
        assertThat(valueFromPtf2).isNotEqualTo(valueFromPtf1); // differs because PTF sequence_function is called directly as mv is considered stale
        assertFreshness(viewName, "STALE");
        assertThat(computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE name = '" + viewName + "'").getOnlyValue()).isNull();

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        assertFreshness(viewName, "UNKNOWN");
        ZonedDateTime lastFreshTime = (ZonedDateTime) computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE name = '" + viewName + "'").getOnlyValue();
        assertThat(lastFreshTime).isNotNull();
        materializedRows = computeActual("SELECT * FROM " + viewName).getMaterializedRows();
        assertThat(materializedRows.size()).isEqualTo(1);
        assertThat(materializedRows.get(0).getField(1)).isEqualTo(2);
        int valueFromPtf3 = (int) materializedRows.get(0).getField(0);
        assertThat(valueFromPtf3).isNotEqualTo(valueFromPtf1);
        assertThat(valueFromPtf3).isNotEqualTo(valueFromPtf2);

        materializedRows = computeActual("SELECT * FROM " + viewName).getMaterializedRows();
        assertThat(materializedRows.size()).isEqualTo(1);
        assertThat(materializedRows.get(0).getField(1)).isEqualTo(2);
        int valueFromPtf4 = (int) materializedRows.get(0).getField(0);
        assertThat(valueFromPtf4).isNotEqualTo(valueFromPtf1);
        assertThat(valueFromPtf4).isNotEqualTo(valueFromPtf2);
        assertThat(valueFromPtf4).isEqualTo(valueFromPtf3); // mv is not stale anymore so all selects until next refresh returns same result
    }

    @Test
    public void testMaterializedViewCreatedFromTableFunctionWithGracePeriod()
            throws InterruptedException
    {
        String viewName = "materialized_view_for_ptf_with_grace_period_" + randomNameSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " GRACE PERIOD INTERVAL '1' SECOND AS SELECT * FROM TABLE(mock.system.sequence_function())");

        int result1 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        int result2 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result2).isNotEqualTo(result1);

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        int result3 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result3).isNotEqualTo(result2);
        Thread.sleep(1001);
        int result4 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result4).isNotEqualTo(result3);
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
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After INSERT to the base table, the MV is still fresh, because it currently does not detect changes to tables in other catalog.
        assertUpdate(secondIceberg, "INSERT INTO common_base_table VALUES 7", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh again
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '17'");

        assertUpdate(secondIceberg, "DROP TABLE common_base_table");
        assertUpdate(defaultIceberg, "DROP TABLE common_base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_on_iceberg2");
    }

    private void assertFreshness(String viewName, String expected)
    {
        assertThat((String) computeScalar("SELECT freshness FROM system.metadata.materialized_views WHERE name = '" + viewName + "'")).isEqualTo(expected);
    }

    public static class SequenceTableFunction
            extends AbstractConnectorTableFunction
    {
        public SequenceTableFunction()
        {
            super("system", "sequence_function", List.of(), GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new SequenceTableFunctionHandle())
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field("next_value", Optional.of(INTEGER)))))
                    .build();
        }
    }

    public static class SequenceTableFunctionHandle
            implements ConnectorTableFunctionHandle
    { }

    public  static class SequenceTableFunctionProcessorProvider
            implements TableFunctionProcessorProvider
    {
        private final SequenceFunctionProcessor sequenceFunctionProcessor = new SequenceFunctionProcessor();
        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split)
        {
            sequenceFunctionProcessor.reset();
            return sequenceFunctionProcessor;
        }
    }

    public static class SequenceFunctionProcessor
            implements TableFunctionSplitProcessor
    {
        private static final AtomicInteger generator = new AtomicInteger(10);
        private final AtomicBoolean finished = new AtomicBoolean(false);

        @Override
        public TableFunctionProcessorState process()
        {
            if (finished.get()) {
                return FINISHED;
            }
            BlockBuilder builder = INTEGER.createBlockBuilder(null, 1);
            INTEGER.writeInt(builder, generator.getAndIncrement());
            finished.set(true);
            return produced(new Page(builder.build()));
        }

        public void reset()
        {
            finished.set(false);
        }
    }

    public record SequenceConnectorSplit()
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = instanceSize(SequenceConnectorSplit.class);

        @JsonIgnore
        @Override
        public Object getInfo()
        {
            return ImmutableMap.builder()
                    .put("ignored", "ignored")
                    .buildOrThrow();
        }

        @JsonIgnore
        @Override
        public SplitWeight getSplitWeight()
        {
            return SplitWeight.standard();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }
    }
}
