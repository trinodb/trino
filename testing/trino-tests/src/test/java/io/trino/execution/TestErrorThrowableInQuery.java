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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test scenarios where query fails unexpected {@link Error} during execution. This is worth testing
 * as we observed in the past that not all code paths handled non-Exception subclasses of Throwable correctly.
 */
public class TestErrorThrowableInQuery
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSystemProperty("task_concurrency", "1")
                .setCatalog("mock")
                .setSchema("default")
                .setClientInfo("{\"clientVersion\":\"testVersion\"}")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.installPlugin(new Plugin()
            {
                @Override
                public Iterable<ConnectorFactory> getConnectorFactories()
                {
                    SchemaTableName stackOverflowErrorTableName = new SchemaTableName("default", "stack_overflow_during_planning");
                    SchemaTableName classFormatErrorTableName = new SchemaTableName("default", "class_format_error_during_planning");

                    MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                            .withListTables((session, s) -> ImmutableList.of(stackOverflowErrorTableName))
                            .withGetColumns(schemaTableName -> ImmutableList.of(
                                    new ColumnMetadata("test_varchar", createUnboundedVarcharType()),
                                    new ColumnMetadata("test_bigint", BIGINT)))
                            .withGetTableHandle((session, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                            .withApplyProjection((session, handle, projections, assignments) -> {
                                MockConnectorTableHandle mockTableHandle = (MockConnectorTableHandle) handle;
                                if (stackOverflowErrorTableName.equals(mockTableHandle.getTableName())) {
                                    throw new StackOverflowError("We run out of stack!!!!!!!!!!!");
                                }
                                if (classFormatErrorTableName.equals(mockTableHandle.getTableName())) {
                                    throw new ClassFormatError("Bad class format!!!!!!!!!!");
                                }
                                throw new TrinoException(NOT_FOUND, "Unknown table: " + mockTableHandle.getTableName());
                            })
                            .build();
                    return ImmutableList.of(connectorFactory);
                }
            });
            queryRunner.createCatalog("mock", "mock", ImmutableMap.of());
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
        return queryRunner;
    }

    @Test
    public void testSystemRuntimeQueriesWorksAfterStackOverflowErrorDuringAnalyzeInExplain()
            throws Exception
    {
        // This tests simulates case when StackOverflowError is throwing during planning because of size and shape of
        // the query. It is possible to construct query such that it makes its way through parsed and then fail during
        // planning but it is hard to do that in a predictable way, hence we use mock connector here.
        assertThatThrownBy(() -> query("EXPLAIN SELECT test_varchar FROM stack_overflow_during_planning"))
                .hasMessageContaining("statement is too large (stack overflow during analysis)");

        assertThat(query("SELECT * FROM system.runtime.queries")).matches(result -> result.getRowCount() > 0);
    }

    @Test
    public void testSystemRuntimeQueriesWorksAfterClassFormatErrorDuringAnalyzeInExplain()
            throws Exception
    {
        assertThatThrownBy(() -> query("EXPLAIN SELECT test_varchar FROM class_format_error_during_planning"))
                .hasMessageContaining("java.lang.ClassFormatError: Bad class format!!!!!!!!!!");

        assertThat(query("SELECT * FROM system.runtime.queries")).matches(result -> result.getRowCount() > 0);
    }
}
