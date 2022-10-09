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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.OutputTableHandle;
import io.trino.operator.TableWriterOperator.TableWriterOperatorFactory;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.split.PageSinkManager;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableWriterNode.CreateTarget;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;

public class TestTableWriterOperator
        extends BaseTableWriterOperatorTest
{
    @Override
    protected AbstractTableWriterOperator createTableWriterOperator(
            Session session,
            DriverContext driverContext,
            TestPageSinkProvider pageSinkProvider,
            OperatorFactory statisticsAggregation,
            List<Type> outputTypes)
    {
        PageSinkManager pageSinkManager = new PageSinkManager(catalogHandle -> {
            checkArgument(catalogHandle.equals(TEST_CATALOG_HANDLE));
            return pageSinkProvider;
        });
        SchemaTableName schemaTableName = new SchemaTableName("testSchema", "testTable");
        TableWriterOperatorFactory factory = new TableWriterOperatorFactory(
                0,
                new PlanNodeId("test"),
                pageSinkManager,
                new CreateTarget(
                        new OutputTableHandle(
                                TEST_CATALOG_HANDLE,
                                schemaTableName,
                                new ConnectorTransactionHandle() {},
                                new ConnectorOutputTableHandle() {}),
                        schemaTableName,
                        false),
                ImmutableList.of(0),
                session,
                statisticsAggregation,
                outputTypes);
        return (AbstractTableWriterOperator) factory.createOperator(driverContext);
    }
}
