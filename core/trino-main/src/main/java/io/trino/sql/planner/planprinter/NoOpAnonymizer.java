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
package io.trino.sql.planner.planprinter;

import io.trino.execution.TableInfo;
import io.trino.metadata.IndexHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;

import static io.trino.sql.planner.Partitioning.ArgumentBinding;
import static io.trino.sql.planner.plan.StatisticsWriterNode.WriteStatisticsTarget;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;

public class NoOpAnonymizer
        implements Anonymizer
{
    @Override
    public String anonymize(Type type, String value)
    {
        return value;
    }

    @Override
    public String anonymize(Symbol symbol)
    {
        return symbol.getName();
    }

    @Override
    public String anonymizeColumn(String column)
    {
        return column;
    }

    @Override
    public String anonymize(Expression expression)
    {
        return expression.toString();
    }

    @Override
    public String anonymize(ColumnHandle columnHandle)
    {
        return columnHandle.toString();
    }

    @Override
    public String anonymize(QualifiedObjectName objectName)
    {
        return objectName.toString();
    }

    @Override
    public String anonymize(ArgumentBinding argument)
    {
        return argument.toString();
    }

    @Override
    public String anonymize(IndexHandle indexHandle)
    {
        return indexHandle.toString();
    }

    @Override
    public String anonymize(TableHandle tableHandle, TableInfo tableInfo)
    {
        return tableHandle.toString();
    }

    @Override
    public String anonymize(PartitioningHandle partitioningHandle)
    {
        return partitioningHandle.toString();
    }

    @Override
    public String anonymize(WriterTarget target)
    {
        return target.toString();
    }

    @Override
    public String anonymize(WriteStatisticsTarget target)
    {
        return target.toString();
    }

    @Override
    public String anonymize(TableHandle tableHandle)
    {
        return tableHandle.toString();
    }

    @Override
    public String anonymize(TableExecuteHandle tableHandle)
    {
        return tableHandle.toString();
    }
}
