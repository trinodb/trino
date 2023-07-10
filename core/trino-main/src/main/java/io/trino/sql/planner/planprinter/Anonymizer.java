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

/**
 * An interface for anonymizing the plan in {@link PlanPrinter}
 */
public interface Anonymizer
{
    String anonymize(Type type, String value);

    String anonymize(Symbol symbol);

    String anonymizeColumn(String column);

    String anonymize(Expression expression);

    String anonymize(ColumnHandle columnHandle);

    String anonymize(QualifiedObjectName objectName);

    String anonymize(ArgumentBinding argument);

    String anonymize(IndexHandle indexHandle);

    String anonymize(TableHandle tableHandle, TableInfo tableInfo);

    String anonymize(PartitioningHandle partitioningHandle);

    String anonymize(WriterTarget writerTarget);

    String anonymize(WriteStatisticsTarget writerTarget);

    String anonymize(TableHandle tableHandle);

    String anonymize(TableExecuteHandle tableHandle);
}
