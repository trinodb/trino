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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionManager;

import java.util.List;

public interface DataDefinitionTask<T extends Statement>
{
    String getName();

    ListenableFuture<Void> execute(
            T statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector);

    default String explain(T statement, List<Expression> parameters)
    {
        StringBuilder builder = new StringBuilder();

        builder.append(SqlFormatter.formatSql(statement));

        if (!parameters.isEmpty()) {
            builder.append("\n")
                    .append("Parameters: ")
                    .append(parameters);
        }

        return builder.toString();
    }
}
