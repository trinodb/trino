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
import com.google.inject.Inject;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Statement;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.SqlFormatterUtil.getFormattedSql;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PrepareTask
        implements DataDefinitionTask<Prepare>
{
    private final SqlParser sqlParser;

    @Inject
    public PrepareTask(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public String getName()
    {
        return "PREPARE";
    }

    @Override
    public ListenableFuture<Void> execute(
            Prepare prepare,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Statement statement = prepare.getStatement();
        if ((statement instanceof Prepare) || (statement instanceof Execute) || (statement instanceof Deallocate)) {
            String type = statement.getClass().getSimpleName().toUpperCase(ENGLISH);
            throw new TrinoException(NOT_SUPPORTED, "Invalid statement type for prepared statement: " + type);
        }

        String sql = getFormattedSql(statement, sqlParser);
        stateMachine.addPreparedStatement(prepare.getName().getValue(), sql);
        return immediateVoidFuture();
    }
}
