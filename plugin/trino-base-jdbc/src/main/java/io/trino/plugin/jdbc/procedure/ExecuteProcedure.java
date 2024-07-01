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
package io.trino.plugin.jdbc.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import java.lang.invoke.MethodHandle;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public final class ExecuteProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle EXECUTE;

    static {
        try {
            EXECUTE = lookup().unreflect(ExecuteProcedure.class.getMethod("execute", ConnectorSession.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final JdbcClient jdbcClient;

    @Inject
    public ExecuteProcedure(JdbcClient jdbcClient)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "execute",
                ImmutableList.of(new Argument("QUERY", VARCHAR)),
                EXECUTE.bindTo(this));
    }

    public void execute(ConnectorSession session, String query)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doExecute(session, query);
        }
    }

    public void doExecute(ConnectorSession session, String query)
    {
        try (Connection connection = jdbcClient.getConnection(session)) {
            connection.setReadOnly(false);
            try (Statement statement = connection.createStatement()) {
                //noinspection SqlSourceToSinkFlow
                statement.executeUpdate(query);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to execute query. " + firstNonNull(e.getMessage(), e), e);
        }
    }
}
