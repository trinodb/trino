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
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class PassThroughProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(PassThroughProcedure.class);
    private final JdbcClient jdbcClient;

    @Inject
    public PassThroughProcedure(
            JdbcClient jdbcClient)
    {
        this.jdbcClient = jdbcClient;
    }

    private static final MethodHandle DIRECT_STATMENT = methodHandle(
            PassThroughProcedure.class,
            "passThrough",
            ConnectorSession.class,
            ConnectorAccessControl.class,
            String.class);

    @Override
    public Procedure get()
    {
        return new Procedure(
            "system",
            "pass_through",
            ImmutableList.of(
                new Argument("STATEMENT", VARCHAR)),
            DIRECT_STATMENT.bindTo(this));
    }

    public void passThrough(ConnectorSession session, ConnectorAccessControl accessControl, String statement)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doPassThrough(session, accessControl, statement);
        }
    }

    public void doPassThrough(ConnectorSession session, ConnectorAccessControl accessControl, String statement)
    {
        try (Connection connection = this.jdbcClient.getConnection(session, new JdbcSplit(Optional.empty()))) {
            execute(connection, statement);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void execute(Connection connection, String query)
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute(query);
        }
        catch (SQLException e) {
            TrinoException exception = new TrinoException(JDBC_ERROR, e);
            exception.addSuppressed(new RuntimeException("Query: " + query));
            throw exception;
        }
    }
}
