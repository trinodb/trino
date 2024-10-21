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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;

public class GrantProvider
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(GatherStatsProvider.class);
    private static final MethodHandle methodHandle;
    private final JdbcClient oracleClient;

    @Inject
    public GrantProvider(JdbcClient client)
    {
        this.oracleClient = client;
    }

    static {
        try {
            methodHandle = lookup().unreflect(GrantProvider.class.getMethod("grant", ConnectorSession.class, String.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public void grant(ConnectorSession session, String schemaName, String tableName, String access, String grantee)
    {
        // this line guarantees that classLoader that we stored in the field will be used inside try/catch
        // as we captured reference to PluginClassLoader during initialization of this class
        // we can use it now to correctly execute the procedure
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doGrant(session, schemaName, tableName, access, grantee);
        }
    }

    public void doGrant(ConnectorSession session, String schemaName, String tableName, String access, String grantee)
    {
        //SchemaTableName sourceTableName = new SchemaTableName(schemaName, tableName);
        log.debug("Grant called with " + schemaName + " ; " + tableName + " ; " + access + " grantee=> " + grantee);
        String sql = "GRANT " + access + " ON " + schemaName + "." + tableName + " TO " + grantee + "";
        log.info("doGrant : " + sql);
        JdbcProcedureHandle.ProcedureQuery procQuery = new JdbcProcedureHandle.ProcedureQuery(sql);
        JdbcProcedureHandle handle = new JdbcProcedureHandle(procQuery,
                java.util.List.of());
        try {
            Connection connection = oracleClient.getConnection(session, null, handle);
            CallableStatement callableStatement = oracleClient.buildProcedure(session, connection, null, handle);
            callableStatement.execute();
            callableStatement.close();
            connection.close();
            log.info("GatherStatsProvider: " + sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        //oracleClient.gatherTableStats(session, schemaName, tableName);
    }

    @Override
    public Procedure get()
    {
        return new Procedure("rdbms", "grant",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("ACCESS", VARCHAR),
                        new Procedure.Argument("GRANTEE", VARCHAR)),
                GrantProvider.methodHandle.bindTo(this));
    }
}
