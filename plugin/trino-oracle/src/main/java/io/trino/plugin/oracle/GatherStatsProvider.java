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
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;

public class GatherStatsProvider
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(GatherStatsProvider.class);
    private static final MethodHandle methodHandle;
    private final JdbcClient oracleClient;

    @Inject
    public GatherStatsProvider(JdbcClient client)
    {
        this.oracleClient = client;
    }

    static {
        try {
            methodHandle = lookup().unreflect(GatherStatsProvider.class.getMethod("gatherTableStats", ConnectorSession.class, String.class, String.class, Double.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public void gatherTableStats(ConnectorSession session, String schemaName, String tableName, Double estimate)
    {
        // this line guarantees that classLoader that we stored in the field will be used inside try/catch
        // as we captured reference to PluginClassLoader during initialization of this class
        // we can use it now to correctly execute the procedure
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doGatherTableStats(session, schemaName, tableName, estimate);
        }
    }

    public void doGatherTableStats(ConnectorSession session, String schemaName, String tableName, Double estimate)
    {
        //SchemaTableName sourceTableName = new SchemaTableName(schemaName, tableName);
        log.debug("Gather table stats called with " + schemaName + " ; " + tableName + " ( " + estimate + " ) ");
        String sql = "BEGIN DBMS_STATS.GATHER_TABLE_STATS('" + schemaName + "', '" + tableName + "'); END;";
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
        return new Procedure("rdbms", "gather_table_stats",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("ESTIMATE_PERCENT", DOUBLE, false, 0.0)),
                        GatherStatsProvider.methodHandle.bindTo(this));
    }
}
