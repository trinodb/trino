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
package io.trino.tests.product.hive;

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Kerberos-enabled Hive environment with impersonation.
 * <p>
 * This environment extends the Kerberos Hive environment with:
 * <ul>
 *   <li>HDFS impersonation enabled - Trino impersonates the end user for HDFS access</li>
 *   <li>Hive Metastore thrift impersonation enabled - Trino impersonates for HMS access</li>
 *   <li>Hive views enabled - Support for querying Hive views</li>
 *   <li>SQL-standard security for Hive catalog</li>
 *   <li>TPCH catalog for test data</li>
 * </ul>
 * <p>
 * This configuration is used for testing Trino's behavior when impersonating
 * end users in a Kerberos-secured environment.
 */
public class HiveKerberosImpersonationEnvironment
        extends HiveKerberosEnvironment
{
    private static final String DEFAULT_TRINO_USER = "hdfs";
    private static final int HIVE_BEELINE_COMMAND_TIMEOUT_SECONDS = 90;
    private static final int HIVE_BEELINE_MAX_ATTEMPTS = 6;
    private static final long HIVE_BEELINE_RETRY_MILLIS = 5_000;

    static {
        // Ensure the Hive JDBC driver is loaded for HiveServer2 connections
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver. " +
                    "Ensure hive-apache-jdbc dependency is on the classpath.", e);
        }
    }

    @Override
    protected boolean isHdfsImpersonationEnabled()
    {
        return true;
    }

    @Override
    protected Map<String, String> getAdditionalCatalogProperties()
    {
        return Map.of(
                "hive.metastore.thrift.impersonation.enabled", "true",
                "hive.security", "sql-standard");
    }

    @Override
    protected Map<String, String> getHiveSiteProperties()
    {
        return Map.of(
                // Match legacy kerberized Hive images used by Tempto authorization suites.
                // Without this, SET ROLE admin is denied for the default test users.
                "hive.users.in.admin.role", "hdfs,hive",
                "hive.security.authenticator.manager", "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator",
                "hive.security.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory",
                "hive.security.authorization.enabled", "true",
                // Keep HS2 metastore access on the service principal; per-user Kerberos doAs breaks SET ROLE admin in this environment.
                "hive.server2.enable.doAs", "false");
    }

    @Override
    protected Map<String, String> getCoreSiteProperties()
    {
        // Hive role listing fetches rows through Hadoop TokenCache, which expects a
        // MapReduce/YARN renewer principal to be present in the Hadoop configuration.
        // The single-node product test environment does not run a ResourceManager,
        // so reuse the existing HDFS service principal as a stable renewer identity.
        return Map.of(
                "mapreduce.jobtracker.kerberos.principal", HDFS_PRINCIPAL + "@" + kdc.getRealm(),
                "yarn.resourcemanager.principal", HDFS_PRINCIPAL + "@" + kdc.getRealm());
    }

    @Override
    protected Map<String, Map<String, String>> getAdditionalCatalogs()
    {
        return Map.of("tpch", Map.of("connector.name", "tpch"));
    }

    /**
     * Creates a Trino JDBC connection using the default Kerberos user.
     */
    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        Connection connection = createTrinoConnection(DEFAULT_TRINO_USER);
        // Tempto kept a session-scoped role for the default Trino executor.
        // ProductTestEnvironment helper methods open a fresh JDBC connection per statement,
        // so we pre-initialize the default connection with admin role to preserve behavior.
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET ROLE admin IN hive");
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    /**
     * Creates a Trino JDBC connection with the specified user.
     *
     * @param jdbcUser the user to connect as
     * @return a new JDBC connection
     */
    @Override
    public Connection createTrinoConnection(String jdbcUser)
            throws SQLException
    {
        // Match Tempto's alice@trino/bob@trino defaults (jdbc:trino://.../hive/default)
        // so unqualified table/view names in migrated authorization tests keep original behavior.
        return TrinoProductTestContainer.createConnection(trino, jdbcUser, "hive", "default");
    }

    /**
     * Executes a SQL query against Trino as the specified user and returns the result.
     *
     * @param user the user to execute as
     * @param sql the SQL query to execute
     * @return the query result
     */
    public QueryResult executeTrinoAs(String user, String sql)
    {
        try (Connection conn = createTrinoConnection(user);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Trino query as " + user + ": " + sql + " (" + e.getMessage() + ")", e);
        }
    }

    /**
     * Executes a DDL or DML statement against Trino as the specified user.
     *
     * @param user the user to execute as
     * @param sql the SQL statement to execute
     * @return the number of affected rows, or 0 for statements that don't return a count
     */
    public int executeTrinoUpdateAs(String user, String sql)
    {
        try (Connection conn = createTrinoConnection(user);
                Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Trino update as " + user + ": " + sql + " (" + e.getMessage() + ")", e);
        }
    }

    // Hive JDBC methods (connects to HiveServer2 in Hadoop container)

    /**
     * Creates a JDBC connection to HiveServer2 in the Hadoop container.
     * <p>
     * HiveServer2 in this environment uses Kerberos authentication.
     */
    public Connection createHiveConnection()
            throws SQLException
    {
        String jdbcUrl = "jdbc:hive2://" + hadoop.getHost() + ":" + hadoop.getMappedPort(HadoopContainer.HIVESERVER2_PORT) + "/default;auth=noSasl";
        return DriverManager.getConnection(jdbcUrl, "hive", "");
    }

    /**
     * Executes a SQL query against Hive and returns the result.
     *
     * @param sql the SQL query to execute
     * @return the query result
     */
    public QueryResult executeHive(String sql)
    {
        try {
            String stdout = executeHiveWithBeeline(sql);
            return QueryResult.forRows(parseBeelineRows(stdout));
        }
        catch (SQLException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to execute Hive query: " + sql + " (" + e.getMessage() + ")", e);
        }
    }

    /**
     * Executes a DDL or DML statement against Hive.
     *
     * @param sql the SQL statement to execute
     * @return the number of affected rows, or 0 for DDL statements
     */
    public int executeHiveUpdate(String sql)
    {
        try {
            executeHiveWithBeeline(sql);
            return 0;
        }
        catch (SQLException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to execute Hive update: " + sql + " (" + e.getMessage() + ")", e);
        }
    }

    private String executeHiveWithBeeline(String sql)
            throws IOException, InterruptedException, SQLException
    {
        SQLException lastFailure = null;
        for (int attempt = 1; attempt <= HIVE_BEELINE_MAX_ATTEMPTS; attempt++) {
            try {
                return executeHiveWithBeelineOnce(sql, HIVE_BEELINE_COMMAND_TIMEOUT_SECONDS);
            }
            catch (SQLException e) {
                lastFailure = e;
                boolean retriable = isRetriableHiveBeelineFailure(e);
                boolean lastAttempt = attempt == HIVE_BEELINE_MAX_ATTEMPTS;
                if (lastAttempt || !retriable) {
                    throw withHiveFailureDiagnostics(e);
                }
                Thread.sleep(HIVE_BEELINE_RETRY_MILLIS);
            }
        }
        throw lastFailure;
    }

    private String executeHiveWithBeelineOnce(String sql, int timeoutSeconds)
            throws IOException, InterruptedException, SQLException
    {
        String sqlWithAdminRole = "SET ROLE admin; " + sql;
        String encodedSql = Base64.getEncoder().encodeToString(sqlWithAdminRole.getBytes(StandardCharsets.UTF_8));
        String realm = kdc.getRealm();
        String hiveServerPrincipal = HIVE_PRINCIPAL + "@" + realm;
        String clientPrincipal = HDFS_PRINCIPAL + "@" + realm;
        String command = """
                set -euo pipefail
                SQL="$(echo '%s' | base64 -d)"
                kinit -kt %s %s >/dev/null 2>&1
                timeout %ss beeline --silent=true --showHeader=false --outputformat=tsv2 \
                    -u 'jdbc:hive2://localhost:%s/default;principal=%s' -n hdfs -e "$SQL"
                """.formatted(
                encodedSql,
                HADOOP_HDFS_KEYTAB,
                clientPrincipal,
                timeoutSeconds,
                HadoopContainer.HIVESERVER2_PORT,
                hiveServerPrincipal);

        Container.ExecResult result = hadoop.execInContainer("bash", "-lc", command);
        if (result.getExitCode() != 0) {
            throw new SQLException("Hive beeline command failed (exit " + result.getExitCode() + "): " + result.getStderr());
        }
        return result.getStdout();
    }

    private static boolean isRetriableHiveBeelineFailure(SQLException exception)
    {
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        return message.contains("SASL authentication not complete")
                || message.contains("Unknown HS2 problem when communicating with Thrift server")
                || message.contains("SessionHiveMetaStoreClient")
                || message.contains("Failed to retrieve roles for hdfs")
                || message.contains("exit 124");
    }

    private SQLException withHiveFailureDiagnostics(SQLException original)
    {
        String message = original.getMessage();
        if (message == null
                || (!message.contains("SessionHiveMetaStoreClient")
                && !message.contains("SASL authentication not complete")
                && !message.contains("Unknown HS2 problem"))) {
            return original;
        }

        StringBuilder diagnostics = new StringBuilder();
        diagnostics.append("\n\n=== Hive failure diagnostics ===\n");
        appendCommandDiagnostics(diagnostics, "ps -ef | grep -E 'HiveMetaStore|HiveServer2|RunJar|metastore' | grep -v grep || true");
        appendCommandDiagnostics(diagnostics, "grep -nE 'metastore|kerberos|sasl|thrift|security|authorization|hive.users.in.admin.role' /opt/hive/conf/hive-site.xml || true");
        appendCommandDiagnostics(diagnostics, "grep -n -A80 -B20 'GSS initiate failed' /tmp/root/hive.log || true");
        appendCommandDiagnostics(diagnostics, "ls -lah /tmp/root || true");
        appendCommandDiagnostics(diagnostics, "for f in /tmp/root/*; do [ -f \"$f\" ] && echo \"===== $f =====\" && tail -n 120 \"$f\"; done || true");
        diagnostics.append("=== End Hive failure diagnostics ===");

        SQLException wrapped = new SQLException(message + diagnostics, original.getSQLState(), original.getErrorCode(), original);
        wrapped.setStackTrace(original.getStackTrace());
        return wrapped;
    }

    private void appendCommandDiagnostics(StringBuilder diagnostics, String command)
    {
        diagnostics.append("$ ").append(command).append('\n');
        try {
            Container.ExecResult result = hadoop.execInContainer("bash", "-lc", command);
            if (!result.getStdout().isBlank()) {
                diagnostics.append(result.getStdout());
            }
            if (!result.getStderr().isBlank()) {
                diagnostics.append(result.getStderr());
            }
            diagnostics.append("[exit ").append(result.getExitCode()).append("]\n");
        }
        catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            diagnostics.append("[diagnostics command failed: ").append(e.getMessage()).append("]\n");
        }
    }

    private static List<List<Object>> parseBeelineRows(String stdout)
    {
        List<List<Object>> rows = new ArrayList<>();
        for (String line : stdout.split("\\R")) {
            String trimmed = line.strip();
            if (!isBeelineDataLine(trimmed)) {
                continue;
            }

            String[] parts = trimmed.split("\t", -1);
            List<Object> row = new ArrayList<>(parts.length);
            for (String part : parts) {
                row.add(parseBeelineValue(part));
            }
            rows.add(row);
        }
        return rows;
    }

    private static boolean isBeelineDataLine(String line)
    {
        if (line.isEmpty()) {
            return false;
        }

        return !line.startsWith("Picked up JAVA_TOOL_OPTIONS:")
                && !line.startsWith("WARNING:")
                && !line.startsWith("SLF4J:")
                && !line.startsWith("Connecting to ")
                && !line.startsWith("Connected to:")
                && !line.startsWith("Driver:")
                && !line.startsWith("Transaction isolation:")
                && !line.startsWith("INFO  :")
                && !line.startsWith("Closing:")
                && !line.startsWith("No rows selected")
                && !line.startsWith("Error:");
    }

    private static Object parseBeelineValue(String value)
    {
        if ("true".equalsIgnoreCase(value)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(value)) {
            return Boolean.FALSE;
        }
        return value;
    }
}
