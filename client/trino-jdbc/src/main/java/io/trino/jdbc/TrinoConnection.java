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
package io.trino.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import io.trino.client.ClientSelectedRole;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import okhttp3.OkHttpClient;

import javax.annotation.Nullable;

import java.net.URI;
import java.nio.charset.CharsetEncoder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.jdbc.ClientInfoProperty.APPLICATION_NAME;
import static io.trino.jdbc.ClientInfoProperty.CLIENT_INFO;
import static io.trino.jdbc.ClientInfoProperty.CLIENT_TAGS;
import static io.trino.jdbc.ClientInfoProperty.TRACE_TOKEN;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TrinoConnection
        implements Connection
{
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean autoCommit = new AtomicBoolean(true);
    private final AtomicInteger isolationLevel = new AtomicInteger(TRANSACTION_READ_UNCOMMITTED);
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final AtomicReference<String> catalog = new AtomicReference<>();
    private final AtomicReference<String> schema = new AtomicReference<>();
    private final AtomicReference<String> path = new AtomicReference<>();
    private final AtomicReference<ZoneId> timeZoneId = new AtomicReference<>();
    private final AtomicReference<Locale> locale = new AtomicReference<>();
    private final AtomicReference<Integer> networkTimeoutMillis = new AtomicReference<>(Ints.saturatedCast(MINUTES.toMillis(2)));
    private final AtomicLong nextStatementId = new AtomicLong(1);

    private final URI jdbcUri;
    private final URI httpUri;
    private final String user;
    private final Optional<String> sessionUser;
    private final boolean compressionDisabled;
    private final boolean assumeLiteralNamesInMetadataCallsForNonConformingClients;
    private final Map<String, String> extraCredentials;
    private final Optional<String> applicationNamePrefix;
    private final Optional<String> source;
    private final Map<ClientInfoProperty, String> clientInfo = new ConcurrentHashMap<>();
    private final Map<String, String> sessionProperties = new ConcurrentHashMap<>();
    private final Map<String, String> preparedStatements = new ConcurrentHashMap<>();
    private final Map<String, ClientSelectedRole> roles = new ConcurrentHashMap<>();
    private final AtomicReference<String> transactionId = new AtomicReference<>();
    private final OkHttpClient httpClient;
    private final Set<TrinoStatement> statements = newSetFromMap(new ConcurrentHashMap<>());

    TrinoConnection(TrinoDriverUri uri, OkHttpClient httpClient)
            throws SQLException
    {
        requireNonNull(uri, "uri is null");
        this.jdbcUri = uri.getJdbcUri();
        this.httpUri = uri.getHttpUri();
        uri.getSchema().ifPresent(schema::set);
        uri.getCatalog().ifPresent(catalog::set);
        this.user = uri.getUser();
        this.sessionUser = uri.getSessionUser();
        this.applicationNamePrefix = uri.getApplicationNamePrefix();
        this.source = uri.getSource();
        this.extraCredentials = uri.getExtraCredentials();
        this.compressionDisabled = uri.isCompressionDisabled();
        this.assumeLiteralNamesInMetadataCallsForNonConformingClients = uri.isAssumeLiteralNamesInMetadataCallsForNonConformingClients();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        uri.getClientInfo().ifPresent(tags -> clientInfo.put(CLIENT_INFO, tags));
        uri.getClientTags().ifPresent(tags -> clientInfo.put(CLIENT_TAGS, tags));
        uri.getTraceToken().ifPresent(tags -> clientInfo.put(TRACE_TOKEN, tags));

        roles.putAll(uri.getRoles());
        timeZoneId.set(ZoneId.systemDefault());
        locale.set(Locale.getDefault());
        sessionProperties.putAll(uri.getSessionProperties());
    }

    @Override
    public Statement createStatement()
            throws SQLException
    {
        return doCreateStatement();
    }

    private TrinoStatement doCreateStatement()
            throws SQLException
    {
        checkOpen();
        TrinoStatement statement = new TrinoStatement(this, this::unregisterStatement);
        registerStatement(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql)
            throws SQLException
    {
        checkOpen();
        String name = "statement" + nextStatementId.getAndIncrement();
        TrinoPreparedStatement statement = new TrinoPreparedStatement(this, this::unregisterStatement, name, sql);
        registerStatement(statement);
        return statement;
    }

    @Override
    public CallableStatement prepareCall(String sql)
            throws SQLException
    {
        throw new NotImplementedException("Connection", "prepareCall");
    }

    @Override
    public String nativeSQL(String sql)
            throws SQLException
    {
        checkOpen();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit)
            throws SQLException
    {
        checkOpen();
        if (autoCommit && !getAutoCommit()) {
            commit();
        }
        this.autoCommit.set(autoCommit);
    }

    @Override
    public boolean getAutoCommit()
            throws SQLException
    {
        checkOpen();
        return autoCommit.get();
    }

    @Override
    public void commit()
            throws SQLException
    {
        checkOpen();
        if (getAutoCommit()) {
            throw new SQLException("Connection is in auto-commit mode");
        }
        if (transactionId.get() == null) {
            // empty transaction
            return;
        }
        try (TrinoStatement statement = doCreateStatement()) {
            statement.internalExecute("COMMIT");
        }
    }

    @Override
    public void rollback()
            throws SQLException
    {
        checkOpen();
        if (getAutoCommit()) {
            throw new SQLException("Connection is in auto-commit mode");
        }
        if (transactionId.get() == null) {
            // empty transaction
            return;
        }
        try (TrinoStatement statement = doCreateStatement()) {
            statement.internalExecute("ROLLBACK");
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        try {
            if (!closed.get()) {
                SqlExceptionHolder heldException = new SqlExceptionHolder();
                for (TrinoStatement statement : statements) {
                    try {
                        statement.close();
                    }
                    catch (SQLException | RuntimeException e) {
                        heldException.hold(e);
                    }
                }
                if (transactionId.get() != null) {
                    try (TrinoStatement statement = doCreateStatement()) {
                        statement.internalExecute("ROLLBACK");
                    }
                    catch (SQLException | RuntimeException e) {
                        heldException.hold(e);
                    }
                }
                heldException.throwIfHeld();
            }
        }
        finally {
            closed.set(true);
        }
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return closed.get();
    }

    @Override
    public DatabaseMetaData getMetaData()
            throws SQLException
    {
        return new TrinoDatabaseMetaData(this, assumeLiteralNamesInMetadataCallsForNonConformingClients);
    }

    @Override
    public void setReadOnly(boolean readOnly)
            throws SQLException
    {
        checkOpen();
        this.readOnly.set(readOnly);
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        return readOnly.get();
    }

    @Override
    public void setCatalog(String catalog)
            throws SQLException
    {
        checkOpen();
        this.catalog.set(catalog);
    }

    @Override
    public String getCatalog()
            throws SQLException
    {
        checkOpen();
        return catalog.get();
    }

    @Override
    public void setTransactionIsolation(int level)
            throws SQLException
    {
        checkOpen();
        getIsolationLevel(level);
        isolationLevel.set(level);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getTransactionIsolation()
            throws SQLException
    {
        checkOpen();
        return isolationLevel.get();
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        checkOpen();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        checkResultSet(resultSetType, resultSetConcurrency);
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        checkResultSet(resultSetType, resultSetConcurrency);
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        checkResultSet(resultSetType, resultSetConcurrency);
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override
    public Map<String, Class<?>> getTypeMap()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getTypeMap");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setTypeMap");
    }

    @Override
    public void setHoldability(int holdability)
            throws SQLException
    {
        checkOpen();
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Changing holdability not supported");
        }
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        checkOpen();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("rollback");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("releaseSavepoint");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        checkHoldability(resultSetHoldability);
        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        checkHoldability(resultSetHoldability);
        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        checkHoldability(resultSetHoldability);
        return prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException("Auto generated keys must be NO_GENERATED_KEYS");
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override
    public Clob createClob()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("createClob");
    }

    @Override
    public Blob createBlob()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("createBlob");
    }

    @Override
    public NClob createNClob()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    @Override
    public SQLXML createSQLXML()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("createSQLXML");
    }

    @Override
    public boolean isValid(int timeout)
            throws SQLException
    {
        if (timeout < 0) {
            throw new SQLException("Timeout is negative");
        }
        return !isClosed();
    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException
    {
        requireNonNull(name, "name is null");

        Optional<ClientInfoProperty> clientInfoProperty = ClientInfoProperty.forName(name);
        if (!clientInfoProperty.isPresent()) {
            // TODO generate a warning
            return;
        }

        if (value != null) {
            clientInfo.put(clientInfoProperty.get(), value);
        }
        else {
            clientInfo.remove(clientInfoProperty.get());
        }
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException
    {
        for (Map.Entry<String, String> entry : fromProperties(properties).entrySet()) {
            setClientInfo(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String getClientInfo(String name)
            throws SQLException
    {
        return Optional.ofNullable(name)
                .flatMap(ClientInfoProperty::forName)
                .map(clientInfo::get)
                .orElse(null);
    }

    @Override
    public Properties getClientInfo()
            throws SQLException
    {
        Properties properties = new Properties();
        for (Map.Entry<ClientInfoProperty, String> entry : clientInfo.entrySet()) {
            properties.setProperty(entry.getKey().getPropertyName(), entry.getValue());
        }
        return properties;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("createStruct");
    }

    @Override
    public void setSchema(String schema)
            throws SQLException
    {
        checkOpen();
        this.schema.set(schema);
    }

    @Override
    public String getSchema()
            throws SQLException
    {
        checkOpen();
        return schema.get();
    }

    public String getTimeZoneId()
    {
        return timeZoneId.get().getId();
    }

    public void setTimeZoneId(String timeZoneId)
    {
        this.timeZoneId.set(ZoneId.of(timeZoneId));
    }

    public Locale getLocale()
    {
        return locale.get();
    }

    public void setLocale(Locale locale)
    {
        this.locale.set(locale);
    }

    /**
     * Adds a session property.
     */
    public void setSessionProperty(String name, String value)
    {
        requireNonNull(name, "name is null");
        requireNonNull(value, "value is null");
        checkArgument(!name.isEmpty(), "name is empty");

        CharsetEncoder charsetEncoder = US_ASCII.newEncoder();
        checkArgument(name.indexOf('=') < 0, "Session property name must not contain '=': %s", name);
        checkArgument(charsetEncoder.canEncode(name), "Session property name is not US_ASCII: %s", name);
        checkArgument(charsetEncoder.canEncode(value), "Session property value is not US_ASCII: %s", value);

        sessionProperties.put(name, value);
    }

    @VisibleForTesting
    Map<String, ClientSelectedRole> getRoles()
    {
        return ImmutableMap.copyOf(roles);
    }

    @Override
    public void abort(Executor executor)
            throws SQLException
    {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException
    {
        checkOpen();
        if (milliseconds < 0) {
            throw new SQLException("Timeout is negative");
        }
        networkTimeoutMillis.set(milliseconds);
    }

    @Override
    public int getNetworkTimeout()
            throws SQLException
    {
        checkOpen();
        return networkTimeoutMillis.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    URI getURI()
    {
        return jdbcUri;
    }

    String getUser()
    {
        return user;
    }

    @VisibleForTesting
    Map<String, String> getExtraCredentials()
    {
        return ImmutableMap.copyOf(extraCredentials);
    }

    @VisibleForTesting
    Map<String, String> getSessionProperties()
    {
        return ImmutableMap.copyOf(sessionProperties);
    }

    boolean shouldStartTransaction()
    {
        return !autoCommit.get() && (transactionId.get() == null);
    }

    String getStartTransactionSql()
            throws SQLException
    {
        return format(
                "START TRANSACTION ISOLATION LEVEL %s, READ %s",
                getIsolationLevel(isolationLevel.get()),
                readOnly.get() ? "ONLY" : "WRITE");
    }

    StatementClient startQuery(String sql, Map<String, String> sessionPropertiesOverride)
    {
        String source = getActualSource();

        Iterable<String> clientTags = Splitter.on(',').trimResults().omitEmptyStrings()
                .split(nullToEmpty(clientInfo.get(CLIENT_TAGS)));

        Map<String, String> allProperties = new HashMap<>(sessionProperties);
        allProperties.putAll(sessionPropertiesOverride);

        // zero means no timeout, so use a huge value that is effectively unlimited
        int millis = networkTimeoutMillis.get();
        Duration timeout = (millis > 0) ? new Duration(millis, MILLISECONDS) : new Duration(999, DAYS);

        ClientSession session = new ClientSession(
                httpUri,
                user,
                sessionUser,
                source,
                Optional.ofNullable(clientInfo.get(TRACE_TOKEN)),
                ImmutableSet.copyOf(clientTags),
                clientInfo.get(CLIENT_INFO),
                catalog.get(),
                schema.get(),
                path.get(),
                timeZoneId.get(),
                locale.get(),
                ImmutableMap.of(),
                ImmutableMap.copyOf(allProperties),
                ImmutableMap.copyOf(preparedStatements),
                ImmutableMap.copyOf(roles),
                extraCredentials,
                transactionId.get(),
                timeout,
                compressionDisabled);

        return newStatementClient(httpClient, session, sql);
    }

    void updateSession(StatementClient client)
    {
        client.getSetSessionProperties().forEach(sessionProperties::put);
        client.getResetSessionProperties().forEach(sessionProperties::remove);

        client.getAddedPreparedStatements().forEach(preparedStatements::put);
        client.getDeallocatedPreparedStatements().forEach(preparedStatements::remove);

        client.getSetRoles().forEach(roles::put);

        client.getSetCatalog().ifPresent(catalog::set);
        client.getSetSchema().ifPresent(schema::set);
        client.getSetPath().ifPresent(path::set);

        if (client.getStartedTransactionId() != null) {
            transactionId.set(client.getStartedTransactionId());
        }
        if (client.isClearTransactionId()) {
            transactionId.set(null);
        }
    }

    private void registerStatement(TrinoStatement statement)
    {
        checkState(statements.add(statement), "Statement is already registered");
    }

    private void unregisterStatement(TrinoStatement statement)
    {
        checkState(statements.remove(statement), "Statement is not registered");
    }

    @VisibleForTesting
    int activeStatements()
    {
        return statements.size();
    }

    private void checkOpen()
            throws SQLException
    {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    private static void checkResultSet(int resultSetType, int resultSetConcurrency)
            throws SQLFeatureNotSupportedException
    {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Result set type must be TYPE_FORWARD_ONLY");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Result set concurrency must be CONCUR_READ_ONLY");
        }
    }

    private static void checkHoldability(int resultSetHoldability)
            throws SQLFeatureNotSupportedException
    {
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Result set holdability must be HOLD_CURSORS_OVER_COMMIT");
        }
    }

    private static String getIsolationLevel(int level)
            throws SQLException
    {
        switch (level) {
            case TRANSACTION_READ_UNCOMMITTED:
                return "READ UNCOMMITTED";
            case TRANSACTION_READ_COMMITTED:
                return "READ COMMITTED";
            case TRANSACTION_REPEATABLE_READ:
                return "REPEATABLE READ";
            case TRANSACTION_SERIALIZABLE:
                return "SERIALIZABLE";
        }
        throw new SQLException("Invalid transaction isolation level: " + level);
    }

    private String getActualSource()
    {
        if (source.isPresent()) {
            return source.get();
        }
        String source = "trino-jdbc";
        String applicationName = clientInfo.get(APPLICATION_NAME);
        if (applicationNamePrefix.isPresent()) {
            source = applicationNamePrefix.get();
            if (applicationName != null) {
                source += applicationName;
            }
        }
        else if (applicationName != null) {
            source = applicationName;
        }
        return source;
    }

    private static final class SqlExceptionHolder
    {
        @Nullable
        private Exception heldException;

        public void hold(Exception exception)
        {
            if (heldException == null) {
                heldException = exception;
            }
            else if (heldException != exception) {
                heldException.addSuppressed(exception);
            }
        }

        public void throwIfHeld()
                throws SQLException
        {
            if (heldException != null) {
                throw new SQLException(heldException);
            }
        }
    }
}
