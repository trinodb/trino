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
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import io.trino.client.ClientSelectedRole;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import jakarta.annotation.Nullable;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ProtocolException;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.Maps.fromProperties;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.jdbc.ClientInfoProperty.APPLICATION_NAME;
import static io.trino.jdbc.ClientInfoProperty.CLIENT_INFO;
import static io.trino.jdbc.ClientInfoProperty.CLIENT_TAGS;
import static io.trino.jdbc.ClientInfoProperty.TRACE_TOKEN;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TrinoConnection
        implements Connection
{
    private static final Logger logger = Logger.getLogger(TrinoConnection.class.getPackage().getName());

    private static final int CONNECTION_TIMEOUT_SECONDS = 30; // Not configurable

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean autoCommit = new AtomicBoolean(true);
    private final AtomicInteger isolationLevel = new AtomicInteger(TRANSACTION_READ_UNCOMMITTED);
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final AtomicReference<String> catalog = new AtomicReference<>();
    private final AtomicReference<String> schema = new AtomicReference<>();
    private final AtomicReference<List<String>> path = new AtomicReference<>(ImmutableList.of());
    private final AtomicReference<String> authorizationUser = new AtomicReference<>();
    private final AtomicReference<ZoneId> timeZoneId = new AtomicReference<>();
    private final AtomicReference<Locale> locale = new AtomicReference<>();
    private final AtomicReference<Integer> networkTimeoutMillis = new AtomicReference<>(Ints.saturatedCast(MINUTES.toMillis(2)));
    private final AtomicLong nextStatementId = new AtomicLong(1);
    private final AtomicReference<Optional<String>> sessionUser = new AtomicReference<>();

    private final URI jdbcUri;
    private final URI httpUri;
    private final Optional<String> user;
    private final boolean compressionDisabled;
    private final Optional<String> encoding;
    private final boolean assumeLiteralNamesInMetadataCallsForNonConformingClients;
    private final boolean assumeLiteralUnderscoreInMetadataCallsForNonConformingClients;
    private final Map<String, String> extraCredentials;
    private final Optional<String> applicationNamePrefix;
    private final Optional<String> source;
    private final Map<ClientInfoProperty, String> clientInfo = new ConcurrentHashMap<>();
    private final Map<String, String> sessionProperties = new ConcurrentHashMap<>();
    private final Map<String, String> preparedStatements = new ConcurrentHashMap<>();
    private final Map<String, ClientSelectedRole> roles = new ConcurrentHashMap<>();
    private final AtomicReference<String> transactionId = new AtomicReference<>();
    private final Call.Factory httpCallFactory;
    private final Call.Factory segmentHttpCallFactory;
    private final Set<TrinoStatement> statements = newSetFromMap(new ConcurrentHashMap<>());
    private boolean useExplicitPrepare = true;
    private boolean assumeNullCatalogMeansCurrentCatalog;
    private final boolean validateConnection;

    TrinoConnection(TrinoDriverUri uri, Call.Factory httpCallFactory, Call.Factory segmentHttpCallFactory)
            throws SQLException
    {
        requireNonNull(uri, "uri is null");
        this.jdbcUri = uri.getUri();
        this.httpUri = uri.getHttpUri();
        uri.getSchema().ifPresent(schema::set);
        uri.getCatalog().ifPresent(catalog::set);
        this.user = uri.getUser();
        this.sessionUser.set(uri.getSessionUser());
        this.applicationNamePrefix = uri.getApplicationNamePrefix();
        this.source = uri.getSource();
        this.extraCredentials = uri.getExtraCredentials();
        this.compressionDisabled = uri.isCompressionDisabled();
        this.encoding = uri.getEncoding();
        this.assumeLiteralNamesInMetadataCallsForNonConformingClients = uri.isAssumeLiteralNamesInMetadataCallsForNonConformingClients();

        if (this.assumeLiteralNamesInMetadataCallsForNonConformingClients) {
            logger.log(Level.WARNING, "Connection config assumeLiteralNamesInMetadataCallsForNonConformingClients is deprecated, please use " +
                    "assumeLiteralUnderscoreInMetadataCallsForNonConformingClients.");
        }

        this.assumeLiteralUnderscoreInMetadataCallsForNonConformingClients = uri.isAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients();

        this.httpCallFactory = requireNonNull(httpCallFactory, "httpCallFactory is null");
        this.segmentHttpCallFactory = requireNonNull(segmentHttpCallFactory, "segmentHttpCallFactory is null");
        uri.getClientInfo().ifPresent(tags -> clientInfo.put(CLIENT_INFO, tags));
        uri.getClientTags().ifPresent(tags -> clientInfo.put(CLIENT_TAGS, Joiner.on(",").join(tags)));
        uri.getTraceToken().ifPresent(tags -> clientInfo.put(TRACE_TOKEN, tags));

        roles.putAll(uri.getRoles());
        timeZoneId.set(uri.getTimeZone());
        locale.set(Locale.getDefault());
        sessionProperties.putAll(uri.getSessionProperties());

        uri.getExplicitPrepare().ifPresent(value -> this.useExplicitPrepare = value);
        uri.getAssumeNullCatalogMeansCurrentCatalog().ifPresent(value -> this.assumeNullCatalogMeansCurrentCatalog = value);

        this.validateConnection = uri.isValidateConnection();
        if (validateConnection) {
            try {
                if (!isConnectionValid(CONNECTION_TIMEOUT_SECONDS)) {
                    throw new SQLException("Invalid authentication to Trino server", "28000");
                }
            }
            catch (UnsupportedOperationException | IOException e) {
                throw new SQLException("Unable to connect to Trino server", "08001", e);
            }
        }
    }

    private boolean isConnectionValid(int timeout)
            throws IOException, UnsupportedOperationException
    {
        HttpUrl url = HttpUrl.get(httpUri)
                .newBuilder()
                .encodedPath("/v1/statement")
                .build();

        Request headRequest = new Request.Builder()
                .url(url)
                .head()
                .build();

        Exception lastException = null;
        Duration timeoutDuration = new Duration(timeout, TimeUnit.SECONDS);
        long start = System.nanoTime();

        while (timeoutDuration.compareTo(nanosSince(start)) > 0) {
            try (Response response = httpCallFactory.newCall(headRequest).execute()) {
                switch (response.code()) {
                    case HTTP_OK:
                        return true;
                    case HTTP_UNAUTHORIZED:
                        return false;
                    case HTTP_BAD_METHOD:
                        throw new UnsupportedOperationException("Trino server does not support HEAD /v1/statement");
                }

                try {
                    MILLISECONDS.sleep(250);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            catch (IOException e) {
                if (getCausalChain(e).stream().anyMatch(TrinoConnection::isTransientConnectionValidationException)) {
                    lastException = e;
                }
                else {
                    throw e;
                }
            }
        }

        throw new IOException(format("Connection validation timed out after %ss", timeout), lastException);
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
        return new TrinoDatabaseMetaData(this, assumeLiteralNamesInMetadataCallsForNonConformingClients, assumeLiteralUnderscoreInMetadataCallsForNonConformingClients);
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
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
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

        if (isClosed()) {
            return false;
        }

        if (!validateConnection) {
            return true;
        }

        try {
            return isConnectionValid(timeout);
        }
        catch (UnsupportedOperationException e) {
            logger.log(Level.FINE, "Trino server does not support connection validation", e);
            return false;
        }
        catch (IOException e) {
            logger.log(Level.FINE, "Connection validation has failed", e);
            return false;
        }
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

    public void setSessionUser(String sessionUser)
    {
        requireNonNull(sessionUser, "sessionUser is null");
        this.sessionUser.set(Optional.of(sessionUser));
    }

    public void clearSessionUser()
    {
        this.sessionUser.set(Optional.empty());
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

        ClientSession session = ClientSession.builder()
                .server(httpUri)
                .user(user)
                .sessionUser(sessionUser.get())
                .authorizationUser(Optional.ofNullable(authorizationUser.get()))
                .source(source)
                .traceToken(Optional.ofNullable(clientInfo.get(TRACE_TOKEN)))
                .clientTags(ImmutableSet.copyOf(clientTags))
                .clientInfo(clientInfo.get(CLIENT_INFO))
                .catalog(catalog.get())
                .schema(schema.get())
                .path(path.get())
                .timeZone(timeZoneId.get())
                .locale(locale.get())
                .properties(ImmutableMap.copyOf(allProperties))
                .preparedStatements(ImmutableMap.copyOf(preparedStatements))
                .roles(ImmutableMap.copyOf(roles))
                .credentials(extraCredentials)
                .transactionId(transactionId.get())
                .clientRequestTimeout(timeout)
                .compressionDisabled(compressionDisabled)
                .encoding(encoding)
                .build();

        return newStatementClient(httpCallFactory, segmentHttpCallFactory, session, sql);
    }

    void updateSession(StatementClient client)
    {
        sessionProperties.putAll(client.getSetSessionProperties());
        client.getResetSessionProperties().forEach(sessionProperties::remove);

        preparedStatements.putAll(client.getAddedPreparedStatements());
        client.getDeallocatedPreparedStatements().forEach(preparedStatements::remove);

        roles.putAll(client.getSetRoles());

        client.getSetCatalog().ifPresent(catalog::set);
        client.getSetSchema().ifPresent(schema::set);
        client.getSetPath().ifPresent(path::set);

        if (client.getSetAuthorizationUser().isPresent()) {
            authorizationUser.set(client.getSetAuthorizationUser().get());
            roles.clear();
        }
        if (client.isResetAuthorizationUser()) {
            authorizationUser.set(null);
            roles.clear();
        }

        if (client.getStartedTransactionId() != null) {
            transactionId.set(client.getStartedTransactionId());
        }
        if (client.isClearTransactionId()) {
            transactionId.set(null);
        }
    }

    void removePreparedStatement(String name)
    {
        preparedStatements.remove(name);
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

    @VisibleForTesting
    String getAuthorizationUser()
    {
        return authorizationUser.get();
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

    private static boolean isTransientConnectionValidationException(Throwable e)
    {
        if (e instanceof InterruptedIOException && e.getMessage().equals("timeout")) {
            return true;
        }
        return e instanceof ProtocolException;
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

    public boolean useExplicitPrepare()
    {
        return this.useExplicitPrepare;
    }

    public boolean getAssumeNullCatalogMeansCurrentCatalog()
    {
        return this.assumeNullCatalogMeansCurrentCatalog;
    }
}
