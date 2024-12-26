package io.trino.adbc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.client.ClientInfoProperty;
import io.trino.client.ClientSelectedRole;
import io.trino.client.StatementClient;
import io.trino.client.StatementClientFactory;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.client.ClientInfoProperty.*;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TrinoConnection implements AdbcConnection {
    private static final Logger logger = Logger.getLogger(TrinoConnection.class.getPackage().getName());

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

    public TrinoConnection(TrinoDriverUri uri, Call.Factory httpCallFactory, Call.Factory segmentHttpCallFactory) {
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
    }

    @Override
    public AdbcStatement createStatement() throws AdbcException {
        checkOpen();
        TrinoStatement statement = new TrinoStatement(this, this::unregisterStatement);
        registerStatement(statement);
        return statement;    }

    @Override
    public ArrowReader getInfo(int @Nullable [] infoCodes) throws AdbcException {
        return null;
    }

    private void checkOpen()
            throws AdbcException
    {
        if (isClosed()) {
            throw AdbcException.invalidState("Connection is closed");
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

    boolean shouldStartTransaction()
    {
        return !autoCommit.get() && (transactionId.get() == null);
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public void close() throws Exception {

    }
}
