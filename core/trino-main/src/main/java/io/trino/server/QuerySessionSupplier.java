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
package io.trino.server;

import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.SqlPath;
import io.trino.transaction.TransactionManager;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.Session.SessionBuilder;
import static io.trino.SystemSessionProperties.TIME_ZONE_ID;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QuerySessionSupplier
        implements SessionSupplier
{
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final Optional<String> path;
    private final Optional<TimeZoneKey> forcedSessionTimeZone;
    private final Optional<String> defaultCatalog;
    private final Optional<String> defaultSchema;

    @Inject
    public QuerySessionSupplier(
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            SqlEnvironmentConfig config)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        requireNonNull(config, "config is null");
        this.path = requireNonNull(config.getPath(), "path is null");
        this.forcedSessionTimeZone = requireNonNull(config.getForcedSessionTimeZone(), "forcedSessionTimeZone is null");
        this.defaultCatalog = requireNonNull(config.getDefaultCatalog(), "defaultCatalog is null");
        this.defaultSchema = requireNonNull(config.getDefaultSchema(), "defaultSchema is null");

        checkArgument(defaultCatalog.isPresent() || defaultSchema.isEmpty(), "Default schema cannot be set if catalog is not set");
    }

    @Override
    public Session createSession(QueryId queryId, SessionContext context)
    {
        Identity identity = context.getIdentity();
        accessControl.checkCanSetUser(identity.getPrincipal(), identity.getUser());

        // authenticated identity is not present for HTTP or if authentication is not setup
        context.getAuthenticatedIdentity().ifPresent(authenticatedIdentity -> {
            // only check impersonation if authenticated user is not the same as the explicitly set user
            if (!authenticatedIdentity.getUser().equals(identity.getUser())) {
                accessControl.checkCanImpersonateUser(authenticatedIdentity, identity.getUser());
            }
        });

        SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                .setQueryId(queryId)
                .setIdentity(identity)
                .setSource(context.getSource())
                .setPath(new SqlPath(path))
                .setRemoteUserAddress(context.getRemoteUserAddress())
                .setUserAgent(context.getUserAgent())
                .setClientInfo(context.getClientInfo())
                .setClientTags(context.getClientTags())
                .setClientCapabilities(context.getClientCapabilities())
                .setTraceToken(context.getTraceToken())
                .setResourceEstimates(context.getResourceEstimates())
                .setProtocolHeaders(context.getProtocolHeaders());

        defaultCatalog.ifPresent(sessionBuilder::setCatalog);
        defaultSchema.ifPresent(sessionBuilder::setSchema);

        if (context.getCatalog() != null) {
            sessionBuilder.setCatalog(context.getCatalog());
        }

        if (context.getSchema() != null) {
            sessionBuilder.setSchema(context.getSchema());
        }

        if (context.getPath() != null) {
            sessionBuilder.setPath(new SqlPath(Optional.of(context.getPath())));
        }

        if (forcedSessionTimeZone.isPresent()) {
            sessionBuilder.setTimeZoneKey(forcedSessionTimeZone.get());
        }
        else {
            String sessionTimeZoneId = context.getSystemProperties().get(TIME_ZONE_ID);
            if (sessionTimeZoneId != null) {
                sessionBuilder.setTimeZoneKey(getTimeZoneKey(sessionTimeZoneId));
            }
            else if (context.getTimeZoneId() != null) {
                sessionBuilder.setTimeZoneKey(getTimeZoneKey(context.getTimeZoneId()));
            }
        }

        if (context.getLanguage() != null) {
            sessionBuilder.setLocale(Locale.forLanguageTag(context.getLanguage()));
        }

        for (Entry<String, String> entry : context.getSystemProperties().entrySet()) {
            sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue());
        }
        for (Entry<String, Map<String, String>> catalogProperties : context.getCatalogSessionProperties().entrySet()) {
            String catalog = catalogProperties.getKey();
            for (Entry<String, String> entry : catalogProperties.getValue().entrySet()) {
                sessionBuilder.setCatalogSessionProperty(catalog, entry.getKey(), entry.getValue());
            }
        }

        for (Entry<String, String> preparedStatement : context.getPreparedStatements().entrySet()) {
            sessionBuilder.addPreparedStatement(preparedStatement.getKey(), preparedStatement.getValue());
        }

        if (context.supportClientTransaction()) {
            sessionBuilder.setClientTransactionSupport();
        }

        Session session = sessionBuilder.build();
        if (context.getTransactionId().isPresent()) {
            session = session.beginTransactionId(context.getTransactionId().get(), transactionManager, accessControl);
        }

        return session;
    }
}
