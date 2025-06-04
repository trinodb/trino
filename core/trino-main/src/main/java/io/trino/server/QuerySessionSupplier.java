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

import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.SqlPath;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.Session.SessionBuilder;
import static io.trino.SystemSessionProperties.TIME_ZONE_ID;
import static io.trino.server.HttpRequestSessionContextFactory.addEnabledRoles;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QuerySessionSupplier
        implements SessionSupplier
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final String defaultPath;
    private final Optional<TimeZoneKey> forcedSessionTimeZone;
    private final Optional<String> defaultCatalog;
    private final Optional<String> defaultSchema;

    @Inject
    public QuerySessionSupplier(
            Metadata metadata,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            SqlEnvironmentConfig config)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.defaultPath = requireNonNull(config.getPath(), "path is null");
        this.forcedSessionTimeZone = requireNonNull(config.getForcedSessionTimeZone(), "forcedSessionTimeZone is null");
        this.defaultCatalog = requireNonNull(config.getDefaultCatalog(), "defaultCatalog is null");
        this.defaultSchema = requireNonNull(config.getDefaultSchema(), "defaultSchema is null");

        checkArgument(defaultCatalog.isPresent() || defaultSchema.isEmpty(), "Default schema cannot be set if catalog is not set");
    }

    @Override
    public Session createSession(QueryId queryId, Span querySpan, SessionContext context)
    {
        Identity originalIdentity = context.getOriginalIdentity();
        accessControl.checkCanSetUser(originalIdentity.getPrincipal(), originalIdentity.getUser());

        // authenticated identity is not present for HTTP or if authentication is not setup
        if (context.getAuthenticatedIdentity().isPresent()) {
            Identity authenticatedIdentity = context.getAuthenticatedIdentity().get();
            // only check impersonation if authenticated user is not the same as the explicitly set user
            if (!authenticatedIdentity.getUser().equals(originalIdentity.getUser())) {
                // add enabled roles for authenticated identity, so impersonation permissions can be assigned to roles
                authenticatedIdentity = addEnabledRoles(authenticatedIdentity, context.getSelectedRole(), metadata);
                accessControl.checkCanImpersonateUser(authenticatedIdentity, originalIdentity.getUser());
            }
        }

        Identity identity = context.getIdentity();
        if (!originalIdentity.getUser().equals(identity.getUser())) {
            // When the current user (user) and the original user are different, we check if the original user can impersonate current user.
            // We preserve the information of original user in the originalIdentity,
            // and it will be used for the impersonation checks and be used as the source of audit information.
            accessControl.checkCanSetUser(originalIdentity.getPrincipal(), identity.getUser());
            accessControl.checkCanImpersonateUser(originalIdentity, identity.getUser());
        }

        // add the enabled roles
        identity = addEnabledRoles(identity, context.getSelectedRole(), metadata);

        SqlPath path = SqlPath.buildPath(context.getPath().orElse(defaultPath), context.getCatalog());
        SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                .setQueryId(queryId)
                .setQuerySpan(querySpan)
                .setIdentity(identity)
                .setOriginalIdentity(originalIdentity)
                .setPath(path)
                .setSource(context.getSource())
                .setRemoteUserAddress(context.getRemoteUserAddress())
                .setUserAgent(context.getUserAgent())
                .setClientInfo(context.getClientInfo())
                .setClientTags(context.getClientTags())
                .setClientCapabilities(context.getClientCapabilities())
                .setTraceToken(context.getTraceToken())
                .setResourceEstimates(context.getResourceEstimates())
                .setProtocolHeaders(context.getProtocolHeaders())
                .setQueryDataEncoding(context.getQueryDataEncoding());

        if (context.getCatalog().isPresent()) {
            sessionBuilder.setCatalog(context.getCatalog());
            sessionBuilder.setSchema(context.getSchema());
        }
        else {
            defaultCatalog.ifPresent(sessionBuilder::setCatalog);
            defaultSchema.ifPresent(sessionBuilder::setSchema);
        }

        if (forcedSessionTimeZone.isPresent()) {
            sessionBuilder.setTimeZoneKey(forcedSessionTimeZone.get());
        }
        else {
            sessionBuilder.setTimeZoneKey(context.getTimeZoneId().map(TimeZoneKey::getTimeZoneKey));
        }

        context.getLanguage().ifPresent(s -> sessionBuilder.setLocale(Locale.forLanguageTag(s)));

        for (Entry<String, String> entry : context.getSystemProperties().entrySet()) {
            if (entry.getKey().equals(TIME_ZONE_ID) && forcedSessionTimeZone.isPresent()) {
                // Skip setting time zone id from session when forced session is set
                continue;
            }
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

        return sessionBuilder.build();
    }
}
