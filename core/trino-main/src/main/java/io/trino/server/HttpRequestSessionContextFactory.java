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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session.ResourceEstimateBuilder;
import io.trino.client.ProtocolDetectionException;
import io.trino.client.ProtocolHeaders;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.SelectedRole.Type;
import io.trino.spi.session.ResourceEstimates;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.transaction.TransactionId;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.trino.client.ProtocolHeaders.detectProtocol;
import static io.trino.spi.security.AccessDeniedException.denySetRole;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HttpRequestSessionContextFactory
{
    private static final Splitter DOT_SPLITTER = Splitter.on('.');
    public static final String AUTHENTICATED_IDENTITY = "trino.authenticated-identity";

    private final Metadata metadata;
    private final GroupProvider groupProvider;
    private final AccessControl accessControl;

    @Inject
    public HttpRequestSessionContextFactory(Metadata metadata, GroupProvider groupProvider, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    public SessionContext createSessionContext(
            MultivaluedMap<String, String> headers,
            Optional<String> alternateHeaderName,
            Optional<String> remoteAddress,
            Optional<Identity> authenticatedIdentity)
            throws WebApplicationException
    {
        ProtocolHeaders protocolHeaders;
        try {
            protocolHeaders = detectProtocol(alternateHeaderName, headers.keySet());
        }
        catch (ProtocolDetectionException e) {
            throw badRequest(e.getMessage());
        }
        Optional<String> catalog = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestCatalog())));
        Optional<String> schema = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestSchema())));
        Optional<String> path = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestPath())));
        assertRequest((catalog.isPresent()) || (schema.isEmpty()), "Schema is set but catalog is not");

        requireNonNull(authenticatedIdentity, "authenticatedIdentity is null");
        Identity identity = buildSessionIdentity(authenticatedIdentity, protocolHeaders, headers);
        SelectedRole selectedRole = parseSystemRoleHeaders(protocolHeaders, headers);

        Optional<String> source = Optional.ofNullable(headers.getFirst(protocolHeaders.requestSource()));
        Optional<String> traceToken = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestTraceToken())));
        Optional<String> userAgent = Optional.ofNullable(headers.getFirst(USER_AGENT));
        Optional<String> remoteUserAddress = requireNonNull(remoteAddress, "remoteAddress is null");
        Optional<String> timeZoneId = Optional.ofNullable(headers.getFirst(protocolHeaders.requestTimeZone()));
        Optional<String> language = Optional.ofNullable(headers.getFirst(protocolHeaders.requestLanguage()));
        Optional<String> clientInfo = Optional.ofNullable(headers.getFirst(protocolHeaders.requestClientInfo()));
        Set<String> clientTags = parseClientTags(protocolHeaders, headers);
        Set<String> clientCapabilities = parseClientCapabilities(protocolHeaders, headers);
        ResourceEstimates resourceEstimates = parseResourceEstimate(protocolHeaders, headers);

        // parse session properties
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        for (Entry<String, String> entry : parseSessionHeaders(protocolHeaders, headers).entrySet()) {
            String fullPropertyName = entry.getKey();
            String propertyValue = entry.getValue();
            List<String> nameParts = DOT_SPLITTER.splitToList(fullPropertyName);
            if (nameParts.size() == 1) {
                String propertyName = nameParts.get(0);

                assertRequest(!propertyName.isEmpty(), "Invalid %s header", protocolHeaders.requestSession());

                // catalog session properties cannot be validated until the transaction has stated, so we delay system property validation also
                systemProperties.put(propertyName, propertyValue);
            }
            else if (nameParts.size() == 2) {
                String catalogName = nameParts.get(0);
                String propertyName = nameParts.get(1);

                assertRequest(!catalogName.isEmpty(), "Invalid %s header", protocolHeaders.requestSession());
                assertRequest(!propertyName.isEmpty(), "Invalid %s header", protocolHeaders.requestSession());

                // catalog session properties cannot be validated until the transaction has stated
                catalogSessionProperties.computeIfAbsent(catalogName, id -> new HashMap<>()).put(propertyName, propertyValue);
            }
            else {
                throw badRequest(format("Invalid %s header", protocolHeaders.requestSession()));
            }
        }
        requireNonNull(catalogSessionProperties, "catalogSessionProperties is null");
        catalogSessionProperties = catalogSessionProperties.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));

        Map<String, String> preparedStatements = parsePreparedStatementsHeaders(protocolHeaders, headers);

        String transactionIdHeader = headers.getFirst(protocolHeaders.requestTransactionId());
        boolean clientTransactionSupport = transactionIdHeader != null;
        Optional<TransactionId> transactionId = parseTransactionId(transactionIdHeader);
        return new SessionContext(
                protocolHeaders,
                catalog,
                schema,
                path,
                authenticatedIdentity,
                identity,
                selectedRole,
                source,
                traceToken,
                userAgent,
                remoteUserAddress,
                timeZoneId,
                language,
                clientTags,
                clientCapabilities,
                resourceEstimates,
                systemProperties.build(),
                catalogSessionProperties,
                preparedStatements,
                transactionId,
                clientTransactionSupport,
                clientInfo);
    }

    public Identity extractAuthorizedIdentity(
            HttpServletRequest servletRequest,
            HttpHeaders httpHeaders,
            Optional<String> alternateHeaderName)
    {
        return extractAuthorizedIdentity(
                Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY)),
                httpHeaders.getRequestHeaders(),
                alternateHeaderName);
    }

    public Identity extractAuthorizedIdentity(
            Optional<Identity> optionalAuthenticatedIdentity,
            MultivaluedMap<String, String> headers,
            Optional<String> alternateHeaderName)
            throws AccessDeniedException
    {
        ProtocolHeaders protocolHeaders;
        try {
            protocolHeaders = detectProtocol(alternateHeaderName, headers.keySet());
        }
        catch (ProtocolDetectionException e) {
            throw badRequest(e.getMessage());
        }

        Identity identity = buildSessionIdentity(optionalAuthenticatedIdentity, protocolHeaders, headers);

        accessControl.checkCanSetUser(identity.getPrincipal(), identity.getUser());

        // authenticated may not present for HTTP or if authentication is not setup
        optionalAuthenticatedIdentity.ifPresent(authenticatedIdentity -> {
            // only check impersonation if authenticated user is not the same as the explicitly set user
            if (!authenticatedIdentity.getUser().equals(identity.getUser())) {
                // load enabled roles for authenticated identity, so impersonation permissions can be assigned to roles
                authenticatedIdentity = Identity.from(authenticatedIdentity)
                        .withEnabledRoles(metadata.listEnabledRoles(authenticatedIdentity))
                        .build();
                accessControl.checkCanImpersonateUser(authenticatedIdentity, identity.getUser());
            }
        });

        return addEnabledRoles(identity, parseSystemRoleHeaders(protocolHeaders, headers), metadata);
    }

    public static Identity addEnabledRoles(Identity identity, SelectedRole selectedRole, Metadata metadata)
    {
        if (selectedRole.getType() == Type.NONE) {
            return identity;
        }
        Set<String> enabledRoles = metadata.listEnabledRoles(identity);
        if (selectedRole.getType() == Type.ROLE) {
            String role = selectedRole.getRole().orElseThrow();
            if (!enabledRoles.contains(role)) {
                denySetRole(role);
            }
            enabledRoles = ImmutableSet.of(role);
        }
        return Identity.from(identity)
                .withEnabledRoles(enabledRoles)
                .build();
    }

    private Identity buildSessionIdentity(Optional<Identity> authenticatedIdentity, ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        String trinoUser = trimEmptyToNull(headers.getFirst(protocolHeaders.requestUser()));
        String user = trinoUser != null ? trinoUser : authenticatedIdentity.map(Identity::getUser).orElse(null);
        assertRequest(user != null, "User must be set");
        SelectedRole systemRole = parseSystemRoleHeaders(protocolHeaders, headers);
        ImmutableSet.Builder<String> systemEnabledRoles = ImmutableSet.builder();
        if (systemRole.getType() == Type.ROLE) {
            systemEnabledRoles.add(systemRole.getRole().orElseThrow());
        }
        return authenticatedIdentity
                .map(identity -> Identity.from(identity).withUser(user))
                .orElseGet(() -> Identity.forUser(user))
                .withEnabledRoles(systemEnabledRoles.build())
                .withAdditionalConnectorRoles(parseConnectorRoleHeaders(protocolHeaders, headers))
                .withAdditionalExtraCredentials(parseExtraCredentials(protocolHeaders, headers))
                .withAdditionalGroups(groupProvider.getGroups(user))
                .build();
    }

    private static List<String> splitHttpHeader(MultivaluedMap<String, String> headers, String name)
    {
        List<String> values = firstNonNull(headers.get(name), ImmutableList.of());
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return values.stream()
                .map(splitter::splitToList)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    private static Map<String, String> parseSessionHeaders(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        return parseProperty(headers, protocolHeaders.requestSession());
    }

    private static SelectedRole parseSystemRoleHeaders(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        return parseProperty(headers, protocolHeaders.requestRole()).entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase("system"))
                .map(Entry::getValue)
                .map(role -> toSelectedRole(protocolHeaders, role))
                .findFirst()
                .orElse(new SelectedRole(Type.ALL, Optional.empty()));
    }

    private static Map<String, SelectedRole> parseConnectorRoleHeaders(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        ImmutableMap.Builder<String, SelectedRole> roles = ImmutableMap.builder();
        parseProperty(headers, protocolHeaders.requestRole()).forEach((key, value) -> {
            if (key.equalsIgnoreCase("system")) {
                return;
            }
            roles.put(key, toSelectedRole(protocolHeaders, value));
        });
        return roles.build();
    }

    private static SelectedRole toSelectedRole(ProtocolHeaders protocolHeaders, String value)
    {
        SelectedRole role;
        try {
            role = SelectedRole.valueOf(value);
        }
        catch (IllegalArgumentException e) {
            throw badRequest(format("Invalid %s header", protocolHeaders.requestRole()));
        }
        return role;
    }

    private static Map<String, String> parseExtraCredentials(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        return parseProperty(headers, protocolHeaders.requestExtraCredential());
    }

    private static Map<String, String> parseProperty(MultivaluedMap<String, String> headers, String headerName)
    {
        Map<String, String> properties = new HashMap<>();
        for (String header : splitHttpHeader(headers, headerName)) {
            List<String> nameValue = Splitter.on('=').trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", headerName);
            try {
                properties.put(nameValue.get(0), urlDecode(nameValue.get(1)));
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Invalid %s header: %s", headerName, e));
            }
        }
        return properties;
    }

    private static Set<String> parseClientTags(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(headers.getFirst(protocolHeaders.requestClientTags()))));
    }

    private static Set<String> parseClientCapabilities(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(headers.getFirst(protocolHeaders.requestClientCapabilities()))));
    }

    private static ResourceEstimates parseResourceEstimate(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        ResourceEstimateBuilder builder = new ResourceEstimateBuilder();
        parseProperty(headers, protocolHeaders.requestResourceEstimate()).forEach((name, value) -> {
            try {
                switch (name.toUpperCase(ENGLISH)) {
                    case ResourceEstimates.EXECUTION_TIME:
                        builder.setExecutionTime(Duration.valueOf(value));
                        return;
                    case ResourceEstimates.CPU_TIME:
                        builder.setCpuTime(Duration.valueOf(value));
                        return;
                    case ResourceEstimates.PEAK_MEMORY:
                        builder.setPeakMemory(DataSize.valueOf(value));
                        return;
                }
                throw badRequest(format("Unsupported resource name %s", name));
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Unsupported format for resource estimate '%s': %s", value, e));
            }
        });

        return builder.build();
    }

    private static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    private static Map<String, String> parsePreparedStatementsHeaders(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        ImmutableMap.Builder<String, String> preparedStatements = ImmutableMap.builder();
        parseProperty(headers, protocolHeaders.requestPreparedStatement()).forEach((key, sqlString) -> {
            String statementName;
            try {
                statementName = urlDecode(key);
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Invalid %s header: %s", protocolHeaders.requestPreparedStatement(), e.getMessage()));
            }

            // Validate statement
            SqlParser sqlParser = new SqlParser();
            try {
                sqlParser.createStatement(sqlString, new ParsingOptions(AS_DOUBLE /* anything */));
            }
            catch (ParsingException e) {
                throw badRequest(format("Invalid %s header: %s", protocolHeaders.requestPreparedStatement(), e.getMessage()));
            }

            preparedStatements.put(statementName, sqlString);
        });

        return preparedStatements.build();
    }

    private static Optional<TransactionId> parseTransactionId(String transactionId)
    {
        transactionId = trimEmptyToNull(transactionId);
        if (transactionId == null || transactionId.equalsIgnoreCase("none")) {
            return Optional.empty();
        }
        try {
            return Optional.of(TransactionId.valueOf(transactionId));
        }
        catch (Exception e) {
            throw badRequest(e.getMessage());
        }
    }

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(message, Response
                .status(Status.BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(message)
                .build());
    }

    private static String trimEmptyToNull(String value)
    {
        return emptyToNull(nullToEmpty(value).trim());
    }

    private static String urlDecode(String value)
    {
        return URLDecoder.decode(value, UTF_8);
    }
}
