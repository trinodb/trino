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
import com.google.errorprone.annotations.FormatMethod;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session.ResourceEstimateBuilder;
import io.trino.client.ProtocolDetectionException;
import io.trino.client.ProtocolHeaders;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.server.protocol.PreparedStatementEncoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.SelectedRole.Type;
import io.trino.spi.session.ResourceEstimates;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.transaction.TransactionId;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;

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
import static io.trino.server.ServletSecurityUtils.authenticatedIdentity;
import static io.trino.spi.security.AccessDeniedException.denySetRole;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HttpRequestSessionContextFactory
{
    private final PreparedStatementEncoder preparedStatementEncoder;
    private final Metadata metadata;
    private final GroupProvider groupProvider;
    private final AccessControl accessControl;
    private final Optional<String> alternateHeaderName;
    private final QueryDataEncoder.EncoderSelector encoderSelector;

    @Inject
    public HttpRequestSessionContextFactory(
            PreparedStatementEncoder preparedStatementEncoder,
            Metadata metadata,
            GroupProvider groupProvider,
            AccessControl accessControl,
            ProtocolConfig protocolConfig,
            QueryDataEncoder.EncoderSelector encoderSelector)
    {
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
        this.preparedStatementEncoder = requireNonNull(preparedStatementEncoder, "preparedStatementEncoder is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.encoderSelector = requireNonNull(encoderSelector, "encoderSelector is null");
    }

    public SessionContext createSessionContext(
            MultivaluedMap<String, String> headers,
            Optional<String> remoteAddress,
            Optional<Identity> authenticatedIdentity)
    {
        ProtocolHeaders protocolHeaders;
        try {
            protocolHeaders = detectProtocol(alternateHeaderName, headers.keySet());
        }
        catch (ProtocolDetectionException e) {
            throw new BadRequestException(e.getMessage());
        }
        Optional<String> catalog = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestCatalog())));
        Optional<String> schema = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestSchema())));
        Optional<String> path = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestPath())));
        assertRequest(catalog.isPresent() || schema.isEmpty(), "Schema is set but catalog is not");

        requireNonNull(authenticatedIdentity, "authenticatedIdentity is null");
        Identity identity = buildSessionIdentity(authenticatedIdentity, protocolHeaders, headers);
        Identity originalIdentity = buildSessionOriginalIdentity(identity, protocolHeaders, headers);
        SelectedRole selectedRole = parseSystemRoleHeaders(protocolHeaders, headers);

        Optional<String> source = Optional.ofNullable(headers.getFirst(protocolHeaders.requestSource()));
        Optional<String> traceToken = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestTraceToken())));
        Optional<String> userAgent = Optional.ofNullable(headers.getFirst(USER_AGENT));
        Optional<String> remoteUserAddress = requireNonNull(remoteAddress, "remoteAddress is null");
        Optional<String> timeZoneId = Optional.ofNullable(headers.getFirst(protocolHeaders.requestTimeZone()));
        Optional<String> language = Optional.ofNullable(headers.getFirst(protocolHeaders.requestLanguage()));
        Optional<String> clientInfo = Optional.ofNullable(headers.getFirst(protocolHeaders.requestClientInfo()));
        Optional<String> queryDataEncoding = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestQueryDataEncoding())))
                .map(Splitter.on(',').trimResults().omitEmptyStrings()::splitToList)
                .flatMap(encoderSelector::select)
                .map(QueryDataEncoder.Factory::encoding);

        Set<String> clientTags = parseClientTags(protocolHeaders, headers);
        Set<String> clientCapabilities = parseClientCapabilities(protocolHeaders, headers);
        ResourceEstimates resourceEstimates = parseResourceEstimate(protocolHeaders, headers);

        // parse session properties
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        for (Entry<String, String> entry : parseSessionHeaders(protocolHeaders, headers).entrySet()) {
            String fullPropertyName = entry.getKey();
            String propertyValue = entry.getValue();

            switch (parseSessionPropertyName(fullPropertyName)) {
                case ParsedSessionPropertyName(Optional<String> catalogName, String propertyName) when catalogName.isEmpty() -> {
                    assertRequest(!propertyName.isEmpty(), "Invalid %s header", protocolHeaders.requestSession());

                    // catalog session properties cannot be validated until the transaction has started, so we delay system property validation also
                    systemProperties.put(propertyName, propertyValue);
                }
                case ParsedSessionPropertyName(Optional<String> catalogName, String propertyName) -> {
                    assertRequest(!catalogName.orElseThrow().isEmpty(), "Invalid %s header", protocolHeaders.requestSession());
                    assertRequest(!propertyName.isEmpty(), "Invalid %s header", protocolHeaders.requestSession());

                    // catalog session properties cannot be validated until the transaction has started
                    catalogSessionProperties.computeIfAbsent(catalogName.orElseThrow(), id -> new HashMap<>()).put(propertyName, propertyValue);
                }
                default -> throw new BadRequestException(format("Invalid %s header", protocolHeaders.requestSession()));
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
                originalIdentity,
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
                systemProperties.buildOrThrow(),
                catalogSessionProperties,
                preparedStatements,
                transactionId,
                clientTransactionSupport,
                clientInfo,
                queryDataEncoding);
    }

    public Identity extractAuthorizedIdentity(HttpServletRequest servletRequest, HttpHeaders httpHeaders)
    {
        return extractAuthorizedIdentity(authenticatedIdentity(servletRequest), httpHeaders.getRequestHeaders());
    }

    public Identity extractAuthorizedIdentity(Optional<Identity> optionalAuthenticatedIdentity, MultivaluedMap<String, String> headers)
            throws AccessDeniedException
    {
        ProtocolHeaders protocolHeaders;
        try {
            protocolHeaders = detectProtocol(alternateHeaderName, headers.keySet());
        }
        catch (ProtocolDetectionException e) {
            throw new BadRequestException(e.getMessage());
        }

        Identity identity = buildSessionIdentity(optionalAuthenticatedIdentity, protocolHeaders, headers);
        Identity originalIdentity = buildSessionOriginalIdentity(identity, protocolHeaders, headers);

        accessControl.checkCanSetUser(originalIdentity.getPrincipal(), originalIdentity.getUser());

        // authenticated may not present for HTTP or if authentication is not setup
        optionalAuthenticatedIdentity.ifPresent(authenticatedIdentity -> {
            // only check impersonation if authenticated user is not the same as the explicitly set user
            if (!authenticatedIdentity.getUser().equals(originalIdentity.getUser())) {
                // load enabled roles for authenticated identity, so impersonation permissions can be assigned to roles
                authenticatedIdentity = Identity.from(authenticatedIdentity)
                        .withEnabledRoles(metadata.listEnabledRoles(authenticatedIdentity))
                        .build();
                accessControl.checkCanImpersonateUser(authenticatedIdentity, originalIdentity.getUser());
            }
        });

        if (!originalIdentity.getUser().equals(identity.getUser())) {
            accessControl.checkCanSetUser(originalIdentity.getPrincipal(), identity.getUser());
            accessControl.checkCanImpersonateUser(originalIdentity, identity.getUser());
        }

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
        Identity newIdentity = authenticatedIdentity
                .map(identity -> Identity.from(identity).withUser(user))
                .orElseGet(() -> Identity.forUser(user))
                .withAdditionalConnectorRoles(parseConnectorRoleHeaders(protocolHeaders, headers))
                .withAdditionalExtraCredentials(parseExtraCredentials(protocolHeaders, headers))
                .withAdditionalGroups(groupProvider.getGroups(user))
                .build();
        return addEnabledRoles(newIdentity, systemRole, metadata);
    }

    private Identity buildSessionOriginalIdentity(Identity identity, ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        // We derive original identity using this header, but older clients will not send it, so fall back to identity
        Optional<String> optionalOriginalUser = Optional
                .ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestOriginalUser())));
        Optional<String> originalRoles = Optional.ofNullable(trimEmptyToNull(headers.getFirst(protocolHeaders.requestOriginalRole())));
        Identity originalIdentity = optionalOriginalUser.map(originalUser -> {
            Identity newIdentity = Identity.from(identity)
                    .withUser(originalUser)
                    .withExtraCredentials(new HashMap<>())
                    .withGroups(groupProvider.getGroups(originalUser))
                    .build();
            if (originalRoles.isPresent()) {
                newIdentity = addEnabledRoles(newIdentity, SelectedRole.valueOf(originalRoles.get()), metadata);
            }
            return newIdentity;
        }).orElse(identity);
        return originalIdentity;
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
        return roles.buildOrThrow();
    }

    private static SelectedRole toSelectedRole(ProtocolHeaders protocolHeaders, String value)
    {
        SelectedRole role;
        try {
            role = SelectedRole.valueOf(value);
        }
        catch (IllegalArgumentException e) {
            throw new BadRequestException(format("Invalid %s header", protocolHeaders.requestRole()));
        }
        return role;
    }

    private static Map<String, String> parseExtraCredentials(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        Map<String, String> credentials = parseProperty(headers, protocolHeaders.requestExtraCredential());
        for (String name : credentials.keySet()) {
            assertRequest(!name.startsWith("internal$"), "Invalid extra credential name: %s", name);
        }
        return credentials;
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
                throw new BadRequestException(format("Invalid %s header: %s", headerName, e));
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
                throw new BadRequestException(format("Unsupported resource name %s", name));
            }
            catch (IllegalArgumentException e) {
                throw new BadRequestException(format("Unsupported format for resource estimate '%s': %s", value, e));
            }
        });

        return builder.build();
    }

    private static ParsedSessionPropertyName parseSessionPropertyName(String value)
    {
        int lastDotIndex = value.lastIndexOf('.');
        if (lastDotIndex == -1) {
            return new ParsedSessionPropertyName(Optional.empty(), value);
        }
        return new ParsedSessionPropertyName(Optional.of(value.substring(0, lastDotIndex)), value.substring(lastDotIndex + 1));
    }

    @FormatMethod
    private static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw new BadRequestException(format(format, args));
        }
    }

    private Map<String, String> parsePreparedStatementsHeaders(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        ImmutableMap.Builder<String, String> preparedStatements = ImmutableMap.builder();
        parseProperty(headers, protocolHeaders.requestPreparedStatement()).forEach((key, value) -> {
            String statementName;
            try {
                statementName = urlDecode(key);
            }
            catch (IllegalArgumentException e) {
                throw new BadRequestException(format("Invalid %s header: %s", protocolHeaders.requestPreparedStatement(), e.getMessage()));
            }
            String sqlString = preparedStatementEncoder.decodePreparedStatementFromHeader(value);

            // Validate statement
            SqlParser sqlParser = new SqlParser();
            try {
                sqlParser.createStatement(sqlString);
            }
            catch (ParsingException e) {
                throw new BadRequestException(format("Invalid %s header: %s", protocolHeaders.requestPreparedStatement(), e.getMessage()));
            }

            preparedStatements.put(statementName, sqlString);
        });

        return preparedStatements.buildOrThrow();
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
            throw new BadRequestException(e.getMessage());
        }
    }

    private static String trimEmptyToNull(String value)
    {
        return emptyToNull(nullToEmpty(value).trim());
    }

    private static String urlDecode(String value)
    {
        return URLDecoder.decode(value, UTF_8);
    }

    private record ParsedSessionPropertyName(Optional<String> catalog, String name)
    {
        ParsedSessionPropertyName
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(name, "name is null");
        }
    }
}
