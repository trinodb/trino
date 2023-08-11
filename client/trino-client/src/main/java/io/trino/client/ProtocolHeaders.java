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
package io.trino.client;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_CATALOG;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_CLIENT_CAPABILITIES;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_CLIENT_INFO;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_CLIENT_TAGS;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_EXTRA_CREDENTIAL;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_LANGUAGE;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_ORIGINAL_ROLES;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_ORIGINAL_USER;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_PATH;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_PREPARED_STATEMENT;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_QUERY_DATA_ENCODING;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_RESOURCE_ESTIMATE;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_ROLE;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_SCHEMA;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_SESSION;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_SOURCE;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_TIME_ZONE;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_TRACE_TOKEN;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_TRANSACTION_ID;
import static io.trino.client.ProtocolHeaders.Headers.REQUEST_USER;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_ADDED_PREPARE;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_CLEAR_SESSION;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_CLEAR_TRANSACTION_ID;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_DEALLOCATED_PREPARE;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_QUERY_DATA_ENCODING;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_RESET_AUTHORIZATION_USER;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_SET_AUTHORIZATION_USER;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_SET_CATALOG;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_SET_ORIGINAL_ROLES;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_SET_PATH;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_SET_ROLE;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_SET_SCHEMA;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_SET_SESSION;
import static io.trino.client.ProtocolHeaders.Headers.RESPONSE_STARTED_TRANSACTION_ID;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ProtocolHeaders
{
    public static final ProtocolHeaders TRINO_HEADERS = new ProtocolHeaders("Trino");

    enum Headers
    {
        REQUEST_USER("User"),
        REQUEST_ORIGINAL_USER("Original-User"),
        REQUEST_ORIGINAL_ROLES("Original-Roles"),
        REQUEST_SOURCE("Source"),
        REQUEST_CATALOG("Catalog"),
        REQUEST_SCHEMA("Schema"),
        REQUEST_PATH("Path"),
        REQUEST_TIME_ZONE("Time-Zone"),
        REQUEST_LANGUAGE("Language"),
        REQUEST_TRACE_TOKEN("Trace-Token"),
        REQUEST_SESSION("Session"),
        REQUEST_ROLE("Role"),
        REQUEST_PREPARED_STATEMENT("Prepared-Statement"),
        REQUEST_TRANSACTION_ID("Transaction-Id"),
        REQUEST_CLIENT_INFO("Client-Info"),
        REQUEST_CLIENT_TAGS("Client-Tags"),
        REQUEST_CLIENT_CAPABILITIES("Client-Capabilities"),
        REQUEST_RESOURCE_ESTIMATE("Resource-Estimate"),
        REQUEST_EXTRA_CREDENTIAL("Extra-Credential"),
        REQUEST_QUERY_DATA_ENCODING("Query-Data-Encoding"),
        RESPONSE_SET_CATALOG("Set-Catalog"),
        RESPONSE_SET_SCHEMA("Set-Schema"),
        RESPONSE_SET_PATH("Set-Path"),
        RESPONSE_SET_SESSION("Set-Session"),
        RESPONSE_CLEAR_SESSION("Clear-Session"),
        RESPONSE_SET_ROLE("Set-Role"),
        RESPONSE_SET_ORIGINAL_ROLES("Set-Original-Roles"),
        RESPONSE_QUERY_DATA_ENCODING("Query-Data-Encoding"),
        RESPONSE_ADDED_PREPARE("Added-Prepare"),
        RESPONSE_DEALLOCATED_PREPARE("Deallocated-Prepare"),
        RESPONSE_STARTED_TRANSACTION_ID("Started-Transaction-Id"),
        RESPONSE_CLEAR_TRANSACTION_ID("Clear-Transaction-Id"),
        RESPONSE_SET_AUTHORIZATION_USER("Set-Authorization-User"),
        RESPONSE_RESET_AUTHORIZATION_USER("Reset-Authorization-User");

        private final String headerName;

        Headers(final String headerName)
        {
            this.headerName = requireNonNull(headerName, "headerName is null");
        }

        public String withProtocolName(String protocolName)
        {
            return "X-" + protocolName + "-" + headerName;
        }
    }

    private final String name;
    private final String requestUser;
    private final String requestOriginalUser;
    private final String requestOriginalRole;
    private final String requestSource;
    private final String requestCatalog;
    private final String requestSchema;
    private final String requestPath;
    private final String requestTimeZone;
    private final String requestLanguage;
    private final String requestTraceToken;
    private final String requestSession;
    private final String requestRole;
    private final String requestPreparedStatement;
    private final String requestTransactionId;
    private final String requestClientInfo;
    private final String requestClientTags;
    private final String requestClientCapabilities;
    private final String requestResourceEstimate;
    private final String requestExtraCredential;
    private final String requestQueryDataEncoding;
    private final String responseSetCatalog;
    private final String responseSetSchema;
    private final String responseSetPath;
    private final String responseSetSession;
    private final String responseClearSession;
    private final String responseSetRole;
    private final String responseQueryDataEncoding;
    private final String responseAddedPrepare;
    private final String responseDeallocatedPrepare;
    private final String responseStartedTransactionId;
    private final String responseClearTransactionId;
    private final String responseSetAuthorizationUser;
    private final String responseResetAuthorizationUser;
    private final String responseOriginalRole;

    public static ProtocolHeaders createProtocolHeaders(String name)
    {
        // canonicalize trino name
        if (TRINO_HEADERS.getProtocolName().equalsIgnoreCase(name)) {
            return TRINO_HEADERS;
        }
        return new ProtocolHeaders(name);
    }

    private ProtocolHeaders(String name)
    {
        requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "name is empty");
        this.name = name;
        requestUser = REQUEST_USER.withProtocolName(name);
        requestOriginalUser = REQUEST_ORIGINAL_USER.withProtocolName(name);
        requestOriginalRole = REQUEST_ORIGINAL_ROLES.withProtocolName(name);
        requestSource = REQUEST_SOURCE.withProtocolName(name);
        requestCatalog = REQUEST_CATALOG.withProtocolName(name);
        requestSchema = REQUEST_SCHEMA.withProtocolName(name);
        requestPath = REQUEST_PATH.withProtocolName(name);
        requestTimeZone = REQUEST_TIME_ZONE.withProtocolName(name);
        requestLanguage = REQUEST_LANGUAGE.withProtocolName(name);
        requestTraceToken = REQUEST_TRACE_TOKEN.withProtocolName(name);
        requestSession = REQUEST_SESSION.withProtocolName(name);
        requestRole = REQUEST_ROLE.withProtocolName(name);
        requestPreparedStatement = REQUEST_PREPARED_STATEMENT.withProtocolName(name);
        requestTransactionId = REQUEST_TRANSACTION_ID.withProtocolName(name);
        requestClientInfo = REQUEST_CLIENT_INFO.withProtocolName(name);
        requestClientTags = REQUEST_CLIENT_TAGS.withProtocolName(name);
        requestClientCapabilities = REQUEST_CLIENT_CAPABILITIES.withProtocolName(name);
        requestResourceEstimate = REQUEST_RESOURCE_ESTIMATE.withProtocolName(name);
        requestExtraCredential = REQUEST_EXTRA_CREDENTIAL.withProtocolName(name);
        requestQueryDataEncoding = REQUEST_QUERY_DATA_ENCODING.withProtocolName(name);
        responseSetCatalog = RESPONSE_SET_CATALOG.withProtocolName(name);
        responseSetSchema = RESPONSE_SET_SCHEMA.withProtocolName(name);
        responseSetPath = RESPONSE_SET_PATH.withProtocolName(name);
        responseSetSession = RESPONSE_SET_SESSION.withProtocolName(name);
        responseClearSession = RESPONSE_CLEAR_SESSION.withProtocolName(name);
        responseSetRole = RESPONSE_SET_ROLE.withProtocolName(name);
        responseQueryDataEncoding = RESPONSE_QUERY_DATA_ENCODING.withProtocolName(name);
        responseAddedPrepare = RESPONSE_ADDED_PREPARE.withProtocolName(name);
        responseDeallocatedPrepare = RESPONSE_DEALLOCATED_PREPARE.withProtocolName(name);
        responseStartedTransactionId = RESPONSE_STARTED_TRANSACTION_ID.withProtocolName(name);
        responseClearTransactionId = RESPONSE_CLEAR_TRANSACTION_ID.withProtocolName(name);
        responseSetAuthorizationUser = RESPONSE_SET_AUTHORIZATION_USER.withProtocolName(name);
        responseResetAuthorizationUser = RESPONSE_RESET_AUTHORIZATION_USER.withProtocolName(name);
        responseOriginalRole = RESPONSE_SET_ORIGINAL_ROLES.withProtocolName(name);
    }

    public boolean isProtocolHeader(String headerName)
    {
        for (Headers header : Headers.values()) {
            if (header.withProtocolName(name).equalsIgnoreCase(headerName)) {
                return true;
            }
        }
        return false;
    }

    public String getProtocolName()
    {
        return name;
    }

    public String requestUser()
    {
        return requestUser;
    }

    public String requestOriginalUser()
    {
        return requestOriginalUser;
    }

    public String requestOriginalRole()
    {
        return requestOriginalRole;
    }

    public String requestSource()
    {
        return requestSource;
    }

    public String requestCatalog()
    {
        return requestCatalog;
    }

    public String requestSchema()
    {
        return requestSchema;
    }

    public String requestPath()
    {
        return requestPath;
    }

    public String requestTimeZone()
    {
        return requestTimeZone;
    }

    public String requestLanguage()
    {
        return requestLanguage;
    }

    public String requestTraceToken()
    {
        return requestTraceToken;
    }

    public String requestSession()
    {
        return requestSession;
    }

    public String requestRole()
    {
        return requestRole;
    }

    public String requestPreparedStatement()
    {
        return requestPreparedStatement;
    }

    public String requestTransactionId()
    {
        return requestTransactionId;
    }

    public String requestClientInfo()
    {
        return requestClientInfo;
    }

    public String requestClientTags()
    {
        return requestClientTags;
    }

    public String requestClientCapabilities()
    {
        return requestClientCapabilities;
    }

    public String requestResourceEstimate()
    {
        return requestResourceEstimate;
    }

    public String requestExtraCredential()
    {
        return requestExtraCredential;
    }

    public String requestQueryDataEncoding()
    {
        return requestQueryDataEncoding;
    }

    public String responseSetCatalog()
    {
        return responseSetCatalog;
    }

    public String responseSetSchema()
    {
        return responseSetSchema;
    }

    public String responseSetPath()
    {
        return responseSetPath;
    }

    public String responseSetSession()
    {
        return responseSetSession;
    }

    public String responseClearSession()
    {
        return responseClearSession;
    }

    public String responseSetRole()
    {
        return responseSetRole;
    }

    public String responseQueryDataEncoding()
    {
        return responseQueryDataEncoding;
    }

    public String responseAddedPrepare()
    {
        return responseAddedPrepare;
    }

    public String responseDeallocatedPrepare()
    {
        return responseDeallocatedPrepare;
    }

    public String responseStartedTransactionId()
    {
        return responseStartedTransactionId;
    }

    public String responseClearTransactionId()
    {
        return responseClearTransactionId;
    }

    public String responseSetAuthorizationUser()
    {
        return responseSetAuthorizationUser;
    }

    public String responseResetAuthorizationUser()
    {
        return responseResetAuthorizationUser;
    }

    public String responseOriginalRole()
    {
        return responseOriginalRole;
    }

    public static ProtocolHeaders detectProtocol(Optional<String> alternateHeaderName, Set<String> headerNames)
            throws ProtocolDetectionException
    {
        requireNonNull(alternateHeaderName, "alternateHeaderName is null");
        requireNonNull(headerNames, "headerNames is null");

        if (alternateHeaderName.isPresent() && !alternateHeaderName.get().equalsIgnoreCase("Trino")) {
            String headerPrefix = "x-" + alternateHeaderName.get().toLowerCase(ENGLISH);
            if (headerNames.stream().anyMatch(header -> header.toLowerCase(ENGLISH).startsWith(headerPrefix))) {
                if (headerNames.stream().anyMatch(header -> header.toLowerCase(ENGLISH).startsWith("x-trino-"))) {
                    throw new ProtocolDetectionException("Both Trino and " + alternateHeaderName.get() + " headers detected");
                }
                return createProtocolHeaders(alternateHeaderName.get());
            }
        }

        return TRINO_HEADERS;
    }
}
