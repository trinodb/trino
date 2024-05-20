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
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class ProtocolHeaders
{
    public static final ProtocolHeaders TRINO_HEADERS = new ProtocolHeaders("Trino", value -> value.toLowerCase(ENGLISH));

    private final String name;
    private final String requestUser;
    private final String requestOriginalUser;
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
    private final String responseSetCatalog;
    private final String responseSetSchema;
    private final String responseSetPath;
    private final String responseSetSession;
    private final String responseClearSession;
    private final String responseSetRole;
    private final String responseAddedPrepare;
    private final String responseDeallocatedPrepare;
    private final String responseStartedTransactionId;
    private final String responseClearTransactionId;
    private final String responseSetAuthorizationUser;
    private final String responseResetAuthorizationUser;

    public static ProtocolHeaders createProtocolHeaders(String name)
    {
        // canonicalize trino name
        if (TRINO_HEADERS.getProtocolName().equalsIgnoreCase(name)) {
            return TRINO_HEADERS;
        }
        return new ProtocolHeaders(name, identity());
    }

    public static ProtocolHeaders createProtocolHeadersPreservingCase(String name)
    {
        // canonicalize trino name
        if (TRINO_HEADERS.getProtocolName().equalsIgnoreCase(name)) {
            return new ProtocolHeaders(TRINO_HEADERS.getProtocolName(), identity());
        }
        return new ProtocolHeaders(name, identity());
    }

    private ProtocolHeaders(String name, Function<String, String> transform)
    {
        requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "name is empty");
        this.name = name;
        String prefix = "X-" + name + "-";
        requestUser = transform.apply(prefix + "User");
        requestOriginalUser = transform.apply(prefix + "Original-User");
        requestSource = transform.apply(prefix + "Source");
        requestCatalog = transform.apply(prefix + "Catalog");
        requestSchema = transform.apply(prefix + "Schema");
        requestPath = transform.apply(prefix + "Path");
        requestTimeZone = transform.apply(prefix + "Time-Zone");
        requestLanguage = transform.apply(prefix + "Language");
        requestTraceToken = transform.apply(prefix + "Trace-Token");
        requestSession = transform.apply(prefix + "Session");
        requestRole = transform.apply(prefix + "Role");
        requestPreparedStatement = transform.apply(prefix + "Prepared-Statement");
        requestTransactionId = transform.apply(prefix + "Transaction-Id");
        requestClientInfo = transform.apply(prefix + "Client-Info");
        requestClientTags = transform.apply(prefix + "Client-Tags");
        requestClientCapabilities = transform.apply(prefix + "Client-Capabilities");
        requestResourceEstimate = transform.apply(prefix + "Resource-Estimate");
        requestExtraCredential = transform.apply(prefix + "Extra-Credential");
        responseSetCatalog = transform.apply(prefix + "Set-Catalog");
        responseSetSchema = transform.apply(prefix + "Set-Schema");
        responseSetPath = transform.apply(prefix + "Set-Path");
        responseSetSession = transform.apply(prefix + "Set-Session");
        responseClearSession = transform.apply(prefix + "Clear-Session");
        responseSetRole = transform.apply(prefix + "Set-Role");
        responseAddedPrepare = transform.apply(prefix + "Added-Prepare");
        responseDeallocatedPrepare = transform.apply(prefix + "Deallocated-Prepare");
        responseStartedTransactionId = transform.apply(prefix + "Started-Transaction-Id");
        responseClearTransactionId = transform.apply(prefix + "Clear-Transaction-Id");
        responseSetAuthorizationUser = transform.apply(prefix + "Set-Authorization-User");
        responseResetAuthorizationUser = transform.apply(prefix + "Reset-Authorization-User");
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
