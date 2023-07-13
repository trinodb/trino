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
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ProtocolHeaders
{
    public static final ProtocolHeaders TRINO_HEADERS = new ProtocolHeaders("Trino");

    private final String name;
    private final String requestUser;
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
        String prefix = "X-" + name + "-";
        requestUser = prefix + "User";
        requestSource = prefix + "Source";
        requestCatalog = prefix + "Catalog";
        requestSchema = prefix + "Schema";
        requestPath = prefix + "Path";
        requestTimeZone = prefix + "Time-Zone";
        requestLanguage = prefix + "Language";
        requestTraceToken = prefix + "Trace-Token";
        requestSession = prefix + "Session";
        requestRole = prefix + "Role";
        requestPreparedStatement = prefix + "Prepared-Statement";
        requestTransactionId = prefix + "Transaction-Id";
        requestClientInfo = prefix + "Client-Info";
        requestClientTags = prefix + "Client-Tags";
        requestClientCapabilities = prefix + "Client-Capabilities";
        requestResourceEstimate = prefix + "Resource-Estimate";
        requestExtraCredential = prefix + "Extra-Credential";
        responseSetCatalog = prefix + "Set-Catalog";
        responseSetSchema = prefix + "Set-Schema";
        responseSetPath = prefix + "Set-Path";
        responseSetSession = prefix + "Set-Session";
        responseClearSession = prefix + "Clear-Session";
        responseSetRole = prefix + "Set-Role";
        responseAddedPrepare = prefix + "Added-Prepare";
        responseDeallocatedPrepare = prefix + "Deallocated-Prepare";
        responseStartedTransactionId = prefix + "Started-Transaction-Id";
        responseClearTransactionId = prefix + "Clear-Transaction-Id";
    }

    public String getProtocolName()
    {
        return name;
    }

    public String requestUser()
    {
        return requestUser;
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
