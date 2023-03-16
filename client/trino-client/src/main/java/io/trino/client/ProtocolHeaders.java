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
    private final String prefix;

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
        this.prefix = "X-" + name + "-";
    }

    public String getProtocolName()
    {
        return name;
    }

    public String requestUser()
    {
        return prefix + "User";
    }

    public String requestSource()
    {
        return prefix + "Source";
    }

    public String requestCatalog()
    {
        return prefix + "Catalog";
    }

    public String requestSchema()
    {
        return prefix + "Schema";
    }

    public String requestPath()
    {
        return prefix + "Path";
    }

    public String requestTimeZone()
    {
        return prefix + "Time-Zone";
    }

    public String requestLanguage()
    {
        return prefix + "Language";
    }

    public String requestTraceToken()
    {
        return prefix + "Trace-Token";
    }

    public String requestSession()
    {
        return prefix + "Session";
    }

    public String requestRole()
    {
        return prefix + "Role";
    }

    public String requestPreparedStatement()
    {
        return prefix + "Prepared-Statement";
    }

    public String requestTransactionId()
    {
        return prefix + "Transaction-Id";
    }

    public String requestClientInfo()
    {
        return prefix + "Client-Info";
    }

    public String requestClientTags()
    {
        return prefix + "Client-Tags";
    }

    public String requestClientCapabilities()
    {
        return prefix + "Client-Capabilities";
    }

    public String requestResourceEstimate()
    {
        return prefix + "Resource-Estimate";
    }

    public String requestExtraCredential()
    {
        return prefix + "Extra-Credential";
    }

    public String responseSetCatalog()
    {
        return prefix + "Set-Catalog";
    }

    public String responseSetSchema()
    {
        return prefix + "Set-Schema";
    }

    public String responseSetPath()
    {
        return prefix + "Set-Path";
    }

    public String responseSetSession()
    {
        return prefix + "Set-Session";
    }

    public String responseClearSession()
    {
        return prefix + "Clear-Session";
    }

    public String responseSetRole()
    {
        return prefix + "Set-Role";
    }

    public String responseAddedPrepare()
    {
        return prefix + "Added-Prepare";
    }

    public String responseDeallocatedPrepare()
    {
        return prefix + "Deallocated-Prepare";
    }

    public String responseStartedTransactionId()
    {
        return prefix + "Started-Transaction-Id";
    }

    public String responseClearTransactionId()
    {
        return prefix + "Clear-Transaction-Id";
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
