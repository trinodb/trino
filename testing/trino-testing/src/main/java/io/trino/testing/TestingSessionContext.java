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
package io.trino.testing;

import io.trino.Session;
import io.trino.server.SessionContext;

import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

public final class TestingSessionContext
{
    private TestingSessionContext() {}

    public static SessionContext fromSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new SessionContext(
                session.getProtocolHeaders(),
                session.getCatalog(),
                session.getSchema(),
                session.getPath().getRawPath(),
                Optional.empty(),
                session.getIdentity(),
                session.getSource(),
                session.getTraceToken(),
                session.getUserAgent(),
                session.getRemoteUserAddress(),
                Optional.of(session.getTimeZoneKey().getId()),
                Optional.of(session.getLocale().getLanguage()),
                session.getClientTags(),
                session.getClientCapabilities(),
                session.getResourceEstimates(),
                session.getSystemProperties(),
                session.getConnectorProperties().entrySet().stream()
                        .collect(toImmutableMap(entry -> entry.getKey().getCatalogName(), Entry::getValue)),
                session.getPreparedStatements(),
                session.getTransactionId(),
                session.isClientTransactionSupport(),
                session.getClientInfo());
    }
}
