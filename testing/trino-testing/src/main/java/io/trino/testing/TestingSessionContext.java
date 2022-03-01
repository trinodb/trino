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
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.SelectedRole.Type;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class TestingSessionContext
{
    private TestingSessionContext() {}

    public static SessionContext fromSession(Session session)
    {
        requireNonNull(session, "session is null");

        Set<String> enabledRoles = session.getIdentity().getEnabledRoles();
        SelectedRole selectedRole;
        if (enabledRoles.isEmpty()) {
            selectedRole = new SelectedRole(Type.NONE, Optional.empty());
        }
        else if (enabledRoles.size() == 1) {
            selectedRole = new SelectedRole(Type.ROLE, Optional.of(enabledRoles.iterator().next()));
        }
        else {
            selectedRole = new SelectedRole(Type.ALL, Optional.empty());
        }

        return new SessionContext(
                session.getProtocolHeaders(),
                session.getCatalog(),
                session.getSchema(),
                session.getPath().getRawPath(),
                Optional.empty(),
                session.getIdentity(),
                selectedRole,
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
                session.getCatalogProperties(),
                session.getPreparedStatements(),
                session.getTransactionId(),
                session.isClientTransactionSupport(),
                session.getClientInfo(),
                session.getTracer());
    }
}
