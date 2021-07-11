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

import io.trino.client.ProtocolHeaders;
import io.trino.spi.security.Identity;
import io.trino.spi.session.ResourceEstimates;
import io.trino.transaction.TransactionId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface SessionContext
{
    ProtocolHeaders getProtocolHeaders();

    Optional<Identity> getAuthenticatedIdentity();

    Identity getIdentity();

    Optional<String> getCatalog();

    Optional<String> getSchema();

    Optional<String> getPath();

    Optional<String> getSource();

    Optional<String> getRemoteUserAddress();

    Optional<String> getUserAgent();

    Optional<String> getClientInfo();

    Set<String> getClientTags();

    Set<String> getClientCapabilities();

    ResourceEstimates getResourceEstimates();

    Optional<String> getTimeZoneId();

    Optional<String> getLanguage();

    Map<String, String> getSystemProperties();

    Map<String, Map<String, String>> getCatalogSessionProperties();

    Map<String, String> getPreparedStatements();

    Optional<TransactionId> getTransactionId();

    Optional<String> getTraceToken();

    boolean supportClientTransaction();
}
