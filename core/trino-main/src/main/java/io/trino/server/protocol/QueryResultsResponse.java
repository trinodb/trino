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
package io.trino.server.protocol;

import io.trino.client.ProtocolHeaders;
import io.trino.client.QueryResults;
import io.trino.spi.security.SelectedRole;
import io.trino.transaction.TransactionId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

record QueryResultsResponse(
        Optional<String> setCatalog,
        Optional<String> setSchema,
        Optional<String> setPath,
        Optional<String> setAuthorizationUser,
        boolean resetAuthorizationUser,
        Map<String, String> setSessionProperties,
        Set<String> resetSessionProperties,
        Map<String, SelectedRole> setRoles,
        Map<String, String> addedPreparedStatements,
        Set<String> deallocatedPreparedStatements,
        Optional<TransactionId> startedTransactionId,
        boolean clearTransactionId,
        ProtocolHeaders protocolHeaders,
        QueryResults queryResults)
{
    QueryResultsResponse {
        requireNonNull(setCatalog, "setCatalog is null");
        requireNonNull(setSchema, "setSchema is null");
        requireNonNull(setPath, "setPath is null");
        requireNonNull(setAuthorizationUser, "setAuthorizationUser is null");
        requireNonNull(setSessionProperties, "setSessionProperties is null");
        requireNonNull(resetSessionProperties, "resetSessionProperties is null");
        requireNonNull(setRoles, "setRoles is null");
        requireNonNull(addedPreparedStatements, "addedPreparedStatements is null");
        requireNonNull(deallocatedPreparedStatements, "deallocatedPreparedStatements is null");
        requireNonNull(startedTransactionId, "startedTransactionId is null");
        requireNonNull(protocolHeaders, "protocolHeaders is null");
        requireNonNull(queryResults, "queryResults is null");
    }
}
