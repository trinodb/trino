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

import jakarta.annotation.Nullable;

import java.io.Closeable;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface StatementClient
        extends Closeable
{
    String getQuery();

    ZoneId getTimeZone();

    boolean isRunning();

    boolean isClientAborted();

    boolean isClientError();

    boolean isFinished();

    StatementStats getStats();

    QueryStatusInfo currentStatusInfo();

    // For backward compatibility and migration path
    QueryData currentData();

    ResultRows currentRows();

    QueryStatusInfo finalStatusInfo();

    default Optional<String> getEncoding()
    {
        return Optional.empty();
    }

    Optional<String> getSetCatalog();

    Optional<String> getSetSchema();

    Optional<List<String>> getSetPath();

    Optional<String> getSetAuthorizationUser();

    boolean isResetAuthorizationUser();

    Set<ClientSelectedRole> getSetOriginalRoles();

    Map<String, String> getSetSessionProperties();

    Set<String> getResetSessionProperties();

    Map<String, ClientSelectedRole> getSetRoles();

    Map<String, String> getAddedPreparedStatements();

    Set<String> getDeallocatedPreparedStatements();

    @Nullable
    String getStartedTransactionId();

    boolean isClearTransactionId();

    boolean advance();

    void cancelLeafStage();

    @Override
    void close();
}
