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
package io.trino.plugin.iceberg;

import io.trino.plugin.iceberg.TestIcebergCopyOnWriteIntegration.QueryInfo;
import io.trino.spi.QueryId;

/**
 * Helper interface for integration tests to work with query management.
 */
public interface QueryManager
{
    /**
     * Determines if a query is done.
     */
    static boolean isDone(QueryInfo info)
    {
        return info.getState().isDone();
    }

    /**
     * Waits for a query to change state.
     */
    QueryInfo waitForStateChange(QueryId queryId, java.util.function.Predicate<QueryInfo> predicate, long timeout);
}
