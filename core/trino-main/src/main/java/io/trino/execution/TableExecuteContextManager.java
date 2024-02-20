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
package io.trino.execution;

import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.QueryId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ThreadSafe
public class TableExecuteContextManager
{
    private final ConcurrentMap<QueryId, TableExecuteContext> contexts = new ConcurrentHashMap<>();

    public void registerTableExecuteContextForQuery(QueryId queryId)
    {
        TableExecuteContext newContext = new TableExecuteContext();
        if (contexts.putIfAbsent(queryId, newContext) != null) {
            throw new IllegalStateException("TableExecuteContext already registered for query " + queryId);
        }
    }

    public void unregisterTableExecuteContextForQuery(QueryId queryId)
    {
        contexts.remove(queryId);
    }

    public TableExecuteContext getTableExecuteContextForQuery(QueryId queryId)
    {
        TableExecuteContext context = contexts.get(queryId);
        if (context == null) {
            throw new IllegalStateException("TableExecuteContext not registered for query " + queryId);
        }
        return context;
    }
}
