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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.QueryTracker.TrackedQuery;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.warnings.WarningCollector;
import io.trino.server.BasicQueryInfo;
import io.trino.server.protocol.Slug;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Plan;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public interface QueryExecution
        extends TrackedQuery
{
    QueryState getState();

    ListenableFuture<QueryState> getStateChange(QueryState currentState);

    void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener);

    void addOutputInfoListener(Consumer<QueryOutputInfo> listener);

    void outputTaskFailed(TaskId taskId, Throwable failure);

    Plan getQueryPlan();

    BasicQueryInfo getBasicQueryInfo();

    QueryInfo getQueryInfo();

    Slug getSlug();

    Duration getTotalCpuTime();

    DataSize getUserMemoryReservation();

    DataSize getTotalMemoryReservation();

    void start();

    void cancelQuery();

    void cancelStage(StageId stageId);

    void failTask(TaskId taskId, Exception reason);

    void recordHeartbeat();

    boolean shouldWaitForMinWorkers();

    /**
     * Add a listener for the final query info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener);

    interface QueryExecutionFactory<T extends QueryExecution>
    {
        T createQueryExecution(PreparedQuery preparedQuery, QueryStateMachine stateMachine, Slug slug, WarningCollector warningCollector);
    }

    /**
     * Output schema and buffer URIs for query.  The info will always contain column names and types.  Buffer locations will always
     * contain the full location set, but may be empty.  Users of this data should keep a private copy of the seen buffers to
     * handle out of order events from the listener.  Once noMoreBufferLocations is set the locations will never change, and
     * it is guaranteed that all previously sent locations are contained in the buffer locations.
     */
    class QueryOutputInfo
    {
        private final List<String> columnNames;
        private final List<Type> columnTypes;
        private final Map<TaskId, URI> bufferLocations;
        private final boolean noMoreBufferLocations;

        public QueryOutputInfo(List<String> columnNames, List<Type> columnTypes, Map<TaskId, URI> bufferLocations, boolean noMoreBufferLocations)
        {
            this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.bufferLocations = ImmutableMap.copyOf(requireNonNull(bufferLocations, "bufferLocations is null"));
            this.noMoreBufferLocations = noMoreBufferLocations;
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }

        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }

        public Map<TaskId, URI> getBufferLocations()
        {
            return bufferLocations;
        }

        public boolean isNoMoreBufferLocations()
        {
            return noMoreBufferLocations;
        }
    }
}
