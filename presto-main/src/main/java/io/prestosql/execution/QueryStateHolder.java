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
package io.prestosql.execution;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;

public class QueryStateHolder
{
    private QueryState state;
    private Optional<ExecutionFailureInfo> failureInfo;

    QueryStateHolder(QueryState state)
    {
        this.state = state;
        this.failureInfo = Optional.empty();
    }

    private QueryStateHolder(ExecutionFailureInfo failure)
    {
        this.state = QueryState.FAILED;
        this.failureInfo = Optional.of(failure);
    }

    public QueryState getState()
    {
        return state;
    }

    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof QueryStateHolder) {
            return state.equals(((QueryStateHolder) obj).state);
        }

        return state.equals(obj);
    }

    public boolean isDone()
    {
        return state.isDone();
    }

    @Override
    public int hashCode()
    {
        return state.hashCode();
    }

    public static QueryStateHolder failedWithThrowable(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        return new QueryStateHolder(toFailure(throwable));
    }

    public static Set<QueryStateHolder> terminalStates()
    {
        return QueryState.TERMINAL_QUERY_STATES.stream().map(QueryStateHolder::new).collect(toImmutableSet());
    }
}
