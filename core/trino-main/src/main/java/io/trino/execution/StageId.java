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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.spi.QueryId;
import io.trino.sql.planner.plan.PlanFragmentId;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public record StageId(QueryId queryId, int id)
{
    private static final int INSTANCE_SIZE = instanceSize(StageId.class);

    @JsonCreator
    public static StageId valueOf(String stageId)
    {
        List<String> ids = QueryId.parseDottedId(stageId, 2, "stageId");
        return valueOf(ids);
    }

    public static StageId valueOf(List<String> ids)
    {
        checkArgument(ids.size() == 2, "Expected two ids but got: %s", ids);
        return new StageId(new QueryId(ids.get(0)), parseInt(ids.get(1)));
    }

    public static StageId create(QueryId queryId, PlanFragmentId fragmentId)
    {
        return new StageId(queryId, parseInt(fragmentId.toString()));
    }

    public StageId(String queryId, int id)
    {
        this(new QueryId(queryId), id);
    }

    public StageId
    {
        requireNonNull(queryId, "queryId is null");
        checkArgument(id >= 0, "id is negative: %s", id);
    }

    @Override
    @JsonValue
    public String toString()
    {
        return queryId + "." + id;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + queryId.getRetainedSizeInBytes();
    }
}
