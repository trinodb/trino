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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class RoutineInfo
{
    private final String routine;
    private final String authorization;
    private final Optional<ClauseInfo> clauseInfo;

    @Deprecated
    public RoutineInfo(String routine, String authorization)
    {
        this(
                routine,
                authorization,
                Optional.empty());
    }

    public RoutineInfo(String routine, String authorization, ClauseInfo clauseInfo)
    {
        this(
                routine,
                authorization,
                Optional.of(requireNonNull(clauseInfo, "clauseInfo is null")));
    }

    @JsonCreator
    public RoutineInfo(
            @JsonProperty("routine") String routine,
            @JsonProperty("authorization") String authorization,
            @JsonProperty("clauseInfo") Optional<ClauseInfo> clauseInfo)
    {
        this.routine = requireNonNull(routine, "routine is null");
        this.authorization = requireNonNull(authorization, "authorization is null");
        this.clauseInfo = requireNonNull(clauseInfo, "clauseInfo is null");
    }

    @JsonProperty
    public String getRoutine()
    {
        return routine;
    }

    @JsonProperty
    public String getAuthorization()
    {
        return authorization;
    }

    @JsonProperty
    public Optional<ClauseInfo> getClauseInfo()
    {
        return clauseInfo;
    }
}
