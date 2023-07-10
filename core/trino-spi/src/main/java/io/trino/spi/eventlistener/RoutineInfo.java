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
import io.trino.spi.Unstable;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class RoutineInfo
{
    private final String routine;
    private final String authorization;

    @JsonCreator
    @Unstable
    public RoutineInfo(
            @JsonProperty("routine") String routine,
            @JsonProperty("authorization") String authorization)
    {
        this.routine = requireNonNull(routine, "routine is null");
        this.authorization = requireNonNull(authorization, "authorization is null");
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
}
