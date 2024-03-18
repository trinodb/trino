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
package io.trino.plugin.varada.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record WarmUpElementState(
        @JsonProperty("state") State state,
        @JsonProperty("temporary_failure_count") int temporaryFailureCount,
        @JsonProperty("last_temporary_failure") long lastTemporaryFailure)
{
    public static final WarmUpElementState VALID = new WarmUpElementState(State.VALID);
    public static final WarmUpElementState FAILED_PERMANENTLY = new WarmUpElementState(State.FAILED_PERMANENTLY);

    public WarmUpElementState(State state)
    {
        this(state,
                state == State.FAILED_TEMPORARILY ? 1 : 0,
                state == State.FAILED_TEMPORARILY ? System.currentTimeMillis() : 0);
    }

    public enum State
    {
        VALID,
        FAILED_PERMANENTLY,
        FAILED_TEMPORARILY
    }
}
