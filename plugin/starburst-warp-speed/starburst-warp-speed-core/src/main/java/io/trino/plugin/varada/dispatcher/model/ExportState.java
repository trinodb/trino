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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record ExportState(
        io.trino.plugin.varada.dispatcher.model.ExportState.State state,
        int temporaryFailureCount, long lastTemporaryFailure)
{
    public static final ExportState NOT_EXPORTED = new ExportState(State.NOT_EXPORTED, 0, 0);
    public static final ExportState EXPORTED = new ExportState(State.EXPORTED, 0, 0);
    public static final ExportState FAILED_PERMANENTLY = new ExportState(State.FAILED_PERMANENTLY, 0, 0);

    @JsonCreator
    public ExportState(
            @JsonProperty("state") State state,
            @JsonProperty("temporary_failure_count") int temporaryFailureCount,
            @JsonProperty("last_temporary_failure") long lastTemporaryFailure)
    {
        this.state = state;
        this.temporaryFailureCount = temporaryFailureCount;
        this.lastTemporaryFailure = lastTemporaryFailure;
    }

    public enum State
    {
        NOT_EXPORTED,
        EXPORTED,
        FAILED_PERMANENTLY,
        FAILED_TEMPORARILY
    }
}
