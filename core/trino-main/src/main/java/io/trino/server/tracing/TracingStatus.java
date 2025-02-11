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
package io.trino.server.tracing;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record TracingStatus(@JsonProperty("enabled") boolean enabled, Instant since, Optional<String> error)
{
    public TracingStatus(boolean enabled, Instant since)
    {
        this(enabled, since, Optional.empty());
    }

    public TracingStatus
    {
        requireNonNull(since, "since is null");
        since = since.truncatedTo(ChronoUnit.SECONDS);
    }
}
