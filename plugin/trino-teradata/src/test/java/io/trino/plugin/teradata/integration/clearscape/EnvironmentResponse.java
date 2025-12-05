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
package io.trino.plugin.teradata.integration.clearscape;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record EnvironmentResponse(
        State state,
        String region,
        String ip)
{
    public EnvironmentResponse
    {
        requireNonNull(state, "state must not be null");
        requireNonNull(region, "region must not be null");
        region = region.toUpperCase(ENGLISH);
    }

    public enum State
    {
        RUNNING,
        STOPPED,
        TERMINATED,
        STOPPING
    }
}
