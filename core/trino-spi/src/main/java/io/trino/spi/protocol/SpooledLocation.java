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
package io.trino.spi.protocol;

import io.airlift.slice.Slice;
import io.trino.spi.Experimental;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Experimental(eta = "2025-05-31")
public sealed interface SpooledLocation
{
    Map<String, List<String>> headers();

    record CoordinatorLocation(Slice identifier, Map<String, List<String>> headers)
            implements SpooledLocation
    {
        public CoordinatorLocation {
            headers = Map.copyOf(requireNonNull(headers, "headers is null"));
        }
    }

    record DirectLocation(URI uri, Map<String, List<String>> headers)
            implements SpooledLocation
    {
        public DirectLocation {
            headers = Map.copyOf(requireNonNull(headers, "headers is null"));
        }
    }

    static DirectLocation directLocation(URI uri, Map<String, List<String>> headers)
    {
        return new DirectLocation(uri, headers);
    }

    static CoordinatorLocation coordinatorLocation(Slice identifier, Map<String, List<String>> headers)
    {
        return new CoordinatorLocation(identifier, headers);
    }
}
