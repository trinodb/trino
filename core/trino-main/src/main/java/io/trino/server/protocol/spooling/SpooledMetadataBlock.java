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
package io.trino.server.protocol.spooling;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.client.spooling.DataAttributes;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledLocation.CoordinatorLocation;
import io.trino.spi.spool.SpooledLocation.DirectLocation;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public sealed interface SpooledMetadataBlock
{
    DataAttributes attributes();

    static SpooledMetadataBlock forInlineData(DataAttributes attributes, byte[] data)
    {
        return new Inlined(attributes, Slices.wrappedBuffer(data));
    }

    static SpooledMetadataBlock forSpooledLocation(SpooledLocation location, DataAttributes attributes)
    {
        return switch (location) {
            case DirectLocation directLocation -> new Spooled(
                    attributes,
                    directLocation.identifier(),
                    Optional.of(directLocation.directUri()),
                    directLocation.headers());
            case CoordinatorLocation coordinatorLocation -> new Spooled(
                    attributes,
                    coordinatorLocation.identifier(),
                    Optional.empty(),
                    coordinatorLocation.headers());
        };
    }

    record Spooled(@Override DataAttributes attributes, Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
            implements SpooledMetadataBlock
    {
        public Spooled {
            requireNonNull(attributes, "attributes is null");
            requireNonNull(identifier, "identifier is null");
            requireNonNull(directUri, "directUri is null");
            requireNonNull(headers, "headers is null");
        }
    }

    record Inlined(@Override DataAttributes attributes, Slice data)
            implements SpooledMetadataBlock
    {
        public Inlined {
            requireNonNull(attributes, "attributes is null");
            requireNonNull(data, "data is null");
        }
    }
}
