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
package io.trino.spooling.filesystem;

import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public record FileSystemSpooledSegmentHandle(String name, Instant validUntil, Optional<Slice> encryptionKey)
        implements SpooledSegmentHandle
{
    public FileSystemSpooledSegmentHandle
    {
        requireNonNull(name, "name is null");
        validUntil = requireNonNull(validUntil, "validUntil is null").truncatedTo(ChronoUnit.MILLIS);
    }

    public static FileSystemSpooledSegmentHandle random(SpoolingContext context, Duration ttl)
    {
        return random(context, ttl, Optional.empty());
    }

    public static FileSystemSpooledSegmentHandle random(SpoolingContext context, Duration ttl, Optional<Slice> encryptionKey)
    {
        return new FileSystemSpooledSegmentHandle(
                context.queryId().getId() + "/" + randomObjectName(),
                Instant.now().plusMillis(ttl.toMillis()),
                encryptionKey);
    }

    private static String randomObjectName()
    {
        return randomUUID().toString().replace("-", "");
    }
}
