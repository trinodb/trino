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

import io.azam.ulidj.ULID;
import io.trino.filesystem.Location;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.QueryId;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;

import java.time.Instant;
import java.util.Optional;
import java.util.Random;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public record FileSystemSpooledSegmentHandle(
        @Override String encoding,
        @Override QueryId queryId,
        byte[] uuid,
        Optional<EncryptionKey> encryptionKey)
        implements SpooledSegmentHandle
{
    private static final String OBJECT_NAME_SEPARATOR = "::";

    public FileSystemSpooledSegmentHandle
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(encryptionKey, "encryptionKey is null");
        verify(uuid.length == 16, "uuid must be 128 bits");
    }

    public static FileSystemSpooledSegmentHandle random(Random random, SpoolingContext context, Instant expireAt)
    {
        return random(random, context, expireAt, Optional.empty());
    }

    public static FileSystemSpooledSegmentHandle random(Random random, SpoolingContext context, Instant expireAt, Optional<EncryptionKey> encryptionKey)
    {
        return new FileSystemSpooledSegmentHandle(
                context.encoding(),
                context.queryId(),
                ULID.generateBinary(expireAt.toEpochMilli(), entropy(random)),
                encryptionKey);
    }

    @Override
    public Instant expirationTime()
    {
        return Instant.ofEpochMilli(ULID.getTimestampBinary(uuid()));
    }

    /**
     * Storage object name starts with the ULID which is ordered lexicographically
     * by the time of the expiration, which makes it possible to find the expired
     * segments by listing the storage objects. This is crucial for the storage
     * cleanup process to be able to efficiently delete the expired segments.
     *
     * @return String lexicographically sortable storage object name
     * @see <a href="https://github.com/ulid/spec">ULID specification</a>
     */
    public String storageObjectName()
    {
        return ULID.fromBinary(uuid) + OBJECT_NAME_SEPARATOR + queryId;
    }

    @Override
    public String identifier()
    {
        return ULID.fromBinary(uuid) + OBJECT_NAME_SEPARATOR + queryId + "." + encoding;
    }

    public static Optional<Instant> getExpirationFromLocation(Location location)
    {
        String filename = location.fileName();
        int index = filename.indexOf(OBJECT_NAME_SEPARATOR);
        if (index == -1) {
            return Optional.empty();
        }

        String uuid = filename.substring(0, index);
        if (!ULID.isValid(uuid)) {
            return Optional.empty();
        }

        return Optional.of(Instant.ofEpochMilli(ULID.getTimestamp(uuid)));
    }

    private static byte[] entropy(Random random)
    {
        byte[] entropy = new byte[16];
        random.nextBytes(entropy);
        return entropy;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("encoding", encoding)
                .add("expires", Instant.ofEpochMilli(ULID.getTimestampBinary(uuid)))
                .add("identifier", identifier())
                .add("encryptionKey", encryptionKey.map(_ -> "[redacted]").orElse("[none"))
                .toString();
    }
}
