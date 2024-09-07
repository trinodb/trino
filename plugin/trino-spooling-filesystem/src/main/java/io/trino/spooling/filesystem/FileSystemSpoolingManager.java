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

import com.google.inject.Inject;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.XxHash64;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.QueryId;
import io.trino.spi.protocol.SpooledLocation;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;
import io.trino.spi.protocol.SpoolingManager;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spooling.filesystem.encryption.ExceptionMappingInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.protocol.SpooledLocation.coordinatorLocation;
import static io.trino.spooling.filesystem.encryption.EncryptionUtils.decryptingInputStream;
import static io.trino.spooling.filesystem.encryption.EncryptionUtils.encryptingOutputStream;
import static io.trino.spooling.filesystem.encryption.EncryptionUtils.generateRandomKey;
import static java.util.Objects.requireNonNull;

public class FileSystemSpoolingManager
        implements SpoolingManager
{
    private static final String ENCRYPTION_KEY_HEADER = "X-Trino-Server-Side-Encryption-Key";
    private static final String ENCRYPTION_KEY_CIPHER = "X-Trino-Server-Side-Encryption";

    private static final String ENCRYPTION_CIPHER_NAME = "AES256";

    private final String location;
    private final TrinoFileSystem fileSystem;
    private final Duration ttl;
    private final boolean encryptionEnabled;
    private final Random random = ThreadLocalRandom.current();

    @Inject
    public FileSystemSpoolingManager(FileSystemSpoolingConfig config, TrinoFileSystemFactory fileSystemFactory)
    {
        requireNonNull(config, "config is null");
        this.location = config.getLocation();
        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null")
                .create(ConnectorIdentity.ofUser("ignored"));
        this.ttl = config.getTtl();
        this.encryptionEnabled = config.isEncryptionEnabled();
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        FileSystemSpooledSegmentHandle filesystemHandle = (FileSystemSpooledSegmentHandle) handle;
        OutputStream stream = fileSystem.newOutputFile(location(filesystemHandle)).create();
        if (filesystemHandle.encryptionKey().isPresent()) {
            return encryptingOutputStream(stream, filesystemHandle.encryptionKey().get());
        }
        return stream;
    }

    @Override
    public FileSystemSpooledSegmentHandle create(SpoolingContext context)
    {
        Instant expireAt = Instant.now().plusMillis(ttl.toMillis());

        if (encryptionEnabled) {
            return FileSystemSpooledSegmentHandle.random(random, context.queryId(), expireAt, Optional.of(generateRandomKey()));
        }
        return FileSystemSpooledSegmentHandle.random(random, context.queryId(), expireAt);
    }

    @Override
    public InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        FileSystemSpooledSegmentHandle segmentHandle = (FileSystemSpooledSegmentHandle) handle;
        checkExpiration(segmentHandle);
        try {
            if (!fileSystem.newInputFile(location(segmentHandle)).exists()) {
                throw new IOException("Segment not found or expired");
            }

            InputStream stream = fileSystem.newInputFile(location(segmentHandle)).newStream();
            if (segmentHandle.encryptionKey().isPresent()) {
                return new ExceptionMappingInputStream(decryptingInputStream(stream, segmentHandle.encryptionKey().get()));
            }
            return stream;
        }
        catch (FileNotFoundException e) {
            throw new IOException("Segment not found or expired", e);
        }
    }

    @Override
    public void acknowledge(SpooledSegmentHandle handle)
            throws IOException
    {
        fileSystem.deleteFile(location((FileSystemSpooledSegmentHandle) handle));
    }

    @Override
    public SpooledLocation location(SpooledSegmentHandle handle)
    {
        // Identifier layout:
        //
        // ulid: byte[16]
        // queryIdLength: byte
        // queryId: string
        // hasEncryptionKey: boolean
        // encryptionKeyHash: long
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        output.writeBytes(fileHandle.uuid());
        output.writeShort(fileHandle.queryId().toString().length());
        output.writeBytes(utf8Slice(fileHandle.queryId().toString()));
        output.writeBoolean(fileHandle.encryptionKey().isPresent());
        if (fileHandle.encryptionKey().isPresent()) {
            output.writeLong(XxHash64.hash(fileHandle.encryptionKey().orElseThrow()));
        }

        return coordinatorLocation(output.slice(), headers(fileHandle));
    }

    @Override
    public SpooledSegmentHandle handle(SpooledLocation location)
    {
        if (!(location instanceof SpooledLocation.CoordinatorLocation coordinatorLocation)) {
            throw new IllegalArgumentException("Cannot convert direct location to handle");
        }

        SliceInput input = coordinatorLocation.identifier().getInput();
        byte[] uuid = new byte[16];
        input.readBytes(uuid);
        short length = input.readShort();
        QueryId queryId = QueryId.valueOf(input.readSlice(length).toStringUtf8());

        if (!input.readBoolean()) {
            return FileSystemSpooledSegmentHandle.of(queryId, uuid, Optional.empty());
        }

        long encryptionKeyHash = input.readLong();
        List<String> encryptionCipher = location.headers().get(ENCRYPTION_KEY_CIPHER);
        if (encryptionCipher == null || encryptionCipher.isEmpty()) {
            throw new IllegalArgumentException("Header %s is missing".formatted(ENCRYPTION_KEY_CIPHER));
        }
        if (!encryptionCipher.getFirst().contentEquals(ENCRYPTION_CIPHER_NAME)) {
            throw new IllegalArgumentException("Unsupported encryption cipher %s".formatted(encryptionCipher));
        }

        List<String> encryptionKey = location.headers().get(ENCRYPTION_KEY_HEADER);
        if (encryptionKey == null || encryptionKey.isEmpty()) {
            throw new IllegalArgumentException("Header %s is missing".formatted(ENCRYPTION_KEY_HEADER));
        }

        Slice key = base64Decode(encryptionKey.getFirst());
        if (encryptionKeyChecksum(key) != encryptionKeyHash) {
            throw new IllegalArgumentException("Encryption key checksum mismatch");
        }

        return FileSystemSpooledSegmentHandle.of(queryId, uuid, Optional.of(key));
    }

    private Map<String, List<String>> headers(SpooledSegmentHandle handle)
    {
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        if (encryptionEnabled) {
            return Map.of(
                    ENCRYPTION_KEY_CIPHER, List.of(ENCRYPTION_CIPHER_NAME),
                    ENCRYPTION_KEY_HEADER, List.of(base64Encode(fileHandle.encryptionKey().orElseThrow())));
        }
        return Map.of();
    }

    private Location location(FileSystemSpooledSegmentHandle handle)
            throws IOException
    {
        checkExpiration(handle);
        return Location.of(location + "/" + handle.storageObjectName());
    }

    private void checkExpiration(FileSystemSpooledSegmentHandle handle)
            throws IOException
    {
        if (handle.expirationTime().isBefore(Instant.now())) {
            throw new IOException("Segment not found or expired");
        }
    }

    private static long encryptionKeyChecksum(Slice key)
    {
        return XxHash64.hash(key);
    }

    private static String base64Encode(Slice slice)
    {
        return Base64.getEncoder().encodeToString(slice.getBytes());
    }

    private static Slice base64Decode(String base64)
    {
        return wrappedBuffer(Base64.getDecoder().decode(base64));
    }
}
