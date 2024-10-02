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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.QueryId;
import io.trino.spi.protocol.SpooledLocation;
import io.trino.spi.protocol.SpooledLocation.DirectLocation;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;
import io.trino.spi.protocol.SpoolingManager;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spooling.filesystem.encryption.EncryptionHeadersTranslator;
import io.trino.spooling.filesystem.encryption.ExceptionMappingInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.spi.protocol.SpooledLocation.coordinatorLocation;
import static io.trino.spooling.filesystem.encryption.EncryptionHeadersTranslator.encryptionHeadersTranslator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.between;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileSystemSpoolingManager
        implements SpoolingManager
{
    private final Location location;
    private final EncryptionHeadersTranslator encryptionHeadersTranslator;
    private final TrinoFileSystem fileSystem;
    private final Duration ttl;
    private final boolean encryptionEnabled;
    private final Random random = ThreadLocalRandom.current();

    @Inject
    public FileSystemSpoolingManager(FileSystemSpoolingConfig config, TrinoFileSystemFactory fileSystemFactory)
    {
        requireNonNull(config, "config is null");
        this.location = Location.of(config.getLocation());
        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null")
                .create(ConnectorIdentity.ofUser("ignored"));
        this.encryptionHeadersTranslator = encryptionHeadersTranslator(location);
        this.ttl = config.getTtl();
        this.encryptionEnabled = config.isEncryptionEnabled();
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        Location storageLocation = location(fileHandle);
        Optional<EncryptionKey> encryption = fileHandle.encryptionKey();

        TrinoOutputFile outputFile;
        if (encryptionEnabled) {
            outputFile = fileSystem.newEncryptedOutputFile(storageLocation, encryption.orElseThrow());
        }
        else {
            outputFile = fileSystem.newOutputFile(storageLocation);
        }

        return outputFile.create();
    }

    @Override
    public FileSystemSpooledSegmentHandle create(SpoolingContext context)
    {
        Instant expireAt = Instant.now().plusMillis(ttl.toMillis());
        if (encryptionEnabled) {
            return FileSystemSpooledSegmentHandle.random(random, context, expireAt, Optional.of(randomAes256()));
        }
        return FileSystemSpooledSegmentHandle.random(random, context, expireAt);
    }

    @Override
    public InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        checkExpiration(fileHandle);
        Optional<EncryptionKey> encryption = fileHandle.encryptionKey();
        Location storageLocation = location(fileHandle);

        TrinoInputFile inputFile;

        if (encryptionEnabled) {
            inputFile = fileSystem.newEncryptedInputFile(storageLocation, encryption.orElseThrow());
        }
        else {
            inputFile = fileSystem.newInputFile(storageLocation);
        }

        checkFileExists(inputFile);
        return new ExceptionMappingInputStream(inputFile.newStream());
    }

    @Override
    public void acknowledge(SpooledSegmentHandle handle)
            throws IOException
    {
        fileSystem.deleteFile(location((FileSystemSpooledSegmentHandle) handle));
    }

    @Override
    public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
            throws IOException
    {
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        Location storageLocation = location(fileHandle);
        Duration ttl = remainingTtl(fileHandle.expirationTime());
        Optional<EncryptionKey> key = fileHandle.encryptionKey();

        Optional<DirectLocation> directLocation;
        if (encryptionEnabled) {
            directLocation = fileSystem.encryptedPreSignedUri(storageLocation, ttl, key.orElseThrow())
                    .map(uri -> new DirectLocation(uri.uri(), uri.headers()));
        }
        else {
            directLocation = fileSystem.preSignedUri(storageLocation, ttl)
                    .map(uri -> new DirectLocation(uri.uri(), uri.headers()));
        }

        if (directLocation.isEmpty()) {
            throw new IOException("Failed to generate pre-signed URI for query %s and segment %s".formatted(fileHandle.queryId(), fileHandle.identifier()));
        }
        return directLocation;
    }

    @Override
    public SpooledLocation location(SpooledSegmentHandle handle)
    {
        // Identifier layout:
        //
        // ulid: byte[16]
        // queryIdLength: byte
        // encodingLength: byte
        // queryId: string
        // encoding: string
        // isEncrypted: boolean
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        output.writeBytes(fileHandle.uuid());
        output.writeShort(fileHandle.queryId().toString().length());
        output.writeShort(fileHandle.encoding().length());
        output.writeBytes(fileHandle.queryId().toString().getBytes(UTF_8));
        output.writeBytes(fileHandle.encoding().getBytes(UTF_8));
        output.writeBoolean(fileHandle.encryptionKey().isPresent());
        return coordinatorLocation(output.slice(), headers(fileHandle));
    }

    private Map<String, List<String>> headers(FileSystemSpooledSegmentHandle fileHandle)
    {
        return fileHandle.encryptionKey()
                .map(encryptionHeadersTranslator::createHeaders)
                .orElse(ImmutableMap.of());
    }

    @Override
    public SpooledSegmentHandle handle(SpooledLocation location)
    {
        if (!(location instanceof SpooledLocation.CoordinatorLocation coordinatorLocation)) {
            throw new IllegalArgumentException("Cannot convert direct location to handle");
        }

        BasicSliceInput input = coordinatorLocation.identifier().getInput();
        byte[] uuid = new byte[16];
        input.readBytes(uuid);
        short queryLength = input.readShort();
        short encodingLength = input.readShort();

        QueryId queryId = QueryId.valueOf(input.readSlice(queryLength).toStringUtf8());
        String encoding = input.readSlice(encodingLength).toStringUtf8();
        if (!input.readBoolean()) {
            return new FileSystemSpooledSegmentHandle(encoding, queryId, uuid, Optional.empty());
        }
        return new FileSystemSpooledSegmentHandle(encoding, queryId, uuid, Optional.of(encryptionHeadersTranslator.extractKey(location.headers())));
    }

    private Location location(FileSystemSpooledSegmentHandle handle)
            throws IOException
    {
        checkExpiration(handle);
        return location.appendPath(handle.storageObjectName());
    }

    private Duration remainingTtl(Instant expiresAt)
    {
        return new Duration(between(Instant.now(), expiresAt).toMillis(), MILLISECONDS);
    }

    private void checkExpiration(FileSystemSpooledSegmentHandle handle)
            throws IOException
    {
        if (handle.expirationTime().isBefore(Instant.now())) {
            throw new IOException("Segment not found or expired");
        }
    }

    private static void checkFileExists(TrinoInputFile inputFile)
            throws IOException
    {
        if (!inputFile.exists()) {
            throw new IOException("Segment not found or expired");
        }
    }
}
