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
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledLocation.DirectLocation;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;
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

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.spi.spool.SpooledLocation.coordinatorLocation;
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
    private final FileSystemLayout fileSystemLayout;
    private final Duration ttl;
    private final Duration directAccessTtl;
    private final boolean encryptionEnabled;
    private final boolean explicitAckEnabled;
    private final Random random = ThreadLocalRandom.current();

    @Inject
    public FileSystemSpoolingManager(FileSystemSpoolingConfig config, TrinoFileSystemFactory fileSystemFactory, FileSystemLayout fileSystemLayout)
    {
        requireNonNull(config, "config is null");
        this.location = Location.of(config.getLocation());
        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null")
                .create(ConnectorIdentity.ofUser("ignored"));
        this.fileSystemLayout = requireNonNull(fileSystemLayout, "fileSystemLayout is null");
        this.encryptionHeadersTranslator = encryptionHeadersTranslator(location);
        this.ttl = config.getTtl();
        this.directAccessTtl = config.getDirectAccessTtl();
        this.encryptionEnabled = config.isEncryptionEnabled();
        this.explicitAckEnabled = config.isExplicitAckEnabled();
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        Location storageLocation = fileSystemLayout.location(location, fileHandle);
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
        Location storageLocation = fileSystemLayout.location(location, fileHandle);

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
        if (!explicitAckEnabled) {
            return; // Let the segment pruning do the magic
        }
        fileSystem.deleteFile(fileSystemLayout.location(location, (FileSystemSpooledSegmentHandle) handle));
    }

    @Override
    public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
            throws IOException
    {
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        Location storageLocation = fileSystemLayout.location(location, fileHandle);
        Duration ttl = remainingTtl(fileHandle.expirationTime(), directAccessTtl);
        Optional<EncryptionKey> key = fileHandle.encryptionKey();

        Optional<DirectLocation> directLocation;
        if (encryptionEnabled) {
            directLocation = fileSystem.encryptedPreSignedUri(storageLocation, ttl, key.orElseThrow())
                    .map(uri -> new DirectLocation(serialize(fileHandle), uri.uri(), uri.headers()));
        }
        else {
            directLocation = fileSystem.preSignedUri(storageLocation, ttl)
                    .map(uri -> new DirectLocation(serialize(fileHandle), uri.uri(), uri.headers()));
        }

        if (directLocation.isEmpty()) {
            throw new IOException("Failed to generate pre-signed URI for segment %s".formatted(fileHandle.identifier()));
        }
        return directLocation;
    }

    @Override
    public SpooledLocation location(SpooledSegmentHandle handle)
    {
        FileSystemSpooledSegmentHandle fileHandle = (FileSystemSpooledSegmentHandle) handle;
        return coordinatorLocation(serialize(fileHandle), headers(fileHandle));
    }

    private static Slice serialize(FileSystemSpooledSegmentHandle fileHandle)
    {
        // Identifier layout:
        //
        // ulid: byte[16]
        // encodingLength: short
        // encoding: byte[encodingLength]
        // isEncrypted: boolean

        byte[] encoding = fileHandle.encoding().getBytes(UTF_8);
        Slice slice = Slices.allocate(16 + SIZE_OF_SHORT + encoding.length + SIZE_OF_BYTE);

        SliceOutput output = slice.getOutput();
        output.writeBytes(fileHandle.uuid());
        output.writeShort(fileHandle.encoding().length());
        output.writeBytes(encoding);
        output.writeBoolean(fileHandle.encryptionKey().isPresent());
        return output.slice();
    }

    private Map<String, List<String>> headers(FileSystemSpooledSegmentHandle fileHandle)
    {
        return fileHandle.encryptionKey()
                .map(encryptionHeadersTranslator::createHeaders)
                .orElse(ImmutableMap.of());
    }

    @Override
    public SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers)
    {
        BasicSliceInput input = identifier.getInput();
        byte[] uuid = new byte[16];
        input.readBytes(uuid);
        short encodingLength = input.readShort();

        String encoding = input.readSlice(encodingLength).toStringUtf8();
        if (!input.readBoolean()) {
            return new FileSystemSpooledSegmentHandle(encoding, uuid, Optional.empty());
        }
        return new FileSystemSpooledSegmentHandle(encoding, uuid, Optional.of(encryptionHeadersTranslator.extractKey(headers)));
    }

    private Duration remainingTtl(Instant expiresAt, Duration accessTtl)
    {
        Duration remainingTTL = new Duration(between(Instant.now(), expiresAt).toMillis(), MILLISECONDS);
        if (accessTtl.compareTo(remainingTTL) < 0) {
            return accessTtl;
        }
        return remainingTTL;
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
