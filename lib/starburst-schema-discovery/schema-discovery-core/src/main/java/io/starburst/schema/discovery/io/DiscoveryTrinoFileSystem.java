/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.io;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.spi.TrinoException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.IO;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.LOCATION_DOES_NOT_EXISTS;
import static java.util.Objects.requireNonNull;

public class DiscoveryTrinoFileSystem
{
    private final TrinoFileSystem trinoFileSystem;
    private final Map<Location, Long> lengthCache = new ConcurrentHashMap<>();

    public DiscoveryTrinoFileSystem(TrinoFileSystem trinoFileSystem)
    {
        this.trinoFileSystem = requireNonNull(trinoFileSystem, "trinoFileSystem is null");
    }

    public TrinoFileStreamPair open(Location location)
    {
        try {
            TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(location);

            Optional<CompressionKind> compressionKind = CompressionKind.forFile(location.fileName());
            if (compressionKind.isPresent()) {
                InputStream streamDecompressor = compressionKind.get().createCodec().createStreamDecompressor(trinoInputFile.newStream());
                return new TrinoFileStreamPair(
                        trinoInputFile,
                        new TrinoDiscoveryInputStream(new BufferedResettableInputStream(streamDecompressor)));
            }

            return new TrinoFileStreamPair(trinoInputFile, trinoInputFile.newStream());
        }
        catch (IOException e) {
            throw new TrinoException(IO, "Could not open: " + location, e);
        }
    }

    public long length(Location location)
    {
        return lengthCache.computeIfAbsent(location, l -> {
            try {
                return trinoFileSystem.newInputFile(l).length();
            }
            catch (IOException e) {
                throw new TrinoException(IO, "Could not get length of: " + location, e);
            }
        });
    }

    public FileIterator listFiles(Location location)
    {
        try {
            return trinoFileSystem.listFiles(location);
        }
        catch (IOException e) {
            throw new TrinoException(IO, "Could not list files in: " + location, e);
        }
    }

    public Set<Location> listDirectories(Location location)
    {
        try {
            return trinoFileSystem.listDirectories(location);
        }
        catch (IOException e) {
            throw new TrinoException(IO, "Could not list directories in: " + location, e);
        }
    }

    public boolean directoryExists(Location location)
    {
        try {
            return trinoFileSystem.directoryExists(location).orElse(false);
        }
        catch (FileNotFoundException e) {
            throw new TrinoException(LOCATION_DOES_NOT_EXISTS, "Location does not exist: " + location);
        }
        catch (IOException e) {
            throw new TrinoException(IO, "Could not check if directory exists: " + location, e);
        }
    }
}
