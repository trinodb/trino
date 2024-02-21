/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;

// acts as it has only s3://dummy/ folder and s3://dummy/file1.json visible
public class ConstantTrinoFileSystem
        implements SchemaDiscoveryScopedTrinoFileSystem
{
    public static final Location FOLDER_PATH = Location.of("s3://dummy/");
    public static final Location FILE_PATH = Location.of("s3://dummy/file1.json");

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        if (!FILE_PATH.equals(location)) {
            throw notSupportedPath(location);
        }
        return new MemoryInputFile(FILE_PATH, utf8Slice("{\"key\":\"value\"}"));
    }

    @Override
    public FileIterator listFiles(Location location)
    {
        return new FileIterator() {
            private boolean used;

            @Override
            public boolean hasNext()
            {
                return !used;
            }

            @Override
            public FileEntry next()
            {
                used = true;
                return new FileEntry(FILE_PATH, 128, Instant.now(), Optional.empty());
            }
        };
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
    {
        return Optional.of(FOLDER_PATH.equals(location));
    }

    @Override
    public Set<Location> listDirectories(Location location)
    {
        return ImmutableSet.of();
    }

    private static UnsupportedOperationException notSupportedPath(Location path)
    {
        return new UnsupportedOperationException("Tried to use: [%s]. Cannot use anything other than: [%s] and [%s]".formatted(path, FOLDER_PATH, FILE_PATH));
    }
}
