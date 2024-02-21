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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public class SortedTrinoFileSystem
        implements SchemaDiscoveryScopedTrinoFileSystem
{
    private final TrinoFileSystem fileSystem;

    public SortedTrinoFileSystem(TrinoFileSystem fileSystem)
    {
        this.fileSystem = fileSystem;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return fileSystem.newInputFile(location);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        ImmutableList.Builder<FileEntry> fileListBuilder = ImmutableList.builder();

        FileIterator fileIterator = fileSystem.listFiles(location);
        while (fileIterator.hasNext()) {
            fileListBuilder.add(fileIterator.next());
        }

        Iterator<FileEntry> sortedFilesIterator = fileListBuilder.build()
                .stream()
                .sorted(Comparator.comparing(f -> f.location().toString()))
                .iterator();

        return new FileIterator()
        {
            @Override
            public boolean hasNext()
            {
                return sortedFilesIterator.hasNext();
            }

            @Override
            public FileEntry next()
            {
                return sortedFilesIterator.next();
            }
        };
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return fileSystem.listDirectories(location)
                .stream()
                .collect(ImmutableSortedSet.toImmutableSortedSet(Comparator.comparing(Location::toString)));
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return fileSystem.directoryExists(location);
    }
}
