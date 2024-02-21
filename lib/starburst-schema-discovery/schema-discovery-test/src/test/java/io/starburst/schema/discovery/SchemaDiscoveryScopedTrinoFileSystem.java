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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.util.Optional;

public interface SchemaDiscoveryScopedTrinoFileSystem
        extends TrinoFileSystem
{
    @Override
    default TrinoInputFile newInputFile(Location location, long length)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    default TrinoOutputFile newOutputFile(Location location)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    default void deleteFile(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    default void deleteDirectory(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    default void renameFile(Location source, Location target)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    default void createDirectory(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    default void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    default Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }
}
