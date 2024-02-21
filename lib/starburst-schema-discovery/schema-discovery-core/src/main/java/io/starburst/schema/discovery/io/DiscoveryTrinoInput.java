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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;

import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.IO;

public class DiscoveryTrinoInput
        implements DiscoveryInput
{
    private final TrinoInputStream inputStream;
    private final DiscoveryTrinoFileSystem fileSystem;
    private final Location location;
    private final TrinoInputFile inputFile;

    public DiscoveryTrinoInput(DiscoveryTrinoFileSystem fileSystem, Location location)
    {
        TrinoFileStreamPair fileStreamPair = fileSystem.open(location);
        this.inputStream = fileStreamPair.inputStream();
        this.inputFile = fileStreamPair.inputFile();
        this.fileSystem = fileSystem;
        this.location = location;
    }

    @Override
    public long length()
    {
        return fileSystem.length(location);
    }

    @Override
    public DiscoveryInput rewind()
    {
        try {
            inputStream.seek(0);
            return this;
        }
        catch (IOException e) {
            throw new TrinoException(IO, "Could not seek to 0", e);
        }
    }

    @Override
    public InputStream asInputStream()
    {
        return inputStream;
    }

    @Override
    public TrinoInputFile asTrinoFile()
    {
        return inputFile;
    }

    @Override
    public void close()
            throws Exception
    {
        inputStream.close();
    }
}
