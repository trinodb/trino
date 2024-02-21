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

import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class TrinoDiscoveryInputStream
        extends TrinoInputStream
{
    private boolean closed;
    private final BufferedResettableInputStream inputStream;

    public TrinoDiscoveryInputStream(BufferedResettableInputStream inputStream)
    {
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
    }

    @Override
    public long getPosition()
    {
        return inputStream.getPos();
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        inputStream.seek(position);
    }

    @Override
    public int read()
            throws IOException
    {
        return inputStream.read();
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            inputStream.close();
        }
    }
}
