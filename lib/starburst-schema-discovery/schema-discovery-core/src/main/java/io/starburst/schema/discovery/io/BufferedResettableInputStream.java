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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import static io.starburst.schema.discovery.internal.TextFileLines.MAX_GUESS_BUFFER_SIZE;
import static java.lang.Math.max;

public class BufferedResettableInputStream
        extends BufferedInputStream
{
    public BufferedResettableInputStream(InputStream in)
    {
        // needs to always be bigger than guess buffer size, as we don't want to fill whole buffer when guess-reading
        // as that would make count = 0 and then spoiling seek(0) logic
        super(in, max(8192, MAX_GUESS_BUFFER_SIZE + 1));
    }

    public void seek(long l)
            throws IOException
    {
        if (l != 0) {
            throw new UnsupportedOperationException("Cannot seek to other place than 0");
        }

        if (count > 0) {
            marklimit = 0;
            markpos = 0;
            reset();
        }
    }

    public long getPos()
    {
        return pos;
    }

    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        if (position != 0) {
            throw new UnsupportedOperationException("Cannot read from other position than from the start");
        }
        return read(buffer, offset, length);
    }
}
