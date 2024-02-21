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

import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.IO;

public class ErroringTrinoFileSystem
        implements SchemaDiscoveryScopedTrinoFileSystem
{
    private final String errorName;
    private final int exceptionAtCount;
    private final int exceptionAtInteractionNumber;
    private final DiscoveryTrinoFileSystem fileSystem;
    private final Location filePath;
    private final boolean failOnListing;
    private final AtomicInteger interactionCounter;

    public ErroringTrinoFileSystem(String directory, String errorName, int exceptionAtCount, int readZeroAfterInteractionNumber)
    {
        this(directory, errorName, exceptionAtCount, false, readZeroAfterInteractionNumber);
    }

    public ErroringTrinoFileSystem(String directory, String errorName, int exceptionAtCount)
    {
        this(directory, errorName, exceptionAtCount, false, Integer.MAX_VALUE);
    }

    public ErroringTrinoFileSystem(String directory, String errorName, int exceptionAtCount, boolean failOnListing)
    {
        this(directory, errorName, exceptionAtCount, failOnListing, Integer.MAX_VALUE);
    }

    public ErroringTrinoFileSystem(String directory, String errorName, int exceptionAtCount, boolean failOnListing, int exceptionAtInteractionNumber)
    {
        filePath = Util.testFilePath(directory);
        fileSystem = Util.fileSystem();
        this.errorName = errorName;
        this.exceptionAtCount = exceptionAtCount;
        this.failOnListing = failOnListing;
        this.exceptionAtInteractionNumber = exceptionAtInteractionNumber;
        this.interactionCounter = new AtomicInteger(0);
    }

    public Location directory()
    {
        return filePath;
    }

    public DiscoveryTrinoFileSystem fileSystem()
    {
        return fileSystem;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        if (errorName.equals(location.fileName())) {
            boolean zeroRead = interactionCounter.incrementAndGet() >= exceptionAtInteractionNumber;
            return new BadTrinoInputFile(fileSystem, location, exceptionAtCount, zeroRead);
        }
        return fileSystem.open(location).inputFile();
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        if (failOnListing) {
            throw new IOException("listFiles fail");
        }
        return fileSystem.listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
    {
        return Optional.of(fileSystem.directoryExists(location));
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        if (failOnListing) {
            throw new IOException("listDirectories fail");
        }
        return fileSystem.listDirectories(location);
    }

    private static class BadTrinoInputFile
            implements TrinoInputFile
    {
        private final DiscoveryTrinoFileSystem trinoFileSystem;
        private final Location location;
        private final int exceptionAtCount;
        private final boolean zeroRead;

        private BadTrinoInputFile(DiscoveryTrinoFileSystem trinoFileSystem, Location location, int exceptionAtCount, boolean zeroRead)
        {
            this.trinoFileSystem = trinoFileSystem;
            this.location = location;
            this.exceptionAtCount = exceptionAtCount;
            this.zeroRead = zeroRead;
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return new BadTrinoInputStream(trinoFileSystem.open(location).inputStream(), exceptionAtCount, zeroRead);
        }

        @Override
        public long length()
                throws IOException
        {
            return 0;
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public boolean exists()
                throws IOException
        {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public Location location()
        {
            throw new UnsupportedOperationException("not implemented");
        }
    }

    private static class BadTrinoInputStream
            extends TrinoInputStream
    {
        private final AtomicInteger counter = new AtomicInteger();
        private final int exceptionAtCount;
        private final boolean zeroRead;
        private final TrinoInputStream inputStream;
        private volatile long position;

        private BadTrinoInputStream(TrinoInputStream realInputStream, int exceptionAtCount, boolean zeroRead)
        {
            this.inputStream = realInputStream;
            this.exceptionAtCount = exceptionAtCount;
            this.zeroRead = zeroRead;
        }

        @Override
        public long getPosition()
        {
            return position;
        }

        @Override
        public void seek(long position)
        {
            try {
                this.position = position;
                this.inputStream.seek(position);
            }
            catch (IOException e) {
                throw new TrinoException(IO, "Could not seek real input stream in BadInputStream", e);
            }
        }

        @Override
        public int read()
                throws IOException
        {
            if (counter.incrementAndGet() == exceptionAtCount) {
                throw new IOException("boom");
            }
            return inputStream.read();
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            int count = 0;
            if (zeroRead) {
                return count;
            }
            for (int i = 0; i < len; i++) {
                int c = read();
                if (c == -1) {
                    break;
                }
                b[off + i] = (byte) c;
                ++count;
            }
            return count;
        }

        @Override
        public void close()
        {
            try {
                inputStream.close();
            }
            catch (IOException e) {
                throw new TrinoException(IO, "Could not close real input stream in BadInputStream", e);
            }
        }
    }
}
