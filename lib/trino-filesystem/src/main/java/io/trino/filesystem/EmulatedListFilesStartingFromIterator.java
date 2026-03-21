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
package io.trino.filesystem;

import java.io.IOException;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class EmulatedListFilesStartingFromIterator
        implements FileIterator
{
    private final FileIterator delegate;
    private final String locationPath;
    private final String startingFrom;
    private FileEntry nextEntry;

    public EmulatedListFilesStartingFromIterator(FileIterator delegate, Location location, String startingFrom)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");

        String locationPath = location.path();
        this.locationPath = (locationPath.isEmpty() || locationPath.endsWith("/")) ? locationPath : locationPath + "/";

        this.startingFrom = requireNonNull(startingFrom, "startingFrom is null");
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        loadNextEntry();
        return nextEntry != null;
    }

    @Override
    public FileEntry next()
            throws IOException
    {
        loadNextEntry();
        if (nextEntry == null) {
            throw new NoSuchElementException();
        }

        FileEntry result = nextEntry;
        nextEntry = null;
        return result;
    }

    private void loadNextEntry()
            throws IOException
    {
        if (nextEntry != null) {
            return;
        }

        while (delegate.hasNext()) {
            FileEntry entry = delegate.next();
            String entryPath = entry.location().path();
            checkState(entryPath.startsWith(locationPath), "Expected listed file to start with directory path '%s': %s", locationPath, entry.location());

            String entryTail = entryPath.substring(locationPath.length());
            if (entryTail.compareTo(startingFrom) >= 0) {
                nextEntry = entry;
                return;
            }
        }

        nextEntry = null;
    }
}
