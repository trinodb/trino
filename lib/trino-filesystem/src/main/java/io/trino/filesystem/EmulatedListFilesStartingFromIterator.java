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
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class EmulatedListFilesStartingFromIterator
        implements FileIterator
{
    private static final Pattern CONSECUTIVE_SLASHES = Pattern.compile("/+");

    private final FileIterator delegate;
    private final String locationPath;
    private final String collapsedLocationPath;
    private final String startingFrom;
    private FileEntry nextEntry;

    public EmulatedListFilesStartingFromIterator(FileIterator delegate, Location location, String startingFrom)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");

        String locationPath = location.path();
        this.locationPath = (locationPath.isEmpty() || locationPath.endsWith("/")) ? locationPath : locationPath + "/";
        this.collapsedLocationPath = CONSECUTIVE_SLASHES.matcher(this.locationPath).replaceAll("/");

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

            // LocalFileSystem, AlluxioFileSystem, and ADLS Gen2 hierarchical canonicalize runs of
            // slashes in returned paths. Try the original prefix first to preserve blob-store keys
            // where `//` is meaningful; fall back to the slash-collapsed form.
            String prefix;
            if (entryPath.startsWith(locationPath)) {
                prefix = locationPath;
            }
            else if (entryPath.startsWith(collapsedLocationPath)) {
                prefix = collapsedLocationPath;
            }
            else {
                throw new IllegalStateException(format("Expected listed file to start with directory path '%s': %s", locationPath, entry.location()));
            }

            String entryTail = entryPath.substring(prefix.length());
            if (entryTail.compareTo(startingFrom) >= 0) {
                nextEntry = entry;
                return;
            }
        }

        nextEntry = null;
    }
}
