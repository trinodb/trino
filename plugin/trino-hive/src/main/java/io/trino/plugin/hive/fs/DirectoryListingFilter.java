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
package io.trino.plugin.hive.fs;

import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * Filters down the full listing of a path prefix to just the files directly in a given directory.
 */
public class DirectoryListingFilter
        implements RemoteIterator<TrinoFileStatus>
{
    private final Path prefix;
    private final RemoteIterator<TrinoFileStatus> delegateIterator;

    @Nullable private TrinoFileStatus nextElement;

    public DirectoryListingFilter(Path prefix, RemoteIterator<TrinoFileStatus> delegateIterator)
            throws IOException
    {
        this.prefix = requireNonNull(prefix, "prefix is null");
        this.delegateIterator = requireNonNull(delegateIterator, "delegateIterator is null");
        this.nextElement = findNextElement();
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        return nextElement != null;
    }

    @Override
    public TrinoFileStatus next()
            throws IOException
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        TrinoFileStatus thisElement = nextElement;
        this.nextElement = findNextElement();
        return thisElement;
    }

    private TrinoFileStatus findNextElement()
            throws IOException
    {
        while (delegateIterator.hasNext()) {
            TrinoFileStatus candidate = delegateIterator.next();
            Path candidatePath = new Path(candidate.getPath());
            Path parent = candidatePath.getParent();
            boolean directChild = candidatePath.isAbsolute() ?
                    (parent != null && parent.equals(prefix)) :
                    (parent == null || parent.toString().isEmpty());

            if (directChild) {
                return candidate;
            }
        }
        return null;
    }
}
