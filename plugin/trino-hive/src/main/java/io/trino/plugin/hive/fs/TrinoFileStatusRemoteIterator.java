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

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class TrinoFileStatusRemoteIterator
        implements RemoteIterator<TrinoFileStatus>
{
    private final FileIterator iterator;
    private final Predicate<FileEntry> filterPredicate;
    @Nullable
    private TrinoFileStatus nextElement;

    public TrinoFileStatusRemoteIterator(FileIterator iterator, Predicate<FileEntry> filterPredicate)
            throws IOException
    {
        this.iterator = requireNonNull(iterator, "iterator is null");
        this.filterPredicate = requireNonNull(filterPredicate, "filterPredicate is null");
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
        while (iterator.hasNext()) {
            FileEntry candidate = iterator.next();

            if (filterPredicate.test(candidate)) {
                return new TrinoFileStatus(candidate);
            }
        }
        return null;
    }
}
