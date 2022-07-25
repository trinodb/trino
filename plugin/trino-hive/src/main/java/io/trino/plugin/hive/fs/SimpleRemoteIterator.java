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

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

class SimpleRemoteIterator
        implements RemoteIterator<LocatedFileStatus>
{
    private final Iterator<LocatedFileStatus> iterator;

    public SimpleRemoteIterator(Iterator<LocatedFileStatus> iterator)
    {
        this.iterator = requireNonNull(iterator, "iterator is null");
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        return iterator.hasNext();
    }

    @Override
    public LocatedFileStatus next()
            throws IOException
    {
        return iterator.next();
    }
}
