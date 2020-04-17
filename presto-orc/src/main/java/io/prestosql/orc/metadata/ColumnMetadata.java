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
package io.prestosql.orc.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ColumnMetadata<T>
{
    private final List<T> metadata;

    public ColumnMetadata(List<T> metadata)
    {
        // the metadata list may contain nulls
        this.metadata = unmodifiableList(new ArrayList<>(requireNonNull(metadata, "metadata is null")));
    }

    public T get(OrcColumnId columnId)
    {
        return metadata.get(columnId.getId());
    }

    public int size()
    {
        return metadata.size();
    }

    @Override
    public String toString()
    {
        return metadata.toString();
    }

    public Stream<T> stream()
    {
        return metadata.stream();
    }
}
