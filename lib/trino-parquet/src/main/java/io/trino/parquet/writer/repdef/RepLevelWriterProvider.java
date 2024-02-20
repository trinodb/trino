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
package io.trino.parquet.writer.repdef;

import com.google.common.collect.Iterables;
import org.apache.parquet.column.values.ValuesWriter;

import java.util.List;
import java.util.Optional;

public interface RepLevelWriterProvider
{
    RepetitionLevelWriter getRepetitionLevelWriter(Optional<RepetitionLevelWriter> nestedWriter, ValuesWriter encoder);

    /**
     * Parent repetition level marks at which level either:
     * 1. A new collection starts
     * 2. A collection is null or empty
     * 3. A primitive column stays
     */
    interface RepetitionLevelWriter
    {
        void writeRepetitionLevels(int parentLevel, int positionsCount);

        void writeRepetitionLevels(int parentLevel);
    }

    static RepetitionLevelWriter getRootRepetitionLevelWriter(List<RepLevelWriterProvider> repLevelWriterProviders, ValuesWriter encoder)
    {
        // Constructs hierarchy of RepetitionLevelWriter from leaf to root
        RepetitionLevelWriter rootRepetitionLevelWriter = Iterables.getLast(repLevelWriterProviders)
                .getRepetitionLevelWriter(Optional.empty(), encoder);
        for (int nestedLevel = repLevelWriterProviders.size() - 2; nestedLevel >= 0; nestedLevel--) {
            RepetitionLevelWriter nestedWriter = rootRepetitionLevelWriter;
            rootRepetitionLevelWriter = repLevelWriterProviders.get(nestedLevel)
                    .getRepetitionLevelWriter(Optional.of(nestedWriter), encoder);
        }
        return rootRepetitionLevelWriter;
    }
}
