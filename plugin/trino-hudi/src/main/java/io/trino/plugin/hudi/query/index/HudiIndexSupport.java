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
package io.trino.plugin.hudi.query.index;

import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;

import java.util.List;

public interface HudiIndexSupport
{
    boolean canApply(TupleDomain<String> tupleDomain);

    default boolean shouldSkipFileSlice(FileSlice slice)
    {
        return false;
    }

    /**
     * Hint the index implementation with the set of partitions known to be relevant for the query
     * after partition pruning. Implementations may use this to scope index lookups to those
     * partitions instead of the whole table. Must be called before any {@link #shouldSkipFileSlice}
     * invocation. Default is a no-op.
     */
    default void setPrunedPartitionPaths(List<String> relativePartitionPaths) {}
}
