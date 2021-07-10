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
package io.trino.operator.join;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.WorkProcessor;
import io.trino.operator.join.DefaultPageJoiner.SavedRow;
import io.trino.spi.Page;
import io.trino.spiller.PartitioningSpiller;
import io.trino.spiller.PartitioningSpillerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public interface PageJoiner
        extends WorkProcessor.Transformation<Page, Page>, Closeable
{
    interface PageJoinerFactory
    {
        PageJoiner getPageJoiner(
                ListenableFuture<LookupSourceProvider> lookupSourceProvider,
                Optional<PartitioningSpillerFactory> partitioningSpillerFactory,
                Iterator<SavedRow> savedRows);
    }

    Map<Integer, SavedRow> getSpilledRows();

    Optional<PartitioningSpiller> getSpiller();
}
