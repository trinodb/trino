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
package io.trino.spi.exchange;

import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.slice.Slice;
import io.trino.spi.Experimental;

/**
 * Service provider interface for an external exchange
 * <p>
 * Used by the engine to exchange data at stage boundaries
 * <p>
 * External exchange is responsible for accepting partitioned data from multiple upstream
 * tasks, grouping that data based on the <code>partitionId</code>
 * (see {@link ExchangeSink#add(int, Slice)}) and allowing the data to be consumed a
 * partition at a time by a set of downstream tasks.
 * <p>
 * To support failure recovery an external exchange implementation is also responsible
 * for data deduplication in an event of a task retry or a speculative execution of a task
 * (when two identical tasks are running at the same time). The deduplication must be done
 * based on the sink identifier (see {@link Exchange#addSink(int)}). The implementation should
 * assume that the data written for the same {@link ExchangeSinkHandle} by multiple sink
 * instances (see {@link Exchange#instantiateSink(ExchangeSinkHandle, int)}) is identical
 * and the data written by an arbitrary instance can be chosen to be delivered while the
 * data written by other instances must be safely discarded
 */
@ThreadSafe
@Experimental(eta = "2023-09-01")
public interface ExchangeManager
{
    /**
     * Called by the coordinator to initiate an external exchange between a pair of stages
     *
     * @param context contains various information about the query and stage being executed
     * @param outputPartitionCount number of distinct partitions to be created (grouped) by the exchange.
     * Values of the <code>partitionId</code> parameter of the {@link ExchangeSink#add(int, Slice)} method
     * will be in the <code>[0..outputPartitionCount)</code> range
     * @param preserveOrderWithinPartition preserve order of records within a single partition written by a single writer.
     * This property does not impose any specific order on the sub partitions of a single output partition written by multiple independent writers.
     * The order is preserved only for the records written by a single writer. The reader will read sub partitions written by different writers in no specific order.
     * This setting is useful when collecting ordered output from a single task that produces a single partition (for example a task that performs a global "order by" operation).
     * May impact performance as it makes certain optimizations not possible.
     * @return {@link Exchange} object to be used by the coordinator to interact with the external exchange
     */
    Exchange createExchange(ExchangeContext context, int outputPartitionCount, boolean preserveOrderWithinPartition);

    /**
     * Called by a worker to create an {@link ExchangeSink} for a specific sink instance.
     * <p>
     * A new sink instance is created by the coordinator for every task attempt (see {@link Exchange#instantiateSink(ExchangeSinkHandle, int)})
     *
     * @param handle returned by {@link Exchange#instantiateSink(ExchangeSinkHandle, int)}
     * @return {@link ExchangeSink} used by the engine to write data to an exchange
     */
    ExchangeSink createSink(ExchangeSinkInstanceHandle handle);

    /**
     * Called by a worker to create an {@link ExchangeSource} to read exchange data.
     *
     * @return {@link ExchangeSource} used by the engine to read data from an exchange
     */
    ExchangeSource createSource();

    /**
     * Provides information if Exchange implementation provided with this plugin supports concurrent reading and writing.
     */
    boolean supportsConcurrentReadAndWrite();

    /**
     * Shutdown the exchange manager by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * queries are using the exchange manager. After this method is called,
     * no methods will be called on the exchange manager or any objects obtained
     * from exchange manager.
     */
    default void shutdown() {}
}
