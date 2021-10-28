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

import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

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
public interface ExchangeManager
{
    /**
     * Called by the coordinator to initiate an external exchange between a pair of stages
     *
     * @param context contains various information about the query and stage being executed
     * @param outputPartitionCount number of distinct partitions to be created (grouped) by the exchange.
     * Values of the <code>partitionId</code> parameter of the {@link ExchangeSink#add(int, Slice)} method
     * will be in the <code>[0..outputPartitionCount)</code> range
     * @return {@link Exchange} object to be used by the coordinator to interact with the external exchange
     */
    Exchange createExchange(ExchangeContext context, int outputPartitionCount);

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
     * Called by a worker to create an {@link ExchangeSource} to read data corresponding to
     * a given list of exchange source handles.
     * <p>
     * Usually a single {@link ExchangeSourceHandle} corresponds to a single output partition
     * (see {@link ExchangeSink#add(int, Slice)}) unless a partition got split by calling
     * {@link Exchange#split(ExchangeSourceHandle, long)}.
     * <p>
     * Based on the partition statistic (such as partition size) coordinator may also decide
     * to process several partitions by the same task. In such scenarios the <code>handles</code>
     * list may contain more than a single element.
     *
     * @param handles list of {@link ExchangeSourceHandle}'s describing what exchange data to
     * read. The full list of handles is returned by {@link Exchange#getSourceHandles}.
     * The coordinator decides what items from that list should be handled by what task and creates
     * sub-lists that are further getting sent to a worker to be read.
     * @return {@link ExchangeSource} used by the engine to read data from an exchange
     */
    ExchangeSource createSource(List<ExchangeSourceHandle> handles);
}
