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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.IncrementalLoadFactorHashArraySizeSupplier;
import io.trino.operator.PagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.join.JoinUtils.channelsToPages;
import static java.util.Objects.requireNonNull;

public class JoinHashSupplier
        implements LookupSourceSupplier
{
    /**
     * This value is purposefully identical to that of IncrementalLoadFactorHashArraySizeSupplier#THRESHOLD_50,
     * as higher load factor means more excessive memory consumption
     */
    private static final int JOIN_POSITIONS_ARRAY_CUTOFF = IncrementalLoadFactorHashArraySizeSupplier.THRESHOLD_50;

    private final Session session;
    private final PagesHash pagesHash;
    private final LongArrayList addresses;
    private final List<Page> pages;
    // Number of bytes retained by the Page objects excluding retained size of the blocks themselves (as those are accounted as part of PageHash)
    // as when the pages are small and the data set is large the memory footprint of Page objects could be substantial
    private final long pageInstancesRetainedSizeInBytes;
    private final Optional<PositionLinks.Factory> positionLinks;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;

    public JoinHashSupplier(
            Session session,
            PagesHashStrategy pagesHashStrategy,
            LongArrayList addresses,
            List<ObjectArrayList<Block>> channels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            HashArraySizeSupplier hashArraySizeSupplier,
            OptionalInt singleBigintJoinChannel)
    {
        this.session = requireNonNull(session, "session is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
        this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
        requireNonNull(channels, "channels is null");
        requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");

        PositionLinks.FactoryBuilder positionLinksFactoryBuilder;
        if (sortChannel.isPresent()) {
            checkArgument(filterFunctionFactory.isPresent(), "filterFunctionFactory not set while sortChannel set");
            positionLinksFactoryBuilder = SortedPositionLinks.builder(
                    addresses.size(),
                    pagesHashStrategy,
                    addresses);
        }
        else {
            positionLinksFactoryBuilder = ArrayPositionLinks.builder(addresses.size());
        }

        this.pages = channelsToPages(channels);
        this.pageInstancesRetainedSizeInBytes = getPageInstancesRetainedSizeInBytes(channels);

        if (singleBigintJoinChannel.isPresent() && addresses.size() <= JOIN_POSITIONS_ARRAY_CUTOFF) {
            this.pagesHash = new BigintPagesHash(addresses, pagesHashStrategy, positionLinksFactoryBuilder, hashArraySizeSupplier, pages, singleBigintJoinChannel.getAsInt());
        }
        else {
            this.pagesHash = new DefaultPagesHash(addresses, pagesHashStrategy, positionLinksFactoryBuilder, hashArraySizeSupplier);
        }
        this.positionLinks = positionLinksFactoryBuilder.isEmpty() ? Optional.empty() : Optional.of(positionLinksFactoryBuilder.build());
    }

    @Override
    public long checksum()
    {
        return positionLinks.map(PositionLinks.Factory::checksum).orElse(0L);
    }

    @Override
    public JoinHash get()
    {
        // We need to create new JoinFilterFunction per each thread using it, since those functions
        // are not thread safe...
        Optional<JoinFilterFunction> filterFunction =
                filterFunctionFactory.map(factory -> factory.create(session.toConnectorSession(), addresses, pages));
        return new JoinHash(
                pagesHash,
                filterFunction,
                positionLinks.map(links -> {
                    List<JoinFilterFunction> searchFunctions = searchFunctionFactories.stream()
                            .map(factory -> factory.create(session.toConnectorSession(), addresses, pages))
                            .collect(toImmutableList());
                    return links.create(searchFunctions);
                }),
                pageInstancesRetainedSizeInBytes);
    }

    public static long getEstimatedRetainedSizeInBytes(
            int positionCount,
            LongArrayList addresses,
            List<ObjectArrayList<Block>> channels,
            long blocksSizeInBytes,
            Optional<Integer> sortChannel,
            OptionalInt singleBigintJoinChannel,
            HashArraySizeSupplier hashArraySizeSupplier)
    {
        long result = 0;
        if (sortChannel.isPresent()) {
            result += SortedPositionLinks.getEstimatedRetainedSizeInBytes(positionCount);
        }
        else {
            result += ArrayPositionLinks.getEstimatedRetainedSizeInBytes(positionCount);
        }
        result += getPageInstancesRetainedSizeInBytes(channels);
        if (singleBigintJoinChannel.isPresent() && addresses.size() <= JOIN_POSITIONS_ARRAY_CUTOFF) {
            result += BigintPagesHash.getEstimatedRetainedSizeInBytes(positionCount, hashArraySizeSupplier, addresses, channels, blocksSizeInBytes);
        }
        else {
            result += DefaultPagesHash.getEstimatedRetainedSizeInBytes(positionCount, hashArraySizeSupplier, addresses, channels, blocksSizeInBytes);
        }
        return result;
    }

    private static long getPageInstancesRetainedSizeInBytes(List<ObjectArrayList<Block>> channels)
    {
        if (channels.isEmpty()) {
            return 0;
        }
        int pagesCount = channels.get(0).size();
        return Page.getInstanceSizeInBytes(channels.size()) * pagesCount;
    }
}
