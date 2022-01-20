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
package io.trino.operator;

import io.trino.Session;
import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.HashTableDataGroupByHash;
import io.trino.operator.hash.IsolatedHashTableFactory;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.trino.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.trino.SystemSessionProperties.isUseEnhancedGroupByEnabled;
import static io.trino.operator.hash.IsolatedHashTableFactory.MAX_SUPPORTED_CHANNELS;
import static io.trino.spi.type.BigintType.BIGINT;

public class GroupByHashFactory
{
    private final JoinCompiler joinCompiler;
    private final BlockTypeOperators blockTypeOperators;
    private final IsolatedHashTableFactory isolatedHashTableFactory;

    @Inject
    public GroupByHashFactory(JoinCompiler joinCompiler, BlockTypeOperators blockTypeOperators, IsolatedHashTableFactory isolatedHashTableFactory)
    {
        this.joinCompiler = joinCompiler;
        this.blockTypeOperators = blockTypeOperators;
        this.isolatedHashTableFactory = isolatedHashTableFactory;
    }

    public GroupByHash createGroupByHash(
            Session session,
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            UpdateMemory updateMemory)
    {
        return createGroupByHash(
                hashTypes,
                hashChannels,
                inputHashChannel,
                expectedSize,
                isDictionaryAggregationEnabled(session),
                isUseEnhancedGroupByEnabled(session),
                updateMemory);
    }

    public GroupByHash createGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            UpdateMemory updateMemory)
    {
        return createGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, false, updateMemory);
    }

    public GroupByHash createGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            boolean useEnhancedGroupBy,
            UpdateMemory updateMemory)
    {
        if (hashTypes.size() == 1 && hashTypes.get(0).equals(BIGINT) && hashChannels.length == 1) {
            return new BigintGroupByHash(hashChannels[0], inputHashChannel.isPresent(), expectedSize, updateMemory);
        }
        if (useEnhancedGroupBy &&
                hashTypes.size() <= MAX_SUPPORTED_CHANNELS &&
                hashTypes.stream()
                        .map(ColumnValueExtractor::columnValueExtractor)
                        .allMatch(extractor -> extractor.map(ColumnValueExtractor::isFixedSize).orElse(false))) {
            return new HashTableDataGroupByHash(
                    isolatedHashTableFactory,
                    hashTypes,
                    hashChannels,
                    inputHashChannel,
                    expectedSize,
                    processDictionary,
                    updateMemory,
                    blockTypeOperators);
        }

        return new MultiChannelGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler, blockTypeOperators, updateMemory);
    }
}
