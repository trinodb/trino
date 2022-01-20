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
package io.trino.operator.hash;

import com.google.common.collect.ImmutableList;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.HashGenerator;
import io.trino.operator.Work;
import io.trino.operator.hash.fixedwidth.FixedWidthEntryStructure;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class HashTableManager
{
    private final HashTableDataGroupByHash groupByHash;
    private final IsolatedHashTableFactory isolatedHashTableFactory;
    private final HashGenerator hashGenerator;
    private final FixedWidthEntryStructure structure;
    private final HashTableValuesAppender valuesAppender;
    private GroupByHashTable containsHashTable;
    private Page lastContainsPage;

    public HashTableManager(
            HashTableDataGroupByHash groupByHash,
            IsolatedHashTableFactory isolatedHashTableFactory,
            List<? extends Type> hashTypes,
            HashGenerator hashGenerator)
    {
        this.groupByHash = groupByHash;
        this.isolatedHashTableFactory = isolatedHashTableFactory;
        this.hashGenerator = hashGenerator;
        this.structure = new FixedWidthEntryStructure(hashTypes.stream()
                .map(ColumnValueExtractor::requiredColumnValueExtractor)
                .peek(columnValueExtractor -> checkArgument(columnValueExtractor.isFixedSize(), "channel is not fixed width " + columnValueExtractor))
                .toArray(ColumnValueExtractor[]::new));
        this.valuesAppender = isolatedHashTableFactory.createAppender(structure);
    }

    public GroupByHashTable getForContains(Page page)
    {
        if (containsHashTable == null || page != lastContainsPage) {
            containsHashTable = isolatedHashTableFactory.createHashTable(structure, blockTypes(page), hashGenerator);
            lastContainsPage = page;
        }
        return containsHashTable;
    }

    public Work<Void> getAddPageWork(Page page)
    {
        WorkFactory<Void> workFactory = isolatedHashTableFactory.createAddPageWorkFactory(structure, blockTypes(page));
        return workFactory.create(groupByHash, getHashTable(page), page);
    }

    public Work<Void> getAAddDictionaryPageWork(Page page)
    {
        WorkFactory<Void> workFactory = isolatedHashTableFactory.createAAddDictionaryPageWork(structure, blockTypes(page));
        return workFactory.create(groupByHash, getHashTable(page), page);
    }

    public Work<GroupByIdBlock> getGetGroupIdsWorkFactory(Page page)
    {
        WorkFactory<GroupByIdBlock> workFactory = isolatedHashTableFactory.createGetGroupIdsWorkFactory(structure, blockTypes(page));
        return workFactory.create(groupByHash, getHashTable(page), page);
    }

    public Work<GroupByIdBlock> getGetDictionaryGroupIdsWorkFactory(Page page)
    {
        WorkFactory<GroupByIdBlock> workFactory = isolatedHashTableFactory.createGetDictionaryGroupIdsWorkFactory(structure, blockTypes(page));
        return workFactory.create(groupByHash, getHashTable(page), page);
    }

    public GroupByHashTable getHashTable(Page page)
    {
        return isolatedHashTableFactory.createHashTable(structure, blockTypes(page), hashGenerator);
    }

    public HashTableValuesAppender valuesAppender()
    {
        return valuesAppender;
    }

    public int valuesLTotalLength()
    {
        return structure.getValuesLength();
    }

    private List<Class<? extends Block>> blockTypes(Page page)
    {
        ImmutableList.Builder<Class<? extends Block>> result = ImmutableList.builder();
        for (int i = 0; i < structure.getHashChannelsCount(); i++) {
            result.add(page.getBlock(i).getClass());
        }
        return result.build();
    }
}
