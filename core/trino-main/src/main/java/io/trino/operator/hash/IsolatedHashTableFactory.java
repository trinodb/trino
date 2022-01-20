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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.HashGenerator;
import io.trino.operator.Work;
import io.trino.operator.hash.HashTableDataGroupByHash.AddDictionaryPageWork;
import io.trino.operator.hash.HashTableDataGroupByHash.AddPageWork;
import io.trino.operator.hash.HashTableDataGroupByHash.GetDictionaryGroupIdsWork;
import io.trino.operator.hash.HashTableDataGroupByHash.GetGroupIdsWork;
import io.trino.operator.hash.fixedwidth.FixedWidthEntryStructure;
import io.trino.operator.hash.fixedwidth.GenericFixedWidthHashTableRowHandler;
import io.trino.operator.hash.fixedwidth.GenericFixedWidthHashTableValuesAppender;
import io.trino.operator.hash.fixedwidth.gen.FixedWidthHashTableRowHandler1Channels;
import io.trino.operator.hash.fixedwidth.gen.FixedWidthHashTableValuesAppender1Channels;
import io.trino.spi.block.Block;
import io.trino.sql.gen.IsolatedClass;

import javax.inject.Inject;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IsolatedHashTableFactory
{
    public static final int MAX_SUPPORTED_CHANNELS = 20;
    private static final Class<? extends HashTableRowHandler>[] DEDICATED_ROW_HANDLERS;
    private static final Class<? extends HashTableValuesAppender>[] DEDICATED_VALUES_APPENDERS;

    private final LoadingCache<HashTableCacheKey, GroupByHashTable> hashTableCache;
    private final LoadingCache<FixedWidthEntryStructure, HashTableValuesAppender> appenderCache;
    private final LoadingCache<StructureAndBlockCacheKey, WorkFactory<Void>> addPageWorkFactoryCache;
    private final LoadingCache<StructureAndBlockCacheKey, WorkFactory<Void>> addDictionaryPageWorkFactoryCache;
    private final LoadingCache<StructureAndBlockCacheKey, WorkFactory<GroupByIdBlock>> getGroupIdsWorkFactoryCache;
    private final LoadingCache<StructureAndBlockCacheKey, WorkFactory<GroupByIdBlock>> getDictionaryGroupIdsWorkFactoryCache;

    static {
        // setup dedicated per number of channels row handler and values appender classes
        DEDICATED_ROW_HANDLERS = new Class[MAX_SUPPORTED_CHANNELS];
        DEDICATED_VALUES_APPENDERS = new Class[MAX_SUPPORTED_CHANNELS];
        for (int i = 0; i < DEDICATED_ROW_HANDLERS.length; i++) {
            try {
                DEDICATED_ROW_HANDLERS[i] = (Class<? extends HashTableRowHandler>) Class.forName(FixedWidthHashTableRowHandler1Channels.class.getPackageName() + "." +
                        format("FixedWidthHashTableRowHandler%dChannels", i + 1));
                DEDICATED_VALUES_APPENDERS[i] = (Class<? extends HashTableValuesAppender>) Class.forName(FixedWidthHashTableValuesAppender1Channels.class.getPackageName() + "." +
                        format("FixedWidthHashTableValuesAppender%dChannels", i + 1));
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Inject
    public IsolatedHashTableFactory()
    {
        this.hashTableCache = CacheBuilder.newBuilder()
                .maximumSize(1024 * 16)
                .build(CacheLoader.from(key -> isolateHashTable(key.getStructure(), key.getHashGenerator())));

        this.appenderCache = CacheBuilder.newBuilder()
                .maximumSize(1024 * 16)
                .build(CacheLoader.from(this::isolateAppender));

        this.addPageWorkFactoryCache = CacheBuilder.newBuilder()
                .maximumSize(1024 * 16)
                .build(CacheLoader.from(key -> isolateWorkFactory(AddPageWork.class)));

        this.addDictionaryPageWorkFactoryCache = CacheBuilder.newBuilder()
                .maximumSize(1024 * 16)
                .build(CacheLoader.from(key -> isolateWorkFactory(AddDictionaryPageWork.class)));

        this.getGroupIdsWorkFactoryCache = CacheBuilder.newBuilder()
                .maximumSize(1024 * 16)
                .build(CacheLoader.from(key -> isolateWorkFactory(GetGroupIdsWork.class)));

        this.getDictionaryGroupIdsWorkFactoryCache = CacheBuilder.newBuilder()
                .maximumSize(1024 * 16)
                .build(CacheLoader.from(key -> isolateWorkFactory(GetDictionaryGroupIdsWork.class)));
    }

    public GroupByHashTable createHashTable(FixedWidthEntryStructure structure, List<Class<? extends Block>> blockTypes, HashGenerator hashGenerator)
    {
        return hashTableCache.getUnchecked(new HashTableCacheKey(structure, blockTypes, hashGenerator));
    }

    public WorkFactory<Void> createAddPageWorkFactory(FixedWidthEntryStructure structure, List<Class<? extends Block>> blockTypes)
    {
        return addPageWorkFactoryCache.getUnchecked(new StructureAndBlockCacheKey(structure, blockTypes));
    }

    public WorkFactory<Void> createAAddDictionaryPageWork(FixedWidthEntryStructure structure, List<Class<? extends Block>> blockTypes)
    {
        return addDictionaryPageWorkFactoryCache.getUnchecked(new StructureAndBlockCacheKey(structure, blockTypes));
    }

    public WorkFactory<GroupByIdBlock> createGetGroupIdsWorkFactory(FixedWidthEntryStructure structure, List<Class<? extends Block>> blockTypes)
    {
        return getGroupIdsWorkFactoryCache.getUnchecked(new StructureAndBlockCacheKey(structure, blockTypes));
    }

    public WorkFactory<GroupByIdBlock> createGetDictionaryGroupIdsWorkFactory(FixedWidthEntryStructure structure, List<Class<? extends Block>> blockTypes)
    {
        return getDictionaryGroupIdsWorkFactoryCache.getUnchecked(new StructureAndBlockCacheKey(structure, blockTypes));
    }

    public HashTableValuesAppender createAppender(FixedWidthEntryStructure structure)
    {
        return appenderCache.getUnchecked(structure);
    }

    private HashTableValuesAppender isolateAppender(FixedWidthEntryStructure structure)
    {
        Class<? extends HashTableValuesAppender> appenderClass = GenericFixedWidthHashTableValuesAppender.class;
        if (structure.getHashChannelsCount() <= DEDICATED_VALUES_APPENDERS.length) {
            appenderClass = DEDICATED_VALUES_APPENDERS[structure.getHashChannelsCount() - 1];
        }

        Class<? extends HashTableValuesAppender> valuesAppenderClass = IsolatedClass.isolateClass(
                new DynamicClassLoader(HashTableValuesAppender.class.getClassLoader()),
                HashTableValuesAppender.class,
                appenderClass,
                AbstractHashTableValuesAppender.class);
        try {
            return valuesAppenderClass.getConstructor(FixedWidthEntryStructure.class).newInstance(structure);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> WorkFactory<T> isolateWorkFactory(Class<? extends Work<T>> workClass)
    {
        Class<? extends Work<Void>> isolatedWorkClass = (Class<? extends Work<Void>>) IsolatedClass.isolateClass(
                new DynamicClassLoader(HashTableDataGroupByHash.class.getClassLoader()),
                Work.class,
                workClass);
        return new WorkFactory(isolatedWorkClass);
    }

    private GroupByHashTable isolateHashTable(FixedWidthEntryStructure structure, HashGenerator hashGenerator)
    {
        Class<? extends HashTableRowHandler> rowHandlerClass = GenericFixedWidthHashTableRowHandler.class;
        if (structure.getHashChannelsCount() <= DEDICATED_ROW_HANDLERS.length) {
            rowHandlerClass = DEDICATED_ROW_HANDLERS[structure.getHashChannelsCount() - 1];
        }

        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(GroupByHashTable.class.getClassLoader());

        try {
            Class<? extends HashTableRowHandler> isolatedRowHandlerClass = IsolatedClass.isolateClass(
                    dynamicClassLoader,
                    HashTableRowHandler.class,
                    rowHandlerClass,
                    RowHandlerGroupByHashTable.class);

            Class<? extends GroupByHashTable> hashTableClass = (Class<? extends GroupByHashTable>) dynamicClassLoader
                    .loadClass(RowHandlerGroupByHashTable.class.getName());

            HashTableRowHandler rowHandler = isolatedRowHandlerClass.getConstructor(FixedWidthEntryStructure.class).newInstance(structure);

            Constructor<? extends GroupByHashTable> hashTableConstructor = hashTableClass.getConstructor(HashGenerator.class, HashTableRowHandler.class);

            return hashTableConstructor.newInstance(hashGenerator, rowHandler);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static class HashTableCacheKey
            extends StructureAndBlockCacheKey
    {
        private final HashGenerator hashGenerator;

        private HashTableCacheKey(FixedWidthEntryStructure structure, List<Class<? extends Block>> blockTypes, HashGenerator hashGenerator)
        {
            super(structure, blockTypes);
            this.hashGenerator = hashGenerator;
        }

        public HashGenerator getHashGenerator()
        {
            return hashGenerator;
        }
    }

    private static class StructureAndBlockCacheKey
    {
        private final FixedWidthEntryStructure structure;
        private final List<Class<? extends Block>> blockTypes;

        private StructureAndBlockCacheKey(
                FixedWidthEntryStructure structure,
                List<Class<? extends Block>> blockTypes)
        {
            this.structure = requireNonNull(structure, "structure is null");
            this.blockTypes = requireNonNull(blockTypes, "blockTypes is null");
        }

        public FixedWidthEntryStructure getStructure()
        {
            return structure;
        }

        public List<Class<? extends Block>> getBlockTypes()
        {
            return blockTypes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StructureAndBlockCacheKey)) {
                return false;
            }
            StructureAndBlockCacheKey that = (StructureAndBlockCacheKey) o;
            return structure.equals(that.structure) && blockTypes.equals(that.blockTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(structure, blockTypes);
        }
    }
}
