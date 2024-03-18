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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.Session;
import io.trino.annotation.NotThreadSafe;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.IsolatedClass;

import java.lang.reflect.Constructor;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;

@NotThreadSafe
public interface GroupByHash
{
    NonEvictableLoadingCache<Type, Class<? extends GroupByHash>> specializedGroupByHashClasses = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                .maximumSize(256),
            CacheLoader.from(type -> isolateGroupByHashClass()));

    static Class<? extends GroupByHash> isolateGroupByHashClass()
    {
        return IsolatedClass.isolateClass(
                    new DynamicClassLoader(GroupByHash.class.getClassLoader()),
                    GroupByHash.class,
                    BigintGroupByHash.class,
                    BigintGroupByHash.AddPageWork.class,
                    BigintGroupByHash.AddDictionaryPageWork.class,
                    BigintGroupByHash.AddRunLengthEncodedPageWork.class,
                    BigintGroupByHash.GetGroupIdsWork.class,
                    BigintGroupByHash.GetDictionaryGroupIdsWork.class,
                    BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class,
                    BigintGroupByHash.DictionaryLookBack.class,
                    BigintGroupByHash.ValuesArray.class,
                    BigintGroupByHash.LongValuesArray.class,
                    BigintGroupByHash.IntegerValuesArray.class,
                    BigintGroupByHash.ShortValuesArray.class,
                    BigintGroupByHash.ByteValuesArray.class);
    }

    static GroupByHash createGroupByHash(
            Session session,
            List<Type> types,
            boolean hasPrecomputedHash,
            int expectedSize,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory)
    {
        boolean dictionaryAggregationEnabled = isDictionaryAggregationEnabled(session);
        return createGroupByHash(types, hasPrecomputedHash, expectedSize, dictionaryAggregationEnabled, hashStrategyCompiler, updateMemory);
    }

    static GroupByHash createGroupByHash(
            List<Type> types,
            boolean hasPrecomputedHash,
            int expectedSize,
            boolean dictionaryAggregationEnabled,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory)
    {
        try {
            if (types.size() == 1 && BigintGroupByHash.isSupportedType(types.get(0))) {
                Type hashType = getOnlyElement(types);
                Constructor<? extends GroupByHash> constructor = specializedGroupByHashClasses.getUnchecked(hashType).getConstructor(boolean.class, int.class, UpdateMemory.class, Type.class);
                return constructor.newInstance(hasPrecomputedHash, expectedSize, updateMemory, hashType);
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        return new FlatGroupByHash(types, hasPrecomputedHash, expectedSize, dictionaryAggregationEnabled, hashStrategyCompiler, updateMemory);
    }

    long getEstimatedSize();

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder);

    Work<?> addPage(Page page);

    /**
     * The order of new group ids need to be the same as the order of incoming rows,
     * i.e. new group ids should be assigned in rows iteration order
     * Example:
     * rows:      A B C B D A E
     * group ids: 1 2 3 2 4 1 5
     */
    Work<int[]> getGroupIds(Page page);

    long getRawHash(int groupId);

    @VisibleForTesting
    int getCapacity();
}
