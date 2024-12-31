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
package io.trino.plugin.paimon;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;
import org.apache.paimon.types.RowKind;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Trino {@link BucketFunction}.
 */
public class FixedBucketTableShuffleFunction
        implements BucketFunction
{
    private final int workerCount;
    private final int bucketCount;
    private final boolean isRowId;
    private final ThreadLocal<Projection> projectionContext;

    public FixedBucketTableShuffleFunction(
            List<Type> partitionChannelTypes,
            PaimonPartitioningHandle partitioningHandle,
            int workerCount)
    {
        TableSchema schema = partitioningHandle.getOriginalSchema();
        this.projectionContext =
                ThreadLocal.withInitial(
                        () ->
                                CodeGenUtils.newProjection(
                                        schema.logicalPrimaryKeysType(), schema.primaryKeys()));
        this.bucketCount = new CoreOptions(schema.options()).bucket();
        this.workerCount = workerCount;
        this.isRowId =
                partitionChannelTypes.size() == 1
                        && partitionChannelTypes.get(0) instanceof RowType;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        if (isRowId) {
            RowBlock rowBlock = (RowBlock) page.getBlock(0);
            try {
                Method method = RowBlock.class.getDeclaredMethod("getRawFieldBlocks");
                method.setAccessible(true);
                page = new Page(rowBlock.getPositionCount(), (Block[]) method.invoke(rowBlock));
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
            catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        PaimonRow paimonRow = new PaimonRow(page.getSingleValuePage(position), RowKind.INSERT);
        BinaryRow pk = projectionContext.get().apply(paimonRow);
        int bucket =
                KeyAndBucketExtractor.bucket(
                        KeyAndBucketExtractor.bucketKeyHashCode(pk), bucketCount);
        return bucket % workerCount;
    }
}
