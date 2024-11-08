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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HivePageSourceProvider.BucketAdaptation;
import io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveColumnHandle.isRowIdColumnHandle;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMappingKind.EMPTY;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.plugin.hive.util.HiveTypeUtil.getHiveTypeForDereferences;
import static java.util.Objects.requireNonNull;

public class HivePageSource
        implements ConnectorPageSource
{
    private final List<ColumnMapping> columnMappings;
    private final Optional<BucketAdapter> bucketAdapter;
    private final Optional<BucketValidator> bucketValidator;
    private final Object[] prefilledValues;
    private final Type[] types;
    private final List<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers;

    private final ConnectorPageSource delegate;

    public HivePageSource(
            List<ColumnMapping> columnMappings,
            Optional<BucketAdaptation> bucketAdaptation,
            Optional<BucketValidator> bucketValidator,
            TypeManager typeManager,
            CoercionContext coercionContext,
            ConnectorPageSource delegate)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(coercionContext, "coercionContext is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;
        this.bucketAdapter = bucketAdaptation.map(BucketAdapter::new);
        this.bucketValidator = requireNonNull(bucketValidator, "bucketValidator is null");

        int size = columnMappings.size();

        prefilledValues = new Object[size];
        types = new Type[size];
        ImmutableList.Builder<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers = ImmutableList.builder();

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            Type type = column.getType();
            types[columnIndex] = type;

            if (columnMapping.getKind() != EMPTY && columnMapping.getBaseTypeCoercionFrom().isPresent()) {
                List<Integer> dereferenceIndices = column.getHiveColumnProjectionInfo()
                        .map(HiveColumnProjectionInfo::getDereferenceIndices)
                        .orElse(ImmutableList.of());
                HiveType fromType = getHiveTypeForDereferences(columnMapping.getBaseTypeCoercionFrom().get(), dereferenceIndices).get();
                HiveType toType = columnMapping.getHiveColumnHandle().getHiveType();
                coercers.add(createCoercer(typeManager, fromType, toType, coercionContext));
            }
            else {
                coercers.add(Optional.empty());
            }

            if (columnMapping.getKind() == EMPTY || isRowIdColumnHandle(column)) {
                prefilledValues[columnIndex] = null;
            }
            else if (columnMapping.getKind() == PREFILLED) {
                prefilledValues[columnIndex] = columnMapping.getPrefilledValue().getValue();
            }
        }
        this.coercers = coercers.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return delegate.isBlocked();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            if (bucketAdapter.isPresent()) {
                dataPage = bucketAdapter.get().filterPageToEligibleRowsOrDiscard(dataPage);
                if (dataPage == null) {
                    return null;
                }
            }
            else {
                // bucket adaptation already validates that data is in the right bucket
                final Page dataPageRef = dataPage;
                bucketValidator.ifPresent(validator -> validator.validate(dataPageRef));
            }

            int batchSize = dataPage.getPositionCount();
            List<Block> blocks = new ArrayList<>();
            for (int fieldId = 0; fieldId < columnMappings.size(); fieldId++) {
                ColumnMapping columnMapping = columnMappings.get(fieldId);
                switch (columnMapping.getKind()) {
                    case PREFILLED:
                    case EMPTY:
                        blocks.add(RunLengthEncodedBlock.create(types[fieldId], prefilledValues[fieldId], batchSize));
                        break;
                    case REGULAR:
                    case SYNTHESIZED:
                        Block block = dataPage.getBlock(columnMapping.getIndex());
                        Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = coercers.get(fieldId);
                        if (coercer.isPresent()) {
                            block = new LazyBlock(batchSize, new CoercionLazyBlockLoader(block, coercer.get()));
                        }
                        blocks.add(block);
                        break;
                    case INTERIM:
                        // interim columns don't show up in output
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            return new Page(batchSize, blocks.toArray(new Block[0]));
        }
        catch (TrinoException e) {
            closeAllSuppress(e, this);
            throw e;
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, this);
            throw new TrinoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    public ConnectorPageSource getPageSource()
    {
        return delegate;
    }

    private static final class CoercionLazyBlockLoader
            implements LazyBlockLoader
    {
        private final Function<Block, Block> coercer;
        private Block block;

        public CoercionLazyBlockLoader(Block block, Function<Block, Block> coercer)
        {
            this.block = requireNonNull(block, "block is null");
            this.coercer = requireNonNull(coercer, "coercer is null");
        }

        @Override
        public Block load()
        {
            checkState(block != null, "Already loaded");

            Block loaded = coercer.apply(block.getLoadedBlock());
            // clear reference to loader to free resources, since load was successful
            block = null;

            return loaded;
        }
    }
}
