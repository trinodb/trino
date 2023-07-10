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
package io.trino.plugin.raptor.legacy;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.raptor.legacy.metadata.ShardDelta;
import io.trino.plugin.raptor.legacy.metadata.ShardInfo;
import io.trino.plugin.raptor.legacy.storage.ShardRewriter;
import io.trino.plugin.raptor.legacy.storage.StorageManager;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.MergePage;
import io.trino.spi.type.UuidType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toUnmodifiableList;

public class RaptorMergeSink
        implements ConnectorMergeSink
{
    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);

    private final ConnectorPageSink pageSink;
    private final StorageManager storageManager;
    private final long transactionId;
    private final int columnCount;
    private final Map<UUID, Entry<OptionalInt, BitSet>> rowsToDelete = new HashMap<>();

    public RaptorMergeSink(ConnectorPageSink pageSink, StorageManager storageManager, long transactionId, int columnCount)
    {
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.transactionId = transactionId;
        this.columnCount = columnCount;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);

        mergePage.getInsertionsPage().ifPresent(pageSink::appendPage);

        mergePage.getDeletionsPage().ifPresent(deletions -> {
            ColumnarRow rowIdRow = toColumnarRow(deletions.getBlock(deletions.getChannelCount() - 1));
            Block shardBucketBlock = rowIdRow.getField(0);
            Block shardUuidBlock = rowIdRow.getField(1);
            Block shardRowIdBlock = rowIdRow.getField(2);

            for (int position = 0; position < rowIdRow.getPositionCount(); position++) {
                OptionalInt bucketNumber = shardBucketBlock.isNull(position)
                        ? OptionalInt.empty()
                        : OptionalInt.of(INTEGER.getInt(shardBucketBlock, position));
                UUID uuid = trinoUuidToJavaUuid(UuidType.UUID.getSlice(shardUuidBlock, position));
                int rowId = toIntExact(BIGINT.getLong(shardRowIdBlock, position));
                Entry<OptionalInt, BitSet> entry = rowsToDelete.computeIfAbsent(uuid, ignored -> Map.entry(bucketNumber, new BitSet()));
                verify(entry.getKey().equals(bucketNumber), "multiple bucket numbers for same shard");
                entry.getValue().set(rowId);
            }
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<CompletableFuture<Collection<Slice>>> futures = new ArrayList<>();

        rowsToDelete.forEach((uuid, entry) -> {
            OptionalInt bucketNumber = entry.getKey();
            BitSet rowIds = entry.getValue();
            ShardRewriter rewriter = storageManager.createShardRewriter(transactionId, bucketNumber, uuid);
            futures.add(rewriter.rewrite(rowIds));
        });

        futures.add(pageSink.finish().thenApply(slices -> {
            List<ShardInfo> newShards = slices.stream()
                    .map(slice -> SHARD_INFO_CODEC.fromJson(slice.getBytes()))
                    .collect(toImmutableList());
            ShardDelta delta = new ShardDelta(ImmutableList.of(), newShards);
            return ImmutableList.of(Slices.wrappedBuffer(SHARD_DELTA_CODEC.toJsonBytes(delta)));
        }));

        return allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> futures.stream()
                        .map(CompletableFuture::join)
                        .flatMap(Collection::stream)
                        .collect(toUnmodifiableList()));
    }

    @Override
    public void abort()
    {
        pageSink.abort();
    }
}
