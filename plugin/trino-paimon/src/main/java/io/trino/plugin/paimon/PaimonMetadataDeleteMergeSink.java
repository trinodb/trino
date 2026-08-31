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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMergeSink;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_COMMIT_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class PaimonMetadataDeleteMergeSink
        implements ConnectorMergeSink
{
    private static final int FRAGMENT_MAGIC = 0x504D4446; // PMDF
    private static final int FRAGMENT_VERSION = 1;
    private static final int FRAGMENT_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES;

    private long deletedRowCount;

    @Override
    public void storeMergedRows(Page page)
    {
        requireNonNull(page, "page is null");
        try {
            validateInputPage(page);
            if (page.getPositionCount() == 0) {
                return;
            }

            int operationChannel = page.getChannelCount() - 3;
            for (int position = 0; position < page.getPositionCount(); position++) {
                byte operation = TINYINT.getByte(page.getBlock(operationChannel), position);
                if (operation != DELETE_OPERATION_NUMBER) {
                    throw new TrinoException(NOT_SUPPORTED,
                            "Paimon metadata-delete merge sink only supports DELETE rows, got merge operation: "
                                    + operation);
                }
            }
            try {
                deletedRowCount = addExact(deletedRowCount, page.getPositionCount());
            }
            catch (ArithmeticException e) {
                throw new TrinoException(
                        PAIMON_COMMIT_ERROR,
                        "Paimon metadata-delete merge row count exceeds the supported range",
                        e);
            }
        }
        catch (Exception e) {
            throw PaimonPageSink.wrapWriteException(e);
        }
    }

    private static void validateInputPage(Page page)
    {
        int inputChannelCount = page.getChannelCount();
        if (inputChannelCount < 3) {
            throw new IllegalArgumentException("inputPage channelCount (%s) must include operation and rowId channels"
                    .formatted(inputChannelCount));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (deletedRowCount == 0) {
            return completedFuture(List.of());
        }
        return completedFuture(List.of(encodeDeletedRowCount(deletedRowCount)));
    }

    static Slice encodeDeletedRowCount(long deletedRowCount)
    {
        if (deletedRowCount < 0) {
            throw new IllegalArgumentException("deletedRowCount is negative: " + deletedRowCount);
        }
        Slice fragment = Slices.allocate(FRAGMENT_SIZE);
        fragment.setInt(0, FRAGMENT_MAGIC);
        fragment.setInt(Integer.BYTES, FRAGMENT_VERSION);
        fragment.setLong(Integer.BYTES + Integer.BYTES, deletedRowCount);
        return fragment;
    }

    static long decodeDeletedRowCount(Slice fragment)
    {
        requireNonNull(fragment, "fragment is null");
        if (fragment.length() != FRAGMENT_SIZE) {
            throw new IllegalArgumentException("Invalid Paimon metadata-delete merge fragment size: "
                    + fragment.length());
        }
        int magic = fragment.getInt(0);
        if (magic != FRAGMENT_MAGIC) {
            throw new IllegalArgumentException("Invalid Paimon metadata-delete merge fragment magic");
        }
        int version = fragment.getInt(Integer.BYTES);
        if (version != FRAGMENT_VERSION) {
            throw new IllegalArgumentException("Unsupported Paimon metadata-delete merge fragment version: "
                    + version);
        }
        long deletedRows = fragment.getLong(Integer.BYTES + Integer.BYTES);
        if (deletedRows < 0) {
            throw new IllegalArgumentException("Invalid Paimon metadata-delete merge row count: " + deletedRows);
        }
        return deletedRows;
    }
}
