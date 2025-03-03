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
package io.trino.plugin.iceberg.delete;

import io.trino.filesystem.TrinoInput;
import org.roaringbitmap.longlong.LongBitmapDataProvider;

import java.io.IOException;

import static java.lang.Math.toIntExact;
import static org.apache.iceberg.deletes.TrinoBitmapPositionDeleteIndex.deserialize;

public final class DeletionVectors
{
    public static final int LENGTH_SIZE_BYTES = 4;
    public static final int CRC_SIZE_BYTES = 4;

    private DeletionVectors() {}

    public static void readDeletionVector(TrinoInput input, long recordCount, Long contentOffset, Long contentSizeInBytes, LongBitmapDataProvider deletedRows)
            throws IOException
    {
        byte[] bytes = input.readFully(contentOffset, LENGTH_SIZE_BYTES + toIntExact(contentSizeInBytes) + CRC_SIZE_BYTES).getBytes();
        deserialize(bytes, recordCount, contentSizeInBytes).forEach(deletedRows::addLong);
    }
}
