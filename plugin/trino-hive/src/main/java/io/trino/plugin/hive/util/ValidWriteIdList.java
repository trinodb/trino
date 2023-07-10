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
package io.trino.plugin.hive.util;

import com.google.common.base.Splitter;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Long.parseLong;

// copied from org.apache.hadoop.hive.common.ValidReaderWriteIdList
public final class ValidWriteIdList
{
    private static final Splitter COMMA_SPLITTER = Splitter.on(',');
    private static final Splitter COLON_SPLITTER = Splitter.on(':');

    private final String tableName;
    private final long[] exceptions;
    private final long minOpenWriteId;
    private final long highWatermark;

    public ValidWriteIdList(String value)
    {
        if (isNullOrEmpty(value)) {
            highWatermark = Long.MAX_VALUE;
            exceptions = new long[0];
            minOpenWriteId = Long.MAX_VALUE;
            tableName = null;
            return;
        }

        List<String> values = COLON_SPLITTER.splitToList(value);
        checkArgument(values.size() >= 3, "invalid write ID list: %s", value);

        tableName = values.get(0).equalsIgnoreCase("null") ? null : values.get(0);
        highWatermark = parseLong(values.get(1));
        minOpenWriteId = parseLong(values.get(2));

        List<String> openWriteIds = List.of();
        List<String> abortedWriteIds = List.of();

        if (values.size() < 4) {
            // ignore
        }
        else if (values.size() == 4) {
            if (!values.get(3).isEmpty()) {
                openWriteIds = COMMA_SPLITTER.splitToList(values.get(3));
            }
        }
        else {
            if (!values.get(3).isEmpty()) {
                openWriteIds = COMMA_SPLITTER.splitToList(values.get(3));
            }
            if (!values.get(4).isEmpty()) {
                abortedWriteIds = COMMA_SPLITTER.splitToList(values.get(4));
            }
        }

        exceptions = new long[openWriteIds.size() + abortedWriteIds.size()];
        int i = 0;
        for (String open : openWriteIds) {
            exceptions[i] = parseLong(open);
            i++;
        }
        for (String abort : abortedWriteIds) {
            exceptions[i] = parseLong(abort);
            i++;
        }

        Arrays.sort(exceptions);
    }

    public String getTableName()
    {
        return tableName;
    }

    public long getHighWatermark()
    {
        return highWatermark;
    }

    public long[] getInvalidWriteIds()
    {
        return exceptions;
    }

    public boolean isWriteIdValid(long writeId)
    {
        if (writeId > highWatermark) {
            return false;
        }
        return Arrays.binarySearch(exceptions, writeId) < 0;
    }

    public boolean isValidBase(long writeId)
    {
        return (writeId < minOpenWriteId) && (writeId <= highWatermark);
    }

    public boolean isWriteIdRangeValid(long minWriteId, long maxWriteId)
    {
        if (minWriteId > highWatermark) {
            return false;
        }
        if ((exceptions.length > 0) && (maxWriteId < exceptions[0])) {
            return true;
        }

        long count = Math.max(0, maxWriteId - highWatermark);
        for (long txn : exceptions) {
            if ((minWriteId <= txn) && (txn <= maxWriteId)) {
                count++;
            }
        }

        return (count == 0) || (count != ((maxWriteId - minWriteId) + 1));
    }
}
