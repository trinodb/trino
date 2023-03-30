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
package io.trino.plugin.hive.metastore.thrift;

import io.trino.hive.thrift.metastore.GetOpenTxnsResponse;
import io.trino.hive.thrift.metastore.TableValidWriteIds;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.util.Collections.binarySearch;
import static java.util.Objects.requireNonNullElse;

// based on org.apache.hadoop.hive.metastore.txn.TxnUtils
public final class TxnUtils
{
    private TxnUtils() {}

    public static String createValidReadTxnList(GetOpenTxnsResponse txns, long currentTxn)
    {
        List<Long> openTxns = txns.getOpenTxns();
        int sizeToHwm = (currentTxn > 0) ? binarySearch(openTxns, currentTxn) : openTxns.size();
        long[] exceptions = new long[abs(sizeToHwm)];
        BitSet inAbortedBits = BitSet.valueOf(txns.getAbortedBits());
        BitSet outAbortedBits = new BitSet();
        long minOpenTxnId = Long.MAX_VALUE;
        int i = 0;
        for (long txn : openTxns) {
            if ((currentTxn > 0) && (txn >= currentTxn)) {
                break;
            }
            if (inAbortedBits.get(i)) {
                outAbortedBits.set(i);
            }
            else if (minOpenTxnId == Long.MAX_VALUE) {
                minOpenTxnId = txn;
            }
            exceptions[i] = txn;
            i++;
        }

        long highWaterMark = (currentTxn > 0) ? min(currentTxn, txns.getTxnHighWaterMark()) : txns.getTxnHighWaterMark();

        StringBuilder builder = new StringBuilder()
                .append(highWaterMark)
                .append(':')
                .append((exceptions.length > 0) ? minOpenTxnId : Long.MAX_VALUE);

        appendTransactions(builder, exceptions, outAbortedBits);

        return builder.toString();
    }

    public static String createValidTxnWriteIdList(long currentTxnId, List<TableValidWriteIds> validIds)
    {
        Map<String, String> lists = new HashMap<>();
        for (TableValidWriteIds ids : validIds) {
            lists.put(ids.getFullTableName(), createValidWriteIdList(ids));
        }

        StringJoiner joiner = new StringJoiner("$")
                .add(String.valueOf(currentTxnId));
        // the result depends on HashMap iteration order (matches Hive behavior)
        lists.values().forEach(joiner::add);
        return joiner.toString();
    }

    public static String createValidWriteIdList(TableValidWriteIds writeIds)
    {
        List<Long> invalids = writeIds.getInvalidWriteIds();
        BitSet abortedBits = BitSet.valueOf(writeIds.getAbortedBits());
        long[] exceptions = new long[invalids.size()];
        int i = 0;
        for (long writeId : invalids) {
            exceptions[i] = writeId;
            i++;
        }

        long minOpenWriteId = writeIds.isSetMinOpenWriteId() ? writeIds.getMinOpenWriteId() : Long.MAX_VALUE;

        StringBuilder builder = new StringBuilder()
                .append(requireNonNullElse(writeIds.getFullTableName(), "null"))
                .append(':').append(writeIds.getWriteIdHighWaterMark())
                .append(':').append((exceptions.length > 0) ? minOpenWriteId : Long.MAX_VALUE);

        appendTransactions(builder, exceptions, abortedBits);

        return builder.toString();
    }

    private static void appendTransactions(StringBuilder builder, long[] exceptions, BitSet abortedBits)
    {
        StringBuilder open = new StringBuilder();
        StringBuilder abort = new StringBuilder();
        for (int i = 0; i < exceptions.length; i++) {
            if (abortedBits.get(i)) {
                if (!abort.isEmpty()) {
                    abort.append(',');
                }
                abort.append(exceptions[i]);
            }
            else {
                if (!open.isEmpty()) {
                    open.append(',');
                }
                open.append(exceptions[i]);
            }
        }
        builder.append(':').append(open);
        builder.append(':').append(abort);
    }
}
