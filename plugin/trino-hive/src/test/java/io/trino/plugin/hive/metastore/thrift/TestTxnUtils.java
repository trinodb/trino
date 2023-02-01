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
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import static io.trino.plugin.hive.metastore.thrift.TxnUtils.createValidReadTxnList;
import static io.trino.plugin.hive.metastore.thrift.TxnUtils.createValidTxnWriteIdList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTxnUtils
{
    @Test
    public void testCreateValidReadTxnList()
    {
        BitSet aborted = new BitSet();
        aborted.set(2, 4);
        ByteBuffer abortedBits = ByteBuffer.wrap(aborted.toByteArray());

        long currentTxn = 7;
        var trinoResponse = new GetOpenTxnsResponse(6, List.of(1L, 2L, 3L), abortedBits);
        String trinoValue = createValidReadTxnList(trinoResponse, currentTxn);

        var hiveResponse = new org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse(
                trinoResponse.getTxnHighWaterMark(),
                trinoResponse.getOpenTxns(),
                trinoResponse.bufferForAbortedBits());
        String hiveValue = org.apache.hadoop.hive.metastore.txn.TxnUtils.createValidReadTxnList(hiveResponse, currentTxn).toString();

        assertThat(trinoValue)
                .isEqualTo(hiveValue)
                .isEqualTo("6:1:1,2,0:3");
    }

    @Test
    public void testCreateValidTxnWriteIdList()
    {
        BitSet aborted = new BitSet();
        aborted.set(2, 4);
        ByteBuffer abortedBits = ByteBuffer.wrap(aborted.toByteArray());

        long currentTxn = 7;
        TableValidWriteIds table1 = new TableValidWriteIds("abc.xyz", 6, List.of(1L, 2L, 3L), abortedBits);
        TableValidWriteIds table2 = new TableValidWriteIds("foo.bar", 5, List.of(3L, 4L), ByteBuffer.wrap(new byte[0]));
        table2.setMinOpenWriteId(2);
        var trinoIds = List.of(table1, table2);
        String trinoValue = createValidTxnWriteIdList(currentTxn, trinoIds);

        var hiveIds = trinoIds.stream()
                .map(TestTxnUtils::toHiveTableValidWriteIds)
                .toList();
        String hiveValue = org.apache.hadoop.hive.metastore.txn.TxnUtils.createValidTxnWriteIdList(currentTxn, hiveIds).toString();

        // the expected result depends on HashMap iteration order (matches Hive behavior)
        assertThat(trinoValue)
                .isEqualTo(hiveValue)
                .isEqualTo("7$foo.bar:5:2:3,4:$abc.xyz:6:9223372036854775807:1,2:3");
    }

    @Test
    public void testCreateValidWriteIdList()
    {
        BitSet aborted = new BitSet();
        aborted.set(2, 4);
        ByteBuffer abortedBits = ByteBuffer.wrap(aborted.toByteArray());

        var trinoIds = new TableValidWriteIds("abc.xyz", 6, List.of(1L, 2L, 3L), abortedBits);
        String trinoValue = TxnUtils.createValidWriteIdList(trinoIds);

        var hiveIds = toHiveTableValidWriteIds(trinoIds);
        String hiveValue = org.apache.hadoop.hive.metastore.txn.TxnUtils.createValidReaderWriteIdList(hiveIds).toString();

        assertThat(trinoValue)
                .isEqualTo(hiveValue)
                .isEqualTo("abc.xyz:6:9223372036854775807:1,2:3");
    }

    private static org.apache.hadoop.hive.metastore.api.TableValidWriteIds toHiveTableValidWriteIds(TableValidWriteIds ids)
    {
        var result = new org.apache.hadoop.hive.metastore.api.TableValidWriteIds(
                ids.getFullTableName(),
                ids.getWriteIdHighWaterMark(),
                ids.getInvalidWriteIds(),
                ids.bufferForAbortedBits());
        if (ids.isSetMinOpenWriteId()) {
            result.setMinOpenWriteId(ids.getMinOpenWriteId());
        }
        return result;
    }
}
