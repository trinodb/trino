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

package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.spi.SplitWeight;
import io.trino.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestHudiSplit
{
    private final JsonCodec<HudiSplit> codec = JsonCodec.jsonCodec(HudiSplit.class);

    @Test
    public void testJsonRoundTrip()
    {
        HudiSplit expectedSplit = new HudiSplit(
                "/user/hive/warehouse/stock_ticks_cow/2018/08/31/871677fb-e0e3-46f8-9cc1-fe497e317216-0_0-28-26_20211216071453747.parquet",
                0L,
                440747L,
                440747L,
                ImmutableList.of(),
                TupleDomain.all(),
                ImmutableList.of(),
                SplitWeight.fromProportion(0.1));

        String json = codec.toJson(expectedSplit);
        HudiSplit actualSplit = codec.fromJson(json);

        assertEquals(actualSplit.getPath(), expectedSplit.getPath());
        assertEquals(actualSplit.getAddresses(), expectedSplit.getAddresses());
        assertEquals(actualSplit.getPartitionKeys(), expectedSplit.getPartitionKeys());
        assertEquals(actualSplit.getStart(), expectedSplit.getStart());
        assertEquals(actualSplit.getLength(), expectedSplit.getLength());
        assertEquals(actualSplit.getFileSize(), expectedSplit.getFileSize());
        assertEquals(actualSplit.getSplitWeight(), expectedSplit.getSplitWeight());
    }
}
