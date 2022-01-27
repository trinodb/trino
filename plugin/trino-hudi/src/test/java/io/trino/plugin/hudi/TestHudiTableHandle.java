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

import io.airlift.json.JsonCodec;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.testng.annotations.Test;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.testng.Assert.assertEquals;

public class TestHudiTableHandle
{
    private final JsonCodec<HudiTableHandle> codec = JsonCodec.jsonCodec(HudiTableHandle.class);

    @Test
    public void testJsonRoundTrip()
    {
        HudiTableHandle expectedHandle = new HudiTableHandle(
                "schema",
                "table",
                "/tmp/hudi_trips",
                HoodieTableType.valueOf(COPY_ON_WRITE.name()),
                TupleDomain.none(),
                TupleDomain.all());

        String json = codec.toJson(expectedHandle);
        HudiTableHandle actualHandle = codec.fromJson(json);

        assertEquals(actualHandle.getSchemaName(), expectedHandle.getSchemaName());
        assertEquals(actualHandle.getTableName(), expectedHandle.getTableName());
        assertEquals(actualHandle.getPartitionPredicates(), expectedHandle.getPartitionPredicates());
        assertEquals(actualHandle.getRegularPredicates(), expectedHandle.getRegularPredicates());
        assertEquals(actualHandle.getTableType(), expectedHandle.getTableType());
        assertEquals(actualHandle.getBasePath(), expectedHandle.getBasePath());
        assertEquals(actualHandle.getTableType(), expectedHandle.getTableType());
    }
}
