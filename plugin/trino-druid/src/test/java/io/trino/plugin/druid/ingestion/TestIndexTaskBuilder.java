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
package io.trino.plugin.druid.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestIndexTaskBuilder
{
    @Test
    public void testIngestionSpec()
            throws JsonProcessingException
    {
        String expected = "{\"type\":\"index\",\"spec\":{\"dataSchema\":{\"dataSource\":\"test_datasource\",\"parser\":{\"type\":\"string\",\"parseSpec\":{\"format\":\"tsv\",\"timestampSpec\":{\"column\":\"dummy_druid_ts\",\"format\":\"auto\"},\"columns\":[\"dummy_druid_ts\",\"col_0\",\"col_1\"],\"dimensionsSpec\":{\"dimensions\":[{\"name\":\"col_0\",\"type\":\"string\"},{\"name\":\"col_1\",\"type\":\"long\"}]}}},\"granularitySpec\":{\"type\":\"uniform\",\"intervals\":[\"1992-01-02/2028-12-01\"],\"segmentGranularity\":\"year\",\"queryGranularity\":\"day\"}},\"ioConfig\":{\"type\":\"index\",\"firehose\":{\"type\":\"local\",\"baseDir\":\"/opt/druid/var/\",\"filter\":\"test_datasource.tsv\"},\"appendToExisting\":false},\"tuningConfig\":{\"type\":\"index\",\"maxRowsPerSegment\":5000000,\"maxRowsInMemory\":250000,\"segmentWriteOutMediumFactory\":{\"type\":\"offHeapMemory\"}}}}";
        IndexTaskBuilder builder = new IndexTaskBuilder();
        builder.setDatasource("test_datasource");
        builder.addColumn("col_0", "string");
        builder.addColumn("col_1", "long");
        builder.setTimestampSpec(new TimestampSpec("dummy_druid_ts", "auto"));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readValue(builder.build(), JsonNode.class);

        assertThat(jsonNode.toString()).isEqualTo(expected);
    }
}
