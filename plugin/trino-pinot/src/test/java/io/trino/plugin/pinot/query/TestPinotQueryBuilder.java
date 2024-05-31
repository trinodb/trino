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
package io.trino.plugin.pinot.query;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

final class TestPinotQueryBuilder
{
    @Test
    public void testParseQueryOption()
    {
        String options = "limitForSegmentQueries:1000,limitForBrokerQueries:1000,targetSegmentPageSizeBytes:1000";
        Map<String, String> parssedOptions = PinotQueryBuilder.parseQueryOptions(options);
        assertThat(parssedOptions).containsExactly(entry("limitForSegmentQueries", "1000"), entry("limitForBrokerQueries", "1000"), entry("targetSegmentPageSizeBytes", "1000"));
    }

    @Test
    public void testParseQueryOptionWithQuotes()
    {
        String options = "enableNullHandling:true,skipUpsert:true,varcharOption:'value'";
        Map<String, String> parssedOptions = PinotQueryBuilder.parseQueryOptions(options);
        assertThat(parssedOptions).containsExactly(entry("enableNullHandling", "true"), entry("skipUpsert", "true"), entry("varcharOption", "'value'"));
    }

    @Test
    public void testParseQueryOptionWithEmptyString()
    {
        String options = "";
        Map<String, String> parssedOptions = PinotQueryBuilder.parseQueryOptions(options);
        assertThat(parssedOptions).isEmpty();
    }
}
