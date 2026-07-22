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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.bigquery.BigQuerySchemaProperties.LOCATION_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQuerySchemaProperties
{
    @Test
    public void testLocationAbsent()
    {
        assertThat(BigQuerySchemaProperties.location(ImmutableMap.of())).isEmpty();
    }

    @Test
    public void testLocationPresent()
    {
        assertThat(BigQuerySchemaProperties.location(ImmutableMap.of(LOCATION_PROPERTY, "asia-northeast1")))
                .contains("asia-northeast1");
    }
}
