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
package io.trino.plugin.pinot.client;

import org.apache.pinot.spi.data.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPinotClientSchemaCodec
{
    @Test
    void testDeserializeSchema()
    {
        Schema schema = PinotClient.createSchemaJsonCodec().fromJson(
                """
                {
                  "schemaName": "region",
                  "unknownProperty": 1,
                  "dimensionFieldSpecs": [{"name": "name", "dataType": "STRING", "fieldType": "DIMENSION"}],
                  "dateTimeFieldSpecs": [{
                    "name": "updated_at_seconds",
                    "dataType": "LONG",
                    "fieldType": "DATE_TIME",
                    "defaultNullValue": 0,
                    "format": "1:SECONDS:EPOCH",
                    "granularity": "1:SECONDS"
                  }]
                }
                """);
        assertThat(schema.getSchemaName()).isEqualTo("region");
        assertThat(schema.getColumnNames()).containsExactlyInAnyOrder("name", "updated_at_seconds");
    }

    @Test
    void testDeserializeMalformedSchema()
    {
        assertThatThrownBy(() -> PinotClient.createSchemaJsonCodec().fromJson("{\"schemaName\": "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
