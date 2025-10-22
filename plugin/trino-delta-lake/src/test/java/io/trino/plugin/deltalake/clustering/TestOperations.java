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
package io.trino.plugin.deltalake.clustering;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.deltalake.clustering.Operation.CREATE_TABLE_KEYWORD;
import static io.trino.plugin.deltalake.clustering.Operation.DELETE;
import static io.trino.plugin.deltalake.clustering.Operation.REPLACE_TABLE_KEYWORD;
import static io.trino.plugin.deltalake.clustering.Operation.UNKNOW_OPERATION;
import static io.trino.plugin.deltalake.clustering.Operation.UPDATE_SCHEMA;
import static io.trino.plugin.deltalake.clustering.Operation.fromString;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

final class TestOperations
{
    @Test
    void testGetOperationNameAndToStringConsistency()
    {
        for (Operation op : Operation.values()) {
            assertThat(op.getOperationName()).isEqualTo(op.toString());
        }
    }

    @Test
    void testFromStringExactMatch()
    {
        assertThat(fromString("DELETE")).isEqualTo(DELETE);
        assertThat(fromString("delete")).isEqualTo(DELETE);
        assertThat(fromString("UPDATE SCHEMA")).isEqualTo(UPDATE_SCHEMA);
    }

    @Test
    void testFromStringKeywordMatch()
    {
        assertThat(fromString("CREATE TABLE")).isEqualTo(CREATE_TABLE_KEYWORD);
        assertThat(fromString("CREATE table AS SELECT")).isEqualTo(CREATE_TABLE_KEYWORD);
        assertThat(fromString("CREATE OR REPLACE TABLE AS SELECT")).isEqualTo(REPLACE_TABLE_KEYWORD);
        assertThat(fromString("CREATE OR replace TABLE")).isEqualTo(REPLACE_TABLE_KEYWORD);
    }

    @Test
    void testFromStringUnknownOperation()
    {
        assertThat(fromString("NON_EXISTENT_OPERATION")).isEqualTo(UNKNOW_OPERATION);
        assertThat(fromString("")).isEqualTo(UNKNOW_OPERATION);
        assertThat(fromString("SOMETHING RANDOM")).isEqualTo(UNKNOW_OPERATION);
    }

    @Test
    void testAllEnumValuesResolvableByFromString()
    {
        for (Operation op : Operation.values()) {
            Operation resolved = Operation.fromString(op.getOperationName());
            assertThat(resolved).isNotNull();
        }
    }
}
