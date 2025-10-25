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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class OperationTest
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
        assertThat(Operation.DELETE).isEqualTo(Operation.fromString("DELETE"));
        assertThat(Operation.DELETE).isEqualTo(Operation.fromString("delete"));
        assertThat(Operation.UPDATE_SCHEMA).isEqualTo(Operation.fromString("UPDATE SCHEMA"));
    }

    @Test
    void testFromStringKeywordMatch()
    {
        assertThat(Operation.CREATE_TABLE_KEYWORD).isEqualTo(Operation.fromString("CREATE TABLE"));
        assertThat(Operation.CREATE_TABLE_KEYWORD).isEqualTo(Operation.fromString("CREATE table AS SELECT"));
        assertThat(Operation.REPLACE_TABLE_KEYWORD).isEqualTo(Operation.fromString("CREATE OR REPLACE TABLE AS SELECT"));
        assertThat(Operation.REPLACE_TABLE_KEYWORD).isEqualTo(Operation.fromString("CREATE OR replace TABLE"));
    }

    @Test
    void testFromStringUnknownOperation()
    {
        assertThat(Operation.UNKNOW_OPERATION).isEqualTo(Operation.fromString("NON_EXISTENT_OPERATION"));
        assertThat(Operation.UNKNOW_OPERATION).isEqualTo(Operation.fromString(""));
        assertThat(Operation.UNKNOW_OPERATION).isEqualTo(Operation.fromString("SOMETHING RANDOM"));
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
