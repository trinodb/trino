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
package io.trino.tests.product.teradata;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

@ProductTest
@RequiresEnvironment(FunctionsEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Functions
class TestTeradataFunctions
{
    @Test
    void testIndex(FunctionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT index('high', 'ig')"))
                .contains(row(2));
    }

    @Test
    void testChar2HexInt(FunctionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT char2hexint('ಠ益ಠ')"))
                .contains(row("0CA076CA0CA0"));
    }

    @Test
    void testToDate(FunctionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT to_date('1988/04/01', 'yyyy/mm/dd')"))
                .contains(row(Date.valueOf("1988-04-01")));
        assertThat(env.executeTrino("SELECT to_date('1988/04/08', 'yyyy/mm/dd')"))
                .contains(row(Date.valueOf("1988-04-08")));
    }

    @Test
    void testToTimestamp(FunctionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT to_timestamp('1988/04/08;02:03:04','yyyy/mm/dd;hh24:mi:ss')"))
                .contains(row(Timestamp.valueOf(LocalDateTime.of(1988, 4, 8, 2, 3, 4))));
    }

    @Test
    void testToChar(FunctionsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT to_char(TIMESTAMP '1988-04-08 14:15:16 +02:09','yyyy/mm/dd;hh24:mi:ss')"))
                .contains(row("1988/04/08;14:15:16"));
    }
}
