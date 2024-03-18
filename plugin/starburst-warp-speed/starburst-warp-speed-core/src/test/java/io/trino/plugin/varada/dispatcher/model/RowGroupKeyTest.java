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
package io.trino.plugin.varada.dispatcher.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RowGroupKeyTest
{
    @Test
    public void testStringRepresentation()
    {
        RowGroupKey rowGroupKey = new RowGroupKey(
                "s",
                "t",
                "fl",
                1L,
                2L,
                3L,
                null,
                "");

        assertThat(rowGroupKey.toString()).isEqualTo("s:t:fl:1:2:3");
        assertThat(rowGroupKey.stringFileNameRepresentation("localStorePath"))
                .isEqualTo("localStorePath/s/t/fl/1/2/3");

        rowGroupKey = new RowGroupKey(
                "s",
                "t",
                "//fl",
                1L,
                2L,
                3L,
                null,
                "");

        assertThat(rowGroupKey.toString()).isEqualTo("s:t:fl:1:2:3");
        assertThat(rowGroupKey.stringFileNameRepresentation("localStorePath"))
                .isEqualTo("localStorePath/s/t/fl/1/2/3");

        rowGroupKey = new RowGroupKey(
                "s",
                "t",
                "fl",
                1L,
                2L,
                3L,
                "",
                "");

        assertThat(rowGroupKey.toString()).isEqualTo("s:t:fl:1:2:3");
        assertThat(rowGroupKey.stringFileNameRepresentation("localStorePath"))
                .isEqualTo("localStorePath/s/t/fl/1/2/3");

        rowGroupKey = new RowGroupKey(
                "s",
                "t",
                "fl",
                1L,
                2L,
                3L,
                "d",
                "");

        assertThat(rowGroupKey.toString()).isEqualTo("s:t:fl:1:2:3:d");
        assertThat(rowGroupKey.stringFileNameRepresentation("localStorePath"))
                .isEqualTo("localStorePath/s/t/fl/1/2/3/d");

        rowGroupKey = new RowGroupKey(
                "  s",
                "t  ",
                " f l ",
                1L,
                2L,
                3L,
                " d ",
                "");

        assertThat(rowGroupKey.toString()).isEqualTo("_s:t_:_f_l_:1:2:3:_d_");
        assertThat(rowGroupKey.stringFileNameRepresentation("localStorePath"))
                .isEqualTo("localStorePath/_s/t_/_f_l_/1/2/3/_d_");
    }
}
