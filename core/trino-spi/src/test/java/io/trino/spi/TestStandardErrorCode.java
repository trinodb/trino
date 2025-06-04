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
package io.trino.spi;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStandardErrorCode
{
    private static final int EXTERNAL_ERROR_START = 0x0100_0000;

    @Test
    public void testUnique()
    {
        Set<Integer> codes = new HashSet<>();
        for (StandardErrorCode code : StandardErrorCode.values()) {
            assertThat(codes.add(code(code)))
                    .describedAs("Code already exists: " + code)
                    .isTrue();
        }
        assertThat(codes).hasSize(StandardErrorCode.values().length);
    }

    @Test
    public void testReserved()
    {
        for (StandardErrorCode errorCode : StandardErrorCode.values()) {
            assertThat(code(errorCode)).isLessThan(EXTERNAL_ERROR_START);
        }
    }

    @Test
    public void testOrdering()
    {
        Iterator<StandardErrorCode> iterator = asList(StandardErrorCode.values()).iterator();

        assertThat(iterator.hasNext()).isTrue();
        int previous = code(iterator.next());

        while (iterator.hasNext()) {
            StandardErrorCode code = iterator.next();
            int current = code(code);
            assertThat(current).as("Code is out of order: " + code).isGreaterThan(previous);
            if (code != GENERIC_INTERNAL_ERROR && code != GENERIC_INSUFFICIENT_RESOURCES && code != UNSUPPORTED_TABLE_TYPE) {
                assertThat(current)
                        .describedAs("Code is not sequential: " + code)
                        .isEqualTo(previous + 1);
            }
            previous = current;
        }
    }

    private static int code(StandardErrorCode error)
    {
        return error.toErrorCode().getCode();
    }
}
