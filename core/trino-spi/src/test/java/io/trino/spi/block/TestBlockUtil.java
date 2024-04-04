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
package io.trino.spi.block;

import org.junit.jupiter.api.Test;

import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBlockUtil
{
    @Test
    public void testCalculateNewArraySize()
    {
        assertThat(calculateNewArraySize(200)).isEqualTo(300);
        assertThat(calculateNewArraySize(200, 10)).isEqualTo(300);
        assertThat(calculateNewArraySize(200, 500)).isEqualTo(500);

        assertThat(calculateNewArraySize(MAX_ARRAY_SIZE - 1)).isEqualTo(MAX_ARRAY_SIZE);
        assertThat(calculateNewArraySize(10, MAX_ARRAY_SIZE)).isEqualTo(MAX_ARRAY_SIZE);

        assertThat(calculateNewArraySize(1, 0)).isEqualTo(64);

        assertThatThrownBy(() -> calculateNewArraySize(Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculateNewArraySize(0, Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculateNewArraySize(MAX_ARRAY_SIZE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot grow array beyond size %d".formatted(MAX_ARRAY_SIZE));
    }
}
