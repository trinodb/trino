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
package io.trino.block;

import com.google.common.base.VerifyException;
import org.testng.annotations.Test;

import java.util.Objects;

import static com.google.common.collect.Streams.stream;
import static io.trino.block.BlockAssertions.generateListWithNulls;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class TestBlockAssertions
{
    @Test
    public void generatesListWithNulls()
    {
        Iterable<Integer> list = generateListWithNulls(10, 0.2F, () -> 1);

        assertThat(stream(list).filter(Objects::isNull).count()).isEqualTo(2);
    }

    @Test
    public void failsIfPositionCountToLow()
    {
        assertThatThrownBy(() -> generateListWithNulls(4, 0.2F, () -> 1)).isInstanceOf(VerifyException.class);
    }
}
