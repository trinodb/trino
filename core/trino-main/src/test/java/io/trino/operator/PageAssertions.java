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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static org.assertj.core.api.Assertions.assertThat;

public final class PageAssertions
{
    private PageAssertions() {}

    public static void assertPageEquals(List<? extends Type> types, Page actualPage, Page expectedPage)
    {
        assertThat(types.size()).isEqualTo(actualPage.getChannelCount());
        assertThat(actualPage.getChannelCount()).isEqualTo(expectedPage.getChannelCount());
        assertThat(actualPage.getPositionCount()).isEqualTo(expectedPage.getPositionCount());
        for (int i = 0; i < actualPage.getChannelCount(); i++) {
            assertBlockEquals(types.get(i), actualPage.getBlock(i), expectedPage.getBlock(i));
        }
    }
}
