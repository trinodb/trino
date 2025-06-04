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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSymbolAllocator
{
    @Test
    public void testUnique()
    {
        SymbolAllocator allocator = new SymbolAllocator();
        Set<Symbol> symbols = ImmutableSet.<Symbol>builder()
                .add(allocator.newSymbol("foo_1_0", BigintType.BIGINT))
                .add(allocator.newSymbol("foo", BigintType.BIGINT))
                .add(allocator.newSymbol("foo", BigintType.BIGINT))
                .add(allocator.newSymbol("foo", BigintType.BIGINT))
                .build();

        assertThat(symbols).hasSize(4);
    }

    @Test
    public void testNonAscii()
    {
        SymbolAllocator allocator = new SymbolAllocator();
        Set<Symbol> symbols = ImmutableSet.<Symbol>builder()
                .add(allocator.newSymbol("カラム", BigintType.BIGINT))
                .add(allocator.newSymbol("col", BigintType.BIGINT))
                .build();

        assertThat(symbols).containsExactlyInAnyOrder(
                new Symbol(BigintType.BIGINT, "col"),
                new Symbol(BigintType.BIGINT, "col_0"));
    }
}
