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
package io.trino.plugin.raptor.legacy.storage.organization;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestShardRange
{
    @Test
    public void testEnclosesIsSymmetric()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, BOOLEAN, TIMESTAMP_MILLIS);
        ShardRange range = ShardRange.of(new Tuple(types, 2L, "aaa", true, 1L), new Tuple(types, 5L, "ccc", false, 2L));
        assertThat(range.encloses(range)).isTrue();
    }

    @Test
    public void testEnclosingRange()
    {
        List<Type> types1 = ImmutableList.of(BIGINT);
        ShardRange range1 = ShardRange.of(new Tuple(types1, 2L), new Tuple(types1, 5L));

        ShardRange enclosesRange1 = ShardRange.of(new Tuple(types1, 1L), new Tuple(types1, 10L));
        ShardRange notEnclosesRange1 = ShardRange.of(new Tuple(types1, 1L), new Tuple(types1, 4L));

        assertThat(enclosesRange1.encloses(range1)).isTrue();
        assertThat(notEnclosesRange1.encloses(range1)).isFalse();

        List<Type> types2 = ImmutableList.of(BIGINT, VARCHAR);
        ShardRange range2 = ShardRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "ccc"));
        ShardRange enclosesRange2 = ShardRange.of(new Tuple(types2, 1L, "ccc"), new Tuple(types2, 10L, "ccc"));
        ShardRange notEnclosesRange2 = ShardRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "bbb"));

        assertThat(range2.encloses(range2)).isTrue();
        assertThat(enclosesRange2.encloses(range2)).isTrue();
        assertThat(notEnclosesRange2.encloses(range2)).isFalse();
    }

    @Test
    public void testOverlapsIsSymmetric()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, BOOLEAN, TIMESTAMP_MILLIS);
        ShardRange range = ShardRange.of(new Tuple(types, 2L, "aaa", true, 1L), new Tuple(types, 5L, "ccc", false, 2L));
        assertThat(range.overlaps(range)).isTrue();
    }

    @Test
    public void testOverlappingRange()
    {
        List<Type> types1 = ImmutableList.of(BIGINT);
        ShardRange range1 = ShardRange.of(new Tuple(types1, 2L), new Tuple(types1, 5L));

        ShardRange enclosesRange1 = ShardRange.of(new Tuple(types1, 1L), new Tuple(types1, 10L));
        ShardRange overlapsRange1 = ShardRange.of(new Tuple(types1, 1L), new Tuple(types1, 4L));
        ShardRange notOverlapsRange1 = ShardRange.of(new Tuple(types1, 6L), new Tuple(types1, 8L));

        assertThat(enclosesRange1.overlaps(range1)).isTrue();
        assertThat(overlapsRange1.overlaps(range1)).isTrue();
        assertThat(notOverlapsRange1.overlaps(range1)).isFalse();

        List<Type> types2 = ImmutableList.of(BIGINT, VARCHAR);
        ShardRange range2 = ShardRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "ccc"));
        ShardRange enclosesRange2 = ShardRange.of(new Tuple(types2, 1L, "ccc"), new Tuple(types2, 10L, "ccc"));
        ShardRange overlapsRange2 = ShardRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "bbb"));
        ShardRange notOverlapsRange2 = ShardRange.of(new Tuple(types2, 6L, "aaa"), new Tuple(types2, 8L, "bbb"));

        assertThat(enclosesRange2.encloses(range2)).isTrue();
        assertThat(overlapsRange2.overlaps(range2)).isTrue();
        assertThat(notOverlapsRange2.overlaps(range2)).isFalse();
    }

    @Test
    public void testAdjacentRange()
    {
        List<Type> types1 = ImmutableList.of(BIGINT);
        ShardRange range1 = ShardRange.of(new Tuple(types1, 2L), new Tuple(types1, 5L));
        ShardRange adjacentRange1 = ShardRange.of(new Tuple(types1, 5L), new Tuple(types1, 10L));

        assertThat(range1.adjacent(range1)).isFalse();

        assertThat(adjacentRange1.adjacent(range1)).isTrue();
        assertThat(range1.adjacent(adjacentRange1)).isTrue();

        List<Type> types2 = ImmutableList.of(BIGINT, VARCHAR);
        ShardRange range2 = ShardRange.of(new Tuple(types2, 2L, "aaa"), new Tuple(types2, 5L, "ccc"));
        ShardRange adjacentRange2 = ShardRange.of(new Tuple(types2, 5L, "ccc"), new Tuple(types2, 10L, "ccc"));
        ShardRange subsetAdjacentRange2 = ShardRange.of(new Tuple(types2, 5L, "ddd"), new Tuple(types2, 10L, "ccc"));
        ShardRange overlapsRange2 = ShardRange.of(new Tuple(types2, 3L, "aaa"), new Tuple(types2, 10L, "ccc"));
        ShardRange notAdjacentRange2 = ShardRange.of(new Tuple(types2, 6L, "ccc"), new Tuple(types2, 10L, "ccc"));

        assertThat(adjacentRange2.adjacent(range2)).isTrue();
        assertThat(subsetAdjacentRange2.adjacent(range2)).isTrue();
        assertThat(overlapsRange2.adjacent(range2)).isFalse();
        assertThat(notAdjacentRange2.adjacent(range2)).isFalse();
    }
}
