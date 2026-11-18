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
package io.trino.server.ui;

import org.junit.jupiter.api.Test;

import static io.trino.server.ui.TrimmedBasicQueryInfo.MAX_QUERY_PREVIEW_LENGTH;
import static io.trino.server.ui.TrimmedBasicQueryInfo.truncateToPreview;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrimmedBasicQueryInfo
{
    @Test
    public void testShortQueryNotTruncated()
    {
        assertThat(truncateToPreview("SELECT 1"))
                .isEqualTo("SELECT 1");
    }

    @Test
    public void testQueryAtLimitNotTruncated()
    {
        String query = "a".repeat(MAX_QUERY_PREVIEW_LENGTH);

        assertThat(truncateToPreview(query))
                .isEqualTo(query);
    }

    @Test
    public void testQueryOverLimitTruncated()
    {
        String query = "a".repeat(MAX_QUERY_PREVIEW_LENGTH + 1);

        assertThat(truncateToPreview(query))
                .isEqualTo("a".repeat(MAX_QUERY_PREVIEW_LENGTH - 1) + " ...");
    }

    @Test
    public void testTruncationDoesNotSplitSurrogatePair()
    {
        // The surrogate pair straddles the truncation boundary, so it must be dropped entirely
        String emoji = Character.toString(0x1F600);
        String query = "a".repeat(MAX_QUERY_PREVIEW_LENGTH - 2) + emoji + "b".repeat(10);

        assertThat(truncateToPreview(query))
                .isEqualTo("a".repeat(MAX_QUERY_PREVIEW_LENGTH - 2) + " ...");
    }

    @Test
    public void testTruncationKeepsSurrogatePairBeforeBoundary()
    {
        // The surrogate pair ends exactly at the last retained character, so it must be kept
        String emoji = Character.toString(0x1F600);
        String query = "a".repeat(MAX_QUERY_PREVIEW_LENGTH - 3) + emoji + "b".repeat(10);

        assertThat(truncateToPreview(query))
                .isEqualTo("a".repeat(MAX_QUERY_PREVIEW_LENGTH - 3) + emoji + " ...");
    }
}
