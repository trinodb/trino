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
package io.trino.filesystem.s3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static io.trino.filesystem.s3.S3OutputStream.buildTaggingHeader;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3OutputStreamTagging
{
    @Test
    void testEmptyTags()
    {
        assertThat(buildTaggingHeader(ImmutableMap.of(), ImmutableSet.of(), "any-key")).isEmpty();
    }

    @Test
    void testMatchingPrefix()
    {
        Map<String, String> tags = ImmutableMap.of("env", "prod", "team", "data platform");
        Set<String> prefixes = ImmutableSet.of("data/");
        assertThat(buildTaggingHeader(tags, prefixes, "db/table/data/file.parquet"))
                .hasValue("env=prod&team=data%20platform");
    }

    @Test
    void testNonMatchingPrefix()
    {
        Map<String, String> tags = ImmutableMap.of("env", "prod");
        Set<String> prefixes = ImmutableSet.of("data/");
        assertThat(buildTaggingHeader(tags, prefixes, "db/table/metadata/file.json")).isEmpty();
    }

    @Test
    void testEmptyPrefixes()
    {
        Map<String, String> tags = ImmutableMap.of("env", "prod");
        assertThat(buildTaggingHeader(tags, ImmutableSet.of(), "any-key"))
                .hasValue("env=prod");
    }

    @Test
    void testMultiplePrefixes()
    {
        Map<String, String> tags = ImmutableMap.of("env", "prod");
        Set<String> prefixes = ImmutableSet.of("data/", "extra/");
        assertThat(buildTaggingHeader(tags, prefixes, "db/table/extra/file.parquet"))
                .hasValue("env=prod");
        assertThat(buildTaggingHeader(tags, prefixes, "db/table/data/file.parquet"))
                .hasValue("env=prod");
        assertThat(buildTaggingHeader(tags, prefixes, "db/table/metadata/file.json")).isEmpty();
    }

    @Test
    void testSpecialCharacters()
    {
        Map<String, String> tags = ImmutableMap.of("filter", "a=b&c=d");
        assertThat(buildTaggingHeader(tags, ImmutableSet.of(), "any-key"))
                .hasValue("filter=a%3Db%26c%3Dd");
    }

    @Test
    void testSpacesInValues()
    {
        Map<String, String> tags = ImmutableMap.of("team", "data platform");
        assertThat(buildTaggingHeader(tags, ImmutableSet.of(), "any-key"))
                .hasValue("team=data%20platform");
    }
}
