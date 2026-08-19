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
package io.trino.plugin.elasticsearch;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.elasticsearch.CasePreservingElasticsearchMetadata.TranslationQuality.APPROXIMATE;
import static io.trino.plugin.elasticsearch.CasePreservingElasticsearchMetadata.TranslationQuality.EXACT;
import static io.trino.plugin.elasticsearch.CasePreservingElasticsearchMetadata.translateRegexpLike;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRegexpPushdownTranslator
{
    @Test
    public void testSubstringAndAnchors()
    {
        assertThat(translateRegexpLike("john"))
                .contains(new CasePreservingElasticsearchMetadata.RegexpTranslation(".*(john).*", EXACT));
        assertThat(translateRegexpLike("^john"))
                .contains(new CasePreservingElasticsearchMetadata.RegexpTranslation("(john).*", EXACT));
        assertThat(translateRegexpLike("john$"))
                .contains(new CasePreservingElasticsearchMetadata.RegexpTranslation(".*(john)", EXACT));
        assertThat(translateRegexpLike("^john$"))
                .contains(new CasePreservingElasticsearchMetadata.RegexpTranslation("(john)", EXACT));
    }

    @Test
    public void testCommonJoniSyntaxTranslation()
    {
        assertThat(translateRegexpLike("\\d+"))
                .contains(new CasePreservingElasticsearchMetadata.RegexpTranslation(".*([0-9]+).*", APPROXIMATE));
        assertThat(translateRegexpLike("\\w{2,4}"))
                .contains(new CasePreservingElasticsearchMetadata.RegexpTranslation(".*([A-Za-z0-9_]{2,4}).*", APPROXIMATE));
        assertThat(translateRegexpLike("(?:foo|bar)"))
                .contains(new CasePreservingElasticsearchMetadata.RegexpTranslation(".*((foo|bar)).*", APPROXIMATE));
    }

    @Test
    public void testUnsupportedJoniConstructsAreNotPushed()
    {
        assertThat(translateRegexpLike("foo(?=bar)")).isEmpty();
        assertThat(translateRegexpLike("(foo)\\1")).isEmpty();
        assertThat(translateRegexpLike("\\p{L}+")).isEmpty();
        assertThat(translateRegexpLike("foo^bar")).isEmpty();
    }
}
