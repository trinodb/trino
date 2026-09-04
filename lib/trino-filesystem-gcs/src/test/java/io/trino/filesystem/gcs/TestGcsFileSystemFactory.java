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
package io.trino.filesystem.gcs;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestGcsFileSystemFactory
{
    @Test
    void testAuditHeaders()
    {
        assertThat(GcsFileSystemFactory.auditHeaders("query_id", "alice"))
                .isEqualTo(ImmutableMap.of(
                        "x-goog-custom-audit-trino-query-id", "query_id",
                        "x-goog-custom-audit-trino-user", "alice"));
    }
}
