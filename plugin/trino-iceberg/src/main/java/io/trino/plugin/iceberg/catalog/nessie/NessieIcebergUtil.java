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
package io.trino.plugin.iceberg.catalog.nessie;

import io.trino.spi.connector.SchemaTableName;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Namespace;

final class NessieIcebergUtil
{
    private NessieIcebergUtil() {}

    static ContentKey toKey(SchemaTableName tableName)
    {
        return ContentKey.of(Namespace.parse(tableName.getSchemaName()), tableName.getTableName());
    }

    static ImmutableCommitMeta buildCommitMeta(String author, String commitMsg)
    {
        return CommitMeta.builder()
                .message(commitMsg)
                .author(author)
                .build();
    }
}
