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
package io.trino.spi.connector;

import io.trino.spi.Experimental;

import java.util.Optional;

import static io.trino.spi.connector.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Experimental(eta = "2024-01-01")
public record RelationCommentMetadata(
        SchemaTableName name,
        boolean tableRedirected,
        Optional<String> comment)
{
    public RelationCommentMetadata
    {
        requireNonNull(name, "name is null");
        requireNonNull(comment, "comment is null");
        checkArgument(!tableRedirected || comment.isEmpty(), "Unexpected comment for redirected table");
    }

    public static RelationCommentMetadata forRelation(SchemaTableName name, Optional<String> comment)
    {
        return new RelationCommentMetadata(name, false, comment);
    }

    public static RelationCommentMetadata forRedirectedTable(SchemaTableName name)
    {
        return new RelationCommentMetadata(name, true, Optional.empty());
    }
}
