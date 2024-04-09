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
package io.trino.plugin.hive.metastore;

import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;

import static io.trino.plugin.hive.HiveMetadata.PRESTO_VIEW_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static java.util.Objects.requireNonNull;

public record TableInfo(SchemaTableName tableName, ExtendedRelationType extendedRelationType)
{
    public TableInfo
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(extendedRelationType, "extendedRelationType is null");
    }

    public enum ExtendedRelationType
    {
        TABLE(RelationType.TABLE),
        OTHER_VIEW(RelationType.VIEW),
        OTHER_MATERIALIZED_VIEW(RelationType.MATERIALIZED_VIEW),
        TRINO_VIEW(RelationType.VIEW),
        TRINO_MATERIALIZED_VIEW(RelationType.MATERIALIZED_VIEW);

        private final RelationType relationType;

        ExtendedRelationType(RelationType relationType)
        {
            this.relationType = relationType;
        }

        public RelationType toRelationType()
        {
            return relationType;
        }

        public static ExtendedRelationType fromTableTypeAndComment(String tableType, String comment)
        {
            return switch (tableType) {
                case "VIRTUAL_VIEW" -> {
                    if (PRESTO_VIEW_COMMENT.equals(comment)) {
                        yield TRINO_VIEW;
                    }
                    if (ICEBERG_MATERIALIZED_VIEW_COMMENT.equals(comment)) {
                        yield TRINO_MATERIALIZED_VIEW;
                    }
                    yield OTHER_VIEW;
                }
                case "MATERIALIZED_VIEW" -> ICEBERG_MATERIALIZED_VIEW_COMMENT.equals(comment) ? TRINO_MATERIALIZED_VIEW : OTHER_MATERIALIZED_VIEW;
                default -> TABLE;
            };
        }
    }
}
