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
package io.trino.plugin.postgresql;

import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.stream.Stream;

import static java.lang.String.format;

public class CollationAwareQueryBuilder
        extends DefaultQueryBuilder
{
    @Override
    protected String formatJoinCondition(JdbcClient client, String leftRelationAlias, String rightRelationAlias, JdbcJoinCondition condition)
    {
        boolean needsCollation = Stream.of(condition.getLeftColumn(), condition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .anyMatch(CollationAwareQueryBuilder::isCharType);

        if (needsCollation) {
            return format(
                    "%s.%s COLLATE \"C\" %s %s.%s COLLATE \"C\"",
                    leftRelationAlias,
                    buildJoinColumn(client, condition.getLeftColumn()),
                    condition.getOperator().getValue(),
                    rightRelationAlias,
                    buildJoinColumn(client, condition.getRightColumn()));
        }

        return super.formatJoinCondition(client, leftRelationAlias, rightRelationAlias, condition);
    }

    private static boolean isCharType(Type type)
    {
        return type instanceof CharType || type instanceof VarcharType;
    }
}
