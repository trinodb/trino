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
package io.trino.sql;

import io.trino.spi.TrinoException;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.IntervalDataType;
import io.trino.sql.tree.Statement;
import jakarta.annotation.Nullable;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.toDataType;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.toTypeDescriptor;
import static java.lang.String.format;

public final class SqlFormatterUtil
{
    private SqlFormatterUtil() {}

    /// Persists explicit interval defaults so subsequent readers can distinguish new SQL from
    /// legacy stored bodies that used the historical unparameterized types.
    public static String getFormattedSqlForStorage(Statement statement, SqlParser sqlParser)
    {
        return SqlFormatter.formatSql(sqlParser.createStatement(
                getFormattedSql(statement, sqlParser),
                type -> (IntervalDataType) toDataType(toTypeDescriptor(type))));
    }

    public static String getFormattedSql(Statement statement, SqlParser sqlParser)
    {
        String sql = SqlFormatter.formatSql(statement);

        // verify round-trip
        Statement parsed;
        try {
            parsed = sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            throw formattingFailure(e, "Formatted query does not parse", statement, sql);
        }
        if (!statement.equals(parsed)) {
            throw formattingFailure(null, "Query does not round-trip", statement, sql);
        }

        return sql;
    }

    private static TrinoException formattingFailure(@Nullable Throwable cause, String message, Statement statement, String sql)
    {
        TrinoException exception = new TrinoException(GENERIC_INTERNAL_ERROR, message, cause);
        exception.addSuppressed(new RuntimeException("Statement: " + statement));
        exception.addSuppressed(new RuntimeException(format("Formatted: [%s]", sql)));
        return exception;
    }
}
