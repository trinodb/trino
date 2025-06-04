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
package io.trino.plugin.oracle;

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import oracle.jdbc.OracleTypes;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.plugin.oracle.OracleClient.ORACLE_CHAR_MAX_CHARS;
import static io.trino.plugin.oracle.OracleClient.ORACLE_VARCHAR2_MAX_CHARS;

public class RewriteCast
        extends AbstractRewriteCast
{
    public RewriteCast(BiFunction<ConnectorSession, Type, String> jdbcTypeProvider)
    {
        super(jdbcTypeProvider);
    }

    @Override
    protected String buildCast(Type sourceType, Type targetType, String expression, String castType)
    {
        if (sourceType instanceof CharType sourceCharType) {
            if (targetType instanceof CharType targetCharType && sourceCharType.getLength() < targetCharType.getLength()) {
                // Do not cast unnecessary with extra space padding when target char type has more length than source char type
                return expression;
            }
        }
        return "CAST(%s AS %s)".formatted(expression, castType);
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(JdbcTypeHandle sourceType, Type targetType)
    {
        if (!pushdownSupported(sourceType, targetType)) {
            return Optional.empty();
        }

        if (targetType instanceof CharType charType) {
            return Optional.of(new JdbcTypeHandle(OracleTypes.CHAR, Optional.of(charType.getBaseName()), Optional.of(charType.getLength()), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        if (targetType instanceof VarcharType varcharType) {
            return Optional.of(new JdbcTypeHandle(OracleTypes.VARCHAR, Optional.of(varcharType.getBaseName()), varcharType.getLength(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        return Optional.empty();
    }

    private boolean pushdownSupported(JdbcTypeHandle sourceType, Type targetType)
    {
        if (targetType instanceof CharType charType) {
            // Oracle will throw an error on casts to char(n>ORACLE_CHAR_MAX_CHARS) "ORA-00932: inconsistent datatypes: expected - got NCLOB"
            return charType.getLength() <= ORACLE_CHAR_MAX_CHARS
                    && supportedSourceTypeToCastToChar(sourceType);
        }
        if (targetType instanceof VarcharType varcharType && !varcharType.isUnbounded()) {
            // unbounded varchar and char(n>ORACLE_VARCHAR2_MAX_CHARS) gets written as nclob.
            // pushdown does not happen when comparing nclob type variable, so skipping to pushdown the cast for nclob type variable.
            return varcharType.getLength().orElseThrow() <= ORACLE_VARCHAR2_MAX_CHARS
                    && supportedSourceTypeToCastToVarchar(sourceType);
        }
        return false;
    }

    private static boolean supportedSourceTypeToCastToChar(JdbcTypeHandle sourceType)
    {
        return switch (sourceType.jdbcType()) {
            case OracleTypes.CHAR,
                 OracleTypes.VARCHAR,
                 OracleTypes.NCHAR,
                 OracleTypes.NVARCHAR,
                 OracleTypes.CLOB,
                 OracleTypes.NCLOB -> true;
            default -> false;
        };
    }

    private static boolean supportedSourceTypeToCastToVarchar(JdbcTypeHandle sourceType)
    {
        return switch (sourceType.jdbcType()) {
            case OracleTypes.NUMBER,
                    OracleTypes.VARCHAR,
                    OracleTypes.NVARCHAR,
                    OracleTypes.CLOB,
                    OracleTypes.NCLOB -> true;
            default -> false;
        };
    }
}
