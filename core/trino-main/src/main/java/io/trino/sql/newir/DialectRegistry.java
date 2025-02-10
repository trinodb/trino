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
package io.trino.sql.newir;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.sql.dialect.trino.TrinoDialect;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.IR_DIALECT;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DialectRegistry
{
    private final Map<String, Dialect> dialectMap;

    @Inject
    public DialectRegistry(TrinoDialect trinoDialect)
    {
        requireNonNull(trinoDialect, "trinoDialect is null");
        this.dialectMap = ImmutableMap.of(
                TRINO, trinoDialect,
                IR, IR_DIALECT);
    }

    public Dialect dialect(String name)
    {
        Dialect dialect = dialectMap.get(name);
        if (dialect == null) {
            throw new TrinoException(IR_ERROR, format("dialect %s not registered", name));
        }

        return dialect;
    }
}
