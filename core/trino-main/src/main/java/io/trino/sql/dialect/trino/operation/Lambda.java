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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.type.FunctionType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Lambda
        extends Operation
{
    private static final String NAME = "lambda";

    private final Result result;
    private final Region lambda;

    public Lambda(String resultName, Block lambda)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(lambda, "lambda is null");

        if (lambda.parameters().size() != 1 ||
                !(trinoType(lambda.parameters().getFirst().type()) instanceof RowType ||
                        trinoType(lambda.parameters().getFirst().type()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, format("invalid argument type for lambda: %s. expected RowType or EmptyRowType", trinoType(lambda.parameters().getFirst().type()).getDisplayName()));
        }
        this.lambda = singleBlockRegion(lambda);

        FunctionType resultType = new FunctionType(
                trinoType(lambda.parameters().getFirst().type()).getTypeParameters(),
                trinoType(lambda.getReturnedType()));

        this.result = new Result(resultName, irType(resultType));
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of();
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(lambda);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return ImmutableMap.of();
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "lambda :)";
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (Lambda) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.lambda, that.lambda);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, lambda);
    }
}
