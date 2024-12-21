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
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Return
        implements Operation
{
    private static final String NAME = "return";

    private final Result result;
    private final Value input;
    private final Map<String, Object> attributes;

    public Return(String resultName, Value input, Map<String, Object> sourceAttributes)
    {
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        this.result = new Result(resultName, input.type());

        this.input = input;

        this.attributes = ImmutableMap.of(); // TODO pass relevant attributes (skip attrs like join type)
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<String, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel)
    {
        return "pretty return";
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {return true;}
        if (obj == null || obj.getClass() != this.getClass()) {return false;}
        var that = (Return) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, attributes);
    }
}
