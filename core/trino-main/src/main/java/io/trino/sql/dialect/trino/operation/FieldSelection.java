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
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.FIELD_NAME;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION_ROW;
import static java.util.Objects.requireNonNull;

/**
 * FieldSelection operation selects a row field by name.
 * <p>
 * We compare field names case-sensitive, although in TranslationMap field references are resolved case-insensitive.
 * Explanation:
 * In TranslationMap, all user-provided field references by name are resolved case-insensitive and translated to field references by index.
 * Operations, like this one, are created after TranslationMap, so there are no user-provided field references by name.
 * At this point, the only field references by name are added programmatically (the FieldSelection Operation), and they refer to RowTypes created programmatically.
 * Those RowTypes have lower-case unique field names which can be safely compared case-sensitive.
 * When we add a Parser to create the IR from text, we should assume that the text is a printout of a valid query program,
 * and thus all field references by name are case-safe.
 */
public final class FieldSelection
        implements Operation
{
    private static final String NAME = "field_selection";

    private final Result result;
    private final Value input;
    private final Map<String, Object> attributes;

    public FieldSelection(String resultName, Value input, String fieldName, Map<String, Object> sourceAttributes)
    {
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION_ROW.test(input.type()) || input.type().equals(EMPTY_ROW)) {
            throw new TrinoException(IR_ERROR, "input to the FieldSelection operation must be a relation row type with fields");
        }
        Optional<RowType.Field> matchingField = ((RowType) input.type()).getFields().stream()
                .filter(field -> fieldName.equals(field.getName().orElseThrow())).findFirst();
        if (matchingField.isEmpty()) {
            throw new TrinoException(IR_ERROR, "invalid row field selection: no matching field name");
        }
        this.result = new Result(resultName, matchingField.orElseThrow().getType());

        this.input = input;

        this.attributes = deriveAttributes(fieldName, sourceAttributes);
    }

    private Map<String, Object> deriveAttributes(String fieldName, Map<String, Object> sourceAttributes)
    {
        return FIELD_NAME.asMap(fieldName);
        // TODO add source attributes for the selected field
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
        return "pretty field selection";
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {return true;}
        if (obj == null || obj.getClass() != this.getClass()) {return false;}
        var that = (FieldSelection) obj;
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
