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
package io.trino.plugin.inference;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.inference.OpenInferenceClient.Tensor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.joining;

/**
 * A model input or output. The first dimension of the shape is the number of rows.
 * A tensor with shape {@code [-1]} or {@code [-1, 1]} has one value per row, and maps to a column of the data type.
 * A tensor with shape {@code [-1, n]} has an array of {@code n} values per row, and maps to an array column.
 * The array length {@code -1} means that the length is not fixed.
 */
public record TensorSpecification(String name, TensorDataType dataType, List<Long> shape)
{
    public TensorSpecification
    {
        requireNonNull(name, "name is null");
        requireNonNull(dataType, "dataType is null");
        shape = ImmutableList.copyOf(shape);
    }

    /**
     * @return the specification of a tensor from model metadata, or empty when its data type or shape is not supported
     */
    public static Optional<TensorSpecification> fromMetadata(Tensor tensor)
    {
        if (unsupportedReason(tensor).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(new TensorSpecification(tensor.name(), TensorDataType.fromName(tensor.datatype()).orElseThrow(), tensor.shape()));
    }

    /**
     * @return why the connector does not support the data type or shape of a tensor from model metadata, or empty when it supports them
     */
    public static Optional<String> unsupportedReason(Tensor tensor)
    {
        if (TensorDataType.fromName(tensor.datatype()).isEmpty()) {
            return Optional.of("has the unsupported data type %s. The data type of every input and output must be one of %s".formatted(
                    tensor.datatype(),
                    Arrays.stream(TensorDataType.values()).map(Enum::name).collect(joining(", "))));
        }
        List<Long> shape = requireNonNullElse(tensor.shape(), ImmutableList.of());
        if (shape.isEmpty() || shape.size() > 2) {
            return Optional.of("has the unsupported shape %s. The shape of every input and output must have one or two dimensions: [-1] or [-1, 1] for one value per row, [-1, n] for n values per row, or [-1, -1] for any number of values per row".formatted(shape));
        }
        if (shape.getFirst() != -1) {
            return Optional.of("has the unsupported shape %s. The first dimension of the shape of every input and output must be -1, for a variable number of rows".formatted(shape));
        }
        if (shape.size() == 2 && shape.get(1) != -1 && shape.get(1) <= 0) {
            return Optional.of("has the unsupported shape %s. The second dimension of a shape must be positive, or -1 for any number of values per row".formatted(shape));
        }
        return Optional.empty();
    }

    @JsonIgnore
    public boolean isArray()
    {
        return shape.size() == 2 && shape.get(1) != 1;
    }

    /**
     * @return the number of values per row, or {@code -1} when the length is not fixed
     */
    public long valuesPerRow()
    {
        if (isArray()) {
            return shape.get(1);
        }
        return 1;
    }

    public Type trinoType()
    {
        if (isArray()) {
            return new ArrayType(dataType.trinoType());
        }
        return dataType.trinoType();
    }

    public boolean accepts(Type type)
    {
        if (isArray()) {
            return type instanceof ArrayType arrayType && dataType.accepts(arrayType.getElementType());
        }
        return dataType.accepts(type);
    }

    public String describe()
    {
        return "%s with shape %s".formatted(dataType, shape);
    }
}
