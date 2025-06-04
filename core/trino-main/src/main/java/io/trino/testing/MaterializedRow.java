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
package io.trino.testing;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Materialize all values in a row
 * Special handling is added for Double types for approximate comparisons
 */
public class MaterializedRow
{
    private final int precision;
    private final List<?> values;

    public MaterializedRow(int precision, Object... values)
    {
        this(precision, Arrays.asList(requireNonNull(values, "values is null")));
    }

    public MaterializedRow(List<Object> values)
    {
        this(DEFAULT_PRECISION, values);
    }

    public MaterializedRow(int precision, List<Object> values)
    {
        checkArgument(precision > 0, "Need at least one digit of precision");
        this.precision = precision;

        this.values = (List<?>) processValue(precision, values);
    }

    private static Object processValue(int precision, Object value)
    {
        if (value instanceof Double doubleValue) {
            return new ApproximateDouble(doubleValue, precision);
        }
        if (value instanceof Float floatValue) {
            return new ApproximateFloat(floatValue, precision);
        }
        if (value instanceof List<?> listValue) {
            return listValue.stream()
                    .map(element -> processValue(precision, element))
                    .collect(toList());
        }
        if (value instanceof Map<?, ?> mapValue) {
            Map<Object, Object> map = new HashMap<>();
            for (Entry<?, ?> entry : mapValue.entrySet()) {
                map.put(processValue(precision, entry.getKey()), processValue(precision, entry.getValue()));
            }
            return map;
        }
        if (value instanceof byte[] bytes) {
            return Bytes.fromBytes(bytes);
        }
        return value;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getFieldCount()
    {
        return values.size();
    }

    public List<Object> getFields()
    {
        return values.stream()
                .map(MaterializedRow::processField)
                .collect(toList());
    }

    public Object getField(int field)
    {
        checkElementIndex(field, values.size());
        return processField(values.get(field));
    }

    private static Object processField(Object value)
    {
        if (value instanceof ApproximateNumeric approximateNumeric) {
            return approximateNumeric.getValue();
        }
        if (value instanceof List<?> listValue) {
            return listValue.stream()
                    .map(MaterializedRow::processField)
                    .collect(toList());
        }
        if (value instanceof Map<?, ?> mapValue) {
            Map<Object, Object> map = new HashMap<>();
            for (Entry<?, ?> entry : mapValue.entrySet()) {
                map.put(processField(entry.getKey()), processField(entry.getValue()));
            }
            return map;
        }
        if (value instanceof Bytes bytes) {
            return bytes.getBytes();
        }

        return value;
    }

    @Override
    public String toString()
    {
        return values.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MaterializedRow o = (MaterializedRow) obj;
        return Objects.equals(values, o.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(values);
    }

    private abstract static class ApproximateNumeric
    {
        public abstract Number getValue();

        protected abstract Number getNormalizedValue();

        @Override
        public String toString()
        {
            return getValue().toString();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            ApproximateNumeric o = (ApproximateNumeric) obj;
            return Objects.equals(getNormalizedValue(), o.getNormalizedValue());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getNormalizedValue());
        }
    }

    private static class ApproximateDouble
            extends ApproximateNumeric
    {
        private final Double value;
        private final int precision;

        private ApproximateDouble(Double value, int precision)
        {
            this.value = requireNonNull(value, "value is null");
            this.precision = precision;
        }

        @Override
        public Number getValue()
        {
            return value;
        }

        @Override
        protected Number getNormalizedValue()
        {
            if (value.isNaN() || value.isInfinite()) {
                return value;
            }
            return new BigDecimal(getValue().doubleValue()).round(new MathContext(precision)).doubleValue();
        }
    }

    private static class ApproximateFloat
            extends ApproximateNumeric
    {
        private final Float value;
        private final int precision;

        private ApproximateFloat(Float value, int precision)
        {
            this.value = requireNonNull(value, "value is null");
            this.precision = precision;
        }

        @Override
        public Number getValue()
        {
            return value;
        }

        @Override
        protected Number getNormalizedValue()
        {
            if (value.isNaN() || value.isInfinite()) {
                return value;
            }
            return new BigDecimal(getValue().floatValue()).round(new MathContext(precision)).floatValue();
        }
    }
}
