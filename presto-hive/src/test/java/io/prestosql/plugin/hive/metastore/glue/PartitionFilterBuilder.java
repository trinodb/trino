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
package io.prestosql.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PartitionFilterBuilder
{
    public static final int DECIMAL_TYPE_PRECISION = 10;
    public static final int DECIMAL_TYPE_SCALE = 5;
    public static final DecimalType DECIMAL_TYPE = DecimalType.createDecimalType(DECIMAL_TYPE_PRECISION, DECIMAL_TYPE_SCALE);

    private final Map<String, Domain> domains = new HashMap<>();

    public PartitionFilterBuilder addStringValues(String columnName, String... values)
    {
        List<Slice> blockValues = Arrays.stream(values).map(Slices::utf8Slice).collect(toImmutableList());
        Domain domain = Domain.multipleValues(VarcharType.VARCHAR, blockValues);
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public PartitionFilterBuilder addBigintValues(String columnName, Long... values)
    {
        Domain domain = Domain.multipleValues(BigintType.BIGINT, Arrays.asList(values));
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public PartitionFilterBuilder addIntegerValues(String columnName, Long... values)
    {
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, Arrays.asList(values));
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public PartitionFilterBuilder addSmallintValues(String columnName, Long... values)
    {
        Domain domain = Domain.multipleValues(SmallintType.SMALLINT, Arrays.asList(values));
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public PartitionFilterBuilder addTinyintValues(String columnName, Long... values)
    {
        Domain domain = Domain.multipleValues(TinyintType.TINYINT, Arrays.asList(values));
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public PartitionFilterBuilder addDecimalValues(String columnName, String... values)
    {
        checkArgument(values.length > 0);
        List<Long> encodedValues = Arrays.stream(values)
                .map(PartitionFilterBuilder::decimalOf)
                .collect(toImmutableList());
        Domain domain = Domain.multipleValues(DECIMAL_TYPE, encodedValues);
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public PartitionFilterBuilder addDateValues(String columnName, Long... values)
    {
        Domain domain = Domain.multipleValues(DateType.DATE, Arrays.asList(values));
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public PartitionFilterBuilder addRanges(String columnName, Range range, Range... ranges)
    {
        ValueSet values = ValueSet.ofRanges(range, ranges);
        Domain domain = Domain.create(values, false);
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public PartitionFilterBuilder addDomain(String columnName, Domain domain)
    {
        domains.merge(columnName, domain, Domain::union);
        return this;
    }

    public TupleDomain<String> build()
    {
        return TupleDomain.withColumnDomains(ImmutableMap.copyOf(this.domains));
    }

    public static Long decimalOf(String value)
    {
        BigDecimal bigDecimalValue = new BigDecimal(value)
                .setScale(DECIMAL_TYPE_SCALE, RoundingMode.UP);
        return bigDecimalValue.unscaledValue().longValue();
    }
}
