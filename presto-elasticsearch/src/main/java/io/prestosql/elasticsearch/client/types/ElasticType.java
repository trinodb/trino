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
package io.prestosql.elasticsearch.client.types;

import com.google.common.collect.ImmutableList;

import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public enum ElasticType
{
    BINARY("binary", false, PrimitiveFieldType::new),
    BOOLEAN("boolean", true, PrimitiveFieldType::new),
    BYTE("byte", true, PrimitiveFieldType::new),
    DATE("date", true, DateTimeFieldType::new),
    DATE_NANOS("date_nanos", true, DateTimeFieldType::new),
    DOUBLE("double", true, PrimitiveFieldType::new),
    FLOAT("float", true, PrimitiveFieldType::new),
    //    HALF_FLOAT(),
    INTEGER("integer", true, PrimitiveFieldType::new),
    IP("ip", true, PrimitiveFieldType::new),
    KEYWORD("keyword", true, PrimitiveFieldType::new),
    LONG("long", true, PrimitiveFieldType::new),
    //    SCALED_FLOAT(),
    SHORT("short", true, PrimitiveFieldType::new),
    TEXT("text", false, PrimitiveFieldType::new),

    NESTED("nested", false, (type) -> new ObjectFieldType(ImmutableList.of())),
    OBJECT("object", false, (type) -> new ObjectFieldType(ImmutableList.of()));

    private final String name;
    private final Function<ElasticType, ElasticFieldType> mapper;
    private final boolean supportsPredicate;

    ElasticType(String name, Boolean supportsPredicate, Function<ElasticType, ElasticFieldType> mapper)
    {
        this.name = requireNonNull(name, "name is null");
        this.supportsPredicate = requireNonNull(supportsPredicate, "supportsPredicate is null");
        this.mapper = requireNonNull(mapper, "mapper is null");
    }

    public static ElasticType of(String elasticType)
    {
        return Stream.of(ElasticType.values())
                .filter(t -> t.name.equals(elasticType.toLowerCase(Locale.ENGLISH)))
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }

    public String getName()
    {
        return name;
    }

    public ElasticFieldType getFieldType()
    {
        return mapper.apply(this);
    }

    public boolean supportsPredicate()
    {
        return supportsPredicate;
    }
}
