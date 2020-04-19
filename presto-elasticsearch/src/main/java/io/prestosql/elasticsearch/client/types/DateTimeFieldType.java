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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DateTimeFieldType
        implements ElasticFieldType
{
    private final ElasticType elasticType;
    private final List<String> formats;

    public DateTimeFieldType(ElasticType elasticType)
    {
        this(elasticType, ImmutableList.of());
    }

    public DateTimeFieldType(ElasticType elasticType, List<String> formats)
    {
        this.elasticType = requireNonNull(elasticType, "elasticType is null");
        requireNonNull(formats, "formats is null");
        this.formats = ImmutableList.copyOf(formats);
    }

    public List<String> getFormats()
    {
        return formats;
    }

    public String getName()
    {
        return elasticType.getName();
    }

    @Override
    public boolean supportsPredicates()
    {
        return elasticType.supportsPredicate();
    }
}
