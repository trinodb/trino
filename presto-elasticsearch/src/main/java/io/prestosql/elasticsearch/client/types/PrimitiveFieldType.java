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

import static java.util.Objects.requireNonNull;

public class PrimitiveFieldType
        implements ElasticFieldType
{
    private final ElasticType elasticType;

    public PrimitiveFieldType(ElasticType elasticType)
    {
        this.elasticType = requireNonNull(elasticType, "elasticType is null");
    }

    public ElasticType getType()
    {
        return elasticType;
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
