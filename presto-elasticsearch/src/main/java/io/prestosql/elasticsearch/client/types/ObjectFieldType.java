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

public class ObjectFieldType
        implements ElasticFieldType
{
    private final List<ElasticField> fields;

    public ObjectFieldType(List<ElasticField> fields)
    {
        requireNonNull(fields, "fields is null");

        this.fields = ImmutableList.copyOf(fields);
    }

    public List<ElasticField> getFields()
    {
        return fields;
    }

    @Override
    public boolean supportsPredicates()
    {
        return false;
    }
}
