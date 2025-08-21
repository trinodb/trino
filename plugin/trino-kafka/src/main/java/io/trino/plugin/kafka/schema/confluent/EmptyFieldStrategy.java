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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.RowType;

import java.util.Optional;

public enum EmptyFieldStrategy
{
    IGNORE,
    MARK,
    FAIL;

    public static final String DUMMY_FIELD_NAME = "$empty_field_marker";
    public static final RowType DUMMY_ROW_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BooleanType.BOOLEAN)));
}
