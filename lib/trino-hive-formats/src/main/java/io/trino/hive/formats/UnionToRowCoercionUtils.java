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
package io.trino.hive.formats;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.List;

import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignatureParameter.namedField;

public final class UnionToRowCoercionUtils
{
    public static final String UNION_FIELD_TAG_NAME = "tag";
    public static final String UNION_FIELD_FIELD_PREFIX = "field";
    public static final Type UNION_FIELD_TAG_TYPE = TINYINT;

    private UnionToRowCoercionUtils() {}

    public static RowType rowTypeForUnionOfTypes(List<Type> types)
    {
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.<RowType.Field>builder()
                .add(RowType.field(UNION_FIELD_TAG_NAME, UNION_FIELD_TAG_TYPE));
        for (int i = 0; i < types.size(); i++) {
            fields.add(RowType.field(UNION_FIELD_FIELD_PREFIX + i, types.get(i)));
        }
        return RowType.from(fields.build());
    }

    public static TypeSignature rowTypeSignatureForUnionOfTypes(List<TypeSignature> typeSignatures)
    {
        ImmutableList.Builder<TypeSignatureParameter> fields = ImmutableList.builder();
        fields.add(namedField(UNION_FIELD_TAG_NAME, UNION_FIELD_TAG_TYPE.getTypeSignature()));
        for (int i = 0; i < typeSignatures.size(); i++) {
            fields.add(namedField(UNION_FIELD_FIELD_PREFIX + i, typeSignatures.get(i)));
        }
        return TypeSignature.rowType(fields.build());
    }
}
