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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.MethodGenerationContext;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.Slice;
import io.trino.spi.type.Type;

import java.util.List;

import static io.airlift.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

public class SqlTypeBytecodeExpression
        extends BytecodeExpression
{
    public static SqlTypeBytecodeExpression constantType(ClassBuilder classBuilder, Type type)
    {
        requireNonNull(classBuilder, "classBuilder is null");
        requireNonNull(type, "type is null");

        BytecodeExpression loadConstant = classBuilder.loadConstant(type, Type.class);
        return new SqlTypeBytecodeExpression(type, loadConstant);
    }

    private final Type type;
    private final BytecodeExpression expression;

    public SqlTypeBytecodeExpression(Type type, BytecodeExpression expression)
    {
        super(type(Type.class));
        this.type = requireNonNull(type, "type is null");
        this.expression = expression;
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return expression;
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return type.getTypeSignature().toString();
    }

    public BytecodeExpression getValue(BytecodeExpression block, BytecodeExpression position)
    {
        Class<?> fromJavaElementType = type.getJavaType();

        if (fromJavaElementType == boolean.class) {
            return invoke("getBoolean", boolean.class, block, position);
        }
        if (fromJavaElementType == long.class) {
            return invoke("getLong", long.class, block, position);
        }
        if (fromJavaElementType == double.class) {
            return invoke("getDouble", double.class, block, position);
        }
        if (fromJavaElementType == Slice.class) {
            return invoke("getSlice", Slice.class, block, position);
        }
        return invoke("getObject", Object.class, block, position).cast(fromJavaElementType);
    }

    public BytecodeExpression writeValue(BytecodeExpression blockBuilder, BytecodeExpression value)
    {
        Class<?> fromJavaElementType = type.getJavaType();

        if (fromJavaElementType == boolean.class) {
            return invoke("writeBoolean", void.class, blockBuilder, value);
        }
        if (fromJavaElementType == long.class) {
            return invoke("writeLong", void.class, blockBuilder, value);
        }
        if (fromJavaElementType == double.class) {
            return invoke("writeDouble", void.class, blockBuilder, value);
        }
        if (fromJavaElementType == Slice.class) {
            return invoke("writeSlice", void.class, blockBuilder, value.cast(Slice.class));
        }
        return invoke("writeObject", void.class, blockBuilder, value.cast(Object.class));
    }
}
