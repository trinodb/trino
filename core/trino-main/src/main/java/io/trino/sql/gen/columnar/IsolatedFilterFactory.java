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
package io.trino.sql.gen.columnar;

import io.airlift.bytecode.DynamicClassLoader;
import io.trino.spi.TrinoException;
import io.trino.sql.gen.IsolatedClass;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.SpecialForm;

import java.lang.reflect.Constructor;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;

public final class IsolatedFilterFactory
{
    private IsolatedFilterFactory() {}

    // IsNullColumnarFilter needs to be isolated by input Type to avoid profile pollution
    // This approach avoids byte code generation for IsNullColumnarFilter
    public static Supplier<ColumnarFilter> createIsolatedIsNullColumnarFilter(SpecialForm specialForm)
    {
        return createIsolatedColumnarFilter(specialForm, IsNullColumnarFilter.class);
    }

    // IsNotNullColumnarFilter needs to be isolated by input Type to avoid profile pollution
    // This approach avoids byte code generation for IsNotNullColumnarFilter
    public static Supplier<ColumnarFilter> createIsolatedIsNotNullColumnarFilter(SpecialForm specialForm)
    {
        return createIsolatedColumnarFilter(specialForm, IsNotNullColumnarFilter.class);
    }

    private static Supplier<ColumnarFilter> createIsolatedColumnarFilter(SpecialForm specialForm, Class<? extends ColumnarFilter> filterClazz)
    {
        checkArgument(specialForm.getForm() == IS_NULL, "specialForm %s should be IS_NULL", specialForm);
        checkArgument(specialForm.getArguments().size() == 1, "specialForm %s should have single argument", specialForm);
        if (!(specialForm.getArguments().get(0) instanceof InputReferenceExpression inputReferenceExpression)) {
            throw new UnsupportedOperationException("IS_NULL columnar evaluation is supported only for InputReferenceExpression");
        }

        Class<? extends ColumnarFilter> isolatedFilter = IsolatedClass.isolateClass(
                new DynamicClassLoader(ColumnarFilterCompiler.class.getClassLoader()),
                ColumnarFilter.class,
                filterClazz);
        Constructor<? extends ColumnarFilter> constructor;
        try {
            constructor = isolatedFilter.getConstructor(InputReferenceExpression.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return () -> {
            try {
                return constructor.newInstance(inputReferenceExpression);
            }
            catch (ReflectiveOperationException e) {
                throw new TrinoException(COMPILER_ERROR, e);
            }
        };
    }
}
