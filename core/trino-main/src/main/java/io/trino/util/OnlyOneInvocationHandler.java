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
package io.trino.util;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.google.common.reflect.AbstractInvocationHandler;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class OnlyOneInvocationHandler
        extends AbstractInvocationHandler
{
    private static final Object NULL_MARKER = new Object();
    private final Table<String, List<?>, Object> results = HashBasedTable.create();
    private final Object delegate;

    public OnlyOneInvocationHandler(Object delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] argumentsArray)
            throws Throwable
    {
        List<?> arguments = ImmutableList.copyOf(argumentsArray);
        Object cachedResult = results.get(method.toString(), arguments);
        if (cachedResult != null) {
            if (cachedResult == NULL_MARKER) {
                return null;
            }
            return cachedResult;
        }
        Object result;
        try {
            result = method.invoke(delegate, argumentsArray);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
        if (result != null) {
            results.put(method.toString(), arguments, result);
        }
        else {
            results.put(method.toString(), arguments, NULL_MARKER);
        }
        return result;
    }
}
