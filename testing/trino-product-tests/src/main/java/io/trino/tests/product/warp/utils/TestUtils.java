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

package io.trino.tests.product.warp.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TestUtils
{
    private static final String suffix = LocalDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("_yyyyMMdd_HHmmss_SSS"));

    private TestUtils()
    {
    }

    public static int countMethodsWithAnnotation(Class<?> clazz, Class<? extends Annotation> annotation)
    {
        int count = 0;

        for (Method m : clazz.getMethods()) {
            if (m.isAnnotationPresent(annotation)) {
                count++;
            }
        }
        return count;
    }

    public static String getFullyQualifiedName(String catalogName, String schemaName, String tableName)
    {
        return catalogName + "." + schemaName + "." + tableName;
    }

    public static String getSuffix()
    {
        return suffix;
    }
}
