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
package io.prestosql;

import io.prestosql.server.testing.TestingPrestoServer;

import java.lang.reflect.Field;

public class TestingServerUtils
{
    private TestingServerUtils() {}

    public static void setTestingServer(Object target, TestingPrestoServer server)
    {
        try {
            // Use reflection to inject TestingPrestoServer into private server field in target object.
            Field serverField = target.getClass().getSuperclass().getDeclaredField("server");
            serverField.setAccessible(true);
            serverField.set(target, server);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
