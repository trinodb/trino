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
package io.trino.testing.eventlistener;

public class NamedClosable
        implements AutoCloseable
{
    private final String name;
    private final AutoCloseable closeable;

    public NamedClosable(String name, AutoCloseable closeable)
    {
        this.name = name;
        this.closeable = closeable;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public void close()
            throws Exception
    {
        closeable.close();
    }
}
