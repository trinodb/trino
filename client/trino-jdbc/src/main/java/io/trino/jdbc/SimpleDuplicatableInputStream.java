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
package io.trino.jdbc;

import com.oracle.bmc.http.client.io.DuplicatableInputStream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class SimpleDuplicatableInputStream
        implements DuplicatableInputStream
{
    private final byte[] data;
    private ByteArrayInputStream stream;

    public SimpleDuplicatableInputStream(byte[] data)
    {
        this.data = data;
        this.stream = new ByteArrayInputStream(data);
    }

    @Override
    public InputStream duplicate()
    {
        return new ByteArrayInputStream(data);
    }
}
