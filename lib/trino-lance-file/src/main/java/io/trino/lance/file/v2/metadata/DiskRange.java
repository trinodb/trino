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
package io.trino.lance.file.v2.metadata;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DiskRange
{
    public static final int BUFFER_DESCRIPTOR_SIZE = 16;

    private final long position;
    private final long length;

    private DiskRange(long position, long length)
    {
        this.position = position;
        this.length = length;
    }

    public long getPosition()
    {
        return position;
    }

    public long getLength()
    {
        return length;
    }

    public static DiskRange of(long position, long length)
    {
        return new DiskRange(position, length);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("position", position)
                .add("length", length)
                .toString();
    }
}
