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
package io.trino.type.collation;

import io.airlift.slice.Slice;
import io.trino.spi.type.CollationComparison;

/// The comparison of the default collation: code point order, which for valid UTF-8 is the order
/// of the bytes.
enum BinaryComparison
        implements CollationComparison
{
    BINARY;

    @Override
    public int compare(Slice left, Slice right)
    {
        return left.compareTo(right);
    }

    @Override
    public boolean equal(Slice left, Slice right)
    {
        return left.equals(right);
    }

    @Override
    public Slice collationKey(Slice value)
    {
        return value;
    }
}
