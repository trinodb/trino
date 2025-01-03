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
package io.trino.parquet.reader.decoders;

import io.trino.parquet.reader.CalendarUtils;
import io.trino.parquet.reader.SimpleSliceInputStream;

public final class HybridCalendarDecoder
{
    private HybridCalendarDecoder() {}

    public static ValueDecoder<int[]> getDateInt32Decoder(ValueDecoder<int[]> delegate)
    {
        return new ValueDecoder<>()
        {
            @Override
            public void init(SimpleSliceInputStream input)
            {
                delegate.init(input);
            }

            @Override
            public void read(int[] values, int offset, int length)
            {
                delegate.read(values, offset, length);
                for (int i = offset; i < offset + length; i++) {
                    values[i] = CalendarUtils.convertDaysToProlepticGregorian(values[i]);
                }
            }

            @Override
            public void skip(int n)
            {
                delegate.skip(n);
            }
        };
    }
}
