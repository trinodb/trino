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
package io.trino.plugin.hive.util;

// based on org.apache.hadoop.hive.ql.io.BucketCodec
public enum AcidBucketCodec
{
    V0 {
        @Override
        public int decodeWriterId(int bucketProperty)
        {
            return bucketProperty;
        }

        @Override
        public int decodeStatementId(int bucketProperty)
        {
            return 0;
        }
    },
    V1 {
        @Override
        public int decodeWriterId(int bucketProperty)
        {
            return (bucketProperty & V1_WRITER_ID_MASK) >>> 16;
        }

        @Override
        public int decodeStatementId(int bucketProperty)
        {
            return (bucketProperty & V1_STATEMENT_ID_MASK);
        }
    };

    private static final int V1_STATEMENT_ID_MASK = /*.*/ 0b0000_0000_0000_0000_0000_1111_1111_1111;
    private static final int V1_WRITER_ID_MASK = /*....*/ 0b0000_1111_1111_1111_0000_0000_0000_0000;
    private static final int CODEC_VERSION_MASK = /*...*/ 0b1110_0000_0000_0000_0000_0000_0000_0000;

    public static AcidBucketCodec forBucket(int bucket)
    {
        int version = (CODEC_VERSION_MASK & bucket) >>> 29;
        return switch (version) {
            case 0 -> V0;
            case 1 -> V1;
            default -> throw new IllegalArgumentException("Invalid bucket 0x%08X. Version=%s".formatted(bucket, version));
        };
    }

    public abstract int decodeWriterId(int bucketProperty);

    public abstract int decodeStatementId(int bucketProperty);
}
