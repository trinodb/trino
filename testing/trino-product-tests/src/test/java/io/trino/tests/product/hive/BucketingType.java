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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public enum BucketingType
{
    NONE {
        @Override
        public String getHiveClustering(String columnName, int buckets)
        {
            return "";
        }

        @Override
        public List<String> getHiveTableProperties()
        {
            return ImmutableList.of();
        }

        @Override
        public List<String> getTrinoTableProperties(String columnName, int buckets)
        {
            return ImmutableList.of();
        }
    },

    BUCKETED_DEFAULT {
        @Override
        public String getHiveClustering(String columnName, int buckets)
        {
            return defaultHiveClustering(columnName, buckets);
        }

        @Override
        public List<String> getHiveTableProperties()
        {
            return ImmutableList.of();
        }

        @Override
        public List<String> getTrinoTableProperties(String columnName, int buckets)
        {
            return ImmutableList.of(
                    "bucketed_by = ARRAY['" + columnName + "']",
                    "bucket_count = " + buckets);
        }
    },

    BUCKETED_V1 {
        @Override
        public String getHiveClustering(String columnName, int buckets)
        {
            return defaultHiveClustering(columnName, buckets);
        }

        @Override
        public List<String> getHiveTableProperties()
        {
            return ImmutableList.of("'bucketing_version'='1'");
        }

        @Override
        public List<String> getTrinoTableProperties(String columnName, int buckets)
        {
            return ImmutableList.of(
                    "bucketing_version = 1",
                    "bucketed_by = ARRAY['" + columnName + "']",
                    "bucket_count = " + buckets);
        }
    },

    BUCKETED_V2 {
        @Override
        public String getHiveClustering(String columnName, int buckets)
        {
            return defaultHiveClustering(columnName, buckets);
        }

        @Override
        public List<String> getHiveTableProperties()
        {
            return ImmutableList.of("'bucketing_version'='2'");
        }

        @Override
        public List<String> getTrinoTableProperties(String columnName, int buckets)
        {
            return ImmutableList.of(
                    "bucketing_version = 2",
                    "bucketed_by = ARRAY['" + columnName + "']",
                    "bucket_count = " + buckets);
        }
    },
    /**/;

    public abstract String getHiveClustering(String columnNames, int buckets);

    public abstract List<String> getHiveTableProperties();

    public abstract List<String> getTrinoTableProperties(String columnName, int buckets);

    private static String defaultHiveClustering(String columnName, int buckets)
    {
        requireNonNull(columnName, "columnName is null");
        return format("CLUSTERED BY(%s) INTO %s BUCKETS", columnName, buckets);
    }
}
