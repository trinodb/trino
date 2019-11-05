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
package io.prestosql.tests.hive;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

enum BucketingType
{
    NONE {
        @Override
        public String getHiveClustering(String columnNames, int buckets)
        {
            return "";
        }

        @Override
        public List<String> getHiveTableProperties()
        {
            return ImmutableList.of();
        }
    },

    BUCKETED_DEFAULT {
        @Override
        public String getHiveClustering(String columnNames, int buckets)
        {
            return defaultHiveClustering(columnNames, buckets);
        }

        @Override
        public List<String> getHiveTableProperties()
        {
            return ImmutableList.of();
        }
    },

    BUCKETED_V1 {
        @Override
        public String getHiveClustering(String columnNames, int buckets)
        {
            return defaultHiveClustering(columnNames, buckets);
        }

        @Override
        public List<String> getHiveTableProperties()
        {
            return ImmutableList.of("'bucketing_version'='1'");
        }
    },

    BUCKETED_V2 {
        @Override
        public String getHiveClustering(String columnNames, int buckets)
        {
            return defaultHiveClustering(columnNames, buckets);
        }

        @Override
        public List<String> getHiveTableProperties()
        {
            return ImmutableList.of("'bucketing_version'='2'");
        }
    },
    /**/;

    public abstract String getHiveClustering(String columnNames, int buckets);

    public abstract List<String> getHiveTableProperties();

    private static String defaultHiveClustering(String columnNames, int buckets)
    {
        requireNonNull(columnNames, "columnNames is null");
        return format("CLUSTERED BY(%s) INTO %s BUCKETS", columnNames, buckets);
    }
}
