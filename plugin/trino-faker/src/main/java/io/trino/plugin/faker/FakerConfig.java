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

package io.trino.plugin.faker;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.util.Locale;

public class FakerConfig
{
    private double nullProbability = 0.5;
    private long defaultLimit = 1000L;
    private Locale locale = Locale.ENGLISH;
    private boolean sequenceDetectionEnabled = true;
    private boolean dictionaryDetectionEnabled = true;

    @Max(1)
    @Min(0)
    public double getNullProbability()
    {
        return nullProbability;
    }

    @Config("faker.null-probability")
    @ConfigDescription("Default null probability for any column in any table that allows them")
    public FakerConfig setNullProbability(double value)
    {
        this.nullProbability = value;
        return this;
    }

    @Min(1)
    public long getDefaultLimit()
    {
        return defaultLimit;
    }

    @Config("faker.default-limit")
    @ConfigDescription("Default number of rows for each table, when the LIMIT clause is not specified in the query")
    public FakerConfig setDefaultLimit(long value)
    {
        this.defaultLimit = value;
        return this;
    }

    public Locale getLocale()
    {
        return locale;
    }

    @Config("faker.locale")
    @ConfigDescription("Default locale for generating character based data, specified as a IETF BCP 47 language tag string")
    public FakerConfig setLocale(String value)
    {
        this.locale = new Locale.Builder().setLanguageTag(value).build();
        return this;
    }

    public boolean isSequenceDetectionEnabled()
    {
        return sequenceDetectionEnabled;
    }

    @Config("faker.sequence-detection-enabled")
    @ConfigDescription(
            """
            If true, when creating a table using existing data, columns with the number of distinct values close to
            the number of rows are treated as sequences""")
    public FakerConfig setSequenceDetectionEnabled(boolean value)
    {
        this.sequenceDetectionEnabled = value;
        return this;
    }

    public boolean isDictionaryDetectionEnabled()
    {
        return dictionaryDetectionEnabled;
    }

    @Config("faker.dictionary-detection-enabled")
    @ConfigDescription(
            """
            If true, when creating a table using existing data, columns with a low number of distinct values
            are treated as dictionaries, and get the allowed_values column property populated with random values""")
    public FakerConfig setDictionaryDetectionEnabled(boolean value)
    {
        this.dictionaryDetectionEnabled = value;
        return this;
    }
}
