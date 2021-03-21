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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class MergeDetails
{
    public static final int DEFAULT_CASE_OPERATION_NUMBER = -1;
    public static final int INSERT_OPERATION_NUMBER = 1;
    public static final int DELETE_OPERATION_NUMBER = 2;
    public static final int UPDATE_OPERATION_NUMBER = 3;

    private final List<MergeCaseDetails> cases;

    @JsonCreator
    public MergeDetails(@JsonProperty("cases") List<MergeCaseDetails> cases)
    {
        this.cases = requireNonNull(cases, "cases is null");
    }

    @JsonProperty
    public List<MergeCaseDetails> getCases()
    {
        return cases;
    }

    @Override
    public String toString()
    {
        return format("MergeDetails{cases=[%s]}", cases.stream().map(MergeCaseDetails::toString).collect(joining(", ")));
    }
}
