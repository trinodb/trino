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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.AbstractIterator;
import io.trino.plugin.hive.aws.AwsApiCallStats;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class AwsSdkUtil
{
    private AwsSdkUtil() {}

    /**
     * Helper method to get all results from a paginated API.
     *
     * @param request request object reused for subsequent requests with
     * {@code setNextToken} being used to set the next token in the request object
     */
    public static <Request, Result> Stream<Result> getPaginatedResults(
            Function<Request, Result> submission,
            Request request,
            BiConsumer<Request, String> setNextToken,
            Function<Result, String> extractNextToken,
            AwsApiCallStats stats)
    {
        requireNonNull(submission, "submission is null");
        requireNonNull(request, "request is null");
        requireNonNull(setNextToken, "setNextToken is null");
        requireNonNull(extractNextToken, "extractNextToken is null");

        Iterator<Result> iterator = new AbstractIterator<>()
        {
            private String nextToken;
            private boolean firstRequest = true;

            @Override
            protected Result computeNext()
            {
                if (nextToken == null && !firstRequest) {
                    return endOfData();
                }

                setNextToken.accept(request, nextToken);
                Result result = stats.call(() -> submission.apply(request));
                firstRequest = false;
                nextToken = extractNextToken.apply(result);
                return result;
            }
        };

        return stream(iterator);
    }
}
