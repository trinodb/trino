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
package io.trino.plugin.base.metrics;

import io.airlift.stats.TDigest;
import io.trino.spi.metrics.Distribution;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleLists;

import java.util.List;
import java.util.Optional;

public sealed interface TDigestHistogram
        extends Distribution<TDigestHistogram>
        permits EmptyTDigestHistogram, MultiValueTDigestHistogram, SingleValueHistogram
{
    // This is important so that we can instruct Jackson to ignore this property
    // in certain places (e.g. UiQueryResource)
    String DIGEST_PROPERTY = "digest";

    static Builder builder()
    {
        return new Builder();
    }

    static TDigestHistogram fromValue(double value)
    {
        return new SingleValueHistogram(value);
    }

    static TDigestHistogram fromDigest(TDigest digest)
    {
        if (digest.getCount() == 0) {
            return new EmptyTDigestHistogram();
        }
        if (digest.getCount() == 1) {
            return new SingleValueHistogram(digest.getMax());
        }
        return new MultiValueTDigestHistogram(TDigest.copyOf(digest));
    }

    static TDigestHistogram empty()
    {
        return new EmptyTDigestHistogram();
    }

    @Override
    default TDigestHistogram mergeWith(TDigestHistogram other)
    {
        return mergeWith(List.of(other));
    }

    @Override
    default TDigestHistogram mergeWith(List<TDigestHistogram> others)
    {
        if (others.isEmpty()) {
            return this;
        }

//        int expectedSize = toIntExact(this.getTotal() + others.stream()
//                .mapToLong(TDigestHistogram::getTotal)
//                .sum());

        // TODO: Create a TDigest with expected size to minimize internal resizing
        //  see: https://github.com/airlift/airlift/pull/1622
        TDigest result = new TDigest();
        mergeTo(result, this);

        for (TDigestHistogram other : others) {
            mergeTo(result, other);
        }

        if (result.getCount() == 0) {
            return new EmptyTDigestHistogram();
        }

        if (result.getCount() == 1) {
            return new SingleValueHistogram(result.getMax());
        }

        return new MultiValueTDigestHistogram(result);
    }

    static void mergeTo(TDigest values, TDigestHistogram histogram)
    {
        switch (histogram) {
            case EmptyTDigestHistogram _ -> {}
            case SingleValueHistogram singleValueHistogram -> values.add(singleValueHistogram.value());
            case MultiValueTDigestHistogram multiValueHistogram -> values.mergeWith(multiValueHistogram.getDigest());
        }
    }

    static Optional<TDigestHistogram> merge(List<TDigestHistogram> histograms)
    {
        if (histograms.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(histograms.get(0).mergeWith(histograms.subList(1, histograms.size())));
    }

    class Builder
    {
        private final DoubleList values = DoubleLists.synchronize(new DoubleArrayList());

        Builder() {}

        public void add(double value)
        {
            values.add(value);
        }

        public synchronized TDigestHistogram build()
        {
            if (values.isEmpty()) {
                return TDigestHistogram.empty();
            }
            if (values.size() == 1) {
                return TDigestHistogram.fromValue(values.getDouble(0));
            }

            // TODO: we could avoid growing digest internally
            //  if we had a constructor that takes the estimated size
            //  See: https://github.com/airlift/airlift/pull/1622
            //  TDigest digest = new TDigest(values.size());
            TDigest digest = new TDigest();
            for (int i = 0; i < values.size(); i++) {
                digest.add(values.getDouble(i));
            }

            // Doesn't copy the digest again as we just built it
            return new MultiValueTDigestHistogram(digest);
        }
    }
}
