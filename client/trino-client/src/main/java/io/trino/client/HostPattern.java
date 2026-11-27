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
package io.trino.client;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class HostPattern
{
    private static final String WILDCARD = "*";
    private static final Splitter LABEL_SPLITTER = Splitter.on('.');

    private final List<String> labels;

    private HostPattern(List<String> labels)
    {
        this.labels = ImmutableList.copyOf(requireNonNull(labels, "labels is null"));
        String pattern = String.join(".", labels);
        checkArgument(labels.stream().noneMatch(String::isEmpty), "Host pattern '%s' contains an empty label", pattern);
        checkArgument(labels.stream().allMatch(label -> label.equals(WILDCARD) || !label.contains(WILDCARD)), "Host pattern '%s' can only use * as a whole label", pattern);
        checkArgument(!labels.stream().allMatch(WILDCARD::equals), "Host pattern '%s' matches every host", pattern);
    }

    public static HostPattern parse(String pattern)
    {
        if (!pattern.contains(":")) {
            return new HostPattern(hostLabels(pattern));
        }
        String address = CharMatcher.anyOf("[]").trimFrom(pattern);
        checkArgument(InetAddresses.isInetAddress(address), "Host pattern '%s' cannot contain a port", pattern);
        return new HostPattern(ImmutableList.of(InetAddresses.toAddrString(InetAddresses.forString(address))));
    }

    public static List<String> hostLabels(String host)
    {
        String normalized = host.toLowerCase(ENGLISH);
        if (normalized.endsWith(".")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return LABEL_SPLITTER.splitToList(normalized);
    }

    public boolean matches(String host)
    {
        return matches(hostLabels(host));
    }

    // A leading * matches one or more labels, because bucket names can contain dots.
    // Any other * matches exactly one label.
    public boolean matches(List<String> hostLabels)
    {
        int patternSuffixLabels = labels.size();
        if (labels.get(0).equals(WILDCARD)) {
            patternSuffixLabels--;
            if (hostLabels.size() <= patternSuffixLabels) {
                return false;
            }
        }
        else if (hostLabels.size() != patternSuffixLabels) {
            return false;
        }
        for (int i = 1; i <= patternSuffixLabels; i++) {
            String patternLabel = labels.get(labels.size() - i);
            if (!patternLabel.equals(WILDCARD) && !patternLabel.equals(hostLabels.get(hostLabels.size() - i))) {
                return false;
            }
        }
        return true;
    }
}
