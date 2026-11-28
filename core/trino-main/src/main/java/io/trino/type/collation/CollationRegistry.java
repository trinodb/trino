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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Collation;
import io.trino.spi.type.CollationComparison;
import io.trino.spi.type.CollationComparisons;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.COLLATION_NOT_FOUND;
import static io.trino.spi.type.Collation.UCS_BASIC;

/// The built-in collation catalog: a fixed set of well-known collations.
///
/// | Name | Ordering | Distinguishes |
/// | --- | --- | --- |
/// | `ucs_basic` | Unicode code points | every code point |
/// | `unicode` | the Unicode Collation Algorithm (ISO/IEC 14651), root locale | base letters, accents, and case |
/// | `unicode_ci` | the same, case-insensitive | base letters and accents |
///
/// `ucs_basic` and `unicode` are the collations ISO/IEC 9075-2 defines for the UCS repertoire.
/// `unicode_ci` is the case-insensitive comparison that remote systems commonly offer, such as
/// MySQL's `_ci` collations. Every collation also exists with the `PAD SPACE` pad attribute.
public final class CollationRegistry
        implements CollationComparisons
{
    public static final String UNICODE_NAME = "unicode";
    public static final String UNICODE_CI_NAME = "unicode_ci";

    // the root collation, ISO/IEC 14651 without tailoring, at each catalog strength
    private static final Map<String, RootCollation> ROOT_COLLATIONS = ImmutableMap.of(
            UNICODE_NAME, new RootCollation(Collator.TERTIARY),
            UNICODE_CI_NAME, new RootCollation(Collator.SECONDARY));

    private static final CollationRegistry INSTANCE = new CollationRegistry();

    /// The process-wide catalog. Collations are immutable and the catalog is fixed, so every
    /// type factory shares one instance.
    public static CollationRegistry collationRegistry()
    {
        return INSTANCE;
    }

    private CollationRegistry() {}

    /// Resolves a canonical collation name, failing with
    /// [io.trino.spi.StandardErrorCode#COLLATION_NOT_FOUND] when the name is not in the catalog.
    /// A name in SQL is an identifier, canonicalized before it is resolved.
    public Collation getCollation(String name)
    {
        return findCollation(name).orElseThrow(() -> new TrinoException(COLLATION_NOT_FOUND, "Collation not found: " + name));
    }

    /// Resolves a collation descriptor produced by [Collation#descriptor()].
    public Collation getCollationByDescriptor(String descriptor)
    {
        Collation collation = Collation.fromDescriptor(descriptor);
        // fails when the catalog has no collation of the name
        getCollation(collation.name());
        return collation;
    }

    public Optional<Collation> findCollation(String name)
    {
        if (name.equals(UCS_BASIC.name())) {
            return Optional.of(UCS_BASIC);
        }
        if (!ROOT_COLLATIONS.containsKey(name)) {
            return Optional.empty();
        }
        return Optional.of(Collation.of(name));
    }

    /// The comparison a collation in the catalog names.
    ///
    /// @throws IllegalArgumentException if the collation is not in the catalog
    @Override
    public CollationComparison comparison(Collation collation)
    {
        if (collation.name().equals(UCS_BASIC.name())) {
            return collation.isPadSpace() ? BinaryPadSpaceComparison.BINARY_PAD_SPACE : BinaryComparison.BINARY;
        }
        RootCollation root = ROOT_COLLATIONS.get(collation.name());
        if (root == null) {
            throw new IllegalArgumentException("Collation not found: " + collation);
        }
        return collation.isPadSpace() ? root.padSpace().get() : root.noPad().get();
    }

    /// The names of every collation in the catalog, sorted.
    public List<String> collationNames()
    {
        return ImmutableList.<String>builder()
                .add(UCS_BASIC.name())
                .addAll(ROOT_COLLATIONS.keySet())
                .build().stream()
                .sorted()
                .collect(toImmutableList());
    }

    static RuleBasedCollator rootCollator(int strength)
    {
        // getInstance can return a shared instance, so configure a private thawed copy
        RuleBasedCollator collator = (RuleBasedCollator) Collator.getInstance(ULocale.ROOT).cloneAsThawed();
        collator.setStrength(strength);
        // Spaces and punctuation are significant, so that trailing spaces are governed only by the
        // collation's pad attribute
        collator.setAlternateHandlingShifted(false);
        return (RuleBasedCollator) collator.freeze();
    }

    // the comparisons of a root collation with each pad attribute, built on first use
    private record RootCollation(Supplier<CollationComparison> noPad, Supplier<CollationComparison> padSpace)
    {
        RootCollation(int strength)
        {
            this(memoize(() -> new IcuCollation(rootCollator(strength))), memoize(() -> new IcuPadSpaceCollation(rootCollator(strength))));
        }
    }
}
