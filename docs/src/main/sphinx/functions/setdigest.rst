=========================
Set Digest functions
=========================

Trino offers several functions that deal with the
`MinHash <https://en.wikipedia.org/wiki/MinHash>`_ technique.

MinHash is used to quickly estimate the
`Jaccard similarity coefficient <https://en.wikipedia.org/wiki/Jaccard_index>`_
between two sets.

It is commonly used in data mining to detect near-duplicate web pages at scale.
By using this information, the search engines efficiently avoid showing
within the search results two pages that are nearly identical.

The following naive example showcases how the Set Digest functions are
used to estimate the similarity between texts::

    WITH text_input(id, text) AS (VALUES
                                 (1, 'The quick brown fox jumps over the lazy dog'),
                                 (2, 'The quick red fox jumps over the lazy dog'),
                                 (3, 'The quick and the lazy'),
                                 (4, 'The quick brown fox jumps over the dog')),
         text_ngrams(id, ngrams) AS (
             SELECT id,
                    transform(
                      ngrams(
                        split(text, ' '),
                        4
                      ),
                      token -> array_join(token, ' ')
                    )
             FROM text_input
         ),
         minhash_digest(id, digest) AS (
             SELECT id,
                    (SELECT make_set_digest(v) FROM unnest(ngrams) u(v))
             FROM text_ngrams
         ),
         setdigest_side_by_side(id1, digest1, id2, digest2) AS (
             SELECT m1.id as id1,
                    m1.digest as digest1,
                    m2.id as id2,
                    m2.digest as digest2
             FROM (SELECT id, digest FROM minhash_digest) m1
             JOIN (SELECT id, digest FROM minhash_digest) m2
               ON m1.id != m2.id AND m1.id < m2.id
        )
    SELECT id1,
           id2,
           intersection_cardinality(digest1, digest2) AS intersection_cardinality,
           jaccard_index(digest1, digest2)            AS jaccard_index
    FROM setdigest_side_by_side
    ORDER BY id1, id2;

which results in:

.. code-block:: text

     id1 | id2 | intersection_cardinality |    jaccard_index
    -----+-----+--------------------------+---------------------
       1 |   2 |                        3 | 0.16666666666666666
       1 |   3 |                        0 |                 0.0
       1 |   4 |                        4 |                 0.6
       2 |   3 |                        0 |                 0.0
       2 |   4 |                        1 |                 0.0
       3 |   4 |                        0 |                 0.0

Data structures
---------------

Trino implements Set Digest data sketches by encapsulating the following components:

- `HyperLogLog <https://en.wikipedia.org/wiki/HyperLogLog>`_
- `MinHash with a single hash function <http://en.wikipedia.org/wiki/MinHash#Variant_with_a_single_hash_function>`_

The Hyperloglog structure is used for the approximation of the distinct elements
in the original set.

The MinHash structure is used to store a low memory footprint signature of the original set.
The similarity of any two sets is estimated by comparing their signatures.

The Trino type for this data structure is called ``setdigest``.
Trino offers the ability to merge multiple Set Digest data sketches.

Serialization
-------------

Data sketches can be serialized to and deserialized from ``varbinary``. This
allows them to be stored for later use.


Functions
---------

.. function:: make_set_digest(x) -> setdigest

    Composes all input values of ``x`` into a ``setdigest``.

    Create a ``setdigest`` corresponding to a ``bigint`` array::

        SELECT make_set_digest(value)
        FROM (VALUES 1, 2, 3) T(value);

    Create a ``setdigest`` corresponding to a ``varchar`` array::

        SELECT make_set_digest(value)
        FROM (VALUES 'Trino', 'SQL', 'on', 'everything') T(value);

.. function:: merge_set_digest(setdigest) -> setdigest

    Returns the ``setdigest`` of the aggregate union of the individual ``setdigest``
    Set Digest structures.

.. function:: cardinality(setdigest) -> long
    :noindex:

    Returns the cardinality of the set digest from its internal
    ``HyperLogLog`` component.

    Examples::

        SELECT cardinality(make_set_digest(value))
        FROM (VALUES 1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5) T(value);
        -- 5

.. function:: intersection_cardinality(x,y) -> long

    Returns the estimation for the cardinality of the intersection of the two set digests.

    ``x`` and ``y`` must be of type  ``setdigest``

    Examples::

        SELECT intersection_cardinality(make_set_digest(v1), make_set_digest(v2))
        FROM (VALUES (1, 1), (NULL, 2), (2, 3), (3, 4)) T(v1, v2);
        -- 3

.. function:: jaccard_index(x, y) -> double

    Returns the estimation of `Jaccard index <https://en.wikipedia.org/wiki/Jaccard_index>`_ for
    the two set digests.

    ``x`` and ``y`` must be of type  ``setdigest``.

    Examples::

        SELECT jaccard_index(make_set_digest(v1), make_set_digest(v2))
        FROM (VALUES (1, 1), (NULL,2), (2, 3), (NULL, 4)) T(v1, v2);
        -- 0.5

.. function:: hash_counts(x) -> map(bigint, smallint)

    Returns a map containing the `Murmur3Hash128 <https://en.wikipedia.org/wiki/MurmurHash#MurmurHash3>`_
    hashed values and the count of their occurences within
    the internal ``MinHash`` structure belonging to ``x``.

    ``x`` must be of type  ``setdigest``.

    Examples::

        SELECT hash_counts(make_set_digest(value))
        FROM (VALUES 1, 1, 1, 2, 2) T(value);
        -- {19144387141682250=3, -2447670524089286488=2}
