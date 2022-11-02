==============================
String functions and operators
==============================

String operators
----------------

The ``||`` operator performs concatenation.

The ``LIKE`` statement can be used for pattern matching and is documented in
:ref:`like_operator`.

String functions
----------------

.. note::

    These functions assume that the input strings contain valid UTF-8 encoded
    Unicode code points.  There are no explicit checks for valid UTF-8 and
    the functions may return incorrect results on invalid UTF-8.
    Invalid UTF-8 data can be corrected with :func:`from_utf8`.

    Additionally, the functions operate on Unicode code points and not user
    visible *characters* (or *grapheme clusters*).  Some languages combine
    multiple code points into a single user-perceived *character*, the basic
    unit of a writing system for a language, but the functions will treat each
    code point as a separate unit.

    The :func:`lower` and :func:`upper` functions do not perform
    locale-sensitive, context-sensitive, or one-to-many mappings required for
    some languages. Specifically, this will return incorrect results for
    Lithuanian, Turkish and Azeri.

.. function:: chr(n) -> varchar

    Returns the Unicode code point ``n`` as a single character string.

.. function:: codepoint(string) -> integer

    Returns the Unicode code point of the only character of ``string``.

.. function:: concat(string1, ..., stringN) -> varchar

    Returns the concatenation of ``string1``, ``string2``, ``...``, ``stringN``.
    This function provides the same functionality as the
    SQL-standard concatenation operator (``||``).

.. function:: concat_ws(string0, string1, ..., stringN) -> varchar

    Returns the concatenation of ``string1``, ``string2``, ``...``, ``stringN``
    using ``string0`` as a separator. If ``string0`` is null, then the return
    value is null. Any null values provided in the arguments after the
    separator are skipped.

.. function:: concat_ws(string0, array(varchar)) -> varchar
    :noindex:

    Returns the concatenation of elements in the array using ``string0`` as a
    separator. If ``string0`` is null, then the return value is null. Any
    null values in the array are skipped.

.. function:: format(format, args...) -> varchar
    :noindex:

    See :func:`format`.

.. function:: hamming_distance(string1, string2) -> bigint

    Returns the Hamming distance of ``string1`` and ``string2``,
    i.e. the number of positions at which the corresponding characters are different.
    Note that the two strings must have the same length.

.. function:: length(string) -> bigint

    Returns the length of ``string`` in characters.

.. function:: levenshtein_distance(string1, string2) -> bigint

    Returns the Levenshtein edit distance of ``string1`` and ``string2``,
    i.e. the minimum number of single-character edits (insertions,
    deletions or substitutions) needed to change ``string1`` into ``string2``.

.. function:: lower(string) -> varchar

    Converts ``string`` to lowercase.

.. function:: lpad(string, size, padstring) -> varchar

    Left pads ``string`` to ``size`` characters with ``padstring``.
    If ``size`` is less than the length of ``string``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padstring`` must be non-empty.

.. function:: ltrim(string) -> varchar

    Removes leading whitespace from ``string``.

.. function:: luhn_check(string) -> boolean

    Tests whether a ``string`` of digits is valid according to the
    `Luhn algorithm <https://en.wikipedia.org/wiki/Luhn_algorithm>`_.

    This checksum function, also known as ``modulo 10`` or ``mod 10``, is
    widely applied on credit card numbers and government identification numbers
    to distinguish valid numbers from mistyped, incorrect numbers.

    Valid identification number::

        select luhn_check('79927398713');
        -- true

    Invalid identification number::

        select luhn_check('79927398714');
        -- false


.. function:: position(substring IN string) -> bigint

    Returns the starting position of the first instance of ``substring`` in
    ``string``. Positions start with ``1``. If not found, ``0`` is returned.

    .. note::

        This SQL-standard function has special syntax and uses the
        ``IN`` keyword for the arguments. See also :func:`strpos`.

.. function:: replace(string, search) -> varchar

    Removes all instances of ``search`` from ``string``.

.. function:: replace(string, search, replace) -> varchar
    :noindex:

    Replaces all instances of ``search`` with ``replace`` in ``string``.

.. function:: reverse(string) -> varchar

    Returns ``string`` with the characters in reverse order.

.. function:: rpad(string, size, padstring) -> varchar

    Right pads ``string`` to ``size`` characters with ``padstring``.
    If ``size`` is less than the length of ``string``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padstring`` must be non-empty.

.. function:: rtrim(string) -> varchar

    Removes trailing whitespace from ``string``.

.. function:: soundex(char) -> string

   ``soundex`` returns a character string containing the phonetic representation of ``char``.
    It is typically used to evaluate the similarity of two expressions phonetically, that is
    how the string sounds when spoken::

        SELECT name
        FROM nation
        WHERE SOUNDEX(name)  = SOUNDEX('CHYNA');

         name  |
        -------+----
         CHINA |
        (1 row)

.. function:: split(string, delimiter) -> array(varchar)

    Splits ``string`` on ``delimiter`` and returns an array.

.. function:: split(string, delimiter, limit) -> array(varchar)
    :noindex:

    Splits ``string`` on ``delimiter`` and returns an array of size at most
    ``limit``. The last element in the array always contain everything
    left in the ``string``. ``limit`` must be a positive number.

.. function:: split_part(string, delimiter, index) -> varchar

    Splits ``string`` on ``delimiter`` and returns the field ``index``.
    Field indexes start with ``1``. If the index is larger than
    the number of fields, then null is returned.

.. function:: split_to_map(string, entryDelimiter, keyValueDelimiter) -> map<varchar, varchar>

    Splits ``string`` by ``entryDelimiter`` and ``keyValueDelimiter`` and returns a map.
    ``entryDelimiter`` splits ``string`` into key-value pairs. ``keyValueDelimiter`` splits
    each pair into key and value.

.. function:: split_to_multimap(string, entryDelimiter, keyValueDelimiter) -> map(varchar, array(varchar))

    Splits ``string`` by ``entryDelimiter`` and ``keyValueDelimiter`` and returns a map
    containing an array of values for each unique key. ``entryDelimiter`` splits ``string``
    into key-value pairs. ``keyValueDelimiter`` splits each pair into key and value. The
    values for each key will be in the same order as they appeared in ``string``.

.. function:: strpos(string, substring) -> bigint

    Returns the starting position of the first instance of ``substring`` in
    ``string``. Positions start with ``1``. If not found, ``0`` is returned.

.. function:: strpos(string, substring, instance) -> bigint
    :noindex:

    Returns the position of the N-th ``instance`` of ``substring`` in ``string``.
    When ``instance`` is a negative number the search will start from the end of ``string``.
    Positions start with ``1``. If not found, ``0`` is returned.

.. function:: starts_with(string, substring) -> boolean

    Tests whether ``substring`` is a prefix of ``string``.

.. function:: substr(string, start) -> varchar

    This is an alias for :func:`substring`.

.. function:: substring(string, start) -> varchar

    Returns the rest of ``string`` from the starting position ``start``.
    Positions start with ``1``. A negative starting position is interpreted
    as being relative to the end of the string.

.. function:: substr(string, start, length) -> varchar
    :noindex:

    This is an alias for :func:`substring`.

.. function:: substring(string, start, length) -> varchar
    :noindex:

    Returns a substring from ``string`` of length ``length`` from the starting
    position ``start``. Positions start with ``1``. A negative starting
    position is interpreted as being relative to the end of the string.

.. function:: translate(source, from, to) -> varchar

   Returns the ``source`` string translated by replacing characters found in the
   ``from`` string with the corresponding characters in the ``to`` string.  If the ``from``
   string contains duplicates, only the first is used.  If the ``source`` character
   does not exist in the ``from`` string, the ``source`` character will be copied
   without translation.  If the index of the matching character in the ``from``
   string is beyond the length of the ``to`` string, the ``source`` character will
   be omitted from the resulting string.

   Here are some examples illustrating the translate function::

       SELECT translate('abcd', '', ''); -- 'abcd'
       SELECT translate('abcd', 'a', 'z'); -- 'zbcd'
       SELECT translate('abcda', 'a', 'z'); -- 'zbcdz'
       SELECT translate('Palhoça', 'ç','c'); -- 'Palhoca'
       SELECT translate('abcd', 'b', U&'\+01F600'); -- a😀cd
       SELECT translate('abcd', 'a', ''); -- 'bcd'
       SELECT translate('abcd', 'a', 'zy'); -- 'zbcd'
       SELECT translate('abcd', 'ac', 'z'); -- 'zbd'
       SELECT translate('abcd', 'aac', 'zq'); -- 'zbd'

.. function:: trim(string) -> varchar
    :noindex:

    Removes leading and trailing whitespace from ``string``.

.. function:: trim( [ [ specification ] [ string ] FROM ] source ) -> varchar

    Removes any leading and/or trailing characters as specified up to and
    including ``string`` from ``source``::

      SELECT trim('!' FROM '!foo!'); -- 'foo'
      SELECT trim(LEADING FROM '  abcd');  -- 'abcd'
      SELECT trim(BOTH '$' FROM '$var$'); -- 'var'
      SELECT trim(TRAILING 'ER' FROM upper('worker')); -- 'WORK'

.. function:: upper(string) -> varchar

    Converts ``string`` to uppercase.

.. function:: word_stem(word) -> varchar

    Returns the stem of ``word`` in the English language.

.. function:: word_stem(word, lang) -> varchar
    :noindex:

    Returns the stem of ``word`` in the ``lang`` language.

Unicode functions
-----------------

.. function:: normalize(string) -> varchar

    Transforms ``string`` with NFC normalization form.

.. function:: normalize(string, form) -> varchar
    :noindex:

    Transforms ``string`` with the specified normalization form.
    ``form`` must be one of the following keywords:

    ======== ===========
    Form     Description
    ======== ===========
    ``NFD``  Canonical Decomposition
    ``NFC``  Canonical Decomposition, followed by Canonical Composition
    ``NFKD`` Compatibility Decomposition
    ``NFKC`` Compatibility Decomposition, followed by Canonical Composition
    ======== ===========

    .. note::

        This SQL-standard function has special syntax and requires
        specifying ``form`` as a keyword, not as a string.

.. function:: to_utf8(string) -> varbinary

    Encodes ``string`` into a UTF-8 varbinary representation.

.. function:: from_utf8(binary) -> varchar

    Decodes a UTF-8 encoded string from ``binary``. Invalid UTF-8 sequences
    are replaced with the Unicode replacement character ``U+FFFD``.

.. function:: from_utf8(binary, replace) -> varchar
    :noindex:

    Decodes a UTF-8 encoded string from ``binary``. Invalid UTF-8 sequences
    are replaced with ``replace``. The replacement string ``replace`` must either
    be a single character or empty (in which case invalid characters are
    removed).
