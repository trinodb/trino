==============================
Binary functions and operators
==============================

Binary operators
----------------

The ``||`` operator performs concatenation.

Binary functions
----------------

.. function:: concat(binary1, ..., binaryN) -> varbinary
    :noindex:

    Returns the concatenation of ``binary1``, ``binary2``, ``...``, ``binaryN``.
    This function provides the same functionality as the
    SQL-standard concatenation operator (``||``).

.. function:: length(binary) -> bigint
    :noindex:

    Returns the length of ``binary`` in bytes.

.. function:: lpad(binary, size, padbinary) -> varbinary
    :noindex:

    Left pads ``binary`` to ``size`` bytes with ``padbinary``.
    If ``size`` is less than the length of ``binary``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padbinary`` must be non-empty.

.. function:: rpad(binary, size, padbinary) -> varbinary
    :noindex:

    Right pads ``binary`` to ``size`` bytes with ``padbinary``.
    If ``size`` is less than the length of ``binary``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padbinary`` must be non-empty.

.. function:: substr(binary, start) -> varbinary
    :noindex:

    Returns the rest of ``binary`` from the starting position ``start``,
    measured in bytes. Positions start with ``1``. A negative starting position
    is interpreted as being relative to the end of the string.

.. function:: substr(binary, start, length) -> varbinary
    :noindex:

    Returns a substring from ``binary`` of length ``length`` from the starting
    position ``start``, measured in bytes. Positions start with ``1``. A
    negative starting position is interpreted as being relative to the end of
    the string.

.. _function-reverse-varbinary:

.. function:: reverse(binary) -> varbinary
    :noindex:

    Returns ``binary`` with the bytes in reverse order.

Base64 encoding functions
-------------------------

The Base64 functions implement the encoding specified in :rfc:`4648`.

.. function:: from_base64(string) -> varbinary

    Decodes binary data from the base64 encoded ``string``.

.. function:: to_base64(binary) -> varchar

    Encodes ``binary`` into a base64 string representation.

.. function:: from_base64url(string) -> varbinary

    Decodes binary data from the base64 encoded ``string`` using the URL safe alphabet.

.. function:: to_base64url(binary) -> varchar

    Encodes ``binary`` into a base64 string representation using the URL safe alphabet.

.. function:: from_base32(string) -> varbinary

    Decodes binary data from the base32 encoded ``string``.

.. function:: to_base32(binary) -> varchar

    Encodes ``binary`` into a base32 string representation.

Hex encoding functions
----------------------

.. function:: from_hex(string) -> varbinary

    Decodes binary data from the hex encoded ``string``.

.. function:: to_hex(binary) -> varchar

    Encodes ``binary`` into a hex string representation.

Integer encoding functions
--------------------------

.. function:: from_big_endian_32(binary) -> integer

    Decodes the 32-bit two's complement big-endian ``binary``.
    The input must be exactly 4 bytes.

.. function:: to_big_endian_32(integer) -> varbinary

    Encodes ``integer`` into a 32-bit two's complement big-endian format.

.. function:: from_big_endian_64(binary) -> bigint

    Decodes the 64-bit two's complement big-endian ``binary``.
    The input must be exactly 8 bytes.

.. function:: to_big_endian_64(bigint) -> varbinary

    Encodes ``bigint`` into a 64-bit two's complement big-endian format.

Floating-point encoding functions
---------------------------------

.. function:: from_ieee754_32(binary) -> real

    Decodes the 32-bit big-endian ``binary`` in IEEE 754 single-precision floating-point format.
    The input must be exactly 4 bytes.

.. function:: to_ieee754_32(real) -> varbinary

    Encodes ``real`` into a 32-bit big-endian binary according to IEEE 754 single-precision floating-point format.

.. function:: from_ieee754_64(binary) -> double

    Decodes the 64-bit big-endian ``binary`` in IEEE 754 double-precision floating-point format.
    The input must be exactly 8 bytes.

.. function:: to_ieee754_64(double) -> varbinary

    Encodes ``double`` into a 64-bit big-endian binary according to IEEE 754 double-precision floating-point format.

Hashing functions
-----------------

.. function:: crc32(binary) -> bigint

    Computes the CRC-32 of ``binary``. For general purpose hashing, use
    :func:`xxhash64`, as it is much faster and produces a better quality hash.

.. function:: md5(binary) -> varbinary

    Computes the MD5 hash of ``binary``.

.. function:: sha1(binary) -> varbinary

    Computes the SHA1 hash of ``binary``.

.. function:: sha256(binary) -> varbinary

    Computes the SHA256 hash of ``binary``.

.. function:: sha512(binary) -> varbinary

    Computes the SHA512 hash of ``binary``.

.. function:: spooky_hash_v2_32(binary) -> varbinary

    Computes the 32-bit SpookyHashV2 hash of ``binary``.

.. function:: spooky_hash_v2_64(binary) -> varbinary

    Computes the 64-bit SpookyHashV2 hash of ``binary``.

.. function:: xxhash64(binary) -> varbinary

    Computes the xxHash64 hash of ``binary``.

.. function:: murmur3(binary) -> varbinary

    Computes the 128-bit `MurmurHash3 <https://wikipedia.org/wiki/MurmurHash>`_
    hash of ``binary``.

        SELECT murmur3(from_base64('aaaaaa'));
        -- ba 58 55 63 55 69 b4 2f 49 20 37 2c a0 e3 96 ef

HMAC functions
--------------

.. function:: hmac_md5(binary, key) -> varbinary

    Computes HMAC with MD5 of ``binary`` with the given ``key``.

.. function:: hmac_sha1(binary, key) -> varbinary

    Computes HMAC with SHA1 of ``binary`` with the given ``key``.

.. function:: hmac_sha256(binary, key) -> varbinary

    Computes HMAC with SHA256 of ``binary`` with the given ``key``.

.. function:: hmac_sha512(binary, key) -> varbinary

    Computes HMAC with SHA512 of ``binary`` with the given ``key``.
