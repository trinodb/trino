==============
UUID Functions
==============

.. function:: uuid() -> uuid

    Returns a pseudo randomly generated :ref:`uuid_type` (type 4).

.. function:: uuid_nil() -> uuid

    Returns the nil UUID constant :ref:`uuid_type`, which does not occur as a real UUID

.. function:: uuid_ns_dns() -> uuid

    Returns the DNS namespace constant :ref:`uuid_type` for UUIDs

.. function:: uuid_ns_url() -> uuid

    Returns the URL namespace constant :ref:`uuid_type` for UUIDs

.. function:: uuid_ns_oid() -> uuid

    Returns the ISO object identifier (OID) namespace constant :ref:`uuid_type` for UUIDs

.. function:: uuid_ns_x500() -> uuid

    Returns the X.500 distinguished name (DN) namespace constant :ref:`uuid_type` for UUIDs

.. function:: uuid_v3(nameSpaceUuid, name) -> uuid

    Generates a :ref:`uuid_type` (type 3) in the given namespace and specified input name.

.. function:: uuid_v5(nameSpaceUuid, name) -> uuid

    Generates a :ref:`uuid_type` (type 5) in the given namespace and specified input name.
