====================
IP Address Functions
====================


.. function:: contains(network, address) -> boolean
    :noindex:

    Returns true if the ``address`` exists in the CIDR ``network``::

        SELECT contains('10.0.0.0/8', IPADDRESS '10.255.255.255'); -- true
        SELECT contains('10.0.0.0/8', IPADDRESS '11.255.255.255'); -- false
