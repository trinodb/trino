# IP Address Functions

(ip-address-contains)=
:::{function} contains(network, address) -> boolean
:noindex: true

Returns true if the `address` exists in the CIDR `network`:

```
SELECT contains('10.0.0.0/8', IPADDRESS '10.255.255.255'); -- true
SELECT contains('10.0.0.0/8', IPADDRESS '11.255.255.255'); -- false

SELECT contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8329'); -- true
SELECT contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8328'); -- false
```
:::

:::{function} ip_version(ipaddress) -> integer
Returns `4` for IPv4 and `6` for IPv6.
```sql
SELECT ip_version(IPADDRESS '192.172.1.20');
-- 4
```
:::

:::{function} ip_from_bits(varchar) -> ipaddress
Convert a 32-bit binary IPv4 address to an IPADDRESS.
```sql
SELECT ip_from_bits('11000000101011000000000100010100');
-- 192.172.1.20
```

Or convert a 128-bit binary IPv6 address to an IPADDRESS.
```sql
SELECT ip_from_bits(concat(
    '01001111111111100010100100000000',
    '01010101010001010011001000010000',
    '00100000000000001111100011111111',
    '11111110001000010110011111001111'));
-- 4ffe:2900:5545:3210:2000:f8ff:fe21:67cf
```
:::

:::{function} ip_to_bits(ipaddress) -> varchar
Convert an IPv4 address to its 32-bit representation
```sql
SELECT ip_to_bits(IPADDRESS '192.172.1.20');
-- 11000000101011000000000100010100
```

Or convert an IPv6 address to its 128-bit representation
```sql
SELECT ip_to_bits(IPADDRESS '4ffe:2900:5545:3210:2000:f8ff:fe21:67cf');
-- 0100111111111110...0000000000111110001111111111111110001000010110011111001111
```
:::

:::{function} ip_to_bits(ipaddress, integer) -> varchar
:noindex: true
Convert an IPv4 address to its 32-bit representation or an IPv6 address to its
128-bit representation, keeping the first subnet bits but zeroes for the 
remaining bits.
```sql
SELECT ip_to_bits(IPADDRESS '255.255.255.255', 16);
-- 11111111111111110000000000000000
```

```sql
SELECT ip_to_bits(IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 112);
-- 1111111111111111...1111111111111111111111111111111111111111110000000000000000
```
:::
