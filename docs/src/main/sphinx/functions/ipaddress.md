# IP Address Functions

(ip-address-contains)=
:::{function} contains(network, address) -> boolean
:noindex: true

Returns true if the `address` exists in the CIDR `network`:

```{try-sql}
SELECT contains('10.0.0.0/8', IPADDRESS '10.255.255.255'),
       contains('10.0.0.0/8', IPADDRESS '11.255.255.255'),
       contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8329'),
       contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8328')
```
:::
