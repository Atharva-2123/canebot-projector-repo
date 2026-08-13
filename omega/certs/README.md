# Device certificates

Put the three files issued by the Golain console here:

```
ca.crt        broker CA
device.crt    device certificate
device.key    device private key  (chmod 600)
```

Get them from **Fleet & Devices → your device → MQTT & Certs → Issue certificate**.

The key algorithm decides the transport:

| Algorithm | Broker URL | Port |
|---|---|---|
| ECDSA | `quic://` | 8884 |
| RSA | `ssl://` | 8883 |

**These are secrets — they are gitignored and must not be committed.**
