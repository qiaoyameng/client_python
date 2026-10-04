---
title: HTTP/HTTPS
weight: 1
---

# HTTP

Metrics are usually exposed over HTTP, to be read by the Prometheus server.

The easiest way to do this is via `start_http_server`, which will start a HTTP
server in a daemon thread on the given port:

```python
from prometheus_client import start_http_server

start_http_server(8000)
```

Visit [http://localhost:8000/](http://localhost:8000/) to view the metrics.

The function will return the HTTP server and thread objects, which can be used
to shutdown the server gracefully:

```python
server, t = start_http_server(8000)
server.shutdown()
server.server_close()
t.join()
```

To add Prometheus exposition to an existing HTTP server, see the `MetricsHandler` class
which provides a `BaseHTTPRequestHandler`. It also serves as a simple example of how
to write a custom endpoint.

# HTTPS

By default, the prometheus client will accept only HTTP requests from Prometheus.
To enable HTTPS, `certfile` and `keyfile` need to be provided. The certificate is
presented to Prometheus as a server certificate during the TLS handshake, and
the private key in the key file must belong to the public key in the certificate.

When HTTPS is enabled, you can enable mutual TLS (mTLS) by setting `client_auth_required=True`
(i.e. Prometheus is required to present a client certificate during TLS handshake) and the
client certificate including its hostname is validated against the CA certificate chain.

`client_cafile` can be used to specify a certificate file containing a CA certificate
chain that is used to validate the client certificate. `client_capath` can be used to
specify a certificate directory containing a CA certificate chain that is used to
validate the client certificate. If neither of them is provided, a default CA certificate
chain is used (see Python [ssl.SSLContext.load_default_certs()](https://docs.python.org/3/library/ssl.html#ssl.SSLContext.load_default_certs))

```python
from prometheus_client import start_http_server

start_http_server(8000, certfile="server.crt", keyfile="server.key")
```

# Supported HTTP methods

The prometheus client will handle the following HTTP methods and resources:

* `OPTIONS (any)` - returns HTTP status 200 and an 'Allow' header indicating the
  allowed methods (OPTIONS, GET)
* `GET (any)` - returns HTTP status 200 and the metrics data
* `GET /favicon.ico` - returns HTTP status 200 and an empty response body. Some
  browsers support this to display the returned icon in the browser tab.

Other HTTP methods than these are rejected with HTTP status 405 "Method Not Allowed"
and an 'Allow' header indicating the allowed methods (OPTIONS, GET).

Any returned HTTP errors are also displayed in the response body after a hash
sign and with a brief hint. Example:
```
# HTTP 405 Method Not Allowed: XXX; use OPTIONS or GET
```

# Exposition formats

The exposition format is selected by content negotiation on the `Accept`
header. The Prometheus text format (`text/plain; version=0.0.4`) is the
compatible default. The OpenMetrics text format and the Prometheus protobuf
format (`application/vnd.google.protobuf;
proto=io.prometheus.client.MetricFamily; encoding=delimited`) are used when
requested. A media type listed with `q=0` is never selected.

The protobuf format covers counters, gauges, summaries, classic histograms
and untyped metrics, preserving labels, timestamps and exemplars. Info,
stateset and gaugehistogram metrics are degraded to their text-format
equivalents (gauge/histogram), and native histograms are rejected. It can
also be produced directly:

```python
from prometheus_client import protobuf

output = protobuf.generate_latest()
```
