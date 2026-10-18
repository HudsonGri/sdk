# Objects v1 batch RPCs

`GrpcObjects::put_batch` and `get_batch` use the canonical `PutObjects` and
`GetObjects` streaming RPCs. Each nonempty batch admits 1–1000 ordered objects
once, authorizes every selection before work, and reports individual outcomes.
Batch transport uses gRPC; no HTTP batch provider binding is declared.

Bodies retain the single-object framing and allocation bounds. Uploads begin
with every object's header, followed by each object's indexed body chunks and
completion marker, then the batch completion marker and EOF. The complete
encoded header frame must fit the 16 MiB transport limit. Downloads identify
every selection and its decoded byte bound before the server starts work.
Their indexed headers, bodies and terminal outcomes retain input order.

Empty client batches return immediately without acquiring a credential or
opening an RPC. Retrying a publication batch uses the original per-object
mutation identities; cancellation can leave earlier items published. Each
object is atomic independently. A failed item does not change its peers'
ordered outcomes; malformed transport framing fails the batch.
