# Production implementation TODOs

These concepts are deliberately **not implemented or required to run this project**. They are future work, not additional current acceptance requirements. The shipped applications use plain HTTP/gRPC, and the bundled PostgreSQL uses native trust mode without passwords. No certificates or Kubernetes Secrets are needed for a fresh project install.

| TODO | Future production design work |
| --- | --- |
| HTTPS | Choose gateway/ingress termination, protect external HTTP traffic, and define proxy/trust boundaries. |
| gRPC TLS and mTLS | Encrypt internal streams, verify server identity, and decide whether/how producers and collectors authenticate with client identities. |
| Database TLS | Select verified PostgreSQL transport, trusted CAs, endpoint verification, and driver/deployment configuration. |
| Authentication and authorization | Define identities and API/service access rules. Replace trust-mode database access with scoped roles/credentials. |
| Certificate management | Choose issuance, distribution, trust stores, renewal/rotation, expiry handling, and revocation processes. |
| Credential lifecycle | Choose Secret provisioning/rotation and deployment integration rather than baking credentials into code or images. |