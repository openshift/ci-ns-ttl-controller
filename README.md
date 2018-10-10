# ci-ns-ttl-controller

This tool implements a Kubernetes controller that manages hard and soft times-to-live (TTLs) for namespaces in order to ensure that
namespaces do not persist in order stop resource leaks from test workloads. TTLs can be configured for a namespace by annotating
the namespace either at creation time or at any later point:

| TTL Type |       Annotation Key       |         Annotation Value         |                                                       Description                                                      |
| -------- | -------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Hard     | `ci.openshift.io/ttl.hard` | `time.Duration`-formatted string | The time after namespace creation when the namespace should be reaped.                                                 |
| Soft     | `ci.openshift.io/ttl.soft` | `time.Duration`-formatted string | The time after `Pod` termination or last active marker where, if no other `Pod`s are `Pending` or `Running`, the namespace should be reaped. The last active marker is optional and stored as a RFC3339-formatted date in the `ci.openshift.io/active` annotation. |

Whichever TTL is reached first will be used by the controller to delete the namespace.

The controller will only consider pods matching the `!ci.openshift.io/ttl.ignore` selector for soft TTL calculations, allowing
non-matching pods to keep running while considering the namespace to be inactive.

## Deployment

Deployment of these components requires `system:admin` level control, as it requires creating wide-reaching `ClusterRole`s and
`ClusterRoleBinding`s. To deploy, run:

```
make deploy
```