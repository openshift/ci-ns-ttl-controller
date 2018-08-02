# ci-ns-ttl-controller

This controller manages hard and soft TTLs for namespaces in order to stop resource leaks from test workloads. TTLs can be configured
for a namespace by annotating the namespace:

| TTL Type |       Annotation Key       |         Annotation Value         |                                                       Description                                                      |
| -------- | -------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Hard     | `ci.openshift.io/ttl.hard` | `time.Duration`-formatted string | The time after namespace creation when the namespace should be reaped.                                                 |
| Soft     | `ci.openshift.io/ttl.soft` | `time.Duration`-formatted string | The time after `Pod` termination where, if no other `Pod`s are `Pending` or `Running`, the namespace should be reaped. |

Whichever TTL is reached first will be used by the controller to delete the namespace.

The controller will ignore certain pods for the soft TTL calculations, allowing those pods to keep running while considering the
namespace to be inactive. The deployment configured for the OpenShift CI system will only consider pods matching the
`!ci.openshift.io/ttl.ignore` selector for soft TTL calculations.

## Design

This controller works in two parts. First is a controller that runs off of a `Namespace` informer and determines if the current
requested deletion time has passed and, if so, deletes the namespace. The second controller runs off of both `Namespace` and `Pod`
informers and updates the requested deletion time by reconciling the current state of requested TTLs and namespace activity.