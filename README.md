# KSched

## What?

KSched is a framework for building horizontally scallable Kubernetes Operators using the controller-runtime.

## Why?

KSched is meant for circumstances where reconciling a custom resource is time-consuming or expected to run indefinitely, easily paralleizable, but wasteful to create a new pod or container for every unit of work. 

To solve this, an architecture is used where one controller runs in a typical leader-election fashion with a single leader, and a second controller runs multiple replicas with unique identities. The first controller, called the "supervisor" has the role of assigning work to the replicas of the second controller, called an "executor".

To do this, the supervisor maintains a list of active executors and their statuses, and uses this information when a new resource is received or otherwise becomes ready for work. The custom resource is expected to have some field which indicates which executor it is assigned to, if any, and executors are each expected to have a unique identifier to use for this field. The scheduler is able to use various algorithms to perform this assignment, such as random, round-robin, least units of work, or based on some set of metrics, such as cpu and memory. The scheduler is also responsible for identifying items which have been assigned to removed executors, and either failing or re-assigning them to remaining executors.

In this way, ksched is directly inspired by the operation and interaction of the kube-scheduler and kubelet components, allowing the same pattern to be extended to other custom resources as well.

To support further scalability, resources can be partitioned by their UID, with each partition having its own supervisor and pool of executors. This faciliates adding more resources in the case where adding more resources to the supervisor on a single server is no longer feasible.

Finally, a common pattern is to archive completed objects into a more suitable long-term storage to reduce load on etcd. KSched provides utilites to simplify the process of doing so.

## How?

KSched provides:

* The `Supervisor` type, which provides the functionality needed to implement a supervisor, as described above, using the controller-runtime. It is expected to be used to implement the `Reconcile()` and `SetupWithManager` functions generated by Kubebuilder and the Operator SDK.
* A set of default scheduling algorithms, `Random`, `RoundRobin`, `MinScheduled`, and `MinMetricScore`, to be used by `Supervisor` (users are free to implement their own, however).
* The `Executor` type, which provides acts as an adaptor for the controller-runtime `Reconciler` class to implement the pattern described above.
* Functions to add flags using `spf13/pflag` which provide the required information for supervisors and executors.
* An `Archiver` interface, along with implementations for using a filesystem, S3-compatible storage, SQL database, or MongoDB database, which can be plugged into a `Supervisor` to archive completed objects after a time-to-live.
