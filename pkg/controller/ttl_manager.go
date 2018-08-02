package controller

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	coreapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const (
	// hardTTLAnnotation and softTTLAnnotations are the annotation keys for
	// the requested TTL durations, presented in time.Duration-formatted strings
	hardTTLAnnotation = "ci.openshift.io/ttl.hard"
	softTTLAnnotation = "ci.openshift.io/ttl.soft"

	// ignorePodLabel is negated to create the label selector for pods that we
	// will consider when determining if a namespace is active
	ignorePodLabel = "ci.openshift.io/ttl.ignore"
)

// NewTTLManager returns a new *TTLManager to generate deletion requests.
func NewTTLManager(informer coreinformers.NamespaceInformer, podInformer coreinformers.PodInformer, client kubeclientset.Interface) *TTLManager {
	logger := logrus.WithField("controller", controllerName)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logger.Infof)
	eventBroadcaster.StartRecordingToSink(&coreclient.EventSinkImpl{Interface: coreclient.New(client.CoreV1().RESTClient()).Events("")})

	c := &TTLManager{
		client:          client,
		queue:           workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), controllerName),
		logger:          logger,
		namespaceLister: informer.Lister(),
		podLister:       podInformer.Lister(),
		synced:          []cache.InformerSynced{informer.Informer().HasSynced, podInformer.Informer().HasSynced},
	}

	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.add,
		UpdateFunc: c.update,
	})
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addPod,
		UpdateFunc: c.updatePod,
		DeleteFunc: c.deletePod,
	})

	return c
}

// TTLManager manages deletion requests for namespaces.
type TTLManager struct {
	client kubeclientset.Interface

	namespaceLister corelisters.NamespaceLister
	podLister       corelisters.PodLister
	queue           workqueue.RateLimitingInterface
	synced          []cache.InformerSynced

	logger *logrus.Entry
}

func (c *TTLManager) add(obj interface{}) {
	ns := obj.(*coreapi.Namespace)
	c.logger.Debugf("enqueueing added ns %s/%s", ns.GetNamespace(), ns.GetName())
	c.enqueue(ns)
}

func (c *TTLManager) update(old, obj interface{}) {
	ns := obj.(*coreapi.Namespace)
	c.logger.Debugf("enqueueing updated ns %s/%s", ns.GetNamespace(), ns.GetName())
	c.enqueue(ns)
}

func (c *TTLManager) addPod(obj interface{}) {
	pod := obj.(*coreapi.Pod)
	c.logger.Debugf("enqueueing namespace for added pod %s/%s", pod.GetNamespace(), pod.GetName())
	c.enqueuePod(pod)
}

func (c *TTLManager) updatePod(old, obj interface{}) {
	pod := obj.(*coreapi.Pod)
	c.logger.Debugf("enqueueing namespace for updated pod %s/%s", pod.GetNamespace(), pod.GetName())
	c.enqueuePod(pod)
}

func (c *TTLManager) deletePod(obj interface{}) {
	pod, ok := obj.(*coreapi.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*coreapi.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not an Object %#v", obj))
			return
		}
	}
	c.logger.Debugf("enqueueing namespace for deleted pod %s/%s", pod.GetNamespace(), pod.GetName())
	c.enqueuePod(pod)
}

func (c *TTLManager) enqueuePod(pod *coreapi.Pod) {
	ns, err := c.namespaceLister.Get(pod.ObjectMeta.Namespace)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get namespace for pod %#v: %v", pod, err))
		return
	}
	c.enqueue(ns)
}

// Run runs c; will not return until stopCh is closed. workers determines how
// many clusters will be handled in parallel.
func (c *TTLManager) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Infof("starting %s controller", controllerName)
	defer c.logger.Infof("shutting down %s controller", controllerName)

	c.logger.Infof("Waiting for caches to reconcile for %s controller", controllerName)
	if !cache.WaitForCacheSync(stopCh, c.synced...) {
		utilruntime.HandleError(fmt.Errorf("unable to reconcile caches for %s controller", controllerName))
	}
	c.logger.Infof("Caches are synced for %s controller", controllerName)

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *TTLManager) enqueue(ns metav1.Object) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(ns)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", ns, err))
		return
	}

	c.queue.Add(key)
}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
func (c *TTLManager) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *TTLManager) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.reconcile(key.(string))
	c.handleErr(err, key)

	return true
}

func (c *TTLManager) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	logger := c.logger.WithField("namespace", key)

	logger.Errorf("error syncing namespace: %v", err)
	if c.queue.NumRequeues(key) < maxRetries {
		logger.Errorf("retrying namespace")
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logger.Infof("dropping namespace out of the queue: %v", err)
	c.queue.Forget(key)
}

// reconcile handles the business logic of ensuring that namespaces
// are reaped when they are past their hard or soft TTLs
func (c *TTLManager) reconcile(key string) error {
	logger := c.logger.WithField("namespace", key)
	logger.Infof("reconciling namespace")
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	ns, err := c.namespaceLister.Get(name)
	if errors.IsNotFound(err) {
		logger.Info("not doing work for namespace because it has been deleted")
		return nil
	}
	if err != nil {
		logger.WithError(err).Errorf("unable to retrieve namespace from store")
		return err
	}

	selector := labels.NewSelector()
	requirement, err := labels.NewRequirement(ignorePodLabel, selection.DoesNotExist, []string{})
	if err != nil {
		logger.WithError(err).Error("unable to create requirements for pod selector")
		return err
	}
	selector.Add(*requirement)

	processPods := func() (bool, time.Time, error) {
		pods, err := c.podLister.Pods(name).List(selector)
		if err != nil {
			logger.WithError(err).Error("unable to list pods in namespace")
			return false, time.Time{}, err
		}
		active, lastTransition := digestPods(pods)
		if len(pods) == 0 {
			lastTransition = time.Now()
		}
		return active, lastTransition, nil
	}

	status, err := resolveTtlStatus(ns, processPods, logger)
	if err != nil {
		return err
	}

	shouldUpdate, shouldRemove, requiredDeleteAt := determineDeleteAt(status)
	if shouldRemove {
		return c.removeDeleteAtAnnotation(ns)
	}
	if shouldUpdate {
		status.logger.WithField("requested-delete-at", requiredDeleteAt.Format(time.RFC3339)).Info("updating delete-at")
		return c.updateDeleteAtAnnotation(ns, requiredDeleteAt)
	}
	return nil
}

// ttlStatus wraps digested information for a namesapce for ease of processing
type ttlStatus struct {
	hardTtlPresent, softTtlPresent, deleteAtPresent, active bool
	hardDeleteAt, softDeleteAt, deleteAt                    time.Time
	logger                                                  *logrus.Entry
}

// resolveTtlStatus digests the Namespace and potentially the Pods in the Namespace
// to determine the requested TTLs and current delete-at status, as well as if the
// Namespace is active if a soft TTL is requested
func resolveTtlStatus(ns *coreapi.Namespace, processPods func() (bool, time.Time, error), logger *logrus.Entry) (ttlStatus, error) {
	status := ttlStatus{
		logger: logger,
	}
	deleteAtString, deleteAtPresent := ns.ObjectMeta.Annotations[deleteAtAnnotation]
	status.deleteAtPresent = deleteAtPresent
	if deleteAtPresent {
		status.logger = status.logger.WithField("delete-at", deleteAtString)
		deleteAt, err := time.Parse(time.RFC3339, deleteAtString)
		if err != nil {
			status.logger.WithError(err).Errorf("unable to parse delete-at annotation")
			return status, err
		}
		status.deleteAt = deleteAt
	}

	hardTtlString, hardTtlPresent := ns.ObjectMeta.Annotations[hardTTLAnnotation]
	status.hardTtlPresent = hardTtlPresent
	if hardTtlPresent {
		status.logger = status.logger.WithField("hard-ttl", hardTtlString)
		hardTtl, err := time.ParseDuration(hardTtlString)
		if err != nil {
			status.logger.WithError(err).Errorf("unable to parse hard TTL annotation")
			return status, err
		}

		status.hardDeleteAt = ns.ObjectMeta.CreationTimestamp.Add(hardTtl)
	}

	softTtlString, softTtlPresent := ns.ObjectMeta.Annotations[softTTLAnnotation]
	status.softTtlPresent = softTtlPresent
	if softTtlPresent {
		status.logger = status.logger.WithField("soft-ttl", softTtlString)
		softTtl, err := time.ParseDuration(softTtlString)
		if err != nil {
			status.logger.WithError(err).Errorf("unable to parse hard TTL annotation")
			return status, err
		}

		active, lastTransitionTime, err := processPods()
		if err != nil {
			status.logger.WithError(err).Errorf("unable to process Pods")
			return status, err
		}
		status.active = active
		status.logger = status.logger.WithField("active", active)
		if !lastTransitionTime.IsZero() {
			status.softDeleteAt = lastTransitionTime.Add(softTtl)
		}
	}

	return status, nil
}

// determineDeleteAt holds the logic for the reconciliation loop by digesting
// the current TTL status and determining what the appropriate delete-at annotation
// should be after this reconciliation loop. We return whether an update should
// occur, whether that update should be a removal of the annotation, or what the
// anontation should be updated to
func determineDeleteAt(status ttlStatus) (bool, bool, time.Time) {
	if status.hardTtlPresent {
		if status.deleteAtPresent {
			if status.deleteAt.After(status.hardDeleteAt) {
				status.logger.Info("shortening delete-at to hard TTL")
				return true, false, status.hardDeleteAt
			}
		} else {
			status.logger.Info("adding delete-at at hard TTL")
			return true, false, status.hardDeleteAt
		}
	}

	if status.softTtlPresent {
		if status.active && status.deleteAtPresent {
			if status.hardTtlPresent && !status.deleteAt.Equal(status.hardDeleteAt) {
				status.logger.Info("lengthening delete-at to hard TTL")
				return true, false, status.hardDeleteAt
			} else if !status.hardTtlPresent {
				status.logger.Info("removing delete-at")
				return false, true, time.Time{}
			}
		} else if !status.active {
			if status.deleteAtPresent {
				if status.deleteAt.After(status.softDeleteAt) {
					status.logger.Info("shortening delete-at to soft TTL")
					return true, false, status.softDeleteAt
				} else if !status.hardTtlPresent || !status.deleteAt.Equal(status.hardDeleteAt) {
					// last delete-at was added as a soft TTL but we missed a sync
					// when the namespace was active, but we need to recover now
					status.logger.Info("lengthening delete-at to soft TTL")
					return true, false, status.softDeleteAt
				}
			} else if !status.deleteAtPresent {
				status.logger.Info("adding delete-at at soft TTL")
				return true, false, status.softDeleteAt
			}
		}
	}

	if status.deleteAtPresent && !(status.hardTtlPresent || status.softTtlPresent) {
		status.logger.Info("removing delete-at")
		return false, true, time.Time{}
	}

	return false, false, time.Time{}
}

func (c *TTLManager) removeDeleteAtAnnotation(ns *coreapi.Namespace) error {
	update := ns.DeepCopy()
	delete(update.ObjectMeta.Annotations, deleteAtAnnotation)
	_, err := c.client.CoreV1().Namespaces().Update(update)
	return err
}

func (c *TTLManager) updateDeleteAtAnnotation(ns *coreapi.Namespace, deleteAt time.Time) error {
	update := ns.DeepCopy()
	update.ObjectMeta.Annotations[deleteAtAnnotation] = deleteAt.Format(time.RFC3339)
	_, err := c.client.CoreV1().Namespaces().Update(update)
	return err
}

// digestPods determines if there are active pods in the list given,
// and if not, what the latest termination transition time was.
func digestPods(pods []*coreapi.Pod) (bool, time.Time) {
	var lastTransitionTime time.Time
	for _, pod := range pods {
		if pod.Status.Phase == coreapi.PodPending || pod.Status.Phase == coreapi.PodRunning {
			return true, time.Time{}
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Terminated != nil {
				terminationTime := status.State.Terminated.FinishedAt.Time
				if terminationTime.After(lastTransitionTime) {
					lastTransitionTime = terminationTime
				}
			}
		}
	}
	return false, lastTransitionTime
}
