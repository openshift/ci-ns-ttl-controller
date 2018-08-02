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
func NewTTLManager(informer coreinformers.NamespaceInformer, podLister corelisters.PodLister, client kubeclientset.Interface) *TTLManager {
	logger := logrus.WithField("controller", controllerName)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logger.Infof)
	eventBroadcaster.StartRecordingToSink(&coreclient.EventSinkImpl{Interface: coreclient.New(client.CoreV1().RESTClient()).Events("")})

	c := &TTLManager{
		client:          client,
		queue:           workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), controllerName),
		logger:          logger,
		namespaceLister: informer.Lister(),
		podLister:       podLister,
		synced:          informer.Informer().HasSynced,
	}

	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.add,
		UpdateFunc: c.update,
	})

	return c
}

// TTLManager manages deletion requests for namespaces.
type TTLManager struct {
	client kubeclientset.Interface

	namespaceLister corelisters.NamespaceLister
	podLister       corelisters.PodLister
	queue           workqueue.RateLimitingInterface
	synced          cache.InformerSynced

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

// Run runs c; will not return until stopCh is closed. workers determines how
// many clusters will be handled in parallel.
func (c *TTLManager) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Infof("starting %s controller", controllerName)
	defer c.logger.Infof("shutting down %s controller", controllerName)

	c.logger.Infof("Waiting for caches to reconcile for %s controller", controllerName)
	if !cache.WaitForCacheSync(stopCh, c.synced) {
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

	var deleteAt time.Time
	deleteAtString, deleteAtPresent := ns.ObjectMeta.Annotations[deleteAtAnnotation]
	if deleteAtPresent {
		logger = logger.WithField("delete-at", deleteAtString)
		deleteAt, err = time.Parse(deleteAtString, time.RFC3339)
		if err != nil {
			logger.WithError(err).Errorf("unable to parse delete-at annotation")
			return err
		}
	}

	var hardTtl time.Duration
	var hardDeleteAt time.Time
	hardTtlString, hardTtlPresent := ns.ObjectMeta.Annotations[hardTTLAnnotation]
	if hardTtlPresent {
		logger = logger.WithField("hard-ttl", hardTtlString)
		hardTtl, err = time.ParseDuration(hardTtlString)
		if err != nil {
			logger.WithError(err).Errorf("unable to parse hard TTL annotation")
			return err
		}

		hardDeleteAt = ns.ObjectMeta.CreationTimestamp.Add(hardTtl)
	}

	softTtlString, softTtlPresent := ns.ObjectMeta.Annotations[softTTLAnnotation]
	var softTtl time.Duration
	var softDeleteAt time.Time
	var active bool
	if softTtlPresent {
		logger = logger.WithField("soft-ttl", softTtlString)
		softTtl, err = time.ParseDuration(softTtlString)
		if err != nil {
			logger.WithError(err).Errorf("unable to parse hard TTL annotation")
			return err
		}
		selector := labels.NewSelector()
		requirement, err := labels.NewRequirement(ignorePodLabel, selection.DoesNotExist, []string{})
		if err != nil {
			logger.WithError(err).Error("unable to create requirements for pod selector")
		}
		selector.Add(*requirement)
		pods, err := c.podLister.Pods(name).List(selector)
		if err != nil {
			logger.WithError(err).Error("unable to list pods in namespace")
			return err
		}

		var lastTransitionTime time.Time
		active, lastTransitionTime = digestPods(pods)
		logger = logger.WithField("namespace-active", active)
		softDeleteAt = lastTransitionTime.Add(softTtl)
	}

	if hardTtlPresent {
		if deleteAtPresent && deleteAt.After(hardDeleteAt) {
			logger.Info("shortening delete-at to hard TTL")
			return c.updateDeleteAtAnnotation(ns, hardDeleteAt)
		}

		logger.Info("adding delete-at at hard TTL")
		return c.updateDeleteAtAnnotation(ns, hardDeleteAt)
	}

	if softTtlPresent {
		if active && deleteAtPresent {
			if hardTtlPresent && !deleteAt.Equal(hardDeleteAt) {
				logger.Info("lengthening delete-at to hard TTL")
				return c.updateDeleteAtAnnotation(ns, hardDeleteAt)
			} else if !hardTtlPresent {
				logger.Info("removing delete-at")
				return c.removeDeleteAtAnnotation(ns)
			}
		} else if !active {
			if deleteAtPresent && deleteAt.After(softDeleteAt) {
				logger.Info("shortening delete-at to soft TTL")
				return c.updateDeleteAtAnnotation(ns, softDeleteAt)
			} else if !deleteAtPresent {
				logger.Info("adding delete-at at soft TTL")
				return c.updateDeleteAtAnnotation(ns, softDeleteAt)
			}
		}
	}

	if deleteAtPresent && !(hardTtlPresent || softTtlPresent) {
		logger.Info("removing delete-at")
		return c.removeDeleteAtAnnotation(ns)
	}

	return nil
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
			return true, lastTransitionTime
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
