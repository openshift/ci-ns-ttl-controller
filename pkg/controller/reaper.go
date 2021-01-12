package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	coreapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// maxRetries is the number of times a service will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the
	// sequence of delays between successive queuings of a service.
	//
	// 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.3s, 2.6s, 5.1s, 10.2s, 20.4s, 41s, 82s
	maxRetries = 15

	controllerName = "namespace-ttl-reaper"

	// deleteAtAnnotation is the annotation key for the requested deletion timestamp in RFC3339 format
	deleteAtAnnotation = "ci.openshift.io/delete-at"
)

// NewReaper returns a new *Reaper to reap namespaces.
func NewReaper(informer coreinformers.NamespaceInformer, client kubeclientset.Interface, enableExtremelyVerboseLogging bool) *Reaper {
	logger := logrus.WithField("controller", controllerName)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logger.Infof)
	eventBroadcaster.StartRecordingToSink(&coreclient.EventSinkImpl{Interface: coreclient.New(client.CoreV1().RESTClient()).Events("")})

	c := &Reaper{
		client:                        client,
		queue:                         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), controllerName),
		logger:                        logger,
		lister:                        informer.Lister(),
		synced:                        informer.Informer().HasSynced,
		enableExtremelyVerboseLogging: enableExtremelyVerboseLogging,
	}

	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.add,
		UpdateFunc: c.update,
	})

	return c
}

// Reaper manages reaping namespaces.
type Reaper struct {
	client kubeclientset.Interface

	lister                        corelisters.NamespaceLister
	queue                         workqueue.RateLimitingInterface
	synced                        cache.InformerSynced
	enableExtremelyVerboseLogging bool

	logger *logrus.Entry
}

func (c *Reaper) add(obj interface{}) {
	ns := obj.(*coreapi.Namespace)
	if c.enableExtremelyVerboseLogging {
		c.logger.Debugf("enqueueing added ns %s/%s", ns.GetNamespace(), ns.GetName())
	}
	c.enqueue(ns)
}

func (c *Reaper) update(old, obj interface{}) {
	ns := obj.(*coreapi.Namespace)
	if c.enableExtremelyVerboseLogging {
		c.logger.Debugf("enqueueing updated ns %s/%s", ns.GetNamespace(), ns.GetName())
	}
	c.enqueue(ns)
}

// Run runs c; will not return until stopCh is closed. workers determines how
// many clusters will be handled in parallel.
func (c *Reaper) Run(workers int, stopCh <-chan struct{}) {
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

func (c *Reaper) enqueue(ns metav1.Object) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(ns)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", ns, err))
		return
	}

	c.queue.Add(key)
}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
func (c *Reaper) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *Reaper) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.reconcile(key.(string))
	c.handleErr(err, key)

	return true
}

func (c *Reaper) handleErr(err error, key interface{}) {
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
func (c *Reaper) reconcile(key string) error {
	logger := c.logger.WithField("namespace", key)
	logger.Infof("reconciling namespace")
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	ns, err := c.lister.Get(name)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		logger.WithError(err).Errorf("unable to retrieve namespace from store")
		return err
	}
	if !ns.ObjectMeta.DeletionTimestamp.IsZero() {
		return nil
	}

	if deleteAtString, present := ns.ObjectMeta.Annotations[deleteAtAnnotation]; present {
		deleteAt, err := time.Parse(time.RFC3339, deleteAtString)
		if err != nil {
			logger.WithError(err).Errorf("unable to parse delete-at annotation")
			return nil // retrying this won't help until we see a new update
		}

		if time.Now().After(deleteAt) {
			logger.Info("namespace is past it's TTL, deleting")
			if time.Now().Sub(ns.CreationTimestamp.Time) < time.Hour {
				logger.Error("BUG: Would delete namespace that is less than an hour old")
				return nil
			}
			return c.client.CoreV1().Namespaces().Delete(context.TODO(), name, metav1.DeleteOptions{})
		} else {
			// fuzz the retry into the future so we don't have weird time collisions
			retryAt := deleteAt.Add(10 * time.Second)
			logger.Infof("queuing for future reconciliation at %s", retryAt.Format(time.RFC3339))
			c.queue.AddAfter(key, retryAt.Sub(time.Now()))
		}
	}

	return nil
}
