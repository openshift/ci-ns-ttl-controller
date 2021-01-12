package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"gopkg.in/fsnotify/fsnotify.v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/test-infra/prow/logrusutil"

	"github.com/openshift/ci-ns-ttl-controller/pkg/controller"
)

const (
	resync = 5 * time.Minute
)

type options struct {
	configLocation                string
	numWorkers                    int
	logLevel                      string
	enableExtremelyVerboseLogging bool
}

func main() {
	logrusutil.Init(&logrusutil.DefaultFieldsFormatter{PrintLineNumber: true, DefaultFields: logrus.Fields{"component": "namespace-ttl-controller"}})
	o := options{}
	flag.IntVar(&o.numWorkers, "num-workers", 10, "Number of worker threads.")
	flag.StringVar(&o.logLevel, "log-level", logrus.DebugLevel.String(), "Logging level.")
	flag.BoolVar(&o.enableExtremelyVerboseLogging, "enable-extremely-verbose-logging", false, "If enabled, log each and every pod or namespace event received. Warning: This creates a huge amount of logs.")
	flag.Parse()

	level, err := logrus.ParseLevel(o.logLevel)
	if err != nil {
		logrus.WithError(err).Fatal("failed to parse log level")
	}
	logrus.SetLevel(level)

	clusterConfig, err := loadClusterConfig()
	if err != nil {
		logrus.WithError(err).Fatal("failed to load cluster config")
	}

	client, err := kubernetes.NewForConfig(clusterConfig)
	if err != nil {
		logrus.WithError(err).Fatal("failed to initialize kubernetes client")
	}

	nsInformerFactory := informers.NewSharedInformerFactory(client, resync)

	nsReaper := controller.NewReaper(nsInformerFactory.Core().V1().Namespaces(), client, o.enableExtremelyVerboseLogging)
	nsTtlManager := controller.NewTTLManager(nsInformerFactory.Core().V1().Namespaces(), nsInformerFactory.Core().V1().Pods(), client, o.enableExtremelyVerboseLogging)
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()
	defer close(stop)
	go nsInformerFactory.Start(stop)
	go nsReaper.Run(o.numWorkers, stop)
	go nsTtlManager.Run(o.numWorkers, stop)

	// Wait forever
	select {}
}

// loadClusterConfig loads connection configuration
// for the cluster we're deploying to. We prefer to
// use in-cluster configuration if possible, but will
// fall back to using default rules otherwise.
func loadClusterConfig() (*rest.Config, error) {
	clusterConfig, err := rest.InClusterConfig()
	if err == nil {
		go func() {
			watcher, err := fsnotify.NewWatcher()
			if err != nil {
				logrus.WithError(err).Error("failed to create fsnotify watcher for kubeconfig")
				return
			}
			if err := watcher.Add("/var/run/secrets/kubernetes.io/serviceaccount/token"); err != nil {
				logrus.WithError(err).Error("failed to add serviceaccount token to fsnotify watcher")
				return
			}
			for event := range watcher.Events {
				if event.Op == fsnotify.Chmod {
					// For some reason we get frequent chmod events
					continue
				}
				logrus.WithField("event", event.String()).Info("Token changed, exiting controller to reload it")
				os.Exit(0)
			}
		}()
		return clusterConfig, nil
	}

	credentials, err := clientcmd.NewDefaultClientConfigLoadingRules().Load()
	if err != nil {
		return nil, fmt.Errorf("could not load credentials from config: %v", err)
	}

	clusterConfig, err = clientcmd.NewDefaultClientConfig(*credentials, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("could not load client configuration: %v", err)
	}
	return clusterConfig, nil
}
