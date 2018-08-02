package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/openshift/ci-ns-ttl-controller/pkg/controller"
)

const (
	resync = 5 * time.Minute
)

type options struct {
	configLocation string
	numWorkers     int
	logLevel       string
}

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	o := options{}
	flag.IntVar(&o.numWorkers, "num-workers", 10, "Number of worker threads.")
	flag.StringVar(&o.logLevel, "log-level", logrus.DebugLevel.String(), "Logging level.")
	flag.Parse()

	level, err := logrus.ParseLevel(o.logLevel)
	if err != nil {
		logrus.WithError(err).Fatal("failed to parse log level")
	}
	logrus.SetLevel(level)

	configFile, err := os.Open(o.configLocation)
	if err != nil {
		logrus.WithError(err).Fatal("could not read configuration file")
	}
	defer configFile.Close()

	clusterConfig, err := loadClusterConfig()
	if err != nil {
		logrus.WithError(err).Fatal("failed to load cluster config")
	}

	client, err := kubernetes.NewForConfig(clusterConfig)
	if err != nil {
		logrus.WithError(err).Fatal("failed to initialize kubernetes client")
	}

	nsInformerFactory := informers.NewSharedInformerFactory(client, resync)

	nsReaper := controller.NewReaper(nsInformerFactory.Core().V1().Namespaces(), client)
	nsTtlManager := controller.NewTTLManager(nsInformerFactory.Core().V1().Namespaces(), nsInformerFactory.Core().V1().Pods().Lister(), client)
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
