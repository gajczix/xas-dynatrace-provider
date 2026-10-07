package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	clientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned"
	informers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers/externalversions"

	"github.com/gajczix/xas-dynatrace-provider/pkg/dynatrace"
)

func main() {
	var serverAddress string
	var clusterName string
	var kubeconfig string
	var scrapeInterval time.Duration
	var debug bool

	flag.StringVar(&serverAddress, "server-address", "xas-server.xas-system.svc:8080", "Address of the xAS Server (host:port)")
	flag.StringVar(&clusterName, "cluster-name", "default", "Name of the cluster this provider is running in")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig file. Required only if running out-of-cluster.")
	flag.DurationVar(&scrapeInterval, "scrape-interval", 5*time.Second, "Interval between metric scrape cycles")
	flag.BoolVar(&debug, "debug", false, "Enable verbose debug logging")
	flag.Parse()

	// Setup structured logging
	logLevel := slog.LevelInfo
	if debug {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	slog.Info("Starting xAS Dynatrace Metric Provider",
		"serverAddress", serverAddress,
		"clusterName", clusterName,
		"scrapeInterval", scrapeInterval,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		slog.Error("Failed to build kubeconfig", "error", err)
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("Failed to build Kubernetes clientset", "error", err)
		os.Exit(1)
	}

	xasClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		slog.Error("Failed to build xAS clientset", "error", err)
		os.Exit(1)
	}

	factory := informers.NewSharedInformerFactory(xasClient, 30*time.Second)
	providerLister := factory.Xas().V1().MetricProviderClasses().Lister()

	p := dynatrace.NewProvider(kubeClient, providerLister, serverAddress, clusterName, scrapeInterval)

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	p.Run(ctx)
}
