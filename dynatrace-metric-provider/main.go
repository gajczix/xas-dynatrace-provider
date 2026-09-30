package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var metricProviderClassGVR = schema.GroupVersionResource{
	Group:    "xas.io",
	Version:  "v1",
	Resource: "metricproviderclasses",
}

const defaultMPCName = "dynatrace"

func getKubeConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			if home := homedir.HomeDir(); home != "" {
				kubeconfig = home + "/.kube/config"
			}
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
		}
	}
	return config, nil
}

func parseMetricProviderClass(unstr *unstructured.Unstructured) (*MetricProviderClass, error) {
	mpc := &MetricProviderClass{
		TypeMeta: metav1.TypeMeta{
			APIVersion: unstr.GetAPIVersion(),
			Kind:       unstr.GetKind(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: unstr.GetName(),
		},
		Spec: MetricProviderClassSpec{
			Config: make(map[string]string),
		},
	}

	specType, _, _ := unstructured.NestedString(unstr.Object, "spec", "type")
	mpc.Spec.Type = specType

	if configRaw, found, _ := unstructured.NestedMap(unstr.Object, "spec", "config"); found {
		for k, v := range configRaw {
			if strVal, ok := v.(string); ok {
				mpc.Spec.Config[k] = strVal
			} else {
				mpc.Spec.Config[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	return mpc, nil
}

func main() {
	log.Printf("==========================================================")
	log.Printf(" xAS Dynatrace Metric Provider (P90 Latency Collector)")
	log.Printf(" Spec: xas.io/v1 MetricProviderClass")
	log.Printf("==========================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Printf("Shutdown signal received, exiting...")
		cancel()
	}()

	mpcName := os.Getenv("METRIC_PROVIDER_CLASS")
	if mpcName == "" {
		mpcName = defaultMPCName
	}

	apiToken := os.Getenv("DYNATRACE_API_TOKEN")

	var mpc *MetricProviderClass
	config, err := getKubeConfig()
	if err != nil {
		log.Printf("[DynatraceProvider] Warning: Kubernetes client not available (%v), using local fallback", err)
		endpoint := os.Getenv("DYNATRACE_ENDPOINT")
		if endpoint == "" {
			log.Fatalf("Fatal: DYNATRACE_ENDPOINT environment variable is required when Kubernetes is not available")
		}
		mpc = &MetricProviderClass{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "xas.io/v1",
				Kind:       "MetricProviderClass",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: mpcName,
			},
			Spec: MetricProviderClassSpec{
				Type: "Dynatrace",
				Config: map[string]string{
					"endpoint": endpoint,
				},
			},
		}
	} else {
		dynClient, err := dynamic.NewForConfig(config)
		if err != nil {
			log.Fatalf("Failed to create dynamic client: %v", err)
		}
		clientset, _ := kubernetes.NewForConfig(config)

		log.Printf("[DynatraceProvider] Fetching MetricProviderClass '%s' (xas.io/v1) from cluster...", mpcName)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			unstr, getErr := dynClient.Resource(metricProviderClassGVR).Get(ctx, mpcName, metav1.GetOptions{})
			if getErr == nil {
				mpc, err = parseMetricProviderClass(unstr)
				if err != nil {
					log.Fatalf("Failed to parse MetricProviderClass '%s': %v", mpcName, err)
				}
				log.Printf("[DynatraceProvider] Successfully loaded MetricProviderClass '%s'", mpcName)
				break
			}
			log.Printf("[DynatraceProvider] Waiting for MetricProviderClass '%s' to be available: %v", mpcName, getErr)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}

		// If apiToken is not in env, check apiTokenSecretRef from MetricProviderClass
		if apiToken == "" && clientset != nil {
			secretName := mpc.Spec.Config["apiTokenSecretRef"]
			if secretName != "" {
				ns := os.Getenv("NAMESPACE")
				if ns == "" {
					ns = "default"
				}
				sec, sErr := clientset.CoreV1().Secrets(ns).Get(ctx, secretName, metav1.GetOptions{})
				if sErr == nil {
					if t, ok := sec.Data["api-token"]; ok {
						apiToken = string(t)
					}
				}
			}
		}
	}

	// Environment variable overrides (if explicitly provided)
	if envEndpoint := os.Getenv("DYNATRACE_ENDPOINT"); envEndpoint != "" {
		mpc.Spec.Config["endpoint"] = envEndpoint
	}

	if apiToken == "" {
		apiToken = "dt0c01.samplemocktoken123456789.mocksecret"
	}

	provider, err := NewDynatraceMetricProvider(mpc, apiToken)
	if err != nil {
		log.Fatalf("Failed to initialize provider: %v", err)
	}

	provider.StartPolling(ctx)
}
