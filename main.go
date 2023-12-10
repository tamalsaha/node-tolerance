package main

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2/klogr"
	"kmodules.xyz/resource-metadata/apis/management/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

func NewClient() (client.Client, error) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)

	ctrl.SetLogger(klogr.New())
	cfg := ctrl.GetConfigOrDie()
	cfg.QPS = 100
	cfg.Burst = 100

	mapper, err := apiutil.NewDynamicRESTMapper(cfg)
	if err != nil {
		return nil, err
	}

	kc, err := client.New(cfg, client.Options{
		Scheme: scheme,
		Mapper: mapper,
		//Opts: client.WarningHandlerOptions{
		//	SuppressWarnings:   false,
		//	AllowDuplicateLogs: false,
		//},
	})
	return kc, err
}

func main() {
	if err := useKubebuilderClient(); err != nil {
		panic(err)
	}
}

func useKubebuilderClient() error {
	fmt.Println("Using kubebuilder client")
	kc, err := NewClient()
	if err != nil {
		return err
	}

	var list core.NodeList
	err = kc.List(context.TODO(), &list)
	if err != nil {
		return err
	}

	taintKey := "kubedb.com/autoscaling-group"
	groups, err := calNodeMap(list, taintKey)
	if err != nil {
		return err
	}

	for groupName, resources := range groups {
		fmt.Println(taintKey, groupName)
		fmt.Println(resources)
		fmt.Println("---------------------------")
	}

	return nil
}

func calNodeMap(list core.NodeList, taintKey string) (map[string]core.ResourceList, error) {
	groups := map[string]core.ResourceList{}
	taintedNode := map[string]string{}

	for _, node := range list.Items {
		for _, taint := range node.Spec.Taints {
			if taint.Key == taintKey {
				curResources, found := groups[taint.Value]
				if !found {
					groups[taint.Value] = node.Status.Capacity
					taintedNode[taint.Value] = node.Name
				} else if !equalsComputeResource(curResources, node.Status.Capacity) {
					return nil, fmt.Errorf("taint %s=%s includes nodes with unequal resource capacity, %s[%+v] and %s[%+v]",
						taintKey, taint.Value,
						taintedNode[taint.Value], curResources,
						node.Name, node.Status.Capacity,
					)
				}
			}
		}
	}
	return groups, nil
}

func equalsComputeResource(a, b core.ResourceList) bool {
	cpuA := a[core.ResourceCPU]
	cpuB := b[core.ResourceCPU]
	if !cpuA.Equal(cpuB) {
		return false
	}

	memA := a[core.ResourceMemory]
	memB := b[core.ResourceMemory]
	return memA.Equal(memB)
}
