package main

import (
	"flag"
	"fmt"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"

	redisclient "redis/pkg/generated/clientset/versioned"
	_ "redis/pkg/generated/clientset/versioned/scheme"
	redisinformer "redis/pkg/generated/informers/externalversions"
	"redis/controller"
)

const (
	Namespace = "default"
)

func main() {
	var kubeconfig string
	var apiserver string

	flag.StringVar(&kubeconfig, "kubeconfig", "/home/rke/.kube/config", "absolute path to the kubeconfig file")
	flag.StringVar(&apiserver, "apiserver", "", "apiserver url")
	flag.Parse()

	cfg, err := clientcmd.BuildConfigFromFlags(apiserver, kubeconfig)
	if err != nil {
		panic(err)
	}

	// 创建 redis crd client
	cli, err := redisclient.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}

	// 初始化 Informer factory，第二个参数为将 Indexer 中的数据重新放到 DeltaFIFO 中重新处理的频率， 第三个参数为监听的 namespace。
	// factory := informers.NewSharedInformerFactory(cli, 0)
	factory := redisinformer.NewSharedInformerFactoryWithOptions(cli, 0, redisinformer.WithNamespace(Namespace))

	// 初始化 Informer
	redisInformer := factory.Qwoptcontroller().V1beta1().Redises()

	// 创建 controller 工作队列
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// 在 Informer 中注册 ResourceEventHandler
	redisInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if key, err := cache.MetaNamespaceKeyFunc(obj); err == nil {
				// 将 namespace/redisname 入队列
				queue.Add(key)
			}
		},
		UpdateFunc: func(oldObj interface{}, newObj interface{}) {
			if key, err := cache.MetaNamespaceKeyFunc(newObj); err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if key, err := cache.MetaNamespaceKeyFunc(obj); err == nil {
				queue.Add(key)
			}
		},
	})

	stop := make(chan struct{})
	defer close(stop)

	// 开始 Watch & List
	factory.Start(stop)

	// 等待第一次同步完成，然后再启动 controller 处理任务
	if !cache.WaitForCacheSync(stop, redisInformer.Informer().HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		close(stop)
	}

	// 初始化 Controller 并启动
	c := controller.NewController(queue, redisInformer.Lister())
	go c.Run(stop)

	<-stop


}