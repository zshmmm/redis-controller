package controller

import (
	"fmt"
	redislister "redis/pkg/generated/listers/qwoptcontroller/v1beta1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Controller struct {
	lister redislister.RedisLister
	queue workqueue.RateLimitingInterface
}

func NewController(queue workqueue.RateLimitingInterface, lister redislister.RedisLister) *Controller {
	return &Controller{
		lister: lister,
		queue:  queue,
	}
}

// 队列消费函数
func (c *Controller) processItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}

	defer c.queue.Done(key)

	err := c.process(key.(string))
	c.handleError(err, key)
	return true

}

// 调谐业务逻辑函数，打印 redis 的名字
func (c *Controller) process(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// 非业务逻辑错误无需返回 error，通过 runtime.HandleError 处理
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// 获取 redis 
	redis, err := c.lister.Redises(namespace).Get(name)

	if err != nil {
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("redis '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}

	fmt.Printf("Add/Update/Delete for redis %s\n", redis.GetName())

	return nil
}

// 重新入队逻辑，在处理出错时将 key 重新入队处理
func (c *Controller) handleError(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	// 重新入队 5 次，超过5次后丢弃
	if c.queue.NumRequeues(key) < 5 {
		klog.Infof("Error process redis %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	klog.Infof("Dropping redis %q out of the queue: %v", key, err)
	c.queue.Forget(key)
	runtime.HandleError(err)
}

// 启动 controller
func (c *Controller) Run(stop chan struct{}) {
	go func(c *Controller) {
		for c.processItem() {

		}
	}(c)

	<-stop

	klog.Info("Stopping redis controller")
}