package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// redis 资源结构体，k8s 资源结构一般分为四个部分：Type, Metadata, Spec, Status。
// Kubernetes client 要求注册到 Scheme 中的 API 对象必须实现 runtime.Object 接口。
// 如上 +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object Tag 
// deepcoy-gen 工具根据这个 Tag 为这个数据结构生成返回 runtime.Objec 
type Redis struct {
	metav1.TypeMeta `json:",inline"` // K8S API 的 Group，Version 和 Kind (GVK)
	metav1.ObjectMeta `json:"metadata,omitempty"` // 标准的 k8s metadata 字段，包括 name 和 namespace

	Spec RedisSpec `json:"spec"` // CRD 的自定义字段
	Status RedisStatus `json:"status"` // CRD Spec 对应的状态
}

type RedisSpec struct {
	Image string `json:"image"`
	Port int32 `json:"port"`
	TargetPort int32 `json:"targetPort"`
	Password string `json:"string"`
}

type RedisStatus struct {
	Active string `json:"active"`
	Standby []string `json:"standby"`
	State string `json:"state"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// redis list 资源结构体
type RedisList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Redis `json:"items"`
}