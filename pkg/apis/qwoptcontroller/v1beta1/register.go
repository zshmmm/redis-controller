package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	GroupName = "qwoptcontroller.k8s.io"
	Version = "v1beta1"
)

// SchemeGroupVersion 是 API 组名字和版本
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// Kind 通过 kind 字符串返回 GroupKind 结构体
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 通过 resource 字符串返回 GroupResource 结构体
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder initializes a scheme builder
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is a global function that registers this API group & version to a scheme
	AddToScheme = SchemeBuilder.AddToScheme
)

// Adds the list of known types to Scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Redis{},
		&RedisList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}