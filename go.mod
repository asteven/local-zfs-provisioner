module github.com/asteven/local-zfs-provisioner

go 1.12

require (
	github.com/jawher/mow.cli v1.1.0
	github.com/miekg/dns v1.1.15 // indirect
	github.com/mistifyio/go-zfs v2.1.1+incompatible
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.0.0 // indirect
	github.com/sirupsen/logrus v1.4.2
	k8s.io/api v0.0.0-20190620084959-7cf5895f2711
	k8s.io/apimachinery v0.0.0-20190612205821-1799e75a0719
	k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
	sigs.k8s.io/sig-storage-lib-external-provisioner v4.0.0+incompatible
)
