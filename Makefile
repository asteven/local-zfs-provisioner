# Copyright 2019 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

IMG_NAMESPACE = asteven
IMG_NAME = local-zfs-provisioner
IMG_FQNAME = $(IMG_NAMESPACE)/$(IMG_NAME)
IMG_VERSION = 0.0.4


.PHONY: gofmt container push-container clean
all: test local-zfs-provisioner container

test: gofmt

gofmt:
	gofmt -s -w ./main.go ./provisioner.go

local-zfs-provisioner:
	if [ ! -d ./vendor ]; then dep ensure; fi
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o local-zfs-provisioner ./main.go ./provisioner.go

container: local-zfs-provisioner
	sudo docker build -t $(IMG_FQNAME):$(IMG_VERSION) .
	sudo docker tag $(IMG_FQNAME):$(IMG_VERSION) $(IMG_FQNAME):latest

_push-container: container
	sudo docker push $(IMG_FQNAME):$(IMG_VERSION)

push:
	sudo docker push $(IMG_FQNAME):$(IMG_VERSION)
	# Also update :latest
	sudo docker push $(IMG_FQNAME):latest

clean:
	#go clean -r -x
	rm -f local-zfs-provisioner
	sudo docker rmi $(IMG_FQNAME):$(IMG_VERSION)

