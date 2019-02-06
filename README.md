# Local ZFS Provisioner

This work is based on Ranchers [Local Path Provisioner](https://github.com/rancher/local-path-provisioner).
The original code was changed to work with ZFS datasets instead of local directories.


## Overview

Local ZFS Provisioner provides a way for the Kubernetes users to utilize the local storage in each node. Based on the user configuration, the Local ZFS Provisioner will create `hostPath` based persistent volume on the node automatically. It utilizes the features introduced by Kubernetes [Local Persistent Volume feature](https://kubernetes.io/blog/2018/04/13/local-persistent-volumes-beta/), but make it a simpler solution than the built-in `local` volume feature in Kubernetes.


## Requirement
Kubernetes v1.12+.

## Deployment

### Installation

The Local ZFS Provisioner is designed to run as a Daemonset.
Every node in the cluster will run a pod that handles the provisoning and deletion of Persistent Volumes for Pods running on the node.

In this setup, the directory `/var/lib/local-zfs-provisioner` will be used across all the nodes as the path for mounting the datasets. The provisioner will be installed in the `kube-system` namespace by default.

```
kubectl apply -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/deploy/local-zfs-provisioner.yaml
```

Create a suitable configmap and add it to the cluster. You will have to change this to work
with your zfs pools and datasets.

```
kubectl apply -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/deploy/example-config.yaml
```

## Usage

Create a `hostPath` backed Persistent Volume and a pod uses it:

```
kubectl create -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/example/pvc.yaml
kubectl create -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/example/pod.yaml
```

You should see the PV has been created:
```
$ kubectl get pv
NAME                                       CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS   CLAIM                    STORAGECLASS   REASON   AGE
pvc-5fdc9d7f-2a27-11e9-8180-a4bf0112bd54   2Gi        RWO            Delete           Bound    default/local-zfs-pvc    local-zfs               10s
```

The PVC has been bound:
```
$ kubectl get pvc
NAME             STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
local-zfs-pvc    Bound    pvc-5fdc9d7f-2a27-11e9-8180-a4bf0112bd54   2Gi        RWO            local-zfs      16s
```

And the Pod started running:
```
$ kubectl get pod
NAME              READY     STATUS    RESTARTS   AGE
volume-test-zfs   1/1       Running   0          3s
```

Write something into the pod
```
kubectl exec volume-test-zfs -- sh -c "echo local-zfs-test > /data/test"
```

Now delete the pod using
```
kubectl delete -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/example/pod.yaml
```

After confirm that the pod is gone, recreated the pod using
```
kubectl create -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/example/pod.yaml
```

Check the volume content:
```
$ kubectl exec volume-test-zfs cat /data/test
local-zfs-test
```

Delete the pod and pvc
```
kubectl delete -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/example/pod.yaml
kubectl delete -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/example/pvc.yaml
```

The volume content stored on the node will be automatically cleaned up. You can check the log of the `local-zfs-provisioner-xxx` pod for details.

Now you've verified that the provisioner works as expected.

## Configuration

The configuration of the provisioner is a json file `config.json`, stored in the a config map, e.g.:
```
kind: ConfigMap
apiVersion: v1
metadata:
  name: local-zfs-provisioner-config
  namespace: kube-system
data:
  config.json: |-
    {
        "nodeDatasetMap": [
            {
                "node": "DEFAULT_PATH_FOR_NON_LISTED_NODES",
                "dataset": "pool/data/local-zfs-provisioner"
            },
            {
                "node": "that-other-node",
                "dataset": "tank/local-zfs-provisioner"
            }
        ]
    }

```

### Definition
`nodeDatasetMap` is the place where the user can customize where to store the data on each node.
1. If a node is not listed in the `nodeDatasetMap` map, and Kubernetes wants to create volume on it, the dataset specified in `DEFAULT_PATH_FOR_NON_LISTED_NODES` will be used for provisioning.
2. If a node is listed in the `nodeDatasetMap` map, the specified `dataset` will be used for provisioning.

### Rules
The configuration must obey following rules:
1. `config.json` must be a valid json file.
2. A dataset name can not start with `/`.
3. No duplicate node allowed.

### Reloading

The provisioner supports automatic reloading of configuration. Users can change the configuration using `kubectl apply` or `kubectl edit` with config map `local-zfs-provisioner-config`. It will be a delay between user update the config map and the provisioner pick it up.

When the provisioner detected the configuration changes, it will try to load the new configuration.

If the reload failed due to some reason, the provisioner will report error in the log, and **continue using the last valid configuration for provisioning in the meantime**.

## Uninstall

Before uninstallation, make sure the PVs created by the provisioner has already been deleted. Use `kubectl get pv` and make sure no PV with StorageClass `local-zfs`.

To uninstall, execute:

```
kubectl delete -f https://raw.githubusercontent.com/asteven/local-zfs-provisioner/master/deploy/local-zfs-provisioner.yaml
```

## License

Copyright (c) 2014-2018  [Rancher Labs, Inc.](http://rancher.com/)

Copyright (c) 2019 Steven Armstrong

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
