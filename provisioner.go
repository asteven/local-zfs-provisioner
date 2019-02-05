package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	pvController "github.com/kubernetes-incubator/external-storage/lib/controller"
	//pvController "sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

const (
	KeyNode     = "kubernetes.io/hostname"
	DockerImage = "asteven/local-zfs-provisioner:v0.0.1"

	NodeDefaultNonListedNodes = "DEFAULT_PATH_FOR_NON_LISTED_NODES"
)

var (
	CleanupTimeoutCounts = 120

	ConfigFileCheckInterval = 5 * time.Second
)

type LocalZFSProvisioner struct {
	stopCh     chan struct{}
	kubeClient *clientset.Clientset
	namespace  string
	nodeName   string

	config      *Config
	configData  *ConfigData
	configFile  string
	configMutex *sync.RWMutex
}

type NodeDatasetMapData struct {
	Node  string   `json:"node,omitempty"`
	Dataset string `json:"dataset,omitempty"`
	MountPoint string `json:"mountPoint,omitempty"`
}

type ConfigData struct {
	NodeDatasetMap []*NodeDatasetMapData `json:"nodeDatasetMap,omitempty"`
}

type NodeDatasetMap struct {
	Dataset string
	MountPoint string
}

type Config struct {
	NodeDatasetMap map[string]*NodeDatasetMap
}

func NewProvisioner(stopCh chan struct{}, kubeClient *clientset.Clientset, configFile, namespace string, nodeName string) (*LocalZFSProvisioner, error) {
	p := &LocalZFSProvisioner{
		stopCh: stopCh,

		kubeClient: kubeClient,
		namespace:  namespace,
		nodeName:   nodeName,

		// config will be updated shortly by p.refreshConfig()
		config:      nil,
		configFile:  configFile,
		configData:  nil,
		configMutex: &sync.RWMutex{},
	}
	if err := p.refreshConfig(); err != nil {
		return nil, err
	}
	p.watchAndRefreshConfig()
	return p, nil
}

func (p *LocalZFSProvisioner) refreshConfig() error {
	p.configMutex.Lock()
	defer p.configMutex.Unlock()

	configData, err := loadConfigFile(p.configFile)
	if err != nil {
		return err
	}
	// no need to update
	if reflect.DeepEqual(configData, p.configData) {
		return nil
	}
	config, err := canonicalizeConfig(configData)
	if err != nil {
		return err
	}
	// only update the config if the new config file is valid
	p.configData = configData
	p.config = config

	output, err := json.Marshal(p.configData)
	if err != nil {
		return err
	}
	logrus.Debugf("Applied config: %v", string(output))

	return err
}

func (p *LocalZFSProvisioner) watchAndRefreshConfig() {
	go func() {
		for {
			select {
			case <-time.Tick(ConfigFileCheckInterval):
				if err := p.refreshConfig(); err != nil {
					logrus.Errorf("failed to load the new config file: %v", err)
				}
			case <-p.stopCh:
				logrus.Infof("stop watching config file")
				return
			}
		}
	}()
}

func (p *LocalZFSProvisioner) getNodeDatasetMap(nodeName string) (string, error) {
	p.configMutex.RLock()
	defer p.configMutex.RUnlock()

	if p.config == nil {
		return "", fmt.Errorf("no valid config available")
	}

	c := p.config
	nodeDatasetMap := c.NodeDatasetMap[nodeName]
	if nodeDatasetMap == nil {
		nodeDatasetMap = c.NodeDatasetMap[NodeDefaultNonListedNodes]
		if nodeDatasetMap == nil {
			return nil, fmt.Errorf("config doesn't contain node %v, and no %v available", node, NodeDefaultNonListedNodes)
		}
		logrus.Debugf("config doesn't contain node %v, using %v instead", node, NodeDefaultNonListedNodes)
	}
	return nodeDatasetMap, nil
}

func (p *LocalZFSProvisioner) Provision(opts pvController.VolumeOptions) (*v1.PersistentVolume, error) {
	pvc := opts.PVC
	if pvc.Spec.Selector != nil {
		return nil, fmt.Errorf("claim.Spec.Selector is not supported")
	}
	for _, accessMode := range pvc.Spec.AccessModes {
		if accessMode != v1.ReadWriteOnce {
			return nil, fmt.Errorf("Only support ReadWriteOnce access mode")
		}
	}
	node := opts.SelectedNode
	if opts.SelectedNode == nil {
		return nil, fmt.Errorf("configuration error, no node was specified")
	}

	// TODO: get dataset size from opts
	datasetSize = ""

	pvName := opts.PVName
	pvPath, err := p.runCreateDatasetPod(node.Name, pvName, datasetSize)
	if err != nil {
		logrus.Infof("Failed to create volume %v at %v:%v", pvName, node.Name, pvPath)
		return nil, err
	}

	logrus.Infof("Created volume %v at %v:%v", pvName, node.Name, pvPath)

	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: opts.PersistentVolumeReclaimPolicy,
			AccessModes:                   pvc.Spec.AccessModes,
			VolumeMode:                    &v1.PersistentVolumeFilesystem,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): pvc.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: pvPath,
					Type: &v1.HostPathDirectory
				},
			},
			NodeAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      KeyNode,
									Operator: v1.NodeSelectorOpIn,
									Values: []string{
										node.Name,
									},
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func (p *LocalZFSProvisioner) Delete(pv *v1.PersistentVolume) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to delete volume %v", pv.Name)
	}()
	path, nodeName, err := p.getPathAndNodeForPV(pv)
	if err != nil {
		return err
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
		// TODO: zfs destroy dataset on the node where it lives
		if p.nodeName != nodeName {
			logrus.Infof("Delete: This is not the node you are looking for, move along, move along: I am '%s', while pv is for '%s'", p.nodeName, nodeName)
			return &pvController.IgnoredError{
				Reason: fmt.Sprintf("Wrong node. I am '%s', while pv is for '%s'", p.nodeName, nodeName),
			}
		}
		logrus.Infof("Now would delete volume %v on %v", pv.Name, nodeName)
		return nil

		if err := p.cleanupVolume(pv.Name, path, nodeName); err != nil {
			logrus.Infof("clean up volume %v failed: %v", pv.Name, err)
			return err
		}
		return nil
	}
	logrus.Infof("retained volume %v", pv.Name)
	return nil
}

func (p *LocalZFSProvisioner) getPathAndNodeForPV(pv *v1.PersistentVolume) (path, node string, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to delete volume %v", pv.Name)
	}()

	hostPath := pv.Spec.PersistentVolumeSource.HostPath
	if hostPath == nil {
		return "", "", fmt.Errorf("no HostPath set")
	}
	path = hostPath.Path

	nodeAffinity := pv.Spec.NodeAffinity
	if nodeAffinity == nil {
		return "", "", fmt.Errorf("no NodeAffinity set")
	}
	required := nodeAffinity.Required
	if required == nil {
		return "", "", fmt.Errorf("no NodeAffinity.Required set")
	}

	node = ""
	for _, selectorTerm := range required.NodeSelectorTerms {
		for _, expression := range selectorTerm.MatchExpressions {
			if expression.Key == KeyNode && expression.Operator == v1.NodeSelectorOpIn {
				if len(expression.Values) != 1 {
					return "", "", fmt.Errorf("multiple values for the node affinity")
				}
				node = expression.Values[0]
				break
			}
		}
		if node != "" {
			break
		}
	}
	if node == "" {
		return "", "", fmt.Errorf("cannot find affinited node")
	}
	return path, node, nil
}

func (p *LocalZFSProvisioner) runCreateDatasetPod(nodeName string, pvName string, datasetSize string) (pvPath string, err error) {
    nodeDatasetMap, err := p.getNodeDatasetMap(nodeName)
    if err != nil {
        return nil, err
    }
	args := []string{
		"--dataset", nodeDatasetMap.Dataset,
		"--mount-point", nodeDatasetMap.MountPoint,
	}
    if datasetSize != "" {
        args = append(args, "--size")
        // TODO: convert size from k8s to zfs format
        args = append(args, datasetSize)
    }
	err = p.runDatasetPod(nodeName, "create", pvName, args)
    if err != nil {
        return nil, err
    }
    pvPath := filepath.Join(nodeDatasetMap.MountPoint, pvName)
    logrus.Infof("Successfully created persistent volume at %v:%v", nodeName, pvPath)
    return pvPath, nil

}

func (p *LocalZFSProvisioner) runDeleteDatasetPod(nodeName string, pvName string, datasetSize string) (pvPath string, err error) {
    nodeDatasetMap, err := p.getNodeDatasetMap(nodeName)
    if err != nil {
        return nil, err
    }
	args := []string{
		"--dataset", nodeDatasetMap.Dataset,
		"--mount-point", nodeDatasetMap.MountPoint,
	}
    if datasetSize != "" {
        args = append(args, "--size")
        // TODO: convert size from k8s to zfs format
        args = append(args, datasetSize)
    }
	return p.runDatasetPod(nodeName, "create", pvName, args)

}
func (p *LocalZFSProvisioner) runDatasetPod(nodeName string, action string, pvName string, args []string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to %v persistent volume %v on node %v", action, pvName, nodeName)
	}()
	if nodeName == "" || action == "" || pvName == "" {
		return fmt.Errorf("invalid empty nodeName, action or pvName")
	}

    nodeDatasetMap, err := p.getNodeDatasetMap(nodeName)
    if err != nil {
        return err
    }

	podArgs := []string{"dataset", action}
	if args != nil {
		podArgs = append(podArgs, args...)
	}
	podArgs = append(podArgs, pvName)
	privileged := true
	datasetPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: node + "-" + action + "-" + datasetName,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			NodeName:      nodeName,
			HostNetwork:   true,
			Containers: []v1.Container{
				{
					Name:    "local-zfs-provisioner-"+ action,
					Image:   DockerImage,
					Command: []string{"/local-zfs-provisioner"},
					Args:    &podArgs,
					SecurityContext: &v1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
		},
	}

	pod, err := p.kubeClient.CoreV1().Pods(p.namespace).Create(datasetPod)
	if err != nil {
		return err
	}

	defer func() {
		e := p.kubeClient.CoreV1().Pods(p.namespace).Delete(pod.Name, &metav1.DeleteOptions{})
		if e != nil {
			logrus.Errorf("unable to delete the dataset operation pod: %v", e)
		}
	}()

	completed := false
	for i := 0; i < CleanupTimeoutCounts; i++ {
		if pod, err := p.kubeClient.CoreV1().Pods(p.namespace).Get(pod.Name, metav1.GetOptions{}); err != nil {
			return err
		} else if pod.Status.Phase == v1.PodSucceeded {
			completed = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !completed {
		return fmt.Errorf("dataset operation timed out after %v seconds", CleanupTimeoutCounts)
	}
	return nil
}

func loadConfigFile(configFile string) (cfgData *ConfigData, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to load config file %v", configFile)
	}()
	f, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var data ConfigData
	if err := json.NewDecoder(f).Decode(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

func canonicalizeConfig(data *ConfigData) (cfg *Config, err error) {
	defer func() {
		err = errors.Wrapf(err, "config canonicalization failed")
	}()
	cfg = &Config{}
	cfg.NodeDatasetMap = map[string]*NodeDatasetMap{}
	for _, n := range data.NodeDatasetMap {
		if cfg.NodeDatasetMap[n.Node] != nil {
			return nil, fmt.Errorf("duplicate node %v", n.Node)
		}
		if n.Dataset[0] == "/" {
			return nil, fmt.Errorf("dataset name can not start with '/': %v:%v", n.Node, n.Dataset)
		}
		if n.Dataset == "" {
			return nil, fmt.Errorf("dataset name can not be empty: %v:%v", n.Node, n.Dataset)
		}
		if n.MountPoint[0] != "/" {
			return nil, fmt.Errorf("mountpoint must start with '/': %v:%v", n.Node, n.MountPoint)
		}
		mountPoint, err := filepath.Abs(n.MountPoint)
		if err != nil {
			return nil, err
		}
		if mountPoint == "/" {
			return nil, fmt.Errorf("cannot use root ('/') as mountpoint on node %v", n.Node)
		}
		cfg.NodeDatasetMap[n.Node] = NodeDatasetMap{
			Dataset: n.Dataset,
			MountPoint: mountPoint,
		}
	}
	return cfg, nil
}
