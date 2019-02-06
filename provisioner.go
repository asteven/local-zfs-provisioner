package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	pvController "github.com/kubernetes-incubator/external-storage/lib/controller"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	//pvController "sigs.k8s.io/sig-storage-lib-external-provisioner/controller"

	zfs "github.com/mistifyio/go-zfs"
)

const (
	KeyNode = "kubernetes.io/hostname"

	NodeDefaultNonListedNodes = "DEFAULT_PATH_FOR_NON_LISTED_NODES"
)

var (
	CleanupTimeoutCounts = 120

	ConfigFileCheckInterval = 5 * time.Second
)

type LocalZFSProvisioner struct {
	stopCh          chan struct{}
	kubeClient      *clientset.Clientset
	datasetMountDir string
	namespace       string
	nodeName        string

	config      *Config
	configData  *ConfigData
	configFile  string
	configMutex *sync.RWMutex
}

type NodeDatasetMapData struct {
	Node    string `json:"node,omitempty"`
	Dataset string `json:"dataset,omitempty"`
}

type ConfigData struct {
	NodeDatasetMap []*NodeDatasetMapData `json:"nodeDatasetMap,omitempty"`
}

type NodeDatasetMap struct {
	Dataset string
}

type Config struct {
	NodeDatasetMap map[string]*NodeDatasetMap
}

func NewProvisioner(stopCh chan struct{}, kubeClient *clientset.Clientset, configFile string, datasetMountDir string, namespace string, nodeName string) (*LocalZFSProvisioner, error) {
	p := &LocalZFSProvisioner{
		stopCh: stopCh,

		kubeClient:      kubeClient,
		datasetMountDir: datasetMountDir,
		namespace:       namespace,
		nodeName:        nodeName,

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

func (p *LocalZFSProvisioner) getNodeDatasetMap(nodeName string) (*NodeDatasetMap, error) {
	p.configMutex.RLock()
	defer p.configMutex.RUnlock()

	if p.config == nil {
		return nil, fmt.Errorf("no valid config available")
	}

	c := p.config
	nodeDatasetMap := c.NodeDatasetMap[nodeName]
	if nodeDatasetMap == nil {
		nodeDatasetMap = c.NodeDatasetMap[NodeDefaultNonListedNodes]
		if nodeDatasetMap == nil {
			return nil, fmt.Errorf("config doesn't contain node %v, and no %v available", nodeName, NodeDefaultNonListedNodes)
		}
		logrus.Debugf("config doesn't contain node %v, using %v instead", nodeName, NodeDefaultNonListedNodes)
	}
	return nodeDatasetMap, nil
}

// Provision creates a storage asset and returns a PV object representing it.
func (p *LocalZFSProvisioner) Provision(options pvController.VolumeOptions) (*v1.PersistentVolume, error) {
	node := options.SelectedNode
	if node == nil {
		return nil, fmt.Errorf("configuration error, no node was specified")
	}
	if p.nodeName != node.Name {
		logrus.Infof("Provison: This is not the node you are looking for, move along, move along: I am '%s', while pv is for '%s'", p.nodeName, node.Name)
		return nil, &pvController.IgnoredError{
			Reason: fmt.Sprintf("Wrong node. I am '%s', while pv is for '%s'", p.nodeName, node.Name),
		}
	}

	logrus.Infof("Provisioning volume %v", options)
	pvc := options.PVC
	if pvc.Spec.Selector != nil {
		return nil, fmt.Errorf("claim.Spec.Selector is not supported")
	}
	for _, accessMode := range pvc.Spec.AccessModes {
		if accessMode != v1.ReadWriteOnce {
			return nil, fmt.Errorf("Only support ReadWriteOnce access mode")
		}
	}

	nodeDatasetMap, err := p.getNodeDatasetMap(node.Name)
	if err != nil {
		return nil, err
	}

	// Ensure parent dataset exists.
	_, err = zfs.GetDataset(nodeDatasetMap.Dataset)
	if err != nil {
		_, err = zfs.CreateFilesystem(nodeDatasetMap.Dataset, map[string]string{
			"mountpoint": "legacy",
		})
		if err != nil {
			return nil, err
		}
	}

	// Get capacity from PVC and convert it to a value usable for ZFS quota.
	capacity := pvc.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	quota := strconv.FormatInt(capacity.Value(), 10)

	pvName := options.PVName
	pvPath := filepath.Join(p.datasetMountDir, pvName)
	datasetName := filepath.Join(nodeDatasetMap.Dataset, pvName)

	zfsCreateProperties := map[string]string{
		"quota":      quota,
		"mountpoint": pvPath,
	}

	// TODO: could the dataset already exist? Would that be a valid use case?
	_, err = zfs.CreateFilesystem(datasetName, zfsCreateProperties)
	if err != nil {
		logrus.Infof("Failed to create volume %v at %v:%v", pvName, node.Name, pvPath)
		return nil, err
	}
	// It seems the dataset is mounted at creation time.
	// We do not have to explicitly mount it.
	//dataset, err = dataset.Mount(false, nil)
	//if err != nil {
	//	logrus.Infof("Failed to mount volume %v at %v:%v", pvName, node.Name, pvPath)
	//	return nil, err
	//}

	logrus.Infof("Created volume %v at %v:%v", pvName, node.Name, pvPath)

	fs := v1.PersistentVolumeFilesystem
	hostPathType := v1.HostPathDirectory
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   pvc.Spec.AccessModes,
			VolumeMode:                    &fs,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): pvc.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: pvPath,
					Type: &hostPathType,
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
	}

	return pv, nil
}

// Delete the storage asset that was created by Provision represented by
// the given PV.
func (p *LocalZFSProvisioner) Delete(pv *v1.PersistentVolume) (err error) {
	defer func() {
		err = errors.Wrapf(err, "Failed to delete volume %v", pv.Name)
	}()
	if pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimRetain {
		logrus.Infof("retained volume %v", pv.Name)
		return nil
	}

	pvPath, nodeName, err := p.getPathAndNodeForPV(pv)
	if err != nil {
		return err
	}

	if p.nodeName != nodeName {
		logrus.Infof("Delete: This is not the node you are looking for, move along, move along: I am '%s', while pv is for '%s'", p.nodeName, nodeName)
		return &pvController.IgnoredError{
			Reason: fmt.Sprintf("Wrong node. I am '%s', while pv is for '%s'", p.nodeName, nodeName),
		}
	}
	//nodeDatasetMap, err := p.getNodeDatasetMap(nodeName)
	//if err != nil {
	//	return nil, err
	//}

	// Destroy the dataset.
	dataset, err := zfs.GetDataset(pvPath)
	if err != nil {
		// TODO: should this error be ingored?
		return err
	}
	err = dataset.Destroy(zfs.DestroyDefault)
	if err != nil {
		logrus.Infof("Failed to delete volume %v on %v:%v: %v", pv.Name, nodeName, pvPath, err)
		return err
	}
	// Also delete the mountpoint.
	os.Remove(pvPath)
	logrus.Infof("Deleted volume %v on %v:%v", pv.Name, nodeName, pvPath)
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
		if n.Dataset[0] == '/' {
			return nil, fmt.Errorf("dataset name can not start with '/': %v:%v", n.Node, n.Dataset)
		}
		if n.Dataset == "" {
			return nil, fmt.Errorf("dataset name can not be empty: %v:%v", n.Node, n.Dataset)
		}
		cfg.NodeDatasetMap[n.Node] = &NodeDatasetMap{
			Dataset: n.Dataset,
		}
	}
	return cfg, nil
}
