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

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	pvController "sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
)

const (
	KeyNode     = "kubernetes.io/hostname"
	DockerImage = "asteven/local-zfs-provisioner:v0.0.1"

	NodeDefaultNonListedNodes = "DEFAULT_PATH_FOR_NON_LISTED_NODES"
)

var (
	CleanupTimeoutCounts    = 120
	ConfigFileCheckInterval = 10 * time.Second
)

type LocalZFSProvisioner struct {
	stopCh          chan struct{}
	kubeClient      *clientset.Clientset
	datasetMountDir string
	provisionerName string
	namespace       string
	nodeName        string

	datasetNameAnnotation string

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

func NewProvisioner(stopCh chan struct{}, kubeClient *clientset.Clientset, configFile string, datasetMountDir string, provisionerName string, namespace string, nodeName string) (*LocalZFSProvisioner, error) {
	p := &LocalZFSProvisioner{
		stopCh: stopCh,

		kubeClient:      kubeClient,
		datasetMountDir: datasetMountDir,
		provisionerName: provisionerName,
		namespace:       namespace,
		nodeName:        nodeName,

		datasetNameAnnotation: provisionerName + ".datasetName",

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
	logrus.Debugf("refreshConfig")
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
	logrus.Infof("Provisioning volume %v", options.PVName)
	node := options.SelectedNode
	if node == nil {
		return nil, fmt.Errorf("configuration error, no node was specified")
	}

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

	pvName := options.PVName
	parentDataset := nodeDatasetMap.Dataset
	datasetName := filepath.Join(parentDataset, pvName)
	mountPoint := filepath.Join(p.datasetMountDir, pvName)

	// Get capacity from PVC and convert it to a value usable for ZFS quota.
	capacity := pvc.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	quota := strconv.FormatInt(capacity.Value(), 10)

	err = p.runCreateDatasetPod(node.Name, pvName,
		parentDataset, datasetName, mountPoint, quota)
	if err != nil {
		return nil, fmt.Errorf("Failed to create volume %v at %v:%v: %v", pvName, node.Name, mountPoint, err)
	}

	logrus.Infof("Created volume %v at %v:%v", pvName, node.Name, mountPoint)

	fs := v1.PersistentVolumeFilesystem
	hostPathType := v1.HostPathDirectory

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Annotations: map[string]string{
				p.datasetNameAnnotation: datasetName,
			},
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
					Path: mountPoint,
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

	// Get the dataset backing this pv from the volume's annotation.
	// It is not safe to use this for deletion. It is only used to detect and log
	// any tampering attempts.
	pvDatasetName, ok := pv.Annotations[p.datasetNameAnnotation]
	if !ok {
		return fmt.Errorf("Can not delete volume %v. Missing '%v' annotation.", pv.Name, p.datasetNameAnnotation)
	}

	nodeName, pvMountPoint, err := p.getNodeAndMountPointForPV(pv)
	if err != nil {
		return err
	}

	nodeDatasetMap, err := p.getNodeDatasetMap(nodeName)
	if err != nil {
		return err
	}

	// Re-construct our own safe datasetName and mountPoint for the actual deletion.
	parentDataset := nodeDatasetMap.Dataset
	datasetName := filepath.Join(parentDataset, pv.Name)
	mountPoint := filepath.Join(p.datasetMountDir, pv.Name)

	// Log any tampering attempt.
	if mountPoint != pvMountPoint {
		logrus.Warnf("Volume mount point has been tampered with: got %v, expected %v", pvMountPoint, mountPoint)
	}
	if datasetName != pvDatasetName {
		logrus.Warnf("Volume dataset name has been tampered with: got %v, expected %v", pvDatasetName, datasetName)
	}

	// Schedule the dataset deletion pod.
	err = p.runDeleteDatasetPod(nodeName, pv.Name, datasetName, mountPoint)
	if err != nil {
		return fmt.Errorf("Failed to delete volume %v on %v:%v: %v", pv.Name, nodeName, mountPoint, err)
	}

	logrus.Infof("Deleted volume %v on %v:%v", pv.Name, nodeName, mountPoint)
	return nil
}

func (p *LocalZFSProvisioner) getNodeAndMountPointForPV(pv *v1.PersistentVolume) (nodeName, mountPoint string, err error) {
	hostPath := pv.Spec.PersistentVolumeSource.HostPath
	if hostPath == nil {
		return "", "", fmt.Errorf("no HostPath set")
	}
	mountPoint = hostPath.Path

	nodeAffinity := pv.Spec.NodeAffinity
	if nodeAffinity == nil {
		return "", "", fmt.Errorf("no NodeAffinity set")
	}
	required := nodeAffinity.Required
	if required == nil {
		return "", "", fmt.Errorf("no NodeAffinity.Required set")
	}

	nodeName = ""
	for _, selectorTerm := range required.NodeSelectorTerms {
		for _, expression := range selectorTerm.MatchExpressions {
			if expression.Key == KeyNode && expression.Operator == v1.NodeSelectorOpIn {
				if len(expression.Values) != 1 {
					return "", "", fmt.Errorf("multiple values for the node affinity")
				}
				nodeName = expression.Values[0]
				break
			}
		}
		if nodeName != "" {
			break
		}
	}
	if nodeName == "" {
		return "", "", fmt.Errorf("cannot find affinited node")
	}
	return nodeName, mountPoint, nil
}

func (p *LocalZFSProvisioner) runCreateDatasetPod(
	nodeName string, pvName string,
	parentDataset string, datasetName string,
	mountPoint string, quota string) (err error) {

	if parentDataset == "" {
		return fmt.Errorf("invalid empty parentDataset")
	}
	if datasetName == "" {
		return fmt.Errorf("invalid empty datasetName")
	}
	if mountPoint == "" {
		return fmt.Errorf("invalid empty mountPoint")
	}

	// dataset create --parent $parent_dataset --size $size $dataset $mountpoint
	args := []string{
		"dataset", "create",
		"--parent", parentDataset,
	}

	if quota != "" {
		args = append(args, "--quota", quota)
	}

	args = append(args, datasetName, mountPoint)

	podName := pvName + "-create"
	err = p.runDatasetPod(nodeName, podName, args)
	if err != nil {
		return err
	}

	return nil
}

func (p *LocalZFSProvisioner) runDeleteDatasetPod(
	nodeName string, pvName string,
	datasetName string, mountPoint string) (err error) {

	if datasetName == "" {
		return fmt.Errorf("invalid empty datasetName")
	}
	if mountPoint == "" {
		return fmt.Errorf("invalid empty mountPoint")
	}

	// dataset destroy $dataset $mountpoint
	args := []string{
		"dataset", "destroy",
		datasetName, mountPoint,
	}

	podName := pvName + "-delete"
	return p.runDatasetPod(nodeName, podName, args)
}

func (p *LocalZFSProvisioner) runDatasetPod(nodeName string, podName string, args []string) (err error) {

	if nodeName == "" {
		return fmt.Errorf("invalid empty nodeName")
	}
	if podName == "" {
		return fmt.Errorf("invalid empty podName")
	}

	privileged := true
	hostPathType := v1.HostPathDirectoryOrCreate
	mountPropagation := v1.MountPropagationBidirectional

	datasetPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			NodeName:      nodeName,
			HostNetwork:   true,
			Containers: []v1.Container{
				{
					Name:    podName,
					Image:   DockerImage,
					Command: []string{"/local-zfs-provisioner"},
					Args:    args,
					SecurityContext: &v1.SecurityContext{
						Privileged: &privileged,
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:             "dataset-mount-dir",
							ReadOnly:         false,
							MountPath:        p.datasetMountDir,
							MountPropagation: &mountPropagation,
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "dataset-mount-dir",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: p.datasetMountDir,
							Type: &hostPathType,
						},
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
