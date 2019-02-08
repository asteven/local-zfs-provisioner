package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/jawher/mow.cli"
	"github.com/pkg/errors"

	//pvController "github.com/kubernetes-incubator/external-storage/lib/controller"
	pvController "sigs.k8s.io/sig-storage-lib-external-provisioner/controller"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	zfs "github.com/mistifyio/go-zfs"
)

var (
	Version                = "0.0.1"
	DefaultProvisionerName = "asteven/local-zfs"
	DefaultNamespace       = "kube-system"
	DefaultDatasetMountDir = "/var/lib/local-zfs-provisioner"
)

func RegisterShutdownChannel(done chan struct{}) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Receive %v to exit", sig)
		close(done)
	}()
}

func startController(configFile string, datasetMountDir string, provisionerName string, namespace string) error {
	stopCh := make(chan struct{})
	RegisterShutdownChannel(stopCh)

	config, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}
	if kubeClient != nil {
		logrus.Debug("kubeClient FTW")
	}

	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		return errors.Wrap(err, "Cannot start Provisioner: failed to get Kubernetes server version")
	}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return fmt.Errorf("NODE_NAME environment variable not set")
	}

	fmt.Println("configFile: ", configFile)
	fmt.Println("datasetMountDir: ", datasetMountDir)
	fmt.Println("provisionerName: ", provisionerName)
	fmt.Println("namespace: ", namespace)

	provisioner, err := NewProvisioner(stopCh, kubeClient, configFile, datasetMountDir, provisionerName, namespace, nodeName)
	if err != nil {
		return err
	}
	pc := pvController.NewProvisionController(
		kubeClient,
		provisionerName,
		provisioner,
		serverVersion.GitVersion,
	)
	logrus.Debug("Provisioner started")
	pc.Run(stopCh)
	logrus.Debug("Provisioner stopped")
	return nil
}

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	app := cli.App("local-zfs-provisioner", "Local ZFS Provisioner")

	app.Version("version", Version)
	app.Spec = "[-d]"

	var (
		debug = app.BoolOpt("d debug", false, "enable debug logging level")
	)

	app.Before = func() {
		if *debug {
			logrus.SetLevel(logrus.DebugLevel)
		}
	}

	app.Command("controller", "start controller", func(cmd *cli.Cmd) {
		var (
			configFile      = cmd.StringOpt("config", "", "Provisioner configuration file.")
			datasetMountDir = cmd.StringOpt("dataset-mount-dir", DefaultDatasetMountDir, "Directory under which to mount the created persistent volumes.")

			provisionerName = cmd.String(cli.StringOpt{
				Name:   "provisioner",
				Value:  DefaultProvisionerName,
				Desc:   "Specify Provisioner name.",
				EnvVar: "PROVISIONER_NAME",
			})
			namespace = cmd.String(cli.StringOpt{
				Name:   "namespace",
				Value:  DefaultNamespace,
				Desc:   "The namespace that Provisioner is running in.",
				EnvVar: "NAMESPACE",
			})
		)
		cmd.Spec = "--config [--dataset-mount-dir] [--provisioner] [--namespace]"
		cmd.Action = func() {
			if *configFile == "" {
				logrus.Fatalf("invalid empty flag %v", "config")
			}
			if *datasetMountDir == "" {
				logrus.Fatalf("invalid empty flag %v", "datasetMountDir")
			}
			if *provisionerName == "" {
				logrus.Fatalf("invalid empty flag %v", "provisioner")
			}
			if *namespace == "" {
				logrus.Fatalf("invalid empty flag %v", "namespace")
			}

			if err := startController(*configFile, *datasetMountDir, *provisionerName, *namespace); err != nil {
				logrus.Fatalf("Error starting provisioner: %v", err)
			}
		}
	})

	app.Command("dataset", "manage datasets", func(datasetCmd *cli.Cmd) {
		datasetCmd.Command("create", "create dataset", func(cmd *cli.Cmd) {
			var (
				datasetName = cmd.StringArg("DATASET", "", "Name of the dataset")
				mountPoint  = cmd.StringArg("MOUNTPOINT", "", "Mountpoint of the dataset")
				quota       = cmd.StringOpt("quota", "", "Quota of the dataset")
				parent      = cmd.StringOpt("parent", "", "Parent dataset under which to create datasets")
			)
			cmd.Spec = "--parent [--quota] DATASET MOUNTPOINT"
			cmd.Action = func() {
				fmt.Println("dataset: ", *datasetName)
				fmt.Println("mountpoint: ", *mountPoint)
				fmt.Println("parent: ", *parent)
				fmt.Println("quota: ", *quota)

				// Ensure parent dataset exists.
				_, err := zfs.GetDataset(*parent)
				if err != nil {
					_, err = zfs.CreateFilesystem(*parent, map[string]string{
						"mountpoint": "legacy",
					})
					if err != nil {
						logrus.Fatalf("Failed to create parent dataset %v: %v", *parent, err)
					}
				}

				zfsCreateProperties := map[string]string{
					"quota":      *quota,
					"mountpoint": *mountPoint,
				}

				// TODO: could the dataset already exist? Would that be a valid use case?
				//		 e.g. depending on the PersistentVolumeReclaimPolicy?
				//dataset, err = zfs.CreateFilesystem(datasetName, zfsCreateProperties)
				_, err = zfs.CreateFilesystem(*datasetName, zfsCreateProperties)
				if err != nil {
					logrus.Fatalf("Failed to create dataset %v: %v", *datasetName, err)
				}

				// It seems the dataset is mounted at creation time.
				// We do not have to explicitly mount it.
				//dataset, err = dataset.Mount(false, nil)
				//if err != nil {
				//	logrus.Fatalf("Failed to mount dataset %v at %v: %v", *datasetName, *mountPoint, err)
				//  return nil, err
				//}

				logrus.Infof("Created and mounted dataset %v at %v", *datasetName, *mountPoint)
			}
		})

		datasetCmd.Command("destroy", "destroy dataset", func(cmd *cli.Cmd) {
			var (
				datasetName = cmd.StringArg("DATASET", "", "Name of the dataset")
				mountPoint  = cmd.StringArg("MOUNTPOINT", "", "Mountpoint of the dataset")
			)
			cmd.Spec = "DATASET MOUNTPOINT"
			cmd.Action = func() {
				fmt.Println("dataset: ", *datasetName)
				fmt.Println("mountpoint: ", *mountPoint)

				// Destroy the dataset.
				dataset, err := zfs.GetDataset(*datasetName)
				if err != nil {
					// TODO: should this error be ingored?
					logrus.Fatalf("Failed to get dataset %v: %v", *datasetName, err)
				}
				err = dataset.Destroy(zfs.DestroyDefault)
				if err != nil {
					logrus.Fatalf("Failed to delete dataset %v: %v", *datasetName, err)
				}
				// Also delete the mountpoint.
				os.Remove(*mountPoint)
				logrus.Infof("Deleted dataset %v", *datasetName)
			}
		})
	})

	app.Run(os.Args)

}
