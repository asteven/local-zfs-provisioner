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
	//pvController "sigs.k8s.io/sig-storage-lib-external-provisioner/controller"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	Version                = "0.0.1"
	DefaultProvisionerName = "asteven/local-zfs"
	DefaultNamespace       = "local-zfs-storage"
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

func startController(configFile string, provisionerName string, namespace string) error {
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

	//serverVersion, err := kubeClient.Discovery().ServerVersion()
	//if err != nil {
	//	return errors.Wrap(err, "Cannot start Provisioner: failed to get Kubernetes server version")
	//}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return fmt.Errorf("NODE_NAME environment variable not set")
	}

	fmt.Println("configFile: ", configFile)
	fmt.Println("provisionerName: ", provisionerName)
	fmt.Println("namespace: ", namespace)
	return nil

}

/*
	provisioner, err := NewProvisioner(stopCh, kubeClient, configFile, namespace, nodeName)
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
*/

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
			configFile = cmd.StringOpt("config", "", "Provisioner configuration file.")

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
		cmd.Spec = "--config"
		cmd.Action = func() {
			if *configFile == "" {
				logrus.Fatalf("invalid empty flag %v", "config")
			}
			if *provisionerName == "" {
				logrus.Fatalf("invalid empty flag %v", "provisioner")
			}
			if *namespace == "" {
				logrus.Fatalf("invalid empty flag %v", "namespace")
			}

			if err := startController(*configFile, *provisionerName, *namespace); err != nil {
				logrus.Fatalf("Error starting daemon: %v", err)
			}
		}
	})
	app.Command("dataset", "manage datasets", func(datasetCmd *cli.Cmd) {
		datasetCmd.Command("create", "create dataset", func(cmd *cli.Cmd) {
			var (
				dataset = cmd.StringArg("DATASET", "", "Name of the dataset to create")
				size    = cmd.StringOpt("size", "", "Size of the dataset")
			)
			cmd.Action = func() {
				fmt.Println("dataset: ", *dataset)
				fmt.Println("size: ", *size)
			}
		})
		datasetCmd.Command("destroy", "destroy dataset", func(cmd *cli.Cmd) {
			var (
				dataset = cmd.StringArg("DATASET", "", "Name of the dataset to destroy")
			)
			cmd.Action = func() {
				fmt.Println("dataset: ", *dataset)
			}
		})
	})

	app.Run(os.Args)

}
