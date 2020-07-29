## Apache Spark & Kafka on K8S

This prototyp platform was developed as a part of the study project https://github.com/Thuridus/Big-Data 

The Lambda architecture was chosen for the proptytp.
For pros and cons regarding Lambda and Kappa architecture refer to e.g  https://towardsdatascience.com/a-brief-introduction-to-two-data-processing-architectures-lambda-and-kappa-for-big-data-4f35c28005bb

The aim of the project is to provide an correlative illustration between the index "Deutsche Aktien Index" (DAX) and the COVID-19 infections or deaths.
*The data about COVID-19 infections is taken from https://opendata.ecdc.europa.eu/covid19
*The Frankfurt Stock Exchange data https://www.quandl.com/data/FSE-Frankfurt-Stock-Exchange



### Content 
 1. [Overview architecture](#overviewarch)
 2. [Installation](#install)
 


### Overview Architecture <a name="overviewarch"></a>
* The DAX and Covid data is stored in HDFS Cluster  
* The data actualisiation is triggerd by standalone python-import-pod and done with every hour 
* Apache Kafka producer notificate Pyspark App about arrival of new data 
* PySpark App access the new arrived data and stored the results in MySQL Database 
* Web Server queries data from database via API
* Memchached server caches executed queries
* The results of the queries are displayed in the GUI


## Installation guide <a name="install"></a>
### Prerequisites <a name="prereq"></a>
This Software is required to run prototype platform
This application is intended to be used on based Ubuntu-OS, but was successfully tested on MacOs and Win10.

```
minikube
helm
```

### Automatic Installation
Installation scripts cab be used to install and deinstall the prototyp platform

The Base-Directory contains a script to setup the whole environment.
There has to be a empty minikube cluster with Ingress-Addon enabled on your system. [Start Minikube](#minikube)
After minikube has started, execute the installation script.
```
sh install_system.sh
```
To deinstall the prototyp
```
sh uninstall_system.sh
```

### Manual installation
Alternatively this system can be installed by executing the necessary commands manually.
The following sections contain the commands that need to be executed to start the components.
The components need to be started in the following order:
1. [Start Minikube](#minikube)
2. [Kafka Cluster](#kafkacluster)
3. [HDFS](#k8s)
4. [MYSQLDB and MemcacheD](#deploydb)
5. [Start Spark](#sparkk8s)
6. [User-Interface](#deployinterface)

### Starting minikube <a name="minikube"></a>

Start minikube with driver depending on your OS.
```
#on Win 10
minikube start --driver=hyperv --memory 5120 --cpus 4

#on Mac
minikube start --vm-driver=hyperkit --memory 5120 --cpus 4

#on Ubuntu
minikube start --driver=none
```

Enable Load Balancer (Ingress)
```
minikube addons enable ingress
```

Pointing Docker daemon to minikube registry (Not needed on Ubuntu)
```
#on Win 10
minikube docker-env
minikube -p minikube docker-env | Invoke-Expression

#on Mac
eval $(minikube docker-env)
```

### Deploy Kafka cluster on K8S: <a name="kafkacluster"></a>
Install Strimzi operator and Cluster Definition.
Execute the following commands to install kafka cluster.
Alternatively run 'sh install_kafka.sh' in the ../kafka-config folder to install whole kafka component.
```
#Install strimzi operator via Helm
helm repo add strimzi http://strimzi.io/charts/
helm install kafka-operator strimzi/strimzi-kafka-operator
```
Apply Kafka Cluster Deployment
```
kubectl apply -f kafka-cluster-def.yaml
```

## Deploy HDFS on K8S: <a name="k8s"></a>
### Deploying HDFS 
Execute the following commands to install the apache hadoop distributed filesystem component.
Alternatively run 'sh install_hdfs.sh' in the ../python_hdfs folder to install whole hdfs component.

```
#Add helm stable repo and install chart for stable hadoop cluster
helm repo add stable https://kubernetes-charts.storage.googleapis.com/
# if using helm for the first time run "helm init"
helm install --namespace=default --set hdfs.dataNode.replicas=2 --set yarn.nodeManager.replicas=1 --set hdfs.webhdfs.enabled=true hadoop stable/hadoop

#Adjust user rights on root folder to be able to acces filesystem via WEBHDFS
kubectl exec -ti hadoop-hadoop-yarn-rm-0 -- hdfs dfs -chmod  777 /
```

### Install Apache Knox - REST API and Application Gateway
Install Apachae Knox API and Gateway using helm chart pfisterer/apache-knox-helm
```
helm repo add pfisterer-knox https://pfisterer.github.io/apache-knox-helm/
helm install --set "knox.hadoop.nameNodeUrl=hdfs://hadoop-hadoop-hdfs-nn:9000/" --set "knox.hadoop.resourceManagerUrl=http://hadoop-hadoop-yarn-rm:8088/ws" --set "knox.hadoop.webHdfsUrl=http://hadoop-hadoop-hdfs-nn:50070/webhdfs/" --set "knox.hadoop.hdfsUIUrl=http://hadoop-hadoop-hdfs-nn:50070" --set "knox.hadoop.yarnUIUrl=http://hadoop-hadoop-yarn-ui:8088" --set "knox.servicetype=LoadBalancer" knox pfisterer-knox/apache-knox-helm
```

### Deploy the import pod on K8S:
Create necessary docker image for Data import POD
```
# After running the docker-env command, navigate to the python_hdfs directory (it contains one dockerfile)
# the created image will connect to the knox-apache-knox-helm-svc via DNS Lookup within the K8S cluster
docker build -t python_download .

#Apply the import deployment inside the python_hdfs directory
kubectl apply -f python_import_deployment.yml
```


### Deploy the database <a name="deploydb"></a>
### Create necessary DB-Pod

Build a connection to the "my-database" folder
```
#navigate to the folder my-database
cd my-database
```

Create a POD and deploy it to minikube
```
kubectl apply -f config-app.yml
kubectl apply -f my-mysql-deployment.yml
```
```
#To be sure that the POD is running and to get the POD-name, enter
kubectl get pods -o wide
#You have to choose the POD with the name my-mysql-deployment-xxxxxxxxx-xxxxx (x=numbers/characters)
```

```
#Enter the pod to check if its working
kubectl exec -ti [my-mysql-deployment-xxxxxxxxx-xxxxx] -- mysql -u root --password=mysecretpw
```
### Create necessary Memcached-Pod

Create a POD and deploy it to minikube
```
kubectl apply -f my-memcache-deployment.yml
```
## Deploy Spark on K8S <a name="sparkk8s"></a>
### Spark Operator
Execute the following commands to install pyspark.
Alternatively run 'sh install_pyspark.sh' in the ../pyspark-app folder to install whole pyspark component.
```
#navigate to the folder "pyspark-app"
#Create a namespace with the name ‘spark-operator’
kubectl create namespace spark-operator

#create a service account with the name ‘spark’
kubectl create serviceaccount spark --namespace=default

#create RBAC role for spark serviceaccount
kubectl create clusterrolebinding spark-operator-role --clusterrole=cluster-admin --serviceaccount=default:spark --namespace=default

#add helm repo
helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator

#install helm chart incubator spark-operator
helm install spark incubator/sparkoperator --namespace spark-operator --set sparkJobNamespace=default
```

### Spark Controller Pod
This pod automatically reacts to kafka messages and starts a custom spark-drive resource via the Kubernetes API
The result of the calculation is then stored to the Database
```
# Create clusterrolebinding for spark controller pod
kubectl create clusterrolebinding default-edit-role --clusterrole=cluster-admin --serviceaccount=default:default --namespace=default

# Create docker image for driver
docker build -t spark_control .

# run deployment
kubectl apply -f spark_control_deployment.yml
```

## Start User-Interface <a name="deployinterface"></a>

Check if service is running on minikube
```
minikube dashboard
```

Navigate to /app/
```
cd ../app/
```

Build Interface-Dockerfile and run interface-deployment
```
docker build -t interface .
```

Run interface-Deployment
```
kubectl apply -f interface-deployment.yml
```
