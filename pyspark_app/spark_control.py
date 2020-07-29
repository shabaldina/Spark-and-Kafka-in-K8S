# Install: pip3 install kafka-python
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from io import StringIO
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
import pywebhdfs.webhdfs
import socket
import requests
import mysql.connector
import pandas
import threading
import time
import csv
import yaml
import numpy


class SparkDriverJob:
    def __init__(self, hdfs_connection, db_host, db_port, db_user, db_pw, db_name):
        self.ExecutionActive = False
        self.hdfsconnection = hdfs_connection
        self.dbhost = db_host
        self.dbport = db_port
        self.dbuser = db_user
        self.dbpw = db_pw
        self.dbname = db_name
        
    def ClearEnv(self):
        self.hdfsconnection.delete_file_dir("/app", recursive=True)
        self.hdfsconnection.delete_file_dir("/config", recursive=True)
        self.hdfsconnection.delete_file_dir("/result", recursive=True)

    def InitEnv(self):
        print("Creating environment")
        # Create the app folder on hdfs and store file to it
        driverurl = 'https://raw.githubusercontent.com/Thuridus/Big-Data/develop/pyspark-app/pyspark_driver.py'
        drivercontent = requests.get(driverurl).content
        self.hdfsconnection.make_dir("/app", permission=777)
        self.hdfsconnection.create_file("/app/pyspark_driver.py", drivercontent)
        print("Stored pyspark_driver.py on HDFS")
        # Create the config folder on hdfs and store file to it
        deploymenturl = 'https://raw.githubusercontent.com/Thuridus/Big-Data/develop/pyspark-app/python_deployment.yml'
        deploymentcontent = requests.get(deploymenturl).content
        self.hdfsconnection.make_dir("/config", permission=777)
        self.hdfsconnection.create_file("config/python_deployment.yml", deploymentcontent)
        print("Stored deployment.yml on HDFS")
        # Create the result directory
        self.hdfsconnection.make_dir("/result/corona", permission=777)
        self.hdfsconnection.make_dir("/result/dax", permission=777)
        
    # Executes the whole spark logic once
    def RunOnce(self):
        self.StartSparkExecution()

    # Is called whenever a spark execution has to be started
    def StartSparkExecution(self):
        if self.ExecutionActive == False:
            self.ExecutionActive = True
        else:
            print("Spark is still running")
            return

        
        # read python custom resource deployment from HDFS
        yamlfile = self.hdfsconnection.read_file("/config/python_deployment.yml").decode()
        yamlobj = yaml.load(yamlfile)
        group = str(yamlobj["apiVersion"]).split('/')[0]
        version = str(yamlobj["apiVersion"]).split('/')[1]

        # load incluster kubeconfig
        config.load_incluster_config()
        configuration = client.Configuration()
        # init a connection to the custom objects kubernetes API
        k8sapi = client.CustomObjectsApi(client.ApiClient(configuration))
        # init a connection to the V1 kubernetes API
        v1 = client.CoreV1Api(client.ApiClient(configuration))
        try:
            # delete latest output files by recreating output directory
            # We only want one set of outputfiles to exist
            self.hdfsconnection.delete_file_dir("/result", recursive=True)
            self.hdfsconnection.make_dir("/result/corona", permission=777)
            self.hdfsconnection.make_dir("/result/dax", permission=777)
            print("Try to delete existing driver")
            
            # ensure that resource is deleted
            # executor doesnt get deleted automatically
            k8sapi.delete_namespaced_custom_object(group=group, version=version,plural="sparkapplications", name="python-spark", namespace="default", body=client.V1DeleteOptions())
        except ApiException as exception:
            print(exception)

        driverSuccessfull = False
        driverrunning = True
        try:
            print("Try to create driver pod")
            # Create spark executor
            k8sapi.create_namespaced_custom_object(group=group, version=version, namespace="default", plural="sparkapplications", body=yamlobj)
            print("Driver pod created")

            # wait until executor is terminated
            while driverrunning:
                ret = v1.list_pod_for_all_namespaces(watch=False)
                for i in ret.items:
                    if i.metadata.name == "python-spark-driver":
                        if i.status.container_statuses[0].state.terminated == None:
                            driverrunning = True
                        else:
                            print("Driver pod is terminated. Status: " + str(i.status.container_statuses[0].state.terminated))
                            # Executor is terminated evaluate exit code
                            if i.status.container_statuses[0].state.terminated.exit_code == 0:
                                driverSuccessfull = True
                            driverrunning = False
                        break
                if driverrunning:
                    print("Wait for driver pod to finish")
                    time.sleep(5)

            #Delete driver        
            print("Try to delete driver pod")
            k8sapi.delete_namespaced_custom_object(group=group, version=version,plural="sparkapplications", name="python-spark", namespace="default", body=client.V1DeleteOptions())
            print("Driver pod deleted")
        except ApiException as exception:
            print(exception)
        
        if driverSuccessfull:
            # Export result to DB if executor termninated successfully
            self.ExportResultToDB()
        self.ExecutionActive = False

    def ExportResultToDB(self):
        # clear db before inserting new data
        dbconn = mysql.connector.connect(host=self.dbhost, port=self.dbport, user=self.dbuser, password=self.dbpw, database=self.dbname, auth_plugin='mysql_native_password')
        dbcursor = dbconn.cursor()
        dbcursor.execute('DELETE FROM infects')
        dbcursor.execute('DELETE FROM dax')
        dbcursor.close()
        dbconn.close()

        #read results from hdfs into datafram
        coronafilename = ''
        coronadirstat = self.hdfsconnection.list_dir("/result/corona/")['FileStatuses']['FileStatus']
        for dirstat in coronadirstat:
            if dirstat['pathSuffix'] != '_SUCCESS':
                coronafilename = dirstat['pathSuffix']
                break
        
        daxfilename = ''
        daxdirstat = self.hdfsconnection.list_dir("/result/dax/")['FileStatuses']['FileStatus']
        for dirstat in daxdirstat:
            if dirstat['pathSuffix'] != '_SUCCESS':
                daxfilename = dirstat['pathSuffix']
                break

        # replace empty values with zeros
        covidcsv = self.hdfsconnection.read_file("/result/corona/" + coronafilename).decode()
        dataframecovid = pandas.read_csv(StringIO(covidcsv), index_col='date', keep_default_na=False)
        for column in dataframecovid.columns:
            dataframecovid[column] = dataframecovid[column].replace('', numpy.nan, regex=True)
            dataframecovid[column] = dataframecovid[column].fillna(0)

        # rename columns to match the DB layout
        daxcsv = self.hdfsconnection.read_file("/result/dax/" + daxfilename).decode()
        dataframedax = pandas.read_csv(StringIO(daxcsv), index_col='Date')
        dataframedax = dataframedax.rename(columns={"Date" : "date", "open_sum" : "open", "close_sum" : "close", "abs_diff": "diff"})
        dataframedax.index.names = ["date"]
        
        # Insert into db with pandas-dataframe build in function
        dbengine =  create_engine('mysql+pymysql://{user}:{pw}@{host}:{port}/{db}'.format(user=self.dbuser, pw=self.dbpw, db=self.dbname, port=self.dbport, host=self.dbhost))
        with dbengine.connect() as dbconnection:
            dataframecovid.to_sql('infects', dbconnection, if_exists='append')
            dataframedax.to_sql('dax', dbconnection, if_exists='append')
            print("New data successfully inserted")

    # Prints the values of the DB tables infects and dax
    def ValidateExport(self):
        dbtest = mysql.connector.connect(host=self.dbhost, port=self.dbport, user=self.dbuser, password=self.dbpw, database=self.dbname, auth_plugin='mysql_native_password')
        cursor = dbtest.cursor()
        with open('/root/Desktop/github_repo/kafka-config/testout.txt', 'w') as fileop:
            cursor.execute("SELECT * FROM infects")
            for val in cursor:
                print(val, file=fileop)
            cursor.close()
            cursor = dbtest.cursor()
            cursor.execute('SELECT * FROM dax')
            for val in cursor:
                print(val)
            cursor.close()
            dbtest.close()




# Connect to HDFS to gather result data
hdfsweburl = "http://" + str(socket.gethostbyname("knox-apache-knox-helm-svc")) + ":8080"
hdfsconn = pywebhdfs.webhdfs.PyWebHdfsClient(base_uri_pattern=f"{hdfsweburl}/webhdfs/v1/",
                                         request_extra_opts={'verify': False, 'auth': ('admin', 'admin-password')})

# The bootstrap server to connect to
bootstrap = 'my-cluster-kafka-bootstrap:9092'

# Create a comsumer instance
print('Starting KafkaConsumer')
consumer = KafkaConsumer('spark_notification', bootstrap_servers='my-cluster-kafka-bootstrap:9092')

# Get hostname of mysql server
mysqlhost = str(socket.gethostbyname("my-app-mysql-service"))

# Init driver job class
driverjob = SparkDriverJob(hdfsconn, mysqlhost, 3306, 'root', 'mysecretpw', 'mysqldb')
driverjob.ClearEnv()
driverjob.InitEnv()

# on first startup -> start spark execution once
print("start spark execution on fist start")
try:
    driverjob.RunOnce()
    print("First run successfully finished")
except ApiException as exception:
    print("Exeption on first run has occured")
    print(exception)

# Endless loop
while True:
    print("Waiting for kafka notifications")
    # react to every received kafka message
    for msg in consumer:
        print("Message Received: ", msg)
        message = str(msg.value.decode())
        # If message has the correct value -> Start spark execution
        if message == 'new_data_available':
            print("Received import notification from import pod. Starting Spark execution.")
            driverjob.RunOnce()
            print("Driver Job finished. New Data in available in DB")

print("Programm exited")
