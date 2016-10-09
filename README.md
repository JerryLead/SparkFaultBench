# SparkFaultBench
We purpose to design a benchmark tool based on spark system which can test each module performance,stability and scalability through generating different data.Because of numbers of modules, we are not able to build testing for all spark modules. We decide to choose three typical modules which are Spark SQL, MLlib and GraphX to test. The whole procedure:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/procedure.jpg)

## Prepare system and environment
- Prepare machines and take one of them as Master and the others as slaves.  
- Edit the IP of machines in the cluster to a same segment.  
- Create a user “hadoop” for each machine.  
- Configure ssh so that Master can ssh to Slaves without password.  
- Intall JDK1.7 for each machine.  

## Install and configure the Yarn cluster
- Install and configurate Hadoop.  
- Install and configurate Scala.  
- Install and configurate Spark.  
- Open ~/.bashrc to edit environment variables for JAVA_HOME, SCALA_HOME and SPARK_HOME.

## Start Cluster
Start hadoop, yarn, hadoop historyserver and spark historyserver:

    $ start-all.sh  
    $ mr-jobhistory-daemon.sh start historyserver   
    $ $SPARK_HOME/sbin/start-history-server.sh

## Generate test data
The benchmark application has been integrated together as a plug-in for bench4Q, open the plug-in , fill the test name, select the Spark application and different types of applications , you can configure the required parameters, and then generate the data to local , and finally uploaded to the HDFS . Details as follows:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark1.jpg)  

#### Choose Spark Application
For example, choose application mllib:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark2.jpg)  

#### Select algorithm  
Select the algorithm of mllib，for example,  decisionTree:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark3.jpg)  

#### Configure parameters for application
Click “Add Parameters”, configure parameters for the application:   
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark4.jpg) 

#### Generate test file  
Click “Generate”，and generate .txt form test file in local:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark5.jpg)  

#### Upload data to HDFS 
Click “Upload” button, upload data to HDFS:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark6.jpg)  

## Test execution
On the benchmark, configurate parameters information during test execution , and run tests . After running , you can check the test report . Test report describes the test application type , data scale, cluster scale, application descriptions , test goal, the way of data generated. In addition , you can also link to Hadoop testing page , view the specific test conditions.  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark7.jpg)   

#### Set parameter information for running  
Fill in the parameter information needed to run, Including driver-memory，executor-memory，executor-core, class information and so on:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark8.jpg) 

#### Fill execution parameters
Fill execution parameters specific class need when running, including the required jar package , etc:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark9.jpg)   

#### Run Application
Click the “Run” button, you can run the application with information configured before:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark10.jpg)  

#### Form and view test report
Click the “Report” button, a test report would be formed and showed at the end of running:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark11.jpg)  
Test report:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark12.jpg)  

#### View details
Click the “details” button below , you can connect to Hadoop monitor screen and view details:  
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark13.jpg)   
![](https://github.com/JerryLead/SparkFaultBench/blob/master/benchReports/pictures/benchmark14.jpg)   

## Other materials
We provide the following documents in doc/materials/:
- Requirements_Document.docx  
- Design_Document.docx  
- User_Document.docx
- DataMagician_videoDemo.mp4

## License
Copyright 2016 DataMagician

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
