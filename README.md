## Surge Price Calculator 

Surge price in taxi travel industry are introduced based on supply and demand ration.

This project contains three high level module for imlpementation.

1. Custom Kinesis Producer : Generates random customer taxi booking request data with langitude and lattitude.
2. Spark Streaming Application : 10 mins of window processing time for calculating surge price.
3. ElasticSearch+Kibana : Used for storage and visualization.

### Step by Step Guide

1. Luanch EMR cluster from Amazon Console.

#### Login to EC2

```
	ssh -i ~/Downloads/mykey.pem  ec2-user@<Public IP>
```

Switch to Root
	sudo bash


#### Install Git tool

```
	yum install git

```


#### Install Wget

```
	yum install wget
```

#### Install glibc Dependency

```
	yum install glibc.i686
```


Optional : Install JDK

	--Download JDK
	wget https://download.oracle.com/otn-pub/java/jdk/8u191-b12/2787e4a523244c269598db4e85c51e0c/jdk-8u191-linux-i586.tar.gz?AuthParam=1543329360_c990d072ce23c58f138f35501e03f5cc


	sudo alternatives --install /usr/bin/java java /usr/local/java/jdk1.8.0_191/bin/java 2
	sudo alternatives --config java

	sudo alternatives --install /usr/bin/jar jar /usr/local/java/jdk1.8.0_191/bin/jar 2
	sudo alternatives --set jar /usr/local/java/jdk1.8.0_191/bin/jar
	sudo alternatives --set javac /usr/local/java/jdk1.8.0_191/bin/javac

	--Check Java Version
		java -version
	
Optional :  Install Maven
	
	mkdir -p /usr/local/mvn
	cd /usr/local/mvn
	wget https://www-eu.apache.org/dist/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz
	tar -xvf apache-maven-3.6.0-bin.tar.gz

	vi ~/.bash_rc

	export M2_HOME="/usr/local/mvn/apache-maven-3.6.0"
	export M2=$M2_HOME/bin
	export PATH=$M2:$PATH

	source ~/.bash_rc


###Setting AWS Credentials:

```
mkdir ~/.aws/
touch  ~/.aws/credentials

vim ~/.aws/credentials

[default]
aws_secret_access_key=<SECRET KEY>
aws_access_key_id=<KEY>

```

### Launch Spark Job

```
spark-submit   --class surgepricepredictor.SparkStreamingGrabSurgePriceCalculator   --master yarn --deploy-mode client    target/SparkSurgePriceCalculator-0.1-SNAPSHOT.jar
```


Contributors :
	Shashi Vishwakarma

