## Compile Kinesis Data Producer

```
mvn clean install package 
```


## Trigger Kinesis Producer

It needs 4 Arguments for execution 

```
GrabKinensProducer <STREAM_NAME>  <TYPE OF USER>  <REGION NAME> <DATA CSV PATH>
``` 

### Trigger Driver Data
mvn exec:java -Dexec.mainClass="com.grab.kinesis.KinesisProducer.GrabKinensProducer"  -Dexec.args="test driver eu-central-1 /usr/data/driver.csv"

#### Triger Passanger Data

mvn exec:java -Dexec.mainClass="com.grab.kinesis.KinesisProducer.GrabKinensProducer"  -Dexec.args="test passanger eu-central-1 /usr/data/passanger.csv"

