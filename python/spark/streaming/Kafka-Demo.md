
```
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pykafka import KafkaClient
import msgpack


# 定义广播变量结合
def getParamDict(sparkContext):
    if ('paramDict' not in globals()):
        globals()['paramDict'] = sparkContext.broadcast({xxx,xxx})
    return globals()['paramDict']
    

# 外部输出实现
def output(rdd):
 
    paramDict = getParamDict(rdd.context)
 
    def handler(partition):
        # 获取kafka或是外部持久化系统的连接
        mp[topic] = client.topics[topic].get_producer(True, linger_ms=0, max_request_size=10*1024*1024)
  
        for data in p:
            # 针对单条的处理逻辑
            ...
            
            # 单条发送
            mp[topic].produce(xxx)

    rdd.foreachPartition(handler)
    

# 定义ssc
def functionToCreateContext(checkpointDirectory):
    
    sconf = SparkConf().setAppName("Daas")\
                       .set("spark.master", "yarn")\
                       .set("spark.submit.deployMode", "cluster")\
                       .set("spark.driver.memory", "1g")\
                       .set("spark.driver.cores", "1")\
                       .set("spark.streaming.backpressure.enabled", "true")\
                       .set("spark.streaming.kafka.maxRatePerPartition", "10")\
                       .set("spark.dynamicAllocation.enabled", "false")\
                       .set("spark.executor.heartbeatInterval", "1s")
    sc = SparkContext(conf=sconf)
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint(checkpointDirectory)
    
    # 获取广播变量
    paramDict = getParamDict(sc);
    
    fetch_len = 1024 * 1024 * 40
    kafkaStreams = KafkaUtils.createDirectStream(ssc,
                                                 topic,
                                                 kafkaParams={"metadata.broker.list":broker_list,
                                                              "auto.offset.reset": "smallest",
                                                              "auto.commit.interval.ms": "5000",
                                                              "fetch.message.max.bytes": str(fetch_len),
                                                              "group.id": groupid})
             
    # 针对DStream的处理         
    kafkaStreams.flatMap(xxx) # filter等等         
    
    # 输出到文件
    #kafkaStreams.saveAsTextFiles('outputdir', 'txt') # saveAsHadoopFiles ...
       
    # 输出到外部系统（也可包含处理）
    kafkaStreams.foreachRDD(output)
    
    return ssc


def main(groupid, checkpointDirectory):
    topic = ['test']
    broker_list = 'host:9092'
    
    # 实现checkpoint的ssc创建
    ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext(checkpointDirectory))
    #ssc = StreamingContext(sc, 20)
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    groupid = 'test_groupid'
    checkpointDirectory = '/spark/streaming/checkpoint/xxxxx'
    main(groupid, checkpointDirectory)

```
