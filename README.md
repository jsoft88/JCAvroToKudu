# JCAvroToKudu
Implementation of a kafka application to take data from avro data files and insert them into a kudu table. It uses two libraries: 
<a href="https://github.com/jsoft88/JCGenericKafka">JCGenericKafka</a> for implementing kafka producer and consumers, and 
<a href="https://github.com/jsoft88/KuduHelper">KuduHelper</a> to allow consumers to insert records into a kudu table.
<br/><br/>
In the package two main classes are available: org.jc.avrotokudu.main.StartProducers and org.jc.avrotokudu.main.StartConsumers, used to
start producers and consumers, respectively.
<br/>
It is important to mention that this is an use case for GenericKafka library and a great example.
<br/><br/>
<b>StartProducers Usage:</b><br/>
java -cp JCAvroToKudu.jar org.jc.avrotokudu.main.StartProducers &lt;numberOfFiles&gt; &lt;fileInHdfs1,fileInHdfs2,fileInHdfs3,...,fileInHdfsNumOfFiles&gt; 
&lt;fieldToUseAsPartition&gt; &lt;linesToReadPerWorker&gt; &lt;maxNumOfWorkersPerFile&gt; &lt;broker1,broker2&gt; &lt;FQCNOfPartitioner&gt; 
&lt;topic&gt;
<br/><br/>
<b>Explanation:</b>
<ul>
  <li><b>numberOfFiles.</b> Number of files from where avro records are to be read.</li>
  <li><b>fileInHdfs1,fileInHdfs2,fileInHdfs3,...</b> List all the files separated by commas, do not include spaces</li>
  <li><b>fieldToUseAsPartition.</b>What's the key that will be searched inside a record, to retrieve its value and use that value
      as the key to obtain the partition of the topic where a given message will be posted.</li>
  <li><b>linesToReadPerWorker.</b> Each file is splitted into chunks delimited by starting line number and final line number, controlled by the number of lines each worker will read from each file.</li>
  <li><b>maxNumOfWorkersPerFile.</b> Number of workers per file.</li>
  <li><b>broker1,broker2.</b>Provide exactly the addresses of two brokers separated by comma, without spaces.</li>
  <li><b>FQCNOfPartitioner.</b>Fully qualified name of the class to be used as a partitioner.</li>
  <li><b>topic.</b> The topic where messages will be posted</li>
</ul>
<br/>
<b>Example:</b><br/>
java -cp JCAvroToKudu.jar org.jc.avrotokudu.main.StartProducers 1 hdfs://host:port/path/to/file/file1.avsc registration_date 100 3 \<br/>
broker1:port,broker2:port org.jc.avrotokudu.producer.DataPartitioner students
<br/><br/>
<b>StartConsumers Usage:</b><br/>
java -cp JCAvroToKudu.jar org.jc.avrotokudu.main.StartConsumers &lt;maxNumberOfConsumers&gt; &lt;consumerGroup&gt; &lt;topic&gt; &lt;consumerPort&gt; &lt;zookeeper:port&gt; &lt;commaSeparatedBrokers&gt; &lt;maxRetriesOnEmptyPartition&gt; &lt;kuduTableName&gt; &lt;commaSeparatedMasterAddresses&gt;
<br/><br/>
<b>Explanation:</b>
<ul>
  <li><b>maxNumberOfConsumers.</b> Maximum number of consumers, this would normally match the number of partitions of the topic.</li>
  <li><b>consumerGroup.</b> Name of the consumer group.</li>
  <li><b>topic.</b> Topic from which messages are to be consumed.</li>
  <li><b>consumerPort.</b> Port of the brokers where consumers take messages from.</li>
  <li><b>zookeeper:port</b> address of the zookeeper client</li>
  <li><b>commaSeparatedBrokers.</b> list at least two brokers (separated by comma without spaces) to find partition leaders and replicas. Only ip address, do not include port.</li>
  <li><b>maxRetriesOnEmptyPartition.</b> How many times is a consumer going to try to retrieve a message from a partition if it is empty.</li>
  <li><b>kuduTableName.</b>Name of the kudu table where records are to be inserted.</li>
  <li><b>commaSeparatedMasterAddresses.</b> List kudu master addresses separated by comma, without spaces.</li>
</ul>
<br/>
<b>Example:</b><br/>
java -cp JCAvroToKudu.jar org.jc.avrotokudu.main.StartConsumers 30 avroConsumers avroImport 9092 host:2181 xxx.xxx.xxx.xxx,yyy.yyy.yyy.yyy 10 students xyz.xyz.xyz.xyz:7051
<br/><br/>
<b>Notes:</b><br/>
I would recommend starting producers first and after a few seconds start consumers. This is just to avoid maxRetries from being exhausted.
