"""Producer base-class providing common utilites and functionality"""
#Note to self, producer code here is written in an abstract way and not specific to turnstyle or arrival info.
#This means it can be used in future for different producer use cases. 
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        
  
# * Zookeeper is required every time you need to use a Kafka topics command, it’s specified here because 2hen you call the producer.py you will need to access a topic and thus a zookeeper to manager the broker.
# * Broker list is required anytime you want to interact with a Kafka producer 
# * SCHEMA_REGISTRY_URL = A common pattern is to put the instances behind a single virtual IP or round robin DNS such that you can use a single URL in the schema.registry.url configuration but use the entire cluster of Schema Registry instances
#    * This points to the schema info that a topic will use
#    * It provides a centralise avro schema storage
#    * It stores state in a Kafka topic 
# Note all upper case naming as these will be set as constances as per PEP8


        self.broker_properties = {
            
            "ZOOKEEPER_URL" : "localhost:2181",
            "BROKER_URL" : "localhost:9092",
            "SCHEMA_REGISTRY_URL" : "http://localhost:8081" 
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
# Configure Avro producer search for exisitng schema & brokers
        self.producer = AvroProducer({
            'bootstrap.servers': self.broker_properties.get("BROKER_URL"),
            'schema.registry.url' : self.broker_properties.get("SCHEMA_REGISTRY_URL")
            },
            default_key_schema=key_schema, 
            default_value_schema=value_schema
         
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # admin client is needed to access broker info, 

        adminclient = AdminClient({'bootstrap.servers': self.broker_properties.get("BROKER_URL")})
        
        topics = adminclient.list_topics(timeout=15).topics     
        
        newtopic = NewTopic(
            self.topic_name, 
            num_partitions = self.num_partitions,
            replication_factor = self.num_replicas 
        )
        futures = adminclient.create_topics([newtopic])
        
        for topic,future in futures.items():
            try:
                future.result()
                logger.debug("topics created")
            except:
                logger.debug(f"failed to create topic {self.topic_name}")
        
        return

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush(timeout = 10)
        self.producer.close()
        return
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
