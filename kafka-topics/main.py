from confluent_kafka.admin import AdminClient, NewTopic
import os
import json

def lambda_handler(event, context):
    data = json.dumps(event)
    config = json.loads(data)
    admin_client = AdminClient({"bootstrap.servers": os.environ.get('BROKERS')})
    for t in config:
         print(os.environ.get('BROKERS'))
         topic_list = []
         topic = t["topic"]
         partitions = t["partitions"]
         replicas = t["replicas"]
         topic_list.append(NewTopic(topic, num_partitions=partitions, replication_factor=replicas))
         fs = admin_client.create_topics(topic_list)
         for topic, f in fs.items():
             try:
                 f.result()
                 print("TOPIC {} CREATED [OK]".format(topic))
             except Exception as e:
                 print("FAILED TO CREATE TOPIC {}: {}  [FAIL]".format(topic, e))

    md = admin_client.list_topics(timeout=10)
    print(" {} topics:".format(len(md.topics)))
    for t in iter(md.topics.values()):
        print (t)
    return None