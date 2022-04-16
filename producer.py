#!/usr/bin/env python

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from uuid import uuid4
import json
import ccloud_lib

class Photo(object):
    """
    Photo record
    Args:
        "albumId": "id of the album",
		"id": "id of the photo",
		"title": "Photo title",
		"url": "Photo Url",
		"thumbnailUrl": "Thumbnail Url"
    """
    def __init__(self, albumId, id, title, url, thumbnailUrl):
        self.albumId = albumId
        self.id = id
        self.title = title
        self.url = url
        self.thumbnailUrl = thumbnailUrl

def photo_to_dict(photo, ctx):
    return dict(albumId=photo.albumId, id=photo.id, title=photo.title, url=photo.url, thumbnailUrl=photo.thumbnailUrl)

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for Photo record {}: {}".format(msg.key(), err))
        return
    print('Photo record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
		
		
if __name__ == '__main__':
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "Photo",
      "description": "Random Photo placeholder Metadata",
      "type": "object",
      "properties": {
        "albumId": {
          "description": "Album Id",
          "type": "number"
        },
        "id": {
          "description": "Photo Id",
          "type": "number"
        },
        "title": {
          "description": "Title of Photo",
          "type": "string"
        },
        "url": {
          "description": "url of photo",
          "type": "string"
        },
        "thumbnailUrl": {
          "description": "Thumbnail url",
          "type": "string"
        }
      },
      "required": [ "albumId", "id", "title", "url", "thumbnailUrl" ]
    }
    """
    
    #Create Producer Instance
    schema_registry_conf = {'url': conf['schema.registry.url']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema_str, schema_registry_client, photo_to_dict)

    producer_conf = {'bootstrap.servers': conf['bootstrap.servers'],
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)
    
    jsonData = open('bcsample.json')
    data = json.load(jsonData)
    
    for i in range(len(data)):
        photoData = data[i]
        photo = Photo(albumId=photoData['albumId'], id=photoData['id'],title=photoData['title'], url=photoData['url'], thumbnailUrl=photoData['thumbnailUrl'])
        
        producer.produce(topic=topic, key=str(uuid4()), value=photo, on_delivery=delivery_report)
        
    print("\nFlushing records...")
    producer.flush()
