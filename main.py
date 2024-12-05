#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  
#      http://www.apache.org/licenses/LICENSE-2.0
#  
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

'''
SAMPLE TOPIC PAYLOAD:
    {
        "timestamp": "1680869520.993",
        "alias": "Gnn6000_21-101-AGICODE",
        "value": 4.0,
        "quality": "GOOD"
    }

Code Considerations:
    1. Note that the value of timestamp should be a STRING, and should follow the epoch seconds format. 
       Example python code: 
    
        import time
        timestamp= str(time.time())
    
    2. Value of the key: "value" should be a FLOAT.
       Example python code: 
    
        value= float(4)
        
    3. Value of the key: "alias" should be a STRING, and this alias should be associated with an ASSET in AWS IoT SiteWise.
    4. Value of the key: "quality" should be a STRING, and one of 3 possible values: GOOD | BAD | UNCERTAIN
       Example python code: 
       
        quality= "UNCERTAIN"
    
'''


import sys
import time
import traceback
import json

from awsiot.greengrasscoreipc.clientv2 import GreengrassCoreIPCClientV2
from awsiot.greengrasscoreipc.model import (
    SubscriptionResponseMessage,
    UnauthorizedError
)

# Stream Manager imports
import asyncio
import calendar
import logging
import random
import time
import uuid
from stream_manager import (
    AssetPropertyValue,
    ExportDefinition,
    IoTSiteWiseConfig,
    MessageStreamDefinition,
    PutAssetPropertyValueEntry,
    Quality,
    ResourceNotFoundException,
    StrategyOnFull,
    StreamManagerClient,
    TimeInNanos,
    Variant,
)
from stream_manager.util import Util
import unicodedata

# Logging config
logging.basicConfig(level=logging.INFO)

# Convert string to a caseless format for comparison
def normalize_caseless(text):
    return unicodedata.normalize("NFKD", text.casefold())

# Function to publish payload to stream
def swe_stream_pub(alias, timestamp, value, quality="GOOD"):
    
    # Define time
    time_in_nanos= TimeInNanos(time_in_seconds=timestamp)
    # Defing value
    variant= Variant(double_value=value)
    # Define quality
    if normalize_caseless(quality)=="good":
        quality=Quality.GOOD
    elif normalize_caseless(quality)=="bad":
        quality=Quality.BAD
    elif normalize_caseless(quality)=="uncertain":
        quality=Quality.UNCERTAIN
        
    # Define asset entry
    asset = [AssetPropertyValue(value=variant, quality=quality, timestamp=time_in_nanos)]
    entry= PutAssetPropertyValueEntry(entry_id=str(uuid.uuid4()), property_alias=alias, property_values=asset)
    # Convert to JSON Bytes
    json_bytes= Util.validate_and_serialize_to_json_bytes(entry)
    # Append to stream
    sequence_number= sm_client.append_message(stream_name, json_bytes)
    
    # Confirm data entry success
    print('Appended message to stream_name: %s sequence_number: %s message: %s', stream_name, sequence_number, json_bytes.decode("utf-8"))
    
# Main function
def main(logger):
    args = sys.argv[1:]
    topic = args[0]
    global stream_name
    stream_name = args[1]

    try:
        
        ## Stream manager stream and client setup
        global sm_client
        sm_client = StreamManagerClient()
        # Try deleting the stream (if it exists) so that we have a fresh start
        try:
            sm_client.delete_message_stream(stream_name=stream_name)
        except ResourceNotFoundException:
            pass

        exports = ExportDefinition(
            iot_sitewise=[IoTSiteWiseConfig(identifier="IoTSiteWiseExport" + stream_name, batch_size=5)]
        )
        sm_client.create_message_stream(
            MessageStreamDefinition(
                name=stream_name, strategy_on_full=StrategyOnFull.OverwriteOldestData, export_definition=exports
            )
        )
        
        ## Subscribe to local topic
        global ipc_client
        ipc_client = GreengrassCoreIPCClientV2()
        # Subscription operations return a tuple with the response and the operation.
        _, operation = ipc_client.subscribe_to_topic(topic=topic, on_stream_event=on_stream_event,
                                                     on_stream_error=on_stream_error, on_stream_closed=on_stream_closed)
        print('Successfully subscribed to topic: ' + topic)

        
        # Keep the main thread alive, or the process will exit.
        try:
            while True:
                time.sleep(10)
        except InterruptedError:
            print('Subscribe interrupted.')

        # To stop subscribing, close the stream.
        operation.close()
    except UnauthorizedError:
        print('Unauthorized error while subscribing to topic: ' +
              topic, file=sys.stderr)
        traceback.print_exc()
        exit(1)
    except Exception:
        print('Exception occurred', file=sys.stderr)
        traceback.print_exc()
        exit(1)

def on_stream_event(event: SubscriptionResponseMessage) -> None:
    try:
        message = str(event.binary_message.message, 'utf-8')
        topic = event.binary_message.context.topic
        print('Received new message on topic %s: %s' % (topic, message))
        
        # Fetch required values from message payload
        message_json = json.loads(message)
        alias= message_json['alias']
        timestamp= int(float(message_json['timestamp']))
        value= float(message_json['value'])
        quality= message_json['quality']
        
        # Append message to Stream
        swe_stream_pub(alias, timestamp, value, quality)
    except:
        traceback.print_exc()


def on_stream_error(error: Exception) -> bool:
    print('Received a stream error.', file=sys.stderr)
    traceback.print_exc()
    return False  # Return True to close stream, False to keep stream open.


def on_stream_closed() -> None:
    print('Subscribe to topic stream closed.')


if __name__ == "__main__":
   main(logger=logging.getLogger())
