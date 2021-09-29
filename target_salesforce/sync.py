import pkg_resources
from jsonschema.validators import Draft4Validator
import singer
from singer import utils
from target_salesforce.client import SalesforceClient
import json
import sys

LOGGER = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()
        
def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    
    salesforce_client = SalesforceClient(config)

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        
        message_type = o['type']

        if message_type == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            schema = schemas[o['stream']]

            # Validate record
            validators[o['stream']].validate(o['record'])
            
            salesforce_client.upsert(o)
            
            state = None

        elif message_type == 'STATE':
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']

        elif message_type == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']

        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    salesforce_client.flush()
    
    return state