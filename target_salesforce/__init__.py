import io
import sys

import singer
from singer import utils

from target_salesforce.sync import persist_lines, emit_state

REQUIRED_CONFIG_KEYS = [
    'username',
    'password',
    'security_token',
    'mapping'
]

LOGGER = singer.get_logger()

@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    
    state = persist_lines(args.config, input)
        
    emit_state(state)
    LOGGER.debug("Exiting normally")


if __name__ == '__main__':
    main()
