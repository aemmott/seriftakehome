import smart_open
import json_stream
from json_stream.dump import default as stream_enc_default
import json
from collections import defaultdict as ddict
import sys

def isolate_ppo(state, uri):
    print('Received state code: ' + state)
    print('Received uri: ' + uri)


if __name__ == "__main__":
    state = 'NY'
    uri = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-02-01_anthem_index.json.gz'
    if len(sys.argv) > 1:
        state = sys.argv[1]
    if len(sys.argv) > 2:
        uri = sys.argv[2]
    isolate_ppo(state, uri)