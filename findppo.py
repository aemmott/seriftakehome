import smart_open
import json_stream
from json_stream.dump import default as stream_enc_default
import json
from collections import defaultdict as ddict
import sys

def isolate_ppo(state, uri):
    uri_stream = smart_open.open(uri)
    data_stream = json_stream.load(uri_stream)
    reporting_structure_stream = data_stream['reporting_structure']
    decoder = json.JSONDecoder()
    for ro_streamed in reporting_structure_stream:
        ro_dump = json.dumps(ro_streamed, default=stream_enc_default)
        ro = ddict(list, decoder.decode(ro_dump))
        has_ppo = False
        for p in ro['reporting_plans']:
            plan_name = p['plan_name'].split('-', 1)[0]
            name_tokens = plan_name.split(' ')
            if state in name_tokens and 'PPO' in name_tokens:
                has_ppo = True
                break
        if has_ppo:
            cur_mrfs = set([f['location'] for f in ro['in_network_files'] if f['description'] == 'In-Network Negotiated Rates Files'])
            for mrf in cur_mrfs:
                print(mrf.split('?',1)[0].split('/')[-1])
            break

if __name__ == "__main__":
    state = 'NY'
    uri = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-02-01_anthem_index.json.gz'
    if len(sys.argv) > 1:
        state = sys.argv[1]
    if len(sys.argv) > 2:
        uri = sys.argv[2]
    isolate_ppo(state, uri)