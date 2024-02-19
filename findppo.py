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
    file_id_set = set()
    set_init = False
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
            cur_ids = set()
            id_to_uri_map = ddict(set)
            for mrf in cur_mrfs:
                file_name = mrf.split('?',1)[0].split('/')[-1]
                name_tokens = file_name.split('_')
                if state == name_tokens[0]:
                    file_id = name_tokens[1]
                    cur_ids.add(file_id)
                    id_to_uri_map[file_id].add(mrf)
            if not set_init:
                set_init = True
                file_id_set = cur_ids
            else:
                file_id_set.intersection_update(cur_ids)
            if len(file_id_set) == 0:
                print('failure! empty intersection')
                exit(1)
            if len(file_id_set) == 1:
                for id in file_id_set:
                    for uri_output in id_to_uri_map[id]:
                        print(uri_output)
                    break
                break

if __name__ == "__main__":
    state = 'NY'
    uri = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-02-01_anthem_index.json.gz'
    if len(sys.argv) > 1:
        state = sys.argv[1]
    if len(sys.argv) > 2:
        uri = sys.argv[2]
    isolate_ppo(state, uri)