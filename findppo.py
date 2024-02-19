import smart_open
import json_stream
from json_stream.dump import default as stream_enc_default
import json
from collections import defaultdict as ddict
import sys

"""
Given a two-letter state code (AZ, CA, NY, etc) and a uri to a table of contents,
will print out the file uri for the PPO related to that state
"""
def isolate_ppo(state, uri):

    with smart_open.open(uri) as uri_stream: # Note that smart_open.open will infer the needed decompression from file extension
        # Set up object stream
        data_stream = json_stream.load(uri_stream)
        reporting_structure_stream = data_stream['reporting_structure']
        decoder = json.JSONDecoder()

        # We are keeping a set of id tokens across multiple plan sponsors until we can narrow them down to exactly one
        file_id_set = set()
        set_init = False # First time we encounter a state PPO plan we keep all matching id tokens; need to track first instance
        
        # This loop is a transient stream, and will exit early when the problem is solved
        for ro_streamed in reporting_structure_stream:

            # Instantiate the current reporting structure object
            ro_dump = json.dumps(ro_streamed, default=stream_enc_default)
            ro = ddict(list, decoder.decode(ro_dump))

            #Determine if current sponsor includes the state PPO
            has_ppo = False
            for p in ro['reporting_plans']:
                plan_name = ''
                if 'plan_name' in p.keys(): #This check is needed because some faulty objects missing the plan_name keys were encountered, despite being a required field
                    plan_name = p['plan_name'].split('-', 1)[0]
                name_tokens = plan_name.split(' ')
                if state in name_tokens and 'PPO' in name_tokens:
                    has_ppo = True
                    break #We do not need to check other plans from this sponsor if the condition is met once.

            # Identify possible identifiers among the PPO sponsors
            if has_ppo:

                # Get the raw location uris into a set
                cur_mrfs = set([f['location'] for f in ro['in_network_files'] if f['description'] == 'In-Network Negotiated Rates Files'])

                cur_ids = set() #Tracking discovered ids from this sponsor
                id_to_uri_map = ddict(set) #Organizing raw uris for output if solution is found
                
                # Loop over the raw locations
                for mrf in cur_mrfs:
                    file_name = mrf.split('?',1)[0].split('/')[-1] # This parses out the particular file name (drops parameters and full location)
                    
                    # Splitting on '_' is sort of a hack given our knowledge of the problem from the outside.
                    # A more general solution would cover a broader set of cases for identifying sets of files.
                    name_tokens = file_name.split('_') 
                    if state == name_tokens[0]: # Hacky olution knows that the first token matches the state code
                        file_id = name_tokens[1] #Hacky solution knows that the second token will be a unique identifier of the set of files.
                        cur_ids.add(file_id)
                        id_to_uri_map[file_id].add(mrf)

                # Simply keep our id set if this is our first PPO sponsor
                if not set_init:
                    set_init = True
                    file_id_set = cur_ids
                # Otherwise update our set by taking the intersection of what we've seen from other sponsors
                else:
                    file_id_set.intersection_update(cur_ids)

                # A possible fail case
                if len(file_id_set) == 0:
                    print('failure! empty intersection')
                    exit(1)
                
                # We have our solution when the set is reduced to one group identifier
                if len(file_id_set) == 1:
                    for id in file_id_set:
                        for uri_output in id_to_uri_map[id]:
                            print(uri_output)
                        break
                    break # Final break exits the stream

if __name__ == "__main__":
    # Default to the given problem (a particular state and a particular file) but we could try other specifications
    state = 'NY'
    uri = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-02-01_anthem_index.json.gz'

    # Get args if they are given
    if len(sys.argv) > 1:
        state = sys.argv[1]
    if len(sys.argv) > 2:
        uri = sys.argv[2]

    # Print files associated with a state PPO from a given ToC
    isolate_ppo(state, uri)