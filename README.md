# Serif Health Takehome Interview Submission - Andrew Emmott

This repository contains a python script/module that provides my best attempt at solving the provided problem. This README contains a lot of other information about my observations and how I spent my time. I am not completely certain that my solution is correct, but I expect you are interested in my thought process as much as anything else, so please read. 

## Provided Solution

### Set-Up
Latest Python3 (3.12.1) was used. Two third party packages are required, which can be easily installed with the pip tool (or found at https://pypi.org):

`pip install smart_open[http]
pip install json_stream`

A simple bash script was provided to run the above commands, but not all systems will manage python environments in the same way.

### Running the Solution

The module findppo.py can be run to produced the desired output:

`python findppo.py`

The script can take up to two optional arguments. By default the script runs assuming the settings specified for the takehome interview: the state (New York) and the uri of the table of contents.

`python findppo.py NY https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-02-01_anthem_index.json.gz` would produce identical output. In theory one could search for other state's PPOs in other months' data, but I understand that the underlying data is not always consistently organized and so might not work; this was just an obvious and easy way to generalize the script.

As it happens

`python findppo.py NV`

seems to print out a file related to the Nevada PPO. (Not certain it is correct though)

## Description of Development Process and Solution

### Reading and Parsing

Considering question:
> - How do you handle the file size and format efficiently, when the uncompressed file will exceed memory limitations on most systems?

The provided url points to a large compressed file that, if uncompressed, is a few hundred GB of data so a streaming solution is required (that is, read the file once, don't load it into memory) I have experience with remote streaming, but uncompressing gzip on the fly was new to me. Gzip streaming is not easily handled with the python standard library, but I found a third-party library that easily handles the issue: `smart_open`. A library like `json_stream` is also required to parse json in a streaming context; the standard json library would try to load everything into memory and so is inadequate.

### Understanding the Data

Solving the problem required trying to understand the data. The long list of `reporting_structure_object`s is the data of interst here. These objects relate plans to files. Inspection of these objects seemed to suggest a few things:

- `plan_id_type` fields were homogenous within each `reporting_structure_object`; that is, "HIOS" and "EIN" id types were not mixed. Similarly, `plan_id` fields were homogenous within reporting objects; again, each object related one plan id to one set of files.
- For objects where `plan_id_type` equals "HIOS" the plan ids seemed to include the two-letter US State code in them, and the plans were typically (but not always) individual and not group plans. Given the introduction of this EIN lookup tool in the original README -
> - Anthem has an interactive MRF lookup system. This lookup can be used to gather additional information - but it requires you to input the EIN or name of an employer who offers an Anthem health plan: [Anthem EIN lookup](https://www.anthem.com/machine-readable-file/search/).
it was assumed that all HIOS entries were not of interest.
- For object where `plan_id_type` equals "EIN" it seems clear each object pertains to one employer. Further, it did not seem like a given EIN occurred in more than one `reporting_structure_object`. That is, I believe the relationship of employers to `reporting_structure_objects` is 1:1.
- A given employer might participate in several plans. The `plan_name` field typically took the format "<NAME OF PLAN> - <NAME OF EMPLOYER> - Anthem"; with the name of plan varying in each `reporting_plan` object. This "name" seems like a good way to identify New York PPO-using plans, but the file lists are associated with an employer. How to separate the NY PPO files from the rest is not obvious.
- Given the hint
> - Is the description field helpful? Complete? Is Highmark the same as Anthem?
The description field is helpful, but still not clear. If a plan is not branded under Anthem, it seems obvious from the description. The most common description, however, is simply "In-Network Negotiated Rates Files" a few decriptions are simply "PPO Network" but these pertain to files that are clearly for PPOs outside of New York. Given the descriptions seen, I assumed the files of interest have the description "In-Network Negotiated Rates Files" but that this description is not sufficient to identify the NY PPO.
- The actual file names under this description typically have a two-letter state code followed by an underscore in them so the search can probably be further narrowed down to `location` fields with "NY_" in them.
- Using the EIN look up tool with EINs from employers who have plan names that include "NY" and "PPO" reveals a number of files whose label on the website is something like "NY PPO <STUFF>"; the underlying urls match several files appearing in the large table of contents gzip, but unfortunately there isn't anything truly identifying in the file names or descriptions to help solve this problem "correctly". However, this does give us expeted output. It seems that the file name of interest are `NY_GGHAMEDAP33_0[1-8]_08.json.gz` but an algorithmic way of deducing that is still required.
- A number of plan names seem to feasibly relate to the NY PPO. The earliest occuring such name is "NY SG PPO NETWORK" but others likely suffice.
- A final important observation is that the data is very large, but it is also extremely redundant. The compression rate on the table of contents is well over 90%. Reading the entire file is not feasible, and also not necessary if the goal is to find the NY PPO. If the correct set of files can be deduced, reading can stop early.

### Proposed Solution

Given the above, the proposed solution is to begin streaming the `reporting_structure_object` list. For each plan name with tokens "NY" and "PPO" in them, observe the `in_network_files` list. Make a set containing each file with description "In-Network Negotiated Rates Files" and "NY" in the file name. If this is the first time we've examined an "NY PPO" plan name, then keep this set as our global set. Otherwise, find the intersection of our global set and this new set and keep this as our new global set. If there is only one file left, then this is our file. (Pedanitc note: we need some logic that abstracts away the x-of-y part of the name. So really, we're looking to winnow down to one group of files with the same name, not just one file.)

## Honest Assesment of Time Spent

The coding/logic/algorithmic part of this problem was not too difficult or time consuming, but I did spend a good amount of time trying to understand the underlying data and so I honestly divulge my time spent here.

### Finding a File Streaming Solution

As stated, I have experience with both remote file reading and with streaming input, but I had difficulty finding a solution that could stream a gzipped file and was not aware of the smart_open library when I began working on this problem. I spent time trying to define a custom streaming gzip decoder before dicovering the the smart_open library, which suddenly made very short work of the matter. I spent maybe two hours total just trying to get started, but smart_open is a great package to be aware of.

### Understanding the Data

Understanding the ToC data stream and looking for consistencies in the data was time consuming, especially since I would typically let a for-loop run on the REPL to enumerate specific points of interest. I have explained my process of discovery above and walking through that took perhaps a full dev day (maybe six hours). I could have spent less time on this, as I devised my solution early on, but I was interested in the problem and wanted to gain more confidence in my solution and I also spent a good deal of this time just learning about health insurance data in general becuase I found it interesting.

### Actual Coding

I spent about an hour coding the submitted solution. I aimed to finish in 30 minutes. Because I had allowed myself so much time exploring the data first and had a lot of the coding logic already worked out in the REPL I thouhgt it only fair to minimize my time on real coding. This meant I did not refactor or generalize my code as much as I might have under different circumstances. I staged my coding into several commits so that you can see roughly how much time I spent going from beginning to end. Possible enhancements to the solution are discussed below instead. I also added comments to the script that should explain some other thoughts.

## Possible Enhancements and Improvements

1. The logic is condensed into one function, and this function explicitly prints the discovered files. Ideally the function itself would return a list of relevants file urls to be used externally and only running the module directly would expose the printing logic.

2. A lot of string splitting is in this solution to narrow down specific tokens of interest. For readability and reuseability, these bits of logic should be factored out. A more general solution would likely need to reuse these bit of logic anyway, and a descriptive function name would make it clearer what was happening.

3. A more general use of the set intersection solution provided here would be to find all associations between plan names and files, revealing a much simpler data structure as a proper Table of Contents for further data analysis. Ideally, all information that needs to be gleaned from the provided table of contents should be computed in one pass.

4. Defining custom object classes that can be JSON serializable for the expected json objects in the stream would provide for much safer, more useable  and easier to organize code.