#  USGS Dam Removal GeoDeepDive Application 
An application for dam removal built on the GeoDeepDive infrastructure. This project at the top level contains the app-template for GeoDeepDive as well as additional scripts and the modified Stromatolites demo project. 

## Quick Start

The goal of the application template is to query the GDD database to condense the publications that might be related to dam removal science. The results are later used in the modified Stromatolites application. 

##### Filtering Query

```sh
# File: app/step1.txt 
# GDD Dictionary for all dam removals, restricted by terms 
Dictionary: "Dam Removal -- all"
Terms: ("dam" and "removal") and ("stream" or "river")
```

##### Run 

```sh
# Execute the shell file "run" to prompt the app/run_app.py file.
sh run
```

###### Initial Pass

*("dam" and "removal") and ("stream" or "river")*

Note: Previous manual identification total was 6,068 documents.

### Files

```
├── README.md
├── app
│   ├── run_app.py 
│   └── step1.txt  
├── config.yml
├── gdd_demo
│   ├── README.md
│   ├── find_refs.py
│   ├── find_target.py
│   ├── input
│   ├── output
│   │   ├── output-old.tsv
│   │   ├── output.tsv
│   │   └── ref_start.tsv
│   └── var
│       └── target_variables.txt
├── input
│   ├── README.md
│   └── README_diagram.png
├── makefile
├── output  
├── run  # App template run file
├── setup
│   └── setup.sh
├── snorkel_drd  # directory related to setup work related to Snorkel
│   ├── readme.md
│   └── snorkleDRD_candidate_extraction.ipynb
├── stromatolites_demo
│   ├── etc ...
└── testing  # Resources
    ├── removedDams20151214.csv
    └── Random snippets to save
```

### License

CC-BY 4.0 International

### Disclaimer

Provisional Software Disclaimer Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.