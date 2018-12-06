## Dam Year Candidates 

Test for a wide net of candidate sentences related to a dam removal year. 

__Update 4/12__: wider net for additional criteria (rivers, streams, other attributes...) surrounding candidate dams and corenlp ner tags

Documents that contain:

("dam" and "removal") and ("stream" or "river")

## Quick Start

```sh
python dam_year.py
```

__New dataset - Rivers__

Collecting data samples of river terms within journal publications intended for curation

```sh
python rivers.py
```

## Output

```
./cand-df.csv
```