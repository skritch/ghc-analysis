

`/models` organization:
- `/base`: cleaned versions of seed csvs
- `/dimensions`: standard tables for the main entities, mostly geographic data and opioid treatment programs
- `/ghc_analysis_repros`: SQL tables reproducing the GHC website analyses, to document where they come from.
- `/ghc_analysis`: cleaned copies of analyses from the GHC website. If an anaylsis can be reproduced exactly
  from other data, it's omitted here.
- `/analysis`: fact tables of interest; mainly used as the starting point for Python notebooks