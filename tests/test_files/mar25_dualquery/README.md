Dual query CSVs
---

These CSVs are hand-generated and simulate the expected behaviour for the dual query filtering functions.

Step 0 - status.csv:
- Five UIDs -> uid1-uid5
- uid1 has split values, some in column B and some in G
- Missing time stamp at 9am

Step 1 - status_0filtered.csv:
- Four UIDs -> uid1-uid3 & uid5 (uid4 column had all 0s)

Step 2 - status_merged.csv:
- Second uid1 column should be removed and uid1 should have 1s from 1am-8am

Step 3 - status_inferred.csv:
- 9am timestamp should be added back in with empty rows