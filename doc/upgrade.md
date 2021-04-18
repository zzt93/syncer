## Config File Upgrade Guide

### From 1.1 to 1.2

- Replace in case sensitive
  - "schemas" -> "repos"
  - "tables" -> "entities"
  - "rowName" -> "fields"
  - "Record" -> "Field"
  - "records" -> "fields"

### From 1.2 to 1.3

- Producer: remove `masters:`
- Consumer
  - Input: remove `masters:`
  - Filter
     - change to use source code
     - api opt
