The following part is implemented by [Spring EL](https://docs.spring.io/spring/docs/5.0.0.M5/spring-framework-reference/html/expressions.html), i.e. you can use any syntax Spring EL supported
even if I didn't listed.

- `statement`: list of String code to be executed.
  - e.g.
  ```yml
    - statement: ["#type=entity", "isWrite()"]
  ```
- `switcher`
  - support `default` case
  - only execute one case
  - e.g.
  ```yml
    - switcher:
        switch: "entity"
        case:
          "file": ["#docType='plain'", "renameField('uploader_id', 'uploaderId').renameField('parent_id', 'parentId')"]
          "user": ["#suffix='' ", "renameField('superid', 'superId')"]

  ```
- `foreach`: in most cases, you can use [Spring EL's collection projection](https://docs.spring.io/spring/docs/5.0.0.M5/spring-framework-reference/html/expressions.html#expressions-collection-projection) rather than this feature
- `if`
  - `create`: create a new event (or a bunch) and cp value & execute post creation statement
  - `drop`: drop this event
  - `statement`: same with outer `statement`
  - `switcher`: same as above
  - `foreach`
  ```yml
    - if:
        condition: "entity == 'user' && isUpdate()"
        ifBody:
          - create:
              copy: ["id", "entity", "#suffix", "#title", "#docType"]
              postCreation: ["addField('ownerTitle', #title)", "syncByQuery().filter('ownerId', id)", "id = null"]
        elseBody:
          - drop: {}
  ```
- all public method in [`SyncData`](./syncer-data/src/main/java/com/github/zzt93/syncer/data/SyncData.java):
  - `isWrite()`
  - `isUpdate()`
  - `isDelete()`
  - `toWrite()`
  - `toUpdate()`
  - `toDelete()`
  - `getField(String key)`
  - `addExtra(String key, Object value)`
  - `addField(String key, Object value)`
  - `renameField(String oldKey, String newKey)`
  - `removeField(String key)`
  - `removeFields(String... keys)` 
  - `containField(String key)` 
  - `updateField(String key, Object value)` 
  - `syncByQuery()`: update/delete by query, supported by ES/MySQL output channel
    - `SyncByQueryES`
  - `extraQuery(String schemaName, String tableName)`: usually work with `create` to convert one event to multiple events
    - `ExtraQuery`: enhance syncer with multiple dependent query;
- all data field in `SyncData`:
  - `repo`: repo/db/index
  - `entity`: entity or collection
  - `id`: data primary key or similar thing
  - `fields`: data content of this sync event converted from log content according to your `repo` config
  **Notice**:
    - if your interested column config (`fields`) has name of `primary key`, records will have it. Otherwise, it will only in `id` field;
  - `extra`: an extra map to store extra info
