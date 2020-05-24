Sync ES

```
mysql_0          ES
  db.table.id[0, num) ->  db.*/table/id
mysql_1          ES
  db.table.id[num, 2*num) ->  db.*/table/id
mysql_2          ES
  db.table.id[2*num, 3*num) ->  db.*/table/id
```
Sync mysql
```
mysql_0          mysql_0
  db.table.id[0, num) ->  db.table_bak.id
mysql_1          mysql_0
  db.table.id[num, 2*num) ->  db.table_bak.id
mysql_2          mysql_0
  db.table.id[2*num, 3*num) ->  db.table_bak.id
```