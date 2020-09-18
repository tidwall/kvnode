# kvnode

Minimal Key/Value store with basic Redis support. 

- Redis API
- LevelDB disk-based storage
- Raft support with [Uhaha](https://github.com/tidwall/uhaha) commands
- Compatible with existing Redis clients

Commands:

```
SET key value
GET key
DEL key [key ...]
PDEL pattern
KEYS pattern [PIVOT prefix] [LIMIT count] [DESC] [WITHVALUES] [EXCL]
MSET key value [key value ...]
MGET key [key ...]
```

## Key scanning

The `KEYS` command returns keys and values, ordered by keys. 
The `PIVOT` keyword allows for efficient paging.
For example:
```
redis> MSET key1 1 key2 2 key3 3 key4 4
OK
redis> KEYS * LIMIT 2
1) "key1"
2) "key2"
redis> KEYS * PIVOT key2 LIMIT 2
1) "key3"
2) "key4"
```

The `PDEL` commands will delete all items matching the specified pattern.

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License
kvnode source code is available under the MIT [License](/LICENSE).

