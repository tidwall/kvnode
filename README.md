# kvnode

Very simple key value store.

- Redis API
- LevelDB storage
- Raft support

Commands:

```
SET key value
GET key
DEL key [key ...]
PDEL pattern
KEYS pattern [PIVOT prefix] [LIMIT count] [DESC] [WITHVALUES]
MSET key value [key value ...]
MGET key [key ...]
FLUSHDB
SHUTDOWN
```

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License
kvnode source code is available under the MIT [License](/LICENSE).

