# SkipList in goleveldb

Throughput
```bash
SkipList single thread insert test.
Insert entrys num: 1000000, throughput: 545256
PASS
ok      github.com/huayichai/goleveldb/skiplist 3.367s
```

Refer to `https://github.com/huandu/skiplist.git`, but the insertion throughput is 50% slower.