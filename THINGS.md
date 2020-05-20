## Go ML !


## docker 
```bash
docker run --rm -tid --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike/aerospike-server
docker run -it --name aerospike-tools aerospike/aerospike-tools aql -h 172.17.0.2
docker run -it --name --rm aerospike/aerospike-tools - v $(pwd)/:/code aql
docker run -it --rm tensorflow_serving -p 8501:8501 -v "$(pwd)/models:/models" \
-e MODEL_NAME=fraud tensorflow/serving

```

### aerospike
``bash
show namespaces 
show sets
insert into test.foo (PK, foo) values ('123', 'my string')
insert into test.boo (PK, foo) values ('123', 'my string', 'ada')
```