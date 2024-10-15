import: build
	./poems -dir ../chinese-poetry/宋词
	./poems -dir ../chinese-poetry/五代诗词
	./poems -dir ../chinese-poetry/全唐诗


build:
	go build -o poems  .


ca:
	 docker cp dockerfile-elk-es01-1:/usr/share/elasticsearch/config/certs/ca/ca.crt http_ca.crt

test:
	curl --cacert http_ca.crt -u "elastic:123456"  -X GET "https://localhost:9200/_cat/nodes?v&pretty"

