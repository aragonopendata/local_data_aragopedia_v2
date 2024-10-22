curl http://$FRONT_HOST:$FRONT_CACHE_PORT/kos/control/clear-cache
curl http://$FRONT_HOST:$FRONT_CACHE_PORT/recurso/control/clear-cache
curl "$BACK_HOST:$BACK_SOLR_PORT/solr/informesIAEST/update?stream.body=%3Cdelete%3E%3Cquery%3E*:*%3C/query%3E%3C/delete%3E&commit=true"
curl -X POST "$BACK_HOST:$BACK_SOLR_PORT/solr/informesIAEST/update/csv?commit=true" \
-F "stream.file=@$WORKDIR/Transformation/createTreeIAESTtreev2.csv;type=text/plain;charset=utf-8"