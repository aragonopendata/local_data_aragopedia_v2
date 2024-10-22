dump_one_graph('http://opendata.aragon.es/graph/datacube/commonData', 'backup_commonData', null);
SPARQL CLEAR GRAPH <http://opendata.aragon.es/graph/datacube/commonData>;
delete from DB.DBA.load_list;
ld_dir ('/data/restores/virtuoso/commonData/', '*.ttl', 'http://opendata.aragon.es/graph/datacube/commonData');
rdf_loader_run();