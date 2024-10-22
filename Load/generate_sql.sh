if [[ $(stat --printf="%s" $WORKDIR/Transformation/createTreeIAESTtreev2.csv) -ne 0 ]] || [[ -n $(ls -A $WORKDIR/Transformation/dump/zip) ]]
then

    mkdir -p restores/virtuoso/commonData
    mkdir -p restores/virtuoso/zip
    mkdir -p restores/virtuoso/cargas
    mkdir -p restores/virtuoso/ttl

    cd restores/virtuoso
    find $WORKDIR/Transformation/dump -type f -name *.zip -exec cp {} zip/ \;

    cd zip/

    for z in *; do
        unzip $z
    done

    cd ..

    mv zip/*.ttl ttl/
    mv ttl/kos.ttl commonData/
    mv ttl/dsd.ttl commonData/
    mv ttl/properties.ttl commonData/

    cd ttl/

    for i in *; do
        echo "dump_one_graph('http://opendata.aragon.es/graph/datacube/${i::-4}', 'backup_${i::-4}', null);" > ../cargas/${i::-4}.sql
        echo "SPARQL CLEAR GRAPH <http://opendata.aragon.es/graph/datacube/${i::-4}>;" >> ../cargas/${i::-4}.sql    
        echo "delete from DB.DBA.load_list;" >> ../cargas/${i::-4}.sql    
        echo "ld_dir ('/data/restores/virtuoso/ttl/', '${i::-4}.ttl', 'http://opendata.aragon.es/graph/datacube/${i::-4}');" >> ../cargas/${i::-4}.sql
        echo "SPARQL INSERT DATA {GRAPH <http://opendata.aragon.es/graph/datacube/${i::-4}> {<http://opendata.aragon.es/recurso/iaest/dataset/${i::-4}> <http://www.w3.org/2002/07/owl#sameAs> <http://opendata.aragon.es/graph/datacube/${i::-4}> }};" >> ../cargas/${i::-4}.sql
        echo "rdf_loader_run();" >> ../cargas/${i::-4}.sql
    done
fi
