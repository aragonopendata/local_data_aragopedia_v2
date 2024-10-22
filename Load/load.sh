if [[ $(stat --printf="%s" $WORKDIR/Transformation/createTreeIAESTtreev2.csv) -ne 0 ]] || [[ -n $(ls -A $WORKDIR/Transformation/dump/zip) ]]
then
    cd restores/virtuoso/cargas/

    for c in *; do
        cat "$c" | ssh $BACK_USER@$BACK_HOST "$VIRTUOSO $BACK_LOCAL $DB_USER $DB_PASS"
    done

    cd ../commonData/

    cat $WORKDIR/Utils/carga-common.sql | ssh $BACK_USER@$BACK_HOST "$VIRTUOSO $BACK_LOCAL $DB_USER $DB_PASS"
    cat $WORKDIR/Utils/consultaErroresLoadList.sql | ssh $BACK_USER@$BACK_HOST "$VIRTUOSO $BACK_LOCAL $DB_USER $DB_PASS >> logErroresCargaIAEST.log"


fi
