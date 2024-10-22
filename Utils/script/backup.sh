#!/bin/sh

LOCAL_BACKUP_PATH="$WORKDIR/Backup"

mkdir -p "$LOCAL_BACKUP_PATH"

ssh "$BACK_USER@$BACK_HOST" "
    cd $BACKUP_PATH || exit 1;
    rm -f backup_ttl.zip;
    zip -9 backup_ttl.zip backup_*.ttl;
    find backup_*.ttl -delete;
"

scp "$BACK_USER@$BACK_HOST:$BACKUP_PATH/backup_ttl.zip" "$LOCAL_BACKUP_PATH/backup_ttl.zip"

echo "Backup completado y almacenado en $LOCAL_BACKUP_PATH"
