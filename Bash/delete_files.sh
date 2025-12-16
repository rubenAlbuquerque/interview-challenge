# !/bin/bash

# create a new script bash to do the housekeeping

# DIRECTORY="/path/to/your/files"
DIRECTORY="$1"

echo "Arquivos que serão excluídos de $DIRECTORY:"
# find "$DIRECTORY" -type f -mtime +180

find "$DIRECTORY" -type f -mtime +180 -exec rm -f {} \;
echo "Arquivos com mais de 6 meses foram excluídos de $DIRECTORY"


