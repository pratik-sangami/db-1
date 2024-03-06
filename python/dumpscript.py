import sys
import psycopg2

# Initial Checks

    ## If file name is not provided (return error, 'missing file for datadump')

    ## If file name is provided

        # Read the file, extract information such as table columns and data type for columns

        # CONNECT DATABASE

        # CREATE DATABASE "filename"

        # CREATE TABLE with columns extracted with respective datatypes

        # INSERT FILE CONTENTS INTO TABLE
