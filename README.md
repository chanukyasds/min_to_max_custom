# min_to_max_custom

This Extension is Used to create an aggregate that process a column and returns the min and max values in a format of min CUSTOM-TEXT max.

Supported Datatypes are SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE PRECISION.

Steps to install:

Clone the repo

Change the Directory to repo

make

make install

CREATE EXTENSION min_to_max;

SELECT min_to_max(column_name,'custom_text') FROM table_name.

custom_text is your symbol or text that apperars in between of min and max.

Example:

1)SELECT min_to_max(column_name,'->') FROM table_name;

        RETURNS min->max 

2)SELECT min_to_max(column_name,'<->') FROM table_name;
        
        RETURNS min<->max

3)SELECT min_to_max(column_name,NULL) FROM table_name;

        RETURNS min->max    This is the Default.

Thanks.
