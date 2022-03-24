-- Dropping functions and aggregate

DROP FUNCTION IF EXISTS min_to_max_sfunc(internal,anynonarray,text);
DROP FUNCTION IF EXISTS min_to_max_ffunc(internal,anynonarray,text);
DROP AGGREGATE IF EXISTS min_to_max(anynonarray,text);

-- Creating functions and aggregate

CREATE OR REPLACE FUNCTION 
min_to_max_sfunc(internal,anynonarray,text)
RETURNS internal
AS 'min_to_max', 'min_to_max_sfunc'
LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION 
min_to_max_ffunc(internal,anynonarray,text)
RETURNS text
AS 'min_to_max', 'min_to_max_ffunc'
LANGUAGE c IMMUTABLE;

CREATE OR REPLACE AGGREGATE min_to_max(anynonarray,text)(
		
        SFUNC=min_to_max_sfunc,
        STYPE=internal,
        FINALFUNC=min_to_max_ffunc,
        FINALFUNC_EXTRA
		
);