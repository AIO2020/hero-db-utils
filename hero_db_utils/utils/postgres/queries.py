"""
Raw queries that can be formatted and ran using a postgresql engine.
"""

TABLE_COLUMNS_INFO = """
    WITH constrs AS (
        SELECT
            tc.table_schema,
            tc.constraint_name,
            tc.table_name,
            kcu.column_name,
            ccu.table_name||'.'||ccu.column_name AS foreign_column,
            tc.constraint_type
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE (tc.constraint_type = 'FOREIGN KEY' OR tc.constraint_type = 'PRIMARY KEY')
    ) SELECT
        col.table_name, 
        col.ordinal_position,
        col.column_name,
        col.data_type,
        CASE 
            WHEN constrs.constraint_type='PRIMARY KEY' THEN TRUE
            ELSE FALSE END
            AS is_pk,
        CASE 
            WHEN constrs.constraint_type='FOREIGN KEY' THEN constrs.foreign_column
            ELSE NULL END
            AS fk_ref
    FROM information_schema.columns col LEFT JOIN constrs
        ON(
            col.table_schema=constrs.table_schema AND
            col.table_name=constrs.table_name AND
            col.column_name=constrs.column_name
        )
    WHERE col.table_schema = %(schema_name)s AND col.table_name = %(table_name)s
"""

SELECT_TABLES_CATALOG_IN_SCHEMA = """
    SELECT * FROM pg_catalog.pg_tables WHERE schemaname = %(schema_name)s
"""

DROP_DB_CONNECTIONS = """
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = %(db_name)s
    AND pid <> pg_backend_pid()
"""

CHECK_TABLE_EXISTS = """
    SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%(table_name)s)
"""

CHECK_TABLE_EXISTS_IN_SCHEMA = """
    SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%(table_name)s AND table_schema=%(schema_name)s)
"""

GET_TABLES_INFO_IN_SCHEMA = """
    WITH
    tbl as (
        SELECT table_schema,table_name 
        FROM information_schema.tables
        WHERE table_name not like 'pg_%%' AND table_schema IN (%(schema_name)s)
    ),
    row_counts AS(
        SELECT 
        table_schema, 
        table_name, 
        (xpath('/row/c/text()', 
            query_to_xml(format('select count(*) AS c from %%I.%%I', table_schema, table_name), 
            false, 
            true, 
            '')))[1]::text::int AS nrows 
        FROM tbl
    )
    SELECT
        rowsc.table_name,
        rowsc.nrows,
        colsc.ncols
    FROM row_counts rowsc
        JOIN (
            SELECT rowsc.table_name, COUNT(infcols.column_name) AS ncols
            FROM row_counts rowsc JOIN
                INFORMATION_SCHEMA.columns infcols ON (
                    rowsc.table_name=infcols.table_name AND
                    rowsc.table_schema=infcols.table_schema
                )
            GROUP BY rowsc.table_name
        ) colsc ON(rowsc.table_name=colsc.table_name)
    ORDER BY rowsc.table_name
"""
