SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO  

CREATE OR ALTER PROCEDURE StatisticsAnalysis     
    @statistics NVARCHAR(MAX),             
    @plan XML         
AS                    
BEGIN                         
    -- Initialize variables
    DECLARE @TIME BIT = 0;

    -- Format the statistics input as XML
    SET @statistics = '<i>' + REPLACE(@statistics, CHAR(13) + CHAR(10), '</i><i>') + '</i>';
    PRINT @statistics;
    
    DECLARE @xms XML = CONVERT(XML, @statistics);

    -- Drop temporary table if it exists
    IF OBJECT_ID('tempdb..#sttcs', 'U') IS NOT NULL
        DROP TABLE #sttcs;

    -- Parse the statistics and identify types
    SELECT 
        *,
        QueryStatementId = SUM(CASE WHEN rt = 3 THEN 1 ELSE 0 END) OVER(ORDER BY s.rn)
    INTO #sttcs
    FROM (
        SELECT 
            *,
            rt = CASE
                WHEN txt LIKE 'SQL Server parse and compile time:%' THEN 1
                WHEN txt LIKE '%SQL Server Execution Times:%' THEN 2
                WHEN txt LIKE '(% row affected)' THEN 3
                WHEN txt LIKE '(% rows affected)' THEN 3
                WHEN txt LIKE 'Table ''%''. Scan count %' THEN 4
                WHEN NULLIF(txt, '') IS NULL THEN 5
            END
        FROM (
            SELECT 
                rn = ROW_NUMBER() OVER(ORDER BY i.Nod),
                que = i.Nod.query('.'),
                txt = LTRIM(RTRIM(i.Nod.value('.', 'NVARCHAR(MAX)')))
            FROM @xms.nodes('i') i(Nod)
        ) s
    ) s
    ORDER BY rn;

    -- Ensure STATISTICS IO ON was included
    IF NOT EXISTS (SELECT 1 FROM #sttcs WHERE rt = 4)
    BEGIN
        RAISERROR('Minimum output for analysis is STATISTICS IO ON', 16, 1);
        RETURN;
    END;

    -- Determine if execution time information is available
    SET @TIME = CASE 
                    WHEN EXISTS (SELECT 1 FROM #sttcs WHERE rt = 2) THEN NULL 
                    ELSE 0 
                END;

    -- Drop temporary table if it exists
    IF OBJECT_ID('tempdb..#rd', 'U') IS NOT NULL
        DROP TABLE #rd;

    -- Extract logical reads from statistics
    SELECT 
        s.rn, 
        s.txt, 
        ta.*
    INTO #rd
    FROM #sttcs s
    CROSS APPLY (
        SELECT 
            ObjctName = MAX(CASE WHEN typofrow = 1 THEN LTRIM(SUBSTRING(typ.Va, 7, 2000)) END) OVER(),
            [Type] = CASE WHEN typofrow = 2 THEN LTRIM(REVERSE(SUBSTRING(REVERSE(typ.Va), CHARINDEX(' ', REVERSE(typ.Va)), 2000))) END,
            Va = CASE WHEN typofrow = 2 THEN TRY_PARSE(REVERSE(SUBSTRING(REVERSE(typ.Va), 1, CHARINDEX(' ', REVERSE(typ.Va)))) AS INT) END,
            RNum = typ.rnum
        FROM (
            SELECT 
                Va, 
                RNum,
                typofrow = CASE WHEN Va LIKE 'Table %' THEN 1 ELSE 2 END
            FROM (
                SELECT 
                    Va = LTRIM(RTRIM(tnd.Nod.value('.', 'VARCHAR(4000)'))), 
                    RNum = ROW_NUMBER() OVER(ORDER BY tnd.Nod)
                FROM (
                    SELECT CONVERT(XML, ('<i>' + REPLACE(REPLACE(s.txt, ',', '</i><i>'), '.', '</i><i>') + '</i>'))) t(Nod)
                    CROSS APPLY t.Nod.nodes('i') tnd(Nod)
                    WHERE NULLIF(tnd.Nod.value('.', 'VARCHAR(4000)'), '') IS NOT NULL
                ) s
            ) s
        ) typ
    ) ta
    WHERE s.rt = 4;

    -- Prepare dynamic SQL for pivoting logical reads
    DECLARE @Cols NVARCHAR(MAX), @SqlStatementLogicalRd NVARCHAR(MAX), @SqlStatementCreateTable NVARCHAR(MAX);

    SELECT @Cols = STUFF((
        SELECT ', ' + QUOTENAME(s.[Type])
        FROM (
            SELECT rd.[Type], pti = ROW_NUMBER() OVER(PARTITION BY rd.[Type] ORDER BY rd.rn), rd.RNum
            FROM #rd rd
            WHERE rd.[Type] IS NOT NULL
        ) s
        WHERE pti = 1
        ORDER BY s.RNum
        FOR XML PATH(N''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '');

    -- Create temporary table for logical reads
    IF OBJECT_ID('tempdb..##LogicalRd', 'U') IS NOT NULL
        DROP TABLE ##LogicalRd;

    SET @SqlStatementCreateTable = 'CREATE TABLE ##LogicalRd (ObjctName SYSNAME NOT NULL, rn INT NOT NULL, ' + REPLACE(@Cols, ']', '] INT') + ')';
    EXEC sp_executesql @SqlStatementCreateTable;

    -- Populate pivoted logical reads
    SET @SqlStatementLogicalRd = '
    INSERT ##LogicalRd
    SELECT *
    FROM (
        SELECT ObjctName, Va, rn, [Type]
        FROM #rd
    ) ptio
    PIVOT(MAX(ptio.Va) FOR ptio.[Type] IN (' + @Cols + ')) r';
    PRINT @SqlStatementLogicalRd;
    EXEC sp_executesql @SqlStatementLogicalRd;

    -- Handle optional execution plan data
    IF @plan IS NOT NULL
    BEGIN
        -- Drop temporary tables if they exist
        IF OBJECT_ID('tempdb..##sttcslgc', 'U') IS NOT NULL
            DROP TABLE ##sttcslgc;
        IF OBJECT_ID('tempdb..#plan', 'U') IS NOT NULL
            DROP TABLE #plan;

        -- Parse execution plan
        SELECT 
            QueryStatementId = sql.[SqlStatementSl].value('@StatementId', 'INT'),
            QueryTxt = (SELECT sql.SqlStatementSl.value('@StatementText', 'NVARCHAR(MAX)') AS '*' FOR XML PATH(N''), TYPE)
        INTO #plan
        FROM @plan.nodes('*:ShowPlanXML/*:BatchSequence/*:Batch/*:Statements/*:StmtSimple') sql([SqlStatementSl]);

        -- Combine statistics and logical reads with execution plan
        SELECT 
            Num = sttcs.rn, 
            QueryTxt = ISNULL(pl.QueryTxt, sttcs.que), 
            Txt = sttcs.txt, 
            lgc.*, 
            sttcs.QueryStatementId
        INTO ##sttcslgc
        FROM #sttcs sttcs
        LEFT JOIN #plan pl ON sttcs.QueryStatementId = pl.QueryStatementId
        LEFT JOIN ##LogicalRd lgc ON sttcs.rn = lgc.rn
        WHERE sttcs.rt <> 5
        ORDER BY sttcs.rn;
    END
    ELSE
    BEGIN
        -- Combine statistics and logical reads without execution plan
        IF OBJECT_ID('tempdb..##sttcslgc2', 'U') IS NOT NULL
            DROP TABLE ##sttcslgc2;

        SELECT 
            Num = sttcs.rn, 
            QueryTxt = sttcs.que, 
            Txt = sttcs.txt, 
            lgc.*, 
            QueryStatementId = SUM(CASE WHEN rt = 3 THEN 1 ELSE 0 END) OVER(ORDER BY sttcs.rn)
        INTO ##sttcslgc2
        FROM #sttcs sttcs
        LEFT JOIN ##LogicalRd lgc ON sttcs.rn = lgc.rn
        WHERE sttcs.rt <> 5
        ORDER BY sttcs.rn OPTION(RECOMPILE);
    END;

    -- Return results
    IF @plan IS NOT NULL
        SELECT * FROM ##sttcslgc ORDER BY Num;
    ELSE
        SELECT * FROM ##sttcslgc2 ORDER BY Num;
END;
GO
