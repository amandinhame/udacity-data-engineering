class SqlQueries:
    
    # Select from `stagingLegislatures` to insert into `legislatures`
    legislatures_select_insert = """
        SELECT DISTINCT
            sl.legislatureid,
            sl.electionyear
        FROM
            stagingLegislatures sl
        WHERE
            sl.legislatureid NOT IN
            (
                SELECT legislatureid FROM legislatures
            )
    """

    # Identify new deputies that are in the `stagingDeputies` and not in the `deputies`
    # and insert into `newDeputies`
    new_deputies_select_insert = """
        SELECT DISTINCT CAST(SUBSTRING(sd.uri, 53) AS INT) AS deputyId, sd.uri AS uri
        FROM stagingDeputies sd
        WHERE deputyId NOT IN
            (SELECT deputyId FROM deputies)
    """

    # Select `uri` of the new deputies to call the API to get more details
    new_deputies_uri_select = """
        SELECT uri
        FROM newDeputies
    """

    # Select from `stagingDeputiesDetails` to insert into `parties`
    parties_select_insert = """
        SELECT DISTINCT
            TRIM(UPPER(sdd.party))
        FROM stagingDeputiesDetails sdd
        WHERE TRIM(UPPER(sdd.party)) NOT IN
            (SELECT party FROM parties)
    """

    # Columns of `parties` to be used in the insert statement
    parties_columns = ['party']

    # Select from `stagingDeputiesDetails` to insert into `states`
    states_select_insert = """
        SELECT DISTINCT
            TRIM(UPPER(sdd.electionState))
        FROM stagingDeputiesDetails sdd
        WHERE TRIM(UPPER(sdd.electionState)) NOT IN
            (SELECT stateAbb FROM states)
    """

    # Columns of `states` to be used in the insert statement
    states_columns = ['stateAbb']

    # Select from `stagingDeputiesDetails` to insert into `schoolLevels`
    school_levels_select_insert = """
        SELECT DISTINCT
            TRIM(INITCAP(sdd.schoolLevel))
        FROM stagingDeputiesDetails sdd
        WHERE TRIM(INITCAP(sdd.schoolLevel)) NOT IN
            (SELECT schoolLevel FROM schoolLevels)
    """

    # Columns of `schoolLevels` to be used in the insert statement
    school_levels_columns = ['schoolLevel']

    # Select from `stagingDeputiesDetails`, `parties`, `states` and `schoolLevels`
    # to insert into `deputies`
    deputies_select_insert = """
        SELECT
           CAST(sdd.deputyId AS INT),
            INITCAP(sd.name),
            INITCAP(sd.civilName),
            sd.gender,
            sd.birthDate,
            sd.deathDate,
            sdd.deputyDocumentId,
            p.partyId,
            s.stateId,
            sl.schoolLevelId
        FROM stagingDeputiesDetails sdd
        JOIN 
            (SELECT 
                CAST(SUBSTRING(uri, 53) AS INT) AS deputyId,
                name,
                civilName,
                gender,
                birthDate,
                deathDate
            FROM stagingDeputies) sd
        ON CAST(sdd.deputyId AS INT) = sd.deputyId
        JOIN parties p
            ON p.party = TRIM(UPPER(sdd.party))
        JOIN states s
            ON s.stateAbb = TRIM(UPPER(sdd.electionState))
        JOIN schoolLevels sl
            ON sl.schoolLevel = TRIM(INITCAP(sdd.schoolLevel))
        WHERE sd.deputyId NOT IN
            (SELECT deputyId FROM deputies)
    """

    # Select from `stagingExpenses` to insert into `expenseTypes`
    expense_types_select_insert = """
        SELECT DISTINCT
            TRIM(INITCAP(se.description))
        FROM stagingExpenses se
        WHERE TRIM(INITCAP(se.description)) NOT IN
            (SELECT expenseTypeDescription FROM expenseTypes)
    """

    # Columns of `expenseTypes` to be used in the insert statement
    expense_types_columns = ['expenseTypeDescription']

    # Select from `stagingExpenses` and `expenseTypes` to insert into `expenses`
    expenses_select_insert = """
        SELECT DISTINCT
            se.legislatureId,
            CASE LENGTH(se.registerId) WHEN 0 THEN -1 ELSE CAST(se.registerId AS INT) END,
            et.expenseTypeId,
            se.emissionDate,
            UPPER(se.provider),
	        regexp_replace(se.providerDocumentId, '[^[:digit:]]', ''),
            se.month,
            se.year,
            se.documentValue,
	        se.glValue,
	        se.liqValue,
            CAST(se.parcelNu AS INT),
            se.documentId,
            se.documentUrl
        FROM stagingExpenses se
        JOIN expenseTypes et
            ON TRIM(INITCAP(se.description)) = et.expenseTypeDescription
        WHERE
            se.documentId NOT IN
            (SELECT e.documentId
            FROM expenses e
            JOIN stagingExpenses se2
                ON se2.documentValue = e.documentValue AND 
                CAST(se2.emissionDate AS DATE) = e.emissionDate AND
                se2.parcelNu = e.parcelNu AND
                regexp_replace(se2.providerDocumentId, '[^[:digit:]]', '') = e.providerDocumentId AND
                se2.legislatureId = e.legislatureId AND
                se2.month = e.month AND
                se2.year = e.year
            )
    """

    # Columns of `expenses` to be used in the insert statement
    expenses_columns = ['legislatureId', 'deputyId', 'expenseTypeId', 'emissionDate', 'provider',
        'providerDocumentId', 'month', 'year', 'documentValue', 'glValue', 'liqValue', 'parcelNu', 
        'documentId', 'documentUrl']

