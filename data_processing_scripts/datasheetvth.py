def create_and_populate_datasheetvth(cur, con):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS datasheetvth (
        V_drain_25 FLOAT,
        V_gs_25 FLOAT,
        V_th_min FLOAT,
        V_th_typ FLOAT,
        V_th_max FLOAT,
        I_Drain FLOAT,
        Identifier VARCHAR(50) NOT NULL,
        sample VARCHAR(50) NOT NULL,
        device VARCHAR(50) NOT NULL,
        v_drain INTEGER
    );
    """
    cur.execute(create_table_query)

    print("Now populating vth values from the datasheets.. ")
    # Define the SQL query for inserting data
    insert_data_query = """
    INSERT INTO datasheetvth (V_drain_25, V_gs_25, V_th_min, V_th_typ, V_th_max, I_Drain, Identifier, sample, device, v_drain)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Define the data to be inserted
    data_to_insert = [
        (2.527, 2.527, 2, 2.6, 4, 0.005, 'C2M0080120D', 'datasheetvth', 'C2M', 5),
        (2.527, 2.527, 2, 2.6, 4, 0.005, 'C2M0080120D', 'datasheetvth', 'C2M', 10),
        (2.527, 2.527, 2, 2.6, 4, 0.005, 'C2M0080120D', 'datasheetvth', 'C2M', 15),
        (2.527, 2.527, 2, 2.6, 4, 0.005, 'C2M0080120D', 'datasheetvth', 'C2M', 20),
        (2.703, 2.703, 1.8, 2.5, 3.6, 0.005, 'C3M0075120K', 'datasheetvth', 'C3M', 5),
        (2.703, 2.703, 1.8, 2.5, 3.6, 0.005, 'C3M0075120K', 'datasheetvth', 'C3M', 10),
        (2.703, 2.703, 1.8, 2.5, 3.6, 0.005, 'C3M0075120K', 'datasheetvth', 'C3M', 15),
        (2.703, 2.703, 1.8, 2.5, 3.6, 0.005, 'C3M0075120K', 'datasheetvth', 'C3M', 20),
        (4.5, 4.5, 3.5, 4.5, 5.7, 0.0056, 'IMW120R060M1H', 'datasheetvth', 'Infineon', 5),
        (4.5, 4.5, 3.5, 4.5, 5.7, 0.0056, 'IMW120R060M1H', 'datasheetvth', 'Infineon', 10),
        (4.5, 4.5, 3.5, 4.5, 5.7, 0.0056, 'IMW120R060M1H', 'datasheetvth', 'Infineon', 15),
        (4.5, 4.5, 3.5, 4.5, 5.7, 0.0056, 'IMW120R060M1H', 'datasheetvth', 'Infineon', 20),
        (2.674, 2.674, 1.8, 2.8, 4, 0.01, 'LSIC1MO120E0080', 'datasheetvth', 'LF', 5),
        (2.674, 2.674, 1.8, 2.8, 4, 0.01, 'LSIC1MO120E0080', 'datasheetvth', 'LF', 10),
        (2.674, 2.674, 1.8, 2.8, 4, 0.01, 'LSIC1MO120E0080', 'datasheetvth', 'LF', 15),
        (2.674, 2.674, 1.8, 2.8, 4, 0.01, 'LSIC1MO120E0080', 'datasheetvth', 'LF', 20),
        (3.5, 3.5, 1.8, 3.5, None, 0.001, 'SCT30N120', 'datasheetvth', 'STM', 5),
        (3.5, 3.5, 1.8, 3.5, None, 0.001, 'SCT30N120', 'datasheetvth', 'STM', 10),
        (3.5, 3.5, 1.8, 3.5, None, 0.001, 'SCT30N120', 'datasheetvth', 'STM', 15),
        (3.5, 3.5, 1.8, 3.5, None, 0.001, 'SCT30N120', 'datasheetvth', 'STM', 20),
        (10, 2.902, 1.6, 2.8, 4, 0.0044, 'SCT2080KE', 'datasheetvth', 'RP', 5),
        (10, 2.902, 1.6, 2.8, 4, 0.0044, 'SCT2080KE', 'datasheetvth', 'RP', 10),
        (10, 2.902, 1.6, 2.8, 4, 0.0044, 'SCT2080KE', 'datasheetvth', 'RP', 15),
        (10, 2.902, 1.6, 2.8, 4, 0.0044, 'SCT2080KE', 'datasheetvth', 'RP', 20),
        (10, 4.5, 2.7, None, 5.6, 0.005, 'SCT3080KL', 'datasheetvth', 'RT', 5),
        (10, 4.5, 2.7, None, 5.6, 0.005, 'SCT3080KL', 'datasheetvth', 'RT', 10),
        (10, 4.5, 2.7, None, 5.6, 0.005, 'SCT3080KL', 'datasheetvth', 'RT', 15),
        (10, 4.5, 2.7, None, 5.6, 0.005, 'SCT3080KL', 'datasheetvth', 'RT', 20),
    ]

    # Insert data query
    cur.executemany(insert_data_query, data_to_insert)

    # Commit changes to DB
    con.commit()