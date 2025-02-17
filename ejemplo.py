import mysql.connector

# Configurar la conexión a la base de datos de origen
db_origen = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="maestro_saldo"
)

# debito conexion
db_destino = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="maestro_saldo"
)

try:
    # Crear cursor para la base de datos de origen
    cursor_origen = db_origen.cursor()
    
    # Ejecutar el procedimiento almacenado
    cursor_origen.callproc("sp_ObtenerDatosMedioPago")
    
    # Obtener los resultados del procedimiento almacenado
    for resultado in cursor_origen.stored_results():
        datos = resultado.fetchall()  # Lista de tuplas con los datos obtenidos
    
    # Cerrar el cursor de origen
    cursor_origen.close()
    db_origen.close()

    # Si hay datos obtenidos, insertarlos en la tabla 'debito'
    if datos:
        # Crear cursor para la base de datos destino
        cursor_destino = db_destino.cursor()

        # Crear la consulta INSERT dinámicamente
        insert_query = """
        INSERT INTO debito (
            mp_pan, mp_cuenta, mp_identcli, mp_status, mp_indsittar, mp_calpart,
            p1_indrepos, p2_apelli1, p2_nombre, lc_Catargo1_cte, lc_Catargo2_cta,
            m_pago_min, m_deuda_vcda, d_fec_top_pag, i_num_pagos_vcdos, cu_forpago,
            ri_FECVENMOV, ic_impago
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Insertar cada fila en la tabla 'debito'
        cursor_destino.executemany(insert_query, datos)
        
        # Confirmar cambios en la base de datos destino
        db_destino.commit()
        
        print(f" {cursor_destino.rowcount} registros insertados en la tabla 'debito'.")

        # Cerrar cursor y conexión de destino
        cursor_destino.close()
    
    db_destino.close()

except mysql.connector.Error as err:
    print(f" Error: {err}")

