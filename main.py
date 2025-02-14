import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Creación de una sesión de Spark
spark = SparkSession.builder \
    .appName("MySQL Connection") \
    .getOrCreate()

# Configuración de la conexión a la primera base de datos
connection1 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="maestro_saldo"
)

# Consulta a la primera base de datos MySQL
query1 = "SELECT m_pago_min, d_fec_top_pag, i_num_cuenta FROM emi_maestro_cartera_diaria"
cursor1 = connection1.cursor()
cursor1.execute(query1)

# Obtener los resultados de la consulta
rows1 = cursor1.fetchall()

# Crear un DataFrame de Spark a partir de los resultados
columns1 = [desc[0] for desc in cursor1.description]
df1 = spark.createDataFrame(rows1, columns1)

# Mostrar el DataFrame
#df1.show()


# Cerrar la conexión a la primera base de datos
cursor1.close()
connection1.close()


# Configuración de la conexión a la segunda base de datos
connection2 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="maestro_bb"
)

# Consulta a la segunda base de datos MySQL
query2 = "SELECT ma_c_nta, ma_ti_tarj FROM tc_maestro_bb2"
cursor2 = connection2.cursor()
cursor2.execute(query2)

# Obtener los resultados de la consulta
rows2 = cursor2.fetchall()

# Crear un DataFrame de Spark a partir de los resultados
columns2 = [desc[0] for desc in cursor2.description]
df2 = spark.createDataFrame(rows2, columns2)
# Mostrar el DataFrame
#df2.show()
# Cerrar la conexión a la segunda base de datos
cursor2.close()
connection2.close()

# Realizar el JOIN entre los DataFrames
joined_df = df1.join(df2, df1['i_num_cuenta'] == df2['ma_c_nta'])

# Seleccionar las columnas necesarias
selected_columns = [
    col("i_num_cuenta"),
    col("ma_c_nta"),
    col("m_pago_min"),
    col("d_fec_top_pag"),
    col("ma_ti_tarj")
]

result_df = joined_df.select(*selected_columns)

# Mostrar el DataFrame resultante
result_df.show()



# Configuración de la conexión a la nueva base de datos
connection3 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="prueba"
)

cursor3 = connection3.cursor()

# Crear la nueva tabla en la nueva base de datos
create_table_query = """
CREATE TABLE IF NOT EXISTS n_tc_debito (
    id INT AUTO_INCREMENT PRIMARY KEY,
    m_pago_min DECIMAL(15, 2),
    d_fec_top_pag DATE,
    i_num_cuenta VARCHAR(20),
    ma_c_nta VARCHAR(20),
    ma_ti_tarj VARCHAR(1)
)
"""
cursor3.execute(create_table_query)

# Insertar los datos del DataFrame en la nueva tabla
for row in result_df.collect():
    insert_query = """
    INSERT INTO n_tc_debito  (m_pago_min, d_fec_top_pag, i_num_cuenta, ma_c_nta, ma_ti_tarj)
    VALUES (%s, %s, %s, %s, %s)
    """
    cursor3.execute(insert_query, (row['m_pago_min'], row['d_fec_top_pag'], row['i_num_cuenta'], row['ma_c_nta'], row['ma_ti_tarj']))

# Confirmar la transacción
connection3.commit()

# Cerrar la conexión a la nueva base de datos
cursor3.close()
connection3.close()

# Detener la sesión de Spark
spark.stop()

