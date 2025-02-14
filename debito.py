import mysql.connector
from pyspark.sql import SparkSession

#C:\Users\Desarrollador\Desktop\RESPALDO\Downloads\mysql-connector-j-8.1.0
# Creación de una sesión de Spark
jdbc_driver_path = r"C:\Users\Desarrollador\Desktop\debe\mysql-connector-j-8.0.33.jar"
spark = SparkSession.builder \
    .appName("MySQL Connection") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.executor.extraClassPath", jdbc_driver_path) \
    .getOrCreate()

# Configuración de la conexión a la primera base de datos
connection1 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="maestro_saldo"
)

#Tablas

#
# Consulta base de datos MySQL en la base maestro
query1 = """
    SELECT 
        e.m_pago_min, 
        e.d_fec_top_pag, 
        e.i_num_cuenta, 
        t.ma_c_nta, 
        t.ma_ti_tarj 
    FROM 
        emi_maestro_cartera_diaria e
    JOIN 
        tc_maestro_prueba t 
    ON 
        e.i_num_cuenta = t.ma_c_nta

"""

cursor1 = connection1.cursor()
cursor1.execute(query1)

# Obtener los resultados de la consulta
rows1 = cursor1.fetchall()

# Crear un DataFrame de Spark a partir de los resultados
columns1 = [desc[0] for desc in cursor1.description]
df1 = spark.createDataFrame(rows1, columns1)

# Mostrar el DataFrame
df1.show()


#Cargar la información a la nueva tabla que existe solo carga información 
df1.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/maestro_saldo") \
    .option("dbtable", "n_tc_debito") \
    .option("user", "root") \
    .option("password", "admin") \
    .mode("append") \
    .save()


# Cerrar la conexión a la primera base de datos
cursor1.close()
connection1.close()
