import sqlalchemy
import cx_Oracle
import pandas as pd

class sqlazo:
    def __init__(self,usuario,contrasena,host,servicio,puerto=1521) -> None:
        pass
        self.usuario=usuario
        self.contrasena=contrasena
        self.host=host
        self.servicio=servicio
        self.puerto=puerto
        self.conn = sqlalchemy.create_engine('oracle+cx_oracle://{usuario}:{contrasena}@{host}:{puerto}/?service_name={servicio}'.format(
            usuario = self.usuario,
            contrasena = self.contrasena,
            host = self.host,
            servicio = self.servicio,
            puerto = self.puerto  
            ))
    def from_df_create(self,dataframe,tabla):
        dtyp = {c:sqlalchemy.types.VARCHAR(dataframe[c].str.len().max())
                for c in dataframe.columns[dataframe.dtypes == 'object'].tolist()}

        dataframe.to_sql(tabla, self.conn, schema=self.usuario, if_exists='replace', index = False, dtype=dtyp)
    
    def from_df_upsert(self,dataframe,tabla,llave):
        dtyp = {c:sqlalchemy.types.VARCHAR(dataframe[c].str.len().max())
                for c in dataframe.columns[dataframe.dtypes == 'object'].tolist()}
        dataframe.to_sql('temp_{tabla}'.format(tabla=tabla), self.conn, schema=self.usuario, if_exists='replace', index = False, dtype=dtyp)

        ##########
        conkey=list(dataframe.columns)
        sinkey=list(dataframe.columns)
        sinkey.remove(llave)
        ####
        igual=''
        for col in sinkey:
                igu='f.{col}=t.{col}'.format(col=col)
                igual=igual+','+igu
        igual=igual[1:]
        ####
        update = igual
        insert = 'f.'+',f.'.join(conkey)
        values = 't.'+',t.'.join(conkey)
        ###########

        sql = """
        MERGE INTO {tabla} f
        USING temp_{tabla} t
        ON (f.{llave} = t.{llave})
        WHEN MATCHED THEN
                UPDATE SET {update}
        WHEN NOT MATCHED THEN
                INSERT ({insert})
                VALUES ({values})
        """.format(
                tabla = tabla,
                llave = llave,
                update = update, #MENOS LA LLAVE
                insert = insert, #ALLCOL
                values = values #ALLCOL
        )
        print(sql)
        with self.conn.begin() as connect:     # TRANSACTION
                connect.execute(sql)
