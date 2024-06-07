from flask import Flask, render_template, request, redirect, url_for, jsonify

from azure.storage.blob import BlobServiceClient, ContainerClient

import mysql.connector as msql

from mysql.connector import Error, connect

import pandas as pd

app = Flask(__name__)

# Configurações do banco de dados MySQL
db_config = {
    'host': '<ip_host_do_servidor>',
    'user': '<user_name>',
    'password': '<password>',
    'database': '<database_name>'
}

# Rota para a página inicial
@app.route('/')
def index():
    return render_template('index.html')

# Rota para a página de importar os dados
@app.route('/importar', methods=['GET', 'POST'])
def importar():
    if request.method == 'POST':
        try:

            # Conexão com o Azure Storage Account
            CONNECTION_STRING = <CONNECTION_STRING>
            CONTAINER_CLIENT = <CONTAINER_CLIENT>

            blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

            container_client = blob_service_client.get_container_client(CONTAINER_CLIENT)

            blobs_list = container_client.list_blobs()

            for blob in blobs_list:
                blob_client = blob_service_client.get_blob_client(container=CONTAINER_CLIENT, blob=blob.name)

            with open("seu_arquivo_local.csv", "wb") as meu_csv:
                blob_data = blob_client.download_blob()
                meu_csv.write(blob_data.readall())

            # Ler o arquivo CSV com o Pandas
            v_dados = pd.read_csv(meu_csv.name, encoding="latin1", on_bad_lines="warn", sep=";", decimal=",")
            v_dados.fillna(0, inplace=True)        

            cnx = msql.connect(**db_config)

            if cnx.is_connected():

                v_cursor = cnx.cursor()
                v_cursor.execute("SELECT DATABASE()")
                v_registro = v_cursor.fetchone()

                v_cursor.execute("SET autocommit=0")
                v_cursor.execute("SET SESSION sql_mode = ''")

                v_count = 0
                v_commit = 1000

                for i, row in v_dados.iterrows():
                    sql = "INSERT INTO inmet (Data, Hora, PRECIPITACAO_TOTAL, PRESSAO_ATMOSFERICA, PRESSAO_ATMOSFERICA_MAX, PRESSAO_ATMOSFERICA_MIN, RADIACAO_GLOBAL, TEMPERATURA_AR, TEMPERATURA_PONTO_ORVALHO, TEMPERATURA_MAX, TEMPERATURA_MIN, TEMPERATURA_ORVALHO_MAX, TEMPERATURA_ORVALHO_MIN, UMIDADE_REL_MAX, UMIDADE_REL_MIN, UMIDADE_REL_AR, VENTO_DIRECAO, VENTO_RAJADA, VENTO_VELOCIDADE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                    v_cursor.execute(sql, tuple(row))

                    v_count = i

                    if (v_count % v_commit) == 0:                        
                        cnx.commit()

                    cnx.commit()

                v_cursor.close()
                cnx.close()

                return f"Registro(s) Inserido(s): {v_count}"

        except Exception as e:
            # Adiciona mensagem de erro no log e retorna uma mensagem detalhada
            return f"Ocorreu um erro ao processar os dados: {e}"
            
    return render_template('importar.html')

if __name__ == '__main__':
    app.run(debug=True)