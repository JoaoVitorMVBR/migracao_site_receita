import pyodbc
import pandas as pd
from collections import OrderedDict
from azure.data.tables import TableServiceClient

class Fale_conosco:
    
    def __init__(self, str_sql, str_azurite, str_nome_tabela):
        self.str_sql = str_sql
        self.str_azurite = str_azurite
        self.str_nome_tabela = str_nome_tabela

    def conexao_sql_fale_conosco(self, str_sql):
        try:
            connection = pyodbc.connect(str_sql)
            query_sql = """
                    SELECT 
                        [idCategoria] as RowKey
                        ,[Nome] as Categoria
                        ,[Ativo]
                    FROM 
                        [RECEITA].[dbo].[FALE_CONOSCO_CATEGORIA]
                """
            df = pd.read_sql(query_sql, connection)
            connection.close()
            return df.to_dict('records')
        except Exception as e:
            print(f'Erro na conexão {e}')

    def inserindo_etag_fale_conosco(self, etag, lista_para_inserir_azure):
        novo_campo = OrderedDict([('Etag', etag)])
        for elemento in lista_para_inserir_azure:
            elemento.update(novo_campo)
            
    def verificar_entities_fale_conosco(self, lista_sql, lista_azurite):
        trigger = False
        inserir_azurite = []
        for elemento_sql in lista_sql:
            trigger = False
            for elemento_azurite in lista_azurite:
                if elemento_sql['RowKey'] == elemento_azurite['RowKey']:
                    trigger = True
                    if elemento_sql['Categoria'] == elemento_azurite['Categoria'] and elemento_sql['Ativo'] == elemento_azurite['Ativo']:
                        pass
                    else:
                        inserir_azurite.append(elemento_sql)
            if trigger == False:
                inserir_azurite.append(elemento_sql)
        return inserir_azurite
    
    def inserindo_partition_key_fale_conosco(self, partition_key, lista_para_inserir_azure):
        novo_campo = OrderedDict([('PartitionKey', partition_key)])
        for elemento in lista_para_inserir_azure:
            elemento.update(novo_campo)
    
    def ler_entidade_retorna_lista(self, entities):
        lista_azurite = []
        for entity in entities:
            lista_azurite.append(entity)
        return lista_azurite
    
    def conexao_azure_storage(self, str_azurite, table_name): # faz a conexão e retorna uma lista do objeto de entidade da tabela requerida
        try:
            table_service_client = TableServiceClient.from_connection_string(conn_str=str_azurite)
            table_client = table_service_client.get_table_client(table_name=table_name)
            entities = table_client.query_entities("RowKey")
            lista = self.ler_entidade_retorna_lista(entities)
            return lista, table_client
        except Exception as ex:
            print(f"Erro ao acessar a tabela: {ex}")
    
    def converter_rowkey_para_string(self, lista_sql):
        for elemento in lista_sql:
            for chave, valor in elemento.items():
                if chave == 'RowKey':
                    elemento[chave] = str(valor)

    def enviando_azure(self, lista_para_inserir_azure, table_client):
        if lista_para_inserir_azure is not None:
            for entity in lista_para_inserir_azure:
                try:
                    table_client.upsert_entity(entity=entity) 
                except Exception as e:
                    print(f'falha ao inserir {entity}: {str(e)}')

    def atualiza_tabela_fale_conosco(self):
        #acessando banco sql e azure tabela Duvida
        partition_key_duvida = "c124bab6-7011-4cfb-8ca9-4934b45e30f5"
        lista_sql = self.conexao_sql_fale_conosco(self.str_sql)
        lista_azurite, table_client = self.conexao_azure_storage(self.str_azurite, self.str_nome_tabela)
        if lista_sql is not None:
            self.converter_rowkey_para_string(lista_sql)
        lista_para_inserir = self.verificar_entities_fale_conosco(lista_sql, lista_azurite)    
        self.inserindo_partition_key_fale_conosco(partition_key_duvida, lista_para_inserir)
        self.inserindo_etag_fale_conosco('S', lista_para_inserir)
        self.enviando_azure(lista_para_inserir, table_client)