import pyodbc
import pandas as pd
from collections import OrderedDict
from azure.data.tables import TableServiceClient

class Categoria:
    def __init__(self, str_sql, str_azurite, str_nome_tabela):
        self.str_sql = str_sql
        self.str_azurite = str_azurite
        self.str_nome_tabela = str_nome_tabela

    def conexao_sql_duvida(self, str_sql): # faz a conexão busca os dados e retorna um lista de objetos
        try:
            connection = pyodbc.connect(self.str_sql)
            query_sql = """
                        SELECT  
                            [idCategoria] as RowKey 
                            ,[Categoria] as Nome
                            ,[Ativo]
                        FROM 
                            [RECEITA].[dbo].[ARQUIVO_CATEGORIA]
                    """
            df = pd.read_sql(query_sql, connection)
            connection.close()
            return df.to_dict('records')
        except Exception as e:
            print(f'Erro na conexão {e}')

    def inserindo_partition_key(self, partition_key, objeto):
        novo_campo = OrderedDict([('PartitionKey', partition_key)])
        objeto.update(novo_campo)

    def inserir_etag(self, etag, objeto):
        novo_campo = OrderedDict([('Etag', etag)])
        objeto.update(novo_campo)

    def verificar_entities(self, lista_sql, lista_azurite, partition_key):
        lista_para_enviar_azure = []
        inserir = False
        #se lista azurite vazia monta os objetos e os adiciona para inserir
        if not lista_azurite:
            for elemento in lista_sql:
                self.inserindo_partition_key(partition_key, elemento)
                self.inserir_etag(elemento['Ativo'], elemento)
                lista_para_enviar_azure.append(elemento)
        else:
            for elemento_sql in lista_sql:
                for elemento_azurite in lista_azurite:
                    if elemento_sql['RowKey'] == elemento_azurite['RowKey']:
                        if elemento_sql['Nome'] == elemento_azurite['Nome'] and elemento_sql['Ativo'] == elemento_azurite['Ativo']:
                            if elemento_sql['Ativo'] == elemento_azurite['Etag']:
                                inserir = False
                                break
                            else:
                                inserir = True
                        else:
                            inserir = True
                    else:
                        inserir = True
                    
                if inserir == True:
                    lista_para_enviar_azure.append(elemento_sql)
            
            for elemento in lista_para_enviar_azure:
                self.inserindo_partition_key(partition_key, elemento)
                self.inserir_etag(elemento['Ativo'], elemento)
                if 'Etag' in elemento:
                    self.inserir_etag(elemento['Ativo'], elemento)

        return lista_para_enviar_azure
    
    
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

    def atualiza_tabela_categoria(self):
        partition_key_duvida = "b8a4603e-07a2-44f7-8ed4-7c934d837ab4"
        lista_sql = self.conexao_sql_duvida(self.str_sql)
        lista_azurite, table_client = self.conexao_azure_storage(self.str_azurite, self.str_nome_tabela)
        if lista_sql is not None:
            self.converter_rowkey_para_string(lista_sql)
        lista_para_inserir = self.verificar_entities(lista_sql, lista_azurite, partition_key_duvida)    
        self.enviando_azure(lista_para_inserir, table_client)