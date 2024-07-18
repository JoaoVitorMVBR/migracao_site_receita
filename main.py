from src.Duvida import Duvida
from src.Fale_Conosco import Fale_conosco
from src.Sub_Fale_Conosco import Sub_Fale_Conosco
from src.Arquivos import Arquivos
from src.Servicos import Servicos
from src.Categoria import Categoria
from src.Sub_Duvida import Sub_Duvida

def main():
    
    azurite_connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    sql_connection_string = 'DRIVER={SQL Server};SERVER=SRV-R550-SISTEMAS.pmc.intra;DATABASE=RECEITA;UID=ARGOVBR;PWD=PRO24NIM;'
    lista_de_tabelas = ["Duvidas", "SubDuvida", "FaleConosco", "SubFaleConosco", "Servicos", "Arquivos", "Categorias"]
    
    duvida = Duvida(sql_connection_string, azurite_connection_string, lista_de_tabelas[0])
    duvida.atualiza_tabela_duvida()

    sub_duvida = Sub_Duvida(sql_connection_string, azurite_connection_string, lista_de_tabelas[1])
    sub_duvida.atualiza_tabela_sub_duvida()

    fale_conosco = Fale_conosco(sql_connection_string, azurite_connection_string, lista_de_tabelas[2])
    fale_conosco.atualiza_tabela_fale_conosco()

    sub_fale_conosco = Sub_Fale_Conosco(sql_connection_string, azurite_connection_string, lista_de_tabelas[3])
    sub_fale_conosco.atualiza_tabela_sub_fale_conosco()

    arquivos = Arquivos(sql_connection_string, azurite_connection_string, lista_de_tabelas[5])
    arquivos.atualiza_tabela_arquivos()

    servicos = Categoria(sql_connection_string, azurite_connection_string, lista_de_tabelas[6])
    servicos.atualiza_tabela_categoria()

if __name__ == "__main__":
    main()