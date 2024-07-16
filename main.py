from src.Duvida import Duvida
from src.Fale_Conosco import Fale_conosco
from src.Sub_Fale_Conosco import Sub_Fale_Conosco
from src.Arquivos import Arquivos

def main():
    
    azurite_connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    sql_connection_string = 'DRIVER={SQL Server};SERVER=SRV-R550-SISTEMAS.pmc.intra;DATABASE=RECEITA;UID=ARGOVBR;PWD=PRO24NIM;'
    lista_de_tabelas = ["Duvida", "SubDuvida", "FaleConosco", "SubFaleConosco", "Servicos", "Arquivo"]
    
    # duvida = Duvida(sql_connection_string, azurite_connection_string, lista_de_tabelas[0])
    # duvida.atualiza_tabela_duvida()

    # fale_conosco = Fale_conosco(sql_connection_string, azurite_connection_string, lista_de_tabelas[2])
    # fale_conosco.atualiza_tabela_fale_conosco()

    # sub_fale_conosco = Sub_Fale_Conosco(sql_connection_string, azurite_connection_string, lista_de_tabelas[3])
    # sub_fale_conosco.atualiza_tabela_sub_fale_conosco()

    arquivos = Arquivos(sql_connection_string, azurite_connection_string, lista_de_tabelas[5])
    arquivos.atualiza_tabela_arquivos()

if __name__ == "__main__":
    main()