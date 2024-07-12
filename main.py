from src.Duvida import Duvida
from src.FaleConosco import Fale_conosco

def main():
    
    azurite_connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    sql_connection_string = 'DRIVER={SQL Server};SERVER=SRV-R550-SISTEMAS.pmc.intra;DATABASE=RECEITA;UID=ARGOVBR;PWD=PRO24NIM;'
    lista_de_tabelas = ["Duvida", "SubDuvida", "FaleConosco", "SubFaleConosco", "Servicos"]
    
    # duvida = Duvida(sql_connection_string, azurite_connection_string, lista_de_tabelas[0])
    # duvida.atualiza_tabela_duvida()

    fale_conosco = Fale_conosco(sql_connection_string, azurite_connection_string, lista_de_tabelas[2])
    fale_conosco.atualiza_tabela_fale_conosco()

    
if __name__ == "__main__":
    main()