import requests
import pandas as pd
import io
import gzip
import os
from sqlalchemy import create_engine, text
from urllib.parse import urlparse

# --- CONFIGURAÇÕES ---
DATABASE_URL = os.getenv('DATABASE_URL')
URL_DADOS = "https://www.gpro.net/gb/GetMarketFile.asp?market=drivers&type=csv" # CONFIRME SE O LINK É ESSE
TABELA_NOME = "mercado_pilotos"
COLUNA_ID = "id_piloto" 

def get_engine_blindada():
    """
    Função robusta que limpa o link de conexão na força bruta
    para evitar erros de SSL do Aiven/SQLAlchemy.
    """
    if not DATABASE_URL:
        raise ValueError("A variável DATABASE_URL está vazia!")

    # 1. Parse do link original (separa usuário, senha, host, etc)
    # Removemos o prefixo mysql+pymysql:// temporariamente para o urlparse entender
    url_str = DATABASE_URL.replace("mysql+pymysql://", "mysql://")
    parsed = urlparse(url_str)

    # 2. Debug (Sem mostrar a senha)
    print(f"--- DEBUG CONEXÃO ---")
    print(f"Host detectado: {parsed.hostname}")
    print(f"Banco detectado: {parsed.path[1:]}") # Remove a barra inicial
    print(f"Query params removidos: {parsed.query}") # Mostra o que estamos jogando fora (ex: ssl-mode)
    
    # 3. Reconstrução Limpa
    # Montamos a string manualmente apenas com o necessário
    # Formato: mysql+pymysql://USER:PASS@HOST:PORT/DB_NAME
    conn_str_limpa = f"mysql+pymysql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}{parsed.path}"
    
    print("Link reconstruído e limpo com sucesso.")
    print("---------------------")

    # 4. Cria a engine injetando o SSL corretamente via argumentos, não via link
    return create_engine(
        conn_str_limpa, 
        connect_args={'ssl': {'check_hostname': False}}
    )

def main():
    print("Iniciando Script...")
    
    try:
        engine = get_engine_blindada()
    except Exception as e:
        print(f"ERRO FATAL NA CONFIGURAÇÃO: {e}")
        return

    # 1. Teste de Conexão Rápido
    print("Testando conexão com o banco...")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("-> Conexão OK!")
    except Exception as e:
        print(f"ERRO AO CONECTAR: {e}")
        return

    # 2. Carregar dados antigos
    try:
        print(f"Lendo tabela '{TABELA_NOME}'...")
        df_db = pd.read_sql(TABELA_NOME, engine)
        print(f"-> {len(df_db)} registros existentes.")
    except Exception:
        print("-> Tabela nova ou vazia.")
        df_db = pd.DataFrame()

    # 3. Baixar Dados
    print("Baixando CSV...")
    try:
        response = requests.get(URL_DADOS)
        response.raise_for_status()
        
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            # ATENÇÃO: Verifique se o CSV usa vírgula ou ponto e vírgula
            # Se der erro de colunas, troque para sep=';'
            df_novo = pd.read_csv(f)
            
        from datetime import datetime
        df_novo['data_coleta'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"-> Download OK: {len(df_novo)} pilotos.")

    except Exception as e:
        print(f"ERRO NO DOWNLOAD: {e}")
        return

    # 4. Consolidação
    if not df_db.empty:
        # Garante que a coluna ID existe antes de setar index
        if COLUNA_ID in df_novo.columns and COLUNA_ID in df_db.columns:
            df_db.set_index(COLUNA_ID, inplace=True)
            df_novo.set_index(COLUNA_ID, inplace=True)
            
            df_db.update(df_novo)
            novos = df_novo[~df_novo.index.isin(df_db.index)]
            
            df_final = pd.concat([df_db, novos])
            df_final.reset_index(inplace=True)
        else:
            print(f"AVISO: Coluna ID '{COLUNA_ID}' não encontrada. Substituindo tudo.")
            df_final = df_novo
    else:
        df_final = df_novo

    # 5. Salvar
    print("Salvando no Aiven...")
    try:
        df_final.to_sql(
            TABELA_NOME, 
            engine, 
            if_exists='replace', 
            index=False, 
            chunksize=500
        )
        print("-> SUCESSO TOTAL! FIM.")
    except Exception as e:
        print(f"ERRO AO SALVAR: {e}")

if __name__ == "__main__":
    main()
