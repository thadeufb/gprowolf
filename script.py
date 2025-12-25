import requests
import pandas as pd
import io
import gzip
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# --- CONFIGURAÇÕES ---
# O script tenta pegar do GitHub Secrets, senão procura localmente (para testes)
DATABASE_URL = os.getenv('DATABASE_URL')

URL_DADOS = "https://www.gpro.net/gb/GetMarketFile.asp?market=drivers&type=csv" # SEU LINK AQUI
TABELA_NOME = "mercado_pilotos"
COLUNA_ID = "id_piloto" 

def get_engine():
    if not DATABASE_URL:
        raise ValueError("A variável de ambiente DATABASE_URL não foi encontrada.")
    
    # Configuração específica para Aiven (SSL)
    # connect_args={'ssl': {'check_hostname': False}} permite conectar via SSL 
    # sem precisar baixar o arquivo de certificado CA manualmente no GitHub.
    return create_engine(
        DATABASE_URL, 
        connect_args={'ssl': {'check_hostname': False}}
    )

def main():
    print(f"[{datetime.now()}] Iniciando ETL...")
    
    try:
        engine = get_engine()
    except Exception as e:
        print(f"ERRO de Configuração: {e}")
        return

    # 1. Carregar dados antigos (Snapshot anterior)
    try:
        # Verifica conexão e existência da tabela
        with engine.connect() as conn:
            conn.execute(text(f"SELECT 1 FROM {TABELA_NOME} LIMIT 1"))
            
        print("-> Conectado ao Aiven. Lendo dados antigos...")
        df_db = pd.read_sql(TABELA_NOME, engine)
        print(f"-> {len(df_db)} registros carregados.")
    except Exception:
        print("-> Tabela não encontrada ou vazia. Criando do zero.")
        df_db = pd.DataFrame()

    # 2. Baixar Dados
    print("-> Baixando arquivo...")
    try:
        response = requests.get(URL_DADOS)
        response.raise_for_status()
        
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            df_novo = pd.read_csv(f) # Ajuste sep=';' se necessário
            
        df_novo['data_coleta'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"-> Download OK: {len(df_novo)} novos registros.")

    except Exception as e:
        print(f"ERRO no Download: {e}")
        return

    # 3. Consolidação (Upsert)
    if not df_db.empty:
        df_db.set_index(COLUNA_ID, inplace=True)
        df_novo.set_index(COLUNA_ID, inplace=True)
        
        df_db.update(df_novo)
        novos = df_novo[~df_novo.index.isin(df_db.index)]
        
        df_final = pd.concat([df_db, novos])
        df_final.reset_index(inplace=True)
    else:
        df_final = df_novo

    # 4. Salvar no Aiven
    print("-> Salvando no banco...")
    try:
        df_final.to_sql(
            TABELA_NOME, 
            engine, 
            if_exists='replace', 
            index=False, 
            chunksize=500 # Aiven gosta de pacotes menores
        )
        print("-> SUCESSO! Banco atualizado.")
    except Exception as e:
        print(f"ERRO ao salvar: {e}")

if __name__ == "__main__":
    main()
