import requests
import pandas as pd
import io
import gzip
import os
from sqlalchemy import create_engine, text
from urllib.parse import urlparse

# --- CONFIGURAÇÕES ---
DATABASE_URL = os.getenv('DATABASE_URL')
# Substitua pelo link REAL do seu arquivo
URL_DADOS = "https://www.gpro.net/gb/GetMarketFile.asp?market=drivers&type=csv" 
TABELA_NOME = "mercado_pilotos"
COLUNA_ID = "ID" # Certifique-se que o CSV tem essa coluna (ex: id, driverId, etc)

def get_engine_blindada():
    """Limpa o link e prepara a engine"""
    if not DATABASE_URL:
        raise ValueError("A variável DATABASE_URL está vazia!")

    url_str = DATABASE_URL.replace("mysql+pymysql://", "mysql://")
    parsed = urlparse(url_str)
    
    # Reconstrói o link limpo (sem lixo de query params)
    conn_str_limpa = f"mysql+pymysql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}{parsed.path}"
    
    return create_engine(
        conn_str_limpa, 
        connect_args={'ssl': {'check_hostname': False}}
    )

def main():
    print("Iniciando Script (Correção CSV + PK)...")
    
    try:
        engine = get_engine_blindada()
    except Exception as e:
        print(f"ERRO DE CONFIG: {e}")
        return

    # --- TRUQUE PARA O AIVEN ---
    # Tenta desativar a exigência de Primary Key para esta sessão
    try:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION sql_require_primary_key = 0"))
            print("-> Trava de Primary Key desativada temporariamente.")
    except Exception as e:
        print(f"AVISO: Não foi possível desativar a trava de PK: {e}")
        # Se falhar, o script tenta continuar, mas pode dar erro no to_sql depois

    # 1. Carregar dados antigos
    try:
        df_db = pd.read_sql(TABELA_NOME, engine)
        print(f"-> Banco: {len(df_db)} registros carregados.")
    except Exception:
        print("-> Banco: Tabela nova ou vazia.")
        df_db = pd.DataFrame()

    # 2. Baixar e Ler CSV (Com correção de formato)
    print("Baixando CSV...")
    try:
        response = requests.get(URL_DADOS)
        response.raise_for_status()
        
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            # TENTATIVA 1: Ler pulando a primeira linha (se tiver 'sep=')
            try:
                # header=0 significa que a linha 0 (pós skip) é o cabeçalho
                df_novo = pd.read_csv(f, skiprows=1) 
                
                # Verificação de sanidade: Se as colunas parecerem erradas, tenta sem pular
                if "id" not in str(df_novo.columns).lower() and len(df_novo.columns) < 2:
                    print("-> Aviso: Leitura com skiprows=1 pareceu estranha. Tentando leitura normal...")
                    f.seek(0) # Volta pro começo do arquivo
                    df_novo = pd.read_csv(f)
            except:
                f.seek(0)
                df_novo = pd.read_csv(f)

        # Normaliza nomes de colunas (tira espaços e deixa minúsculo)
        df_novo.columns = [c.strip() for c in df_novo.columns]
        
        # IMPORTANTE: Confere se a coluna ID existe mesmo
        if COLUNA_ID not in df_novo.columns:
            print(f"ERRO CRÍTICO: Coluna '{COLUNA_ID}' não encontrada no CSV!")
            print(f"Colunas detectadas: {list(df_novo.columns)}")
            # Tenta achar uma coluna parecida automaticamente
            possiveis = [c for c in df_novo.columns if 'id' in c.lower()]
            if possiveis:
                print(f"Sugestão: Talvez o ID seja '{possiveis[0]}'?")
            return

        from datetime import datetime
        df_novo['data_coleta'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"-> Download OK: {len(df_novo)} registros.")

    except Exception as e:
        print(f"ERRO NO DOWNLOAD/LEITURA: {e}")
        return

    # 3. Consolidação (Upsert)
    if not df_db.empty and COLUNA_ID in df_db.columns:
        df_db.set_index(COLUNA_ID, inplace=True)
        df_novo.set_index(COLUNA_ID, inplace=True)
        
        df_db.update(df_novo)
        novos = df_novo[~df_novo.index.isin(df_db.index)]
        
        df_final = pd.concat([df_db, novos])
        df_final.reset_index(inplace=True)
    else:
        df_final = df_novo

    # 4. Salvar
    print("Salvando no Aiven...")
    try:
        # Novamente desativa a trava antes de salvar (garantia)
        with engine.connect() as conn:
            conn.execute(text("SET SESSION sql_require_primary_key = 0"))
            
            df_final.to_sql(
                TABELA_NOME, 
                conn, # Usa a conexão com a sessão configurada
                if_exists='replace', 
                index=False, 
                chunksize=500
            )
        print("-> SUCESSO TOTAL!")
    except Exception as e:
        print(f"ERRO AO SALVAR: {e}")

if __name__ == "__main__":
    main()
