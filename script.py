
import requests
import pandas as pd
import io
import gzip
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, String # <--- Importante para definir a chave
from urllib.parse import urlparse

# --- CONFIGURAÇÕES ---
DATABASE_URL = os.getenv('DATABASE_URL')
URL_DADOS = "https://www.gpro.net/gb/GetMarketFile.asp?market=drivers&type=csv" 
TABELA_NOME = "mercado_pilotos"

# ATENÇÃO: O nome aqui deve ser IDÊNTICO ao cabeçalho da coluna no CSV/DataFrame
# Baseado no seu log anterior, parece ser "ID" (maiúsculo).
COLUNA_ID = "ID" 

def get_engine_blindada():
    """Gera a conexão limpa e com SSL configurado."""
    if not DATABASE_URL:
        raise ValueError("A variável DATABASE_URL está vazia!")

    # Limpa parâmetros extras do link (evita erro 'ssl-mode')
    url_str = DATABASE_URL.replace("mysql+pymysql://", "mysql://")
    parsed = urlparse(url_str)
    conn_str_limpa = f"mysql+pymysql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}{parsed.path}"
    
    return create_engine(
        conn_str_limpa, 
        connect_args={'ssl': {'check_hostname': False}}
    )

def main():
    print(">>> INICIANDO VERSÃO COM PRIMARY KEY (SOLUÇÃO DEFINITIVA) <<<")
    
    try:
        engine = get_engine_blindada()
    except Exception as e:
        print(f"ERRO DE CONFIGURAÇÃO: {e}")
        return

    # 1. Baixar e Tratar Dados
    print("1. Baixando e processando CSV...")
    df_final = None
    try:
        response = requests.get(URL_DADOS)
        response.raise_for_status()
        
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            try:
                # Tenta pular a primeira linha (caso tenha 'sep=')
                df_novo = pd.read_csv(f, skiprows=1)
                if len(df_novo.columns) < 2: raise ValueError("Vazio")
            except:
                f.seek(0)
                df_novo = pd.read_csv(f)

        # Remove espaços dos nomes das colunas (ex: " ID " vira "ID")
        df_novo.columns = [c.strip() for c in df_novo.columns]
        
        # Validação Crítica do ID
        if COLUNA_ID not in df_novo.columns:
            print(f"ERRO: A coluna '{COLUNA_ID}' não existe no CSV.")
            print(f"Colunas encontradas: {list(df_novo.columns)}")
            return

        from datetime import datetime
        df_novo['data_coleta'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Carrega dados antigos para Upsert
        try:
            with engine.connect() as conn:
                df_db = pd.read_sql(TABELA_NOME, conn)
        except:
            df_db = pd.DataFrame()

        # Lógica de Atualização (Mantém histórico de quem saiu)
        if not df_db.empty and COLUNA_ID in df_db.columns:
            df_db.set_index(COLUNA_ID, inplace=True)
            df_novo.set_index(COLUNA_ID, inplace=True)
            df_db.update(df_novo)
            novos = df_novo[~df_novo.index.isin(df_db.index)]
            df_final = pd.concat([df_db, novos])
            df_final.reset_index(inplace=True)
        else:
            df_final = df_novo
            
        print(f"-> Dados prontos: {len(df_final)} registros.")

    except Exception as e:
        print(f"ERRO NO PROCESSAMENTO: {e}")
        return

    # 2. Salvamento com CHAVE PRIMÁRIA
    print("2. Salvando no Banco...")
    try:
        # engine.begin() abre transação com Commit automático
        with engine.begin() as conn:
            
            # AQUI ESTÁ A SOLUÇÃO DEFINITIVA:
            # Definimos explicitamente que COLUNA_ID é do tipo Integer e é Primary Key.
            # Isso satisfaz o Aiven sem precisar de hacks.
            df_final.to_sql(
                TABELA_NOME, 
                conn, 
                if_exists='replace', 
                index=False, 
                chunksize=500,
                dtype={COLUNA_ID: Integer(primary_key=True)}
            )
            print("-> Tabela salva e PK criada com sucesso!")

    except Exception as e:
        print(f"ERRO CRÍTICO AO SALVAR: {e}")
        return

    # 3. Verificação Final
    print("3. Verificando dados...")
    try:
        with engine.connect() as conn:
            qtde = conn.execute(text(f"SELECT count(*) FROM {TABELA_NOME}")).scalar()
            print(f"--> SUCESSO! Total de registros no banco: {qtde}")
    except Exception as e:
        print(f"Erro ao verificar: {e}")

if __name__ == "__main__":
    main()
