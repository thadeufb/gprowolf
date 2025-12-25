
import requests
import pandas as pd
import io
import gzip
import os
from sqlalchemy import create_engine, text
from urllib.parse import urlparse

# --- CONFIGURAÇÕES ---
DATABASE_URL = os.getenv('DATABASE_URL')
URL_DADOS = "https://www.gpro.net/gb/GetMarketFile.asp?market=drivers&type=csv" 
TABELA_NOME = "mercado_pilotos"
COLUNA_ID = "ID" 

def get_engine_blindada():
    """Gera a conexão limpa."""
    if not DATABASE_URL:
        raise ValueError("A variável DATABASE_URL está vazia!")

    # Limpa URL para evitar erro SSL do Aiven
    url_str = DATABASE_URL.replace("mysql+pymysql://", "mysql://")
    parsed = urlparse(url_str)
    conn_str_limpa = f"mysql+pymysql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}{parsed.path}"
    
    return create_engine(
        conn_str_limpa, 
        connect_args={'ssl': {'check_hostname': False}}
    )

def main():
    print(">>> INICIANDO VERSÃO: ESTRUTURA FIXA (DELETE + APPEND) <<<")
    
    try:
        engine = get_engine_blindada()
    except Exception as e:
        print(f"ERRO DE CONFIGURAÇÃO: {e}")
        return

    # 1. Baixar e Processar
    print("1. Baixando e processando CSV...")
    df_final = None
    try:
        response = requests.get(URL_DADOS)
        response.raise_for_status()
        
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            try:
                df_novo = pd.read_csv(f, skiprows=1)
                if len(df_novo.columns) < 2: raise ValueError("Vazio")
            except:
                f.seek(0)
                df_novo = pd.read_csv(f)

        df_novo.columns = [c.strip() for c in df_novo.columns]
        
        # Garante que temos a coluna ID
        if COLUNA_ID not in df_novo.columns:
            print(f"ERRO: Coluna '{COLUNA_ID}' não encontrada.")
            return

        from datetime import datetime
        df_novo['data_coleta'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Lê o banco atual
        try:
            # Aqui não precisamos de transação complexa, apenas leitura
            df_db = pd.read_sql(TABELA_NOME, engine)
        except:
            # Se der erro aqui, é provável que a tabela NÃO exista.
            # Nesse caso, o script vai falhar no passo 2, o que é CORRETO,
            # pois combinamos que você criaria a tabela manualmente.
            print("AVISO: Não foi possível ler a tabela atual. Assumindo vazia.")
            df_db = pd.DataFrame()

        # Merge (Upsert Logic)
        if not df_db.empty and COLUNA_ID in df_db.columns:
            df_db.set_index(COLUNA_ID, inplace=True)
            df_novo.set_index(COLUNA_ID, inplace=True)
            df_db.update(df_novo)
            novos = df_novo[~df_novo.index.isin(df_db.index)]
            df_final = pd.concat([df_db, novos])
            df_final.reset_index(inplace=True)
        else:
            df_final = df_novo
            
        print(f"-> Dados consolidados: {len(df_final)} registros.")

    except Exception as e:
        print(f"ERRO NO PROCESSAMENTO: {e}")
        return

    # 2. Salvamento Limpo (Usa a tabela existente)
    print("2. Salvando no Banco...")
    try:
        # Abre transação segura (Tudo ou Nada)
        with engine.begin() as conn:
            
            # PASSO A: Limpa os dados velhos, mas MANTÉM a tabela e a Primary Key
            print("-> Limpando dados antigos...")
            conn.execute(text(f"DELETE FROM {TABELA_NOME}"))
            
            # PASSO B: Insere os dados novos
            # if_exists='append' significa: "Use a tabela que já existe, não recrie"
            print("-> Inserindo dados consolidados...")
            df_final.to_sql(
                TABELA_NOME, 
                conn, 
                if_exists='append',  # <--- O SEGREDO ESTÁ AQUI
                index=False, 
                chunksize=500
            )
            
            print("-> SUCESSO! Dados atualizados mantendo a estrutura.")

    except Exception as e:
        print(f"ERRO AO SALVAR: {e}")
        print("DICA: Verifique se a tabela existe no banco e se as colunas batem com o CSV.")
        return

    # 3. Verificação
    try:
        with engine.connect() as conn:
            qtde = conn.execute(text(f"SELECT count(*) FROM {TABELA_NOME}")).scalar()
            print(f"--> Total no banco: {qtde}")
    except:
        pass

if __name__ == "__main__":
    main()
