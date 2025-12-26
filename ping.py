import os
from sqlalchemy import create_engine, text
from urllib.parse import urlparse

# Reutilizando a lógica blindada que já funcionou pra você
DATABASE_URL = os.getenv('DATABASE_URL')

def ping_database():
    if not DATABASE_URL:
        print("Erro: DATABASE_URL não encontrada.")
        return

    # Limpeza do link (igual ao script principal)
    try:
        url_str = DATABASE_URL.replace("mysql+pymysql://", "mysql://")
        parsed = urlparse(url_str)
        conn_str_limpa = f"mysql+pymysql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}{parsed.path}"
        
        # Conecta
        engine = create_engine(
            conn_str_limpa, 
            connect_args={'ssl': {'check_hostname': False}}
        )

        # Executa uma consulta minúscula
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            print("PING: O banco está acordado! (SELECT 1 realizado com sucesso)")

    except Exception as e:
        print(f"PING FALHOU: {e}")
        # Opcional: Levantar erro para o GitHub avisar por e-mail se o banco cair mesmo
        # raise e 

if __name__ == "__main__":
    ping_database()
