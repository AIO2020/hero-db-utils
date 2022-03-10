from hero_db_utils.clients import PostgresDatabaseClient
from hero_db_utils.utils.utils import get_env_params

def get_db_client(**kwargs):
    client_kwargs = get_env_params()
    client_kwargs.update(kwargs)
    engine = client_kwargs.pop('engine')
    if not client_kwargs:
        return None
    if engine == "postgres":
        return PostgresDatabaseClient(**client_kwargs)
    raise ValueError(
        f"Invalid engine '{engine}', client for this engine is not supported"
    )
