"""APIs REST y GraphQL sobre el mismo repositorio de consultas."""

import asyncio
import sys

# psycopg asíncrono no funciona con el ProactorEventLoop por defecto de Windows.
if sys.platform == "win32":  # pragma: no cover
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
