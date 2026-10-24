"""Configuracao global dos testes: garante que a suite rode sem servicos externos.

O `pydantic-settings` carrega o `.env` do diretorio corrente; um `.env` local com
BUS_BACKEND=redis/rabbitmq (ou INSTANCE_ID vazio) quebraria os testes porque o
lifespan tentaria falar com um broker real. Por isso o modo local e um instance_id
fixo sao forcados AQUI, antes de qualquer import de `app` (o `bus` e criado no
import de `app.bus`).
"""

import os

os.environ["BUS_BACKEND"] = "local"
os.environ["INSTANCE_ID"] = "instance-teste"
