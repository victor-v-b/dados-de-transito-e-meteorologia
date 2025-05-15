Este projeto realiza a coleta, processamento e armazenamento de dados de alertas de tráfego (via Waze) e condições meteorológicas, com o objetivo de consolidar essas informações em arquivos Parquet organizados por data.

✅ Pré-requisitos
Python 3.8+

Instale as dependências via terminal:

pip install -r requirements.txt

Para executar em terminal (parâmetros de data são obrigatórios):
python pipeline.py --start-date (YYYY-MM-DD) --end-date (YYYY-MM-DD) 

                    Exemplo:
                    python pipeline.py  --start-date 2025-05-01 --end-date 2025-05-16

**Features**:

-Consome APIs em tempo real, no recorte de tempo estabelecido nos parâmetros ao rodar o script.

-Jsons de backup foram incluidos caso alguma das APIs fique fora do ar, para garantir a integridade do teste

-Mapeamento de bairros e zonas do RJ (geocoding) e de eventos de trânsito em PT-BR

-Armazenamento de dados com transformação de datas para o padrão UTC, valores numéricos para float e utiliza de regex para outros tipos de dados, como por exemplo os dados meteorológicos de vento "17,28 (W)" -> 17.28

-Inclui valor null como padrão para garantir que o schema mantenha a estrutura caso hajam campos ausentes.

-Armazenamento em formato Parquet, particionado por event_date

-Lógica de filtro, para evitar sobrescrição dos dados.
