from mapeamentos import ZONA_MAPPING, TIPO_EVENTO_MAPPING
import pandas as pd
import re
import json
import requests
import argparse
import os
import pytz
from argparse import ArgumentParser
from datetime import datetime, timezone
from dateutil.parser import parse
from typing import Any, Dict, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from geopy.geocoders import Nominatim

# constantes e mapeamentos
FUSO = pytz.timezone('America/Sao_Paulo')

# diretorio das tabelas parquet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# definição de parâmetros obrigatórios (range de data a ser considerado pelo script)
parser = ArgumentParser()
parser.add_argument('--start-date', type=str, required=True)
parser.add_argument('--end-date', type=str, required=True)
args = parser.parse_args()

# consumo das APIs
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_waze():
    url = "https://www.waze.com/row-partnerhub-api/partners/14420996249/waze-feeds/c5c19146-e0f9-44a7-9815-3862c8a6ed67?format=json&types=alerts,traffic&fa=true"
    response = requests.get(url, timeout=10)
    alerts = response.json().get('alerts', [])
    return alerts


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_meteorologia():
    url = "https://websempre.rio.rj.gov.br/json/dados_meteorologicos"
    response = requests.get(url, timeout=10)
    data = response.json().get('features', [])
    return data

# carrega json de exemplo caso uma API esteja fora de alcance


def carrega_json_exemplo(arquivo):
    print(f"📁 Carregando JSON local ({arquivo})")
    with open(f'./jsons de exemplo/{arquivo}', "r", encoding="utf-8") as f:
        if arquivo == 'waze.json':
            dados_locais = json.load(f).get("alerts")
        elif arquivo == 'meteorologia.json':
            dados_locais = json.load(f).get("features")
        return dados_locais

# processamento dos dados


def processa_waze(alerts: List[Dict[str, Any]], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    processado = []
    # iterando pelos objetos 'alert' para obter os dados do waze
    for alert in alerts:
        # pubMillis (published milliseconds) é a chave para o valor de data no waze
        pub_millis = alert.get('pubMillis')

        # conversao ao padrao de data utc
        data_evento = datetime.fromtimestamp(
            pub_millis / 1000, tz=timezone.utc)

        # filtragem temporal
        if not (start_date <= data_evento <= end_date):
            continue

        # uuid seguindo o padrão pubMillis + street
        id = f"{pub_millis}_{alert.get('street', '')}"
        street = alert.get('street', '')
        cidade = alert.get('city', '')
        type = alert.get('type', '')
        subtype = alert.get('subtype', '')
        bairro, zona = '', ''
        confiabilidade = alert.get('reliability')

        # concatena tipo e subtipo e mapeia
        tipo_evento = TIPO_EVENTO_MAPPING.get(
            (type, subtype), f"{type}/{subtype}")

        coordenadas = alert.get('location', {})
        latitude, longitude = coordenadas.get('y'), coordenadas.get('x')

        # geocoding para definir bairro e zona
        try:
            geolocator = Nominatim(user_agent="my_pipeline")
            # traz apenas um resultado caso haja mais de um registro para uma latitude e longitude
            location = geolocator.reverse(
                (latitude, longitude), exactly_one=True)
            address = location.raw.get('address', {})
            bairro = address.get('suburb') or address.get(
                'neighbourhood') or address.get('quarter')
            if bairro:
                zona = ZONA_MAPPING.get(
                    bairro.split(',')[0].strip(), 'Outra zona')
        except Exception as e:
            print('falha no geocoding através do Nominatim')
            pass

        # montando os dicts para o dataframe
        alerta = {
            'id': id,
            'data_evento': data_evento,
            'tipo_evento': tipo_evento,
            'street': street,
            'cidade': cidade,
            'bairro': bairro,
            'zona': zona,
            'latitude': latitude,
            'longitude': longitude,
            'confiabilidade': confiabilidade,
        }
        # incrementa o dict à lista 'processado' criada no inicio da função
        processado.append(alerta)

        # identar para trás <<

    df = pd.DataFrame(processado)
    if not df.empty:
            # conversao de valores numéricos
        df['confiabilidade'] = pd.to_numeric(
        df['confiabilidade'], errors='coerce').astype('Int64')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # caso haja uma data faltante, terá sempre uma coluna 'event_date' como None
        if 'data_evento' in df.columns:
            df['data_evento'] = pd.to_datetime(
                df['data_evento'], errors='coerce')
            df['event_date'] = df['data_evento'].dt.date.astype(str)
        else:
            df['event_date'] = None

    return df


def processa_meteorologia(features: List[Dict[str, Any]], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    processado2 = []
    # iteração pelos objetos 'features' que contém os dados de meteorologia
    for feature in features:
        properties = feature.get("properties", {})
        read_at = properties.get("read_at")  # data (timestamp)
        if not read_at:
            continue

        # parsing da data
        try:
            data_evento = parse(read_at)
            if data_evento.tzinfo is None:
                data_evento = FUSO.localize(data_evento)
            data_evento = data_evento.astimezone(timezone.utc)
        except Exception as e:
            print("Erro de parsing de data", e)
            continue

        # filtrar para estar no range de data
        if not (start_date <= data_evento <= end_date):
            continue

        station = properties.get("station", {})
        data_info = properties.get("data", {})

        #  Substitui valores “N/D”, “-” ou strings vazias por NULL
        def conversor_dados(value: Any) -> Optional[float]:
            if not value or value in ("N/D", "-", "", None):
                return None

            # Converte numéricos com virgula em float e lida com problemas de tipagem
            try:
                match = re.search(r"[\d,\.]+", str(value))
                if match:
                    return float(match.group(0).replace(",", "."))
            except:
                pass
            return None

        # montando os dicts para o dataframe
        dados = {
            "id": str(station.get("id")),
            "data_evento": data_evento,
            "estacao": station.get("name"),
            "temperatura_atual": conversor_dados(data_info.get("temperature")),
            "temperatura_min": conversor_dados(data_info.get("min")),
            "temperatura_max": conversor_dados(data_info.get("max")),
            "umidade": conversor_dados(data_info.get("humidity")),
            "pressao": conversor_dados(data_info.get("pressure")),
            "vento": conversor_dados(data_info.get("wind")),
        }
        processado2.append(dados)

    df = pd.DataFrame(processado2)

    if not df.empty:
        # converter colunas numéricas
        df['temperatura_atual'] = pd.to_numeric(
            df['temperatura_atual'], errors='coerce')
        df['temperatura_min'] = pd.to_numeric(
            df['temperatura_min'], errors='coerce')
        df['temperatura_max'] = pd.to_numeric(
            df['temperatura_max'], errors='coerce')
        df['umidade'] = pd.to_numeric(df['umidade'], errors='coerce')
        df['pressao'] = pd.to_numeric(df['pressao'], errors='coerce')
        df['vento'] = pd.to_numeric(df['vento'], errors='coerce')
        df['data_evento'] = pd.to_datetime(df['data_evento'], errors='coerce')

    if 'data_evento' in df.columns:
        df['data_evento'] = pd.to_datetime(df['data_evento'], errors='coerce')
        df['event_date'] = df['data_evento'].dt.date.astype(str)
    else:
        df['event_date'] = None

    return df

# função que retorna a lista de datas (como strings) já existentes 
def get_datas(base_path: str) -> List[str]:
    if not os.path.exists(base_path):
        return []
    return [d.split('=')[1] for d in os.listdir(base_path) if d.startswith('event_date=')]

# função que filtra apenas os registros do DataFrame com datas que ainda não foram salvas no caminho base
def filter_new_data(df: pd.DataFrame, base_path: str) -> pd.DataFrame:
    existing_dates = get_datas(base_path)
    if not existing_dates or df.empty:
        return df
    return df[~df['event_date'].isin(existing_dates)]


def main():
    # inicializa o parser de argumentos para receber datas de início e fim via linha de comando
    parser = argparse.ArgumentParser(
        description='pipeline para dados de trafego e meteorologia.')
    parser.add_argument('--start-date', required=True,
                        help='start date em formato ISO 8601.')
    parser.add_argument('--end-date', required=True,
                        help='end date em formato ISO 8601 format.')
    args = parser.parse_args()

    # Converte as datas recebidas para objetos datetime com fuso horário UTC
    start_date = parse(args.start_date).astimezone(timezone.utc)
    end_date = parse(args.end_date).astimezone(timezone.utc)

    # Tenta obter dados do Waze via API, se falhar, usa arquivo local 
    try:
        dados_waze = get_waze()
    except:
        dados_waze = carrega_json_exemplo('waze.json')

    try:
        dados_meteorologia = get_meteorologia()
    except:
        dados_meteorologia = carrega_json_exemplo('meteorologia.json')

    print(f"Alertas de Waze: {len(dados_waze)}")
    print(f"Features de Meteorologia: {len(dados_meteorologia)}")

    df_trafego = processa_waze(dados_waze, start_date, end_date)
    df_meteo = processa_meteorologia(dados_meteorologia, start_date, end_date)

    print(f"Tamanho do dataframe Waze: {len(df_trafego)}")
    print(f"Tamanho do dataframe Meteorologia: {len(df_meteo)}")

    # caminhos para armazenamento dos arquivos Parquet
    trafego_path = os.path.join(BASE_DIR, 'trafego_alertas')
    meteo_path = os.path.join(BASE_DIR, 'meteorologia_estacoes')

    # remove registros duplicados com base nas datas
    df_meteo_filtrado = filter_new_data(df_meteo, meteo_path)
    df_trafego_filtrado = filter_new_data(df_trafego, trafego_path)

    # grava os dados no formato Parquet, particionando por data
    if not df_trafego_filtrado.empty:
        df_trafego_filtrado.to_parquet(
            trafego_path,
            partition_cols=['event_date'],
            engine='pyarrow',
            existing_data_behavior='overwrite_or_ignore'
        )

    if not df_meteo_filtrado.empty:
        df_meteo_filtrado.to_parquet(
            meteo_path,
            partition_cols=['event_date'],
            engine='pyarrow',
            existing_data_behavior='overwrite_or_ignore'
        )

    # criacao de diretorios
    os.makedirs(trafego_path, exist_ok=True)
    os.makedirs(meteo_path, exist_ok=True)
    print(
        f"Diretorios criados em: {os.path.abspath(trafego_path)} e {os.path.abspath(meteo_path)}")

    print("Pipeline completada.")

if __name__ == '__main__':
    main()
