import pandas as pd
import h3
import json
import os
import multiprocessing
from datetime import timedelta
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- CONFIGURAÇÕES ---
PASTA_PROJETO = 'D:/Sinistros'
ARQUIVO_CSV = os.path.join(PASTA_PROJETO, 'sinistro_municipio_consolidado_2020_2023.csv')
PASTA_SAIDA = os.path.join(PASTA_PROJETO, 'dados_diarios')
ARQUIVO_ESTATISTICAS = os.path.join(PASTA_PROJETO, 'estatisticas_periodo.js')
RESOLUCAO_H3 = 9

if not os.path.exists(PASTA_SAIDA): 
    os.makedirs(PASTA_SAIDA)

def calcular_custo(row):
    try:
        # Lógica baseada nos dados consolidados de 2020-2023
        if str(row['flg_obito']).lower() in ['verdadeiro', 'true', '1']: return 592941.73
        if str(row['flg_ferimento_leve']).lower() in ['verdadeiro', 'true', '1'] or \
           str(row['flg_ferimento_grave']).lower() in ['verdadeiro', 'true', '1']: return 71655.30
        return 13590.86
    except: return 13590.86

def processar_unico_dia(data_ref, df_full, col_veiculo, pasta_saida):
    """Função executada em paralelo para cada data"""
    dt_str = data_ref.strftime('%Y-%m-%d')
    caminho_arquivo = os.path.join(pasta_saida, f"{dt_str}.js")
    
    # Se o arquivo já existe, pulamos o processamento pesado
    if os.path.exists(caminho_arquivo):
        return dt_str, None, False 

    m6a, m6d = data_ref - timedelta(days=180), data_ref + timedelta(days=180)
    m3a, m3d = data_ref - timedelta(days=90), data_ref + timedelta(days=90)
    
    # Filtro de janela temporal
    mask = (df_full['data_sinistro'] >= m6a) & (df_full['data_sinistro'] <= m6d)
    df_p = df_full[mask].copy()
    
    if df_p.empty:
        return dt_str, 0, True

    json_dia = {}
    verdes = 0
    
    # OTIMIZAÇÃO: Agrupar por H3 uma única vez em vez de filtrar no loop
    for idx, df_h3 in df_p.groupby('h3_index'):
        c6a = float(df_h3[(df_h3['data_sinistro'] >= m6a) & (df_h3['data_sinistro'] < data_ref)]['custo'].sum())
        c6d = float(df_h3[(df_h3['data_sinistro'] >= data_ref) & (df_h3['data_sinistro'] <= m6d)]['custo'].sum())
        
        if (c6d - c6a) < -1000: 
            verdes += 1

        json_dia[idx] = {
            "m": {
                "6a": c6a, 
                "3a": float(df_h3[(df_h3['data_sinistro'] >= m3a) & (df_h3['data_sinistro'] < data_ref)]['custo'].sum()),
                "3d": float(df_h3[(df_h3['data_sinistro'] >= data_ref) & (df_h3['data_sinistro'] < m3d)]['custo'].sum()), 
                "6d": c6d
            },
            "va": df_h3[(df_h3['data_sinistro'] >= m3a) & (df_h3['data_sinistro'] < data_ref)][col_veiculo].value_counts().to_dict(),
            "vd": df_h3[(df_h3['data_sinistro'] >= data_ref) & (df_h3['data_sinistro'] < m3d)][col_veiculo].value_counts().to_dict()
        }
    
    # Salva o arquivo individual do dia
    with open(caminho_arquivo, 'w', encoding='utf-8') as f:
        f.write(f"var dadosDia = {json.dumps(json_dia)};")
        
    return dt_str, verdes, True

if __name__ == '__main__':
    # 1. Preparação dos Dados
    print("1/4 - Carregando base de dados de Itajaí...")
    df = pd.read_csv(ARQUIVO_CSV)
    df['num_latitude'] = pd.to_numeric(df['num_latitude'], errors='coerce')
    df['num_longitude'] = pd.to_numeric(df['num_longitude'], errors='coerce')
    df = df.dropna(subset=['num_latitude', 'num_longitude'])
    df['data_sinistro'] = pd.to_datetime(df['data_sinistro'])
    df['custo'] = df.apply(calcular_custo, axis=1)
    df['h3_index'] = df.apply(lambda x: h3.latlng_to_cell(x['num_latitude'], x['num_longitude'], RESOLUCAO_H3), axis=1)

    # 2. Geometria H3
    print("2/4 - Gerando geometria de referência...")
    geometria = {idx: h3.cell_to_boundary(idx) for idx in df['h3_index'].unique()}
    with open(os.path.join(PASTA_PROJETO, 'geometria_h3.js'), 'w', encoding='utf-8') as f:
        f.write(f"var geometriaH3 = {json.dumps(geometria)};")

    # 3. Processamento Paralelo
    datas_alvo = pd.date_range(start='2020-06-01', end='2023-06-30')
    col_veiculo = next((c for c in df.columns if 'veiculo' in c.lower()), 'des_tipo_veiculo')
    
    estatisticas = {"minVerdes": 9999, "maxVerdes": 0, "historico": {}, "dataMax": "", "dataMin": ""}
    
    print(f"3/4 - Processando {len(datas_alvo)} dias em paralelo...")
    
    # Usamos o número de cores lógicos disponíveis
    num_trabalhadores = multiprocessing.cpu_count()
    
    with ProcessPoolExecutor(max_workers=num_trabalhadores) as executor:
        # Mapeia a função para as datas
        futures = {executor.submit(processar_unico_dia, d, df, col_veiculo, PASTA_SAIDA): d for d in datas_alvo}
        
        for future in tqdm(as_completed(futures), total=len(futures)):
            dt_str, verdes, processado = future.result()
            if verdes is not None:
                estatisticas["historico"][dt_str] = verdes

    # 4. Finalização
    if estatisticas["historico"]:
        estatisticas["dataMax"] = max(estatisticas["historico"], key=estatisticas["historico"].get)
        estatisticas["dataMin"] = min(estatisticas["historico"], key=estatisticas["historico"].get)
        estatisticas["maxVerdes"] = estatisticas["historico"][estatisticas["dataMax"]]
        estatisticas["minVerdes"] = estatisticas["historico"][estatisticas["dataMin"]]
        
        with open(ARQUIVO_ESTATISTICAS, 'w', encoding='utf-8') as f:
            f.write(f"var estatisticasPeriodo = {json.dumps(estatisticas)};")

    print(f"\nConcluído! Pico de redução: {estatisticas.get('maxVerdes')} células em {estatisticas.get('dataMax')}")