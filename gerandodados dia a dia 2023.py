import pandas as pd
import h3
import json
import os
from datetime import timedelta
from tqdm import tqdm

# --- CONFIGURAÇÕES ---
CAMINHO_BASE = r'C:\Users\03149433908\Downloads\sumo-win64extra-1.24.0\sumo-1.24.0\Malha Komprão 2\Sinistros'
ARQUIVO_CSV = os.path.join(CAMINHO_BASE, 'sinistro_municipio_consolidado_2020_2023.csv')
PASTA_SAIDA = os.path.join(CAMINHO_BASE, 'dados_diarios')
RESOLUCAO_H3 = 9

if not os.path.exists(PASTA_SAIDA): os.makedirs(PASTA_SAIDA)

def calcular_custo(row):
    try:
        if str(row['flg_obito']).lower() == 'verdadeiro': return 592941.73
        if str(row['flg_ferimento_leve']).lower() == 'verdadeiro' or \
           str(row['flg_ferimento_grave']).lower() == 'verdadeiro': return 71655.30
        return 13590.86
    except: return 13590.86

print("1/3 - Preparando dados...")
df = pd.read_csv(ARQUIVO_CSV).dropna(subset=['num_latitude', 'num_longitude'])
df.columns = df.columns.str.strip()
col_veiculo = next((c for c in df.columns if 'veiculo' in c.lower()), 'des_tipo_veiculo')

df['data_sinistro'] = pd.to_datetime(df['data_sinistro'])
df['custo'] = df.apply(calcular_custo, axis=1)
df['h3_index'] = df.apply(lambda x: h3.latlng_to_cell(x['num_latitude'], x['num_longitude'], RESOLUCAO_H3), axis=1)

print("2/3 - Salvando geometria...")
geometria = {idx: h3.cell_to_boundary(idx) for idx in df['h3_index'].unique()}
with open(os.path.join(CAMINHO_BASE, 'geometria_h3.js'), 'w', encoding='utf-8') as f:
    f.write(f"var geometriaH3 = {json.dumps(geometria)};")

print("3/3 - Gerando arquivos diários (Janelas de 3 e 6 meses)...")
datas_alvo = pd.date_range(start='2022-07-01', end='2022-12-31')

for data_ref in tqdm(datas_alvo):
    dt_str = data_ref.strftime('%Y-%m-%d')
    m3a = (data_ref - timedelta(days=90), data_ref)
    m3d = (data_ref, data_ref + timedelta(days=90))
    m6a = (data_ref - timedelta(days=180), data_ref)
    m6d = (data_ref, data_ref + timedelta(days=180))
    
    mask = (df['data_sinistro'] >= m6a[0]) & (df['data_sinistro'] <= m6d[1])
    df_p = df[mask]
    
    json_dia = {}
    for idx in df_p['h3_index'].unique():
        df_h3 = df_p[df_p['h3_index'] == idx]
        
        # Filtros para contagem de veículos no Histograma (agora 6 meses)
        v_antes = df_h3[(df_h3['data_sinistro'] >= m6a[0]) & (df_h3['data_sinistro'] < m6a[1])][col_veiculo].value_counts().to_dict()
        v_depois = df_h3[(df_h3['data_sinistro'] >= m6d[0]) & (df_h3['data_sinistro'] < m6d[1])][col_veiculo].value_counts().to_dict()
        
        json_dia[idx] = {
            "m": {
                "6a": float(df_h3[(df_h3['data_sinistro'] >= m6a[0]) & (df_h3['data_sinistro'] < m6a[1])]['custo'].sum()),
                "3a": float(df_h3[(df_h3['data_sinistro'] >= m3a[0]) & (df_h3['data_sinistro'] < m3a[1])]['custo'].sum()),
                "3d": float(df_h3[(df_h3['data_sinistro'] >= m3d[0]) & (df_h3['data_sinistro'] < m3d[1])]['custo'].sum()),
                "6d": float(df_h3[(df_h3['data_sinistro'] >= m6d[0]) & (df_h3['data_sinistro'] < m6d[1])]['custo'].sum())
            },
            "va": v_antes,  # Veículos Antes (Contagem)
            "vd": v_depois  # Veículos Depois (Contagem)
        }
    
    with open(os.path.join(PASTA_SAIDA, f"{dt_str}.js"), 'w', encoding='utf-8') as f:
        f.write(f"var dadosDia = {json.dumps(json_dia)};") 