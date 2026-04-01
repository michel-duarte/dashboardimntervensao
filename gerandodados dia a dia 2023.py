import pandas as pd
import h3
import json
import os
from datetime import timedelta
from tqdm import tqdm

# --- CONFIGURAÇÕES ---
PASTA_PROJETO = '/Users/michelvieiraduarte/Documents/Sinistros'
ARQUIVO_CSV = os.path.join(PASTA_PROJETO, 'sinistro_municipio_consolidado_2020_2023.csv')
PASTA_SAIDA = os.path.join(PASTA_PROJETO, 'dados_diarios')
ARQUIVO_ESTATISTICAS = os.path.join(PASTA_PROJETO, 'estatisticas_periodo.js')
RESOLUCAO_H3 = 9

if not os.path.exists(PASTA_SAIDA): 
    os.makedirs(PASTA_SAIDA)

def carregar_estatisticas_atuais():
    if os.path.exists(ARQUIVO_ESTATISTICAS):
        try:
            with open(ARQUIVO_ESTATISTICAS, 'r', encoding='utf-8') as f:
                conteudo = f.read().replace('var estatisticasPeriodo = ', '').rstrip(';')
                return json.loads(conteudo)
        except: pass
    return {"minVerdes": 9999, "maxVerdes": 0, "historico": {}, "dataMax": "", "dataMin": ""}

def calcular_custo(row):
    try:
        if str(row['flg_obito']).lower() in ['verdadeiro', 'true', '1']: return 592941.73
        if str(row['flg_ferimento_leve']).lower() in ['verdadeiro', 'true', '1'] or \
           str(row['flg_ferimento_grave']).lower() in ['verdadeiro', 'true', '1']: return 71655.30
        return 13590.86
    except: return 13590.86

# 1. Preparação dos Dados
print("1/4 - Carregando base de dados...")
df = pd.read_csv(ARQUIVO_CSV)
df['num_latitude'] = pd.to_numeric(df['num_latitude'], errors='coerce')
df['num_longitude'] = pd.to_numeric(df['num_longitude'], errors='coerce')
df = df.dropna(subset=['num_latitude', 'num_longitude'])
df['data_sinistro'] = pd.to_datetime(df['data_sinistro'])
df['custo'] = df.apply(calcular_custo, axis=1)
df['h3_index'] = df.apply(lambda x: h3.latlng_to_cell(x['num_latitude'], x['num_longitude'], RESOLUCAO_H3), axis=1)

# 2. Geometria H3 (Referência Estática)
print("2/4 - Atualizando geometria...")
geometria = {idx: h3.cell_to_boundary(idx) for idx in df['h3_index'].unique()}
with open(os.path.join(PASTA_PROJETO, 'geometria_h3.js'), 'w', encoding='utf-8') as f:
    f.write(f"var geometriaH3 = {json.dumps(geometria)};")

# 3. Processamento de Dias (Incremental)
estatisticas = carregar_estatisticas_atuais()
datas_alvo = pd.date_range(start='2020-06-01', end='2023-06-30') 
col_veiculo = next((c for c in df.columns if 'veiculo' in c.lower()), 'des_tipo_veiculo')

print("3/4 - Processando janelas temporais...")
for data_ref in tqdm(datas_alvo):
    dt_str = data_ref.strftime('%Y-%m-%d')
    caminho_arquivo = os.path.join(PASTA_SAIDA, f"{dt_str}.js")
    
    # Pular se já processado, mas garantir que está no histórico
    if os.path.exists(caminho_arquivo):
        if dt_str not in estatisticas["historico"]:
            with open(caminho_arquivo, 'r') as f:
                dia_data = json.loads(f.read().replace('var dadosDia = ', '').rstrip(';'))
                estatisticas["historico"][dt_str] = sum(1 for v in dia_data.values() if (v['m']['6d'] - v['m']['6a']) < -1000)
        continue

    m6a, m6d = data_ref - timedelta(days=180), data_ref + timedelta(days=180)
    m3a, m3d = data_ref - timedelta(days=90), data_ref + timedelta(days=90)
    
    mask = (df['data_sinistro'] >= m6a) & (df['data_sinistro'] <= m6d)
    df_p = df[mask]
    
    json_dia = {}
    verdes = 0
    for idx in df_p['h3_index'].unique():
        df_h3 = df_p[df_p['h3_index'] == idx]
        c6a = float(df_h3[(df_h3['data_sinistro'] >= m6a) & (df_h3['data_sinistro'] < data_ref)]['custo'].sum())
        c6d = float(df_h3[(df_h3['data_sinistro'] >= data_ref) & (df_h3['data_sinistro'] <= m6d)]['custo'].sum())
        
        if (c6d - c6a) < -1000: verdes += 1

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
    
    estatisticas["historico"][dt_str] = verdes
    with open(caminho_arquivo, 'w', encoding='utf-8') as f:
        f.write(f"var dadosDia = {json.dumps(json_dia)};")

# 4. Finalização dos Recordes
if estatisticas["historico"]:
    estatisticas["dataMax"] = max(estatisticas["historico"], key=estatisticas["historico"].get)
    estatisticas["dataMin"] = min(estatisticas["historico"], key=estatisticas["historico"].get)
    estatisticas["maxVerdes"] = estatisticas["historico"][estatisticas["dataMax"]]
    estatisticas["minVerdes"] = estatisticas["historico"][estatisticas["dataMin"]]
    
    with open(ARQUIVO_ESTATISTICAS, 'w', encoding='utf-8') as f:
        f.write(f"var estatisticasPeriodo = {json.dumps(estatisticas)};")

print(f"\nSucesso! Recorde: {estatisticas['maxVerdes']} em {estatisticas['dataMax']}")