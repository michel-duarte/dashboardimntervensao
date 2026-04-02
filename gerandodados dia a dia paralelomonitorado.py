import pandas as pd
import h3
import json
import os
import multiprocessing
import time
from datetime import timedelta
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

# --- CONFIGURAÇÕES DE CAMINHO (Ajustado para seu Windows) ---
PASTA_PROJETO = 'D:/Sinistros'
ARQUIVO_CSV = os.path.join(PASTA_PROJETO, 'sinistro_municipio_consolidado_2020_2023.csv')
PASTA_SAIDA = os.path.join(PASTA_PROJETO, 'dados_diarios')
ARQUIVO_ESTATISTICAS = os.path.join(PASTA_PROJETO, 'estatisticas_periodo.js')
RESOLUCAO_H3 = 9

if not os.path.exists(PASTA_SAIDA): 
    os.makedirs(PASTA_SAIDA)

def calcular_custo(row):
    """Cálculo de custo médio baseado na gravidade do sinistro"""
    try:
        if str(row['flg_obito']).lower() in ['verdadeiro', 'true', '1']: return 592941.73
        if str(row['flg_ferimento_leve']).lower() in ['verdadeiro', 'true', '1'] or \
           str(row['flg_ferimento_grave']).lower() in ['verdadeiro', 'true', '1']: return 71655.30
        return 13590.86
    except: return 13590.86

def processar_unico_dia(data_ref, df_full, col_veiculo, pasta_saida):
    """Função de processamento atômico para execução paralela"""
    dt_str = data_ref.strftime('%Y-%m-%d')
    caminho_arquivo = os.path.join(pasta_saida, f"{dt_str}.js")
    
    # Pula se o arquivo já existir
    if os.path.exists(caminho_arquivo):
        return dt_str, None

    m6a, m6d = data_ref - timedelta(days=180), data_ref + timedelta(days=180)
    m3a, m3d = data_ref - timedelta(days=90), data_ref + timedelta(days=90)
    
    # Filtro temporal otimizado
    mask = (df_full['data_sinistro'] >= m6a) & (df_full['data_sinistro'] <= m6d)
    df_p = df_full[mask].copy()
    
    if df_p.empty:
        return dt_str, 0

    json_dia = {}
    verdes = 0
    
    # Agrupamento por H3 (muito mais rápido que múltiplos filtros)
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
    
    with open(caminho_arquivo, 'w', encoding='utf-8') as f:
        f.write(f"var dadosDia = {json.dumps(json_dia)};")
        
    return dt_str, verdes

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

    # 3. Processamento Paralelo com Monitoramento de Status
    datas_alvo = pd.date_range(start='2020-06-01', end='2023-06-30')
    col_veiculo = next((c for c in df.columns if 'veiculo' in c.lower()), 'des_tipo_veiculo')
    num_cores = multiprocessing.cpu_count()
    estatisticas = {"minVerdes": 9999, "maxVerdes": 0, "historico": {}, "dataMax": "", "dataMin": ""}

    print(f"3/4 - Iniciando processamento com {num_cores} núcleos...")
    
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        # Lançar tarefas
        futures = {executor.submit(processar_unico_dia, d, df, col_veiculo, PASTA_SAIDA): d for d in datas_alvo}
        
        # Configurar Barra de Progresso
        pbar = tqdm(total=len(futures), unit="dia")
        concluidos = 0
        
        while concluidos < len(futures):
            # Monitoramento de núcleos ativos
            ativos = sum(1 for f in futures if not f.done())
            executando = min(ativos, num_cores)
            
            pbar.set_description(f"Status: {executando}/{num_cores} núcleos ativos")
            
            # Atualiza progresso
            atual_concluidos = sum(1 for f in futures if f.done())
            if atual_concluidos > concluidos:
                pbar.update(atual_concluidos - concluidos)
                concluidos = atual_concluidos
            
            time.sleep(0.5) # Evita flickering na barra de status
            
        pbar.close()

        # Coletar resultados para estatísticas finais
        for future in futures:
            dt_str, verdes = future.result()
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

    print(f"\nSucesso! Pico de redução: {estatisticas.get('maxVerdes')} em {estatisticas.get('dataMax')}")