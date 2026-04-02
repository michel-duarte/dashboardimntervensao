import os
import json
import re

# Configurações
pasta_dados = 'dados_diarios'
arquivo_saida = 'stats.js'

def extrair_verdes_do_arquivo(caminho_arquivo):
    with open(caminho_arquivo, 'r', encoding='utf-8') as f:
        conteúdo = f.read()
        
        # Remove a atribuição da variável JS para isolar o JSON
        # Procura o que está entre o primeiro '{' e o último '}'
        match = re.search(r'\{.*\}', conteúdo, re.DOTALL)
        if not match:
            return 0
            
        dados = json.loads(match.group(0))
        
        verdes = 0
        for id_hex, info in dados.items():
            antes = info['m'].get('6a', 0)
            depois = info['m'].get('6d', 0)
            diff = depois - antes
            
            # Mesma lógica do seu JavaScript: Redução > 5% é verde
            perc = diff / antes if antes > 0 else (1 if depois > 0 else 0)
            if diff < 0 and abs(perc) > 0.05:
                verdes += 1
        return verdes

def gerar_stats():
    lista_verdes = []
    
    # Varre todos os arquivos .js na pasta
    for nome_arquivo in os.listdir(pasta_dados):
        if nome_arquivo.endswith('.js'):
            caminho = os.path.join(pasta_dados, nome_arquivo)
            try:
                qtd_verdes = extrair_verdes_do_arquivo(caminho)
                lista_verdes.append(qtd_verdes)
                print(f"Processado {nome_arquivo}: {qtd_verdes} verdes")
            except Exception as e:
                print(f"Erro ao processar {nome_arquivo}: {e}")

    if not lista_verdes:
        print("Nenhum dado encontrado.")
        return

    # Cálculos finais
    stats = {
        "minVerdes": min(lista_verdes),
        "maxVerdes": max(lista_verdes),
        "totalArquivos": len(lista_verdes),
        "ultimaAtualizacao": "2023-07-01" # Pode usar datetime.now() aqui
    }

    # Grava o arquivo stats.js
    with open(arquivo_saida, 'w', encoding='utf-8') as f:
        f.write(f"const estatisticasGlobais = {json.dumps(stats, indent=4)};")
    
    print(f"\nSucesso! Arquivo {arquivo_saida} gerado com limites: {stats['minVerdes']} - {stats['maxVerdes']}")

if __name__ == "__main__":
    gerar_stats()