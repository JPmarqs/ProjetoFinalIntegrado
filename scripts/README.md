# Tratamento e Extração de Atributos de Imagens — `tratamento_img.py`

## 1. Conceito

Este script implementa a etapa de **processamento da fonte de dados não
estruturada** do pipeline (imagens de mapa do Mapbox). 
A ideia central é: uma imagem de mapa, sozinha, não é utilizável
por um modelo de Machine Learning — é preciso transformá-la em **atributos
tabulares** (números e categorias) que descrevam visualmente o local do
sinistro (cruzamentos, curvas, densidade de construções, vegetação, textura
da via, iluminação etc.), e que depois alimentam a camada analítica no
Snowflake e o modelo preditivo de risco.

O script cobre, portanto, duas etapas do pipeline ELT:

1. **Extract/Load da camada raw**: busca as imagens estáticas do Mapbox e as
   salva em disco (`image_lab/raw/`), simulando o land dos dados brutos em
   nuvem (S3, no desenho final da arquitetura).
2. **Transform local**: processa cada imagem com visão computacional e gera
   um dataset tabular de atributos (`image_lab/features/`), que depois é
   carregado no Snowflake via `COPY INTO`.

## 2. Visão geral do fluxo

```
CSV de locais (id, lat, lon)
        │
        ▼
[Etapa 1] Download da imagem estática via Mapbox Static Images API
        │  → image_lab/raw/{image_id}.png
        ▼
[Etapa 2] Extração de metadados do arquivo (dimensões, formato, tamanho)
        │
        ▼
[Etapa 3] Extração de atributos visuais (4 grupos, detalhados abaixo)
        │
        ▼
[Etapa 4] Derivação de atributos de risco (regras de negócio sobre os atributos brutos)
        │
        ▼
image_lab/features/atributos_locais.csv  e  .parquet
```

## 3. Etapas detalhadas

### Etapa 1 — Busca da imagem (`buscar_imagem`)

Consulta a **Mapbox Static Images API** (estilo `streets-v12`, mapa
vetorial de ruas — não é imagem de satélite) para cada par `(lat, lon)` do
CSV de entrada, com zoom fixo (`ZOOM = 17`) e resolução `400x300`. Salva o
PNG em `image_lab/raw/{id}.png`.

*Por que mapa vetorial e não satélite?* Porque o objetivo é capturar o
**traçado da via** (retas, curvas, cruzamentos) e a **ocupação do entorno**
(quarteirões construídos vs. vegetação), e o estilo `streets` já renderiza
essas informações de forma limpa e padronizada, facilitando a segmentação
por cor.

### Etapa 2 — Metadados (`extract_image_metadata`)

Informações estruturais do arquivo: formato, largura/altura, modo de cor e
tamanho em bytes. Serve como trilha de auditoria/rastreabilidade
(dado bruto → dado tratado, exigida na seção 5.3 do escopo).

### Etapa 3 — Extração de atributos visuais

Dividida em quatro blocos, cada um resolvendo um tipo de pergunta sobre a
imagem:

**3A. Atributos básicos** (`extract_basic_features`)
Estatísticas gerais de brilho, contraste e cor (médias RGB), mais
**densidade de bordas** via detector de Canny (conta quantos pixels são
"borda" e divide pelo total de pixels). Serve como indicador geral de
complexidade visual da cena.

**3B. Retas e interseções — Transformada de Hough** (`extract_line_features`)
*Conceito:* a Transformada de Hough Probabilística (`HoughLinesP`) detecta
segmentos de reta em uma imagem de bordas. Aqui ela é usada para identificar
o traçado das vias:
- As retas brutas passam por um **agrupamento (clustering)** por ângulo e
  posição (`_cluster_lines`), para evitar contar múltiplas detecções da
  mesma via como retas distintas.
- Pares de retas com ângulos suficientemente diferentes (`ANGLE_DIFF_MIN_DEG`)
  que se cruzam dentro da imagem são contados como **interseções**
  (proxy de cruzamento viário).
- O desvio-padrão dos ângulos (`line_angle_std`) funciona como proxy de
  **curva na via**: vias retas têm ângulos homogêneos; vias curvas ou com
  cruzamentos múltiplos, ângulos dispersos.

**3C. Segmentação de cor em HSV** (`extract_color_segmentation_features`)
*Conceito:* converte a imagem para o espaço de cor HSV (matiz, saturação,
valor), que separa melhor "cor" de "iluminação" do que o RGB. Duas máscaras
são aplicadas:
- **Vegetação**: faixa de matiz verde (`VEGETACAO_HUE_RANGE`) com saturação
  e valor mínimos.
- **Área construída**: como o Mapbox renderiza quarteirões em tons de cinza
  claro sobre um fundo específico, a máscara busca pixels de baixa saturação
  cujo valor (brilho) esteja a uma certa distância da cor de fundo dominante
  da imagem (`_cor_dominante_fundo_hsv`, calculada por quantização e
  contagem de moda).

O resultado é o **percentual da imagem** ocupado por cada classe
(`pct_area_construida`, `pct_vegetacao`).

**3D. Textura — GLCM** (`extract_texture_features`)
*Conceito:* a Matriz de Coocorrência de Níveis de Cinza (Gray-Level
Co-occurrence Matrix) descreve como pares de intensidades de pixel se
repetem espacialmente, capturando padrões de textura que estatísticas
simples (média/desvio) não capturam. A imagem é quantizada para 8 níveis de
cinza e são extraídas quatro propriedades clássicas de textura:
`contrast`, `homogeneity`, `energy` e `correlation`. Servem como proxy da
"granularidade visual" da cena (ex.: área urbana densa tende a ter textura
mais irregular que uma rodovia aberta).

### Etapa 4 — Derivação de atributos de risco (`derivar_atributos_risco`)

Aplica regras de negócio (limiares definidos em "LIMIARES DE NEGÓCIO", no
topo do script) sobre os atributos brutos para gerar variáveis
interpretáveis, prontas para uso analítico e como candidatas a atributos do
modelo de ML:

| Atributo derivado | Regra |
|---|---|
| `possui_cruzamento` | `line_intersection_count >= CRUZAMENTO_MIN_INTERSECOES` |
| `curva_via` | `line_angle_std >= CURVA_STD_ANGLE_THRESHOLD_DEG` |
| `tipo_via` | classificação categórica: `urbana_com_edificacoes` / `urbana_com_cruzamento` / `aberta_rural_arborizada` / `via_simples`, por prioridade de regras |
| `low_light_flag` | `brightness_mean < LOW_LIGHT_BRIGHTNESS_THRESHOLD` |

Esses limiares foram calibrados empiricamente e podem (devem) ser ajustados
e justificados no relatório como parte da avaliação qualitativa (seção 5.4
do escopo).

## 4. Leitura e amostragem do CSV de entrada

- `_ler_csv_com_fallback_encoding`: tenta `utf-8` e `latin-1`, com separador
  `,` ou `;`, e valida a estrutura mínima antes de aceitar o arquivo.
  ⚠️ **Atenção**: para o `datatran2026.csv` (dados brutos da PRF) o
  encoding correto é `utf-8-sig` (UTF-8 com BOM). Este script espera como
  entrada o **CSV de locais já preparado** (`id, lat, lon`), tipicamente
  gerado por um script auxiliar de amostragem (`gerar_amostra_locais.py`),
  não o CSV bruto da PRF diretamente.
- `_padronizar_colunas_local`: normaliza nomes de colunas (`latitude/longitude`
  → `lat/lon`) e corrige vírgula decimal, se presente.
- Amostragem reprodutível: os locais únicos são embaralhados uma única vez
  com semente fixa (`RANDOM_SEED = 42`) e processados em lotes
  (`LOTE_INICIO`, `LIMITE_LOCAIS`), permitindo rodar a extração em partes
  sem sobrepor locais entre execuções — útil para não estourar o limite de
  requisições da API do Mapbox em uma única execução.

## 5. Saídas geradas

| Arquivo | Conteúdo |
|---|---|
| `image_lab/raw/{image_id}.png` | Imagem estática bruta baixada do Mapbox |
| `image_lab/features/atributos_locais.csv` | Dataset tabular final (metadados + atributos + atributos de risco) |
| `image_lab/features/atributos_locais.parquet` | Mesmo dataset em Parquet, formato usado na carga para o S3 processado / Snowflake |

O join final é feito por `image_id`, unindo metadados do arquivo, atributos
extraídos e as coordenadas originais (`lat`, `lon`) do CSV de entrada.

## 6. Como executar

```bash
pip install requests pandas pillow opencv-python-headless scikit-image pyarrow --break-system-packages
```

```bash
python tratamento_img.py caminho/para/locais_teste.csv
```

Se nenhum argumento for informado, o script usa `datatran2026.csv` como
padrão (ver observação sobre encoding acima).

## 7. Parâmetros configuráveis (topo do script)

| Parâmetro | Função |
|---|---|
| `IMG_WIDTH`, `IMG_HEIGHT`, `ZOOM` | Resolução e nível de zoom da imagem buscada |
| `LIMITE_LOCAIS`, `LOTE_INICIO`, `RANDOM_SEED` | Controle de amostragem/lote reprodutível |
| `CANNY_LOW/HIGH`, `HOUGH_*` | Sensibilidade da detecção de bordas e retas |
| `VEGETACAO_*`, `BUILDING_*` | Faixas de cor HSV usadas na segmentação |
| `PCT_AREA_CONSTRUIDA_ALTO`, `PCT_VEGETACAO_ALTO` | Limiares para classificação de `tipo_via` |
| `LOW_LIGHT_BRIGHTNESS_THRESHOLD` | Limiar de brilho para `low_light_flag` |