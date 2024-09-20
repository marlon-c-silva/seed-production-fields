# SEED PRODUCTION FIELDS OU CAMPOS DE PRODUÇÃO DE SEMENTE

Esse projeto busca dados do portal de dados abertos do Ministério da Agricultura e Pecuario do Brasil, trata os dados baseados nos hibridos de determinadas companias e junta os dados de geolocalização dos campos onde foram plantados.


https://dados.agricultura.gov.br/dataset/c7784a6e-f0ec-4196-a1ce-1d2d4784a58e/resource/6ab20c11-73a0-4ab0-8e13-2420d48dd6f5/download/sigefcamposproducaodesementes.csv

---

## main.py

Código responsável pela execução do relatório.

 - Realiza o download do arquivo do site do Ministério da Agricultura e Pecuaria do Brasil, filtra apenas as Safras necessárias para a analise, apenas a espécie "Zea mays L." (Milho) e apenas as marcas "LPHT", "Corteva", "Bayer", "KWS", "LG Sementes", "Syngenta".
 -  Corrige a data de plantio, adicionado uma nova coluna ao dataframe chamada "correct_planting_date".
 - Realiza um Join com a base HYBRIDS_BY_BRAND.
 - Realiza um Join com a base GEO_LAT_LONG.
 - Dropa a coluna "datacolheita"
 - Renomeia as colunas do dataframe baseado no esquema abaixo:

    {"municipio": "city", "uf": "state", "uf": "state", "marca": "brand", "Area": "area", "dataplantio": "harvest_date", "producaobruta": "gross_production", "producaoestimada": "estimated_production"}

 - Realiza um truncate na tabela 'SEED_PRODUCTION_FIELDS' e realiza o upload dos novos dados.
 - Salva uma cópia do dataframe a cada execução afim de conservar logs para auditoria.


## upload_geo_lat_long_to_sql.py

Código responsável por buscar os dados Geolocalização como "city", "state", "city_state", "country", "latitude", "longitude", truncar a tabela atual do banco e subir os dados atualizados.

## upload_hybrids_by_brand_to_sql.py

Código responsável por buscar os dados dos hibridos como "cia", "marca", "hybrid", truncar a tabela atual do banco e subir os dados atualizados.
