import xml.etree.cElementTree as ET
import pandas as pd
import numpy as np
from lxml import etree
from datetime import datetime, timedelta
import cx_Oracle
import re
import win32com.client as win32

#Define o formato que a coluna deve ser carregada
dtype_mapping = {'Nº da linha': str}
dtype_linha = {'Linha': str}

#Carrega as planilhas com fragmentos de dados que devem ser acrescentados
linhas = pd.read_excel('caminho/Index de linhas.xlsx',sheet_name='INDEX', header=7, dtype=dtype_mapping)
verifica_dem = pd.read_excel("caminho/Demanda Outubro.xlsx", sheet_name='RESUMO')
tecologia = pd.read_excel('caminho/Base Veiculos 1.xlsx', sheet_name='Frota')
giroteste = pd.read_excel('caminho/Giros de teste.xlsx')
arquivo = 'caminho/Resultados Comparação.xml'
encerrante = pd.read_excel('caminho/Extras que deveriam estar junto_identificados.xlsx', header=7, dtype=dtype_linha)

# Configurar a conexão com o banco de dados Oracle
dsn = cx_Oracle.makedsn(host='172.xx.x.xxx', port=xxxx, service_name='linux.subnetdb.hpvcn.oraclevcn.com')
conexao = cx_Oracle.connect(user='xxx', password='xxxxxxxxxxxxxxx', dsn = dsn)

# Criar um cursor para executar a consulta
cursor = conexao.cursor()

# Consultas
query_demanda_dia = """
SELECT TRUNC(tur.DATAINI), COUNT(ac.DATA)
FROM passagem.ACESSOS ac
INNER JOIN passagem.TURNOS tur ON ac.TURNOID = tur.TURNOID
WHERE         
    TRUNC(tur.DATAINI) >= TRUNC(SYSDATE, 'MM') 
    AND TRUNC(tur.DATAINI) < TRUNC(SYSDATE)
    AND ac.EVENTOID <> 20
GROUP BY TRUNC(tur.DATAINI)
"""
    
query_demanda_ecx = """
    SELECT
        vi.DATA_OPERACAO,
        COUNT(ac.ID_VIAGEM_VIAGEM)
    FROM
        passagem.ACESSOS ac
    INNER JOIN
        viagem.VIAGEM vi ON vi.ID = ac.ID_VIAGEM_VIAGEM
    WHERE
    vi.DATA_OPERACAO >= TRUNC(SYSDATE, 'MM') AND vi.DATA_OPERACAO < TRUNC(SYSDATE) AND ac.EVENTOID <> 20 
    AND ac.EVENTOID <> 20
    GROUP BY
        vi.DATA_OPERACAO
"""

params = {}  
bd_demanda_dia = pd.read_sql(query_demanda_dia, conexao)
bd_demanda_enx = pd.read_sql(query_demanda_ecx, conexao)

# Fechar o cursor e a conexão com o banco de dados
cursor.close()
conexao.close()

#Trabalhando o XML que contém os resultados totais para comparação
root = ET.parse(arquivo).getroot()
# Defina o namespace (namespace_uri) usando o atributo xmlns da tag raiz
namespace_uri = "http://tempuri.org/DSExpTurnosViagens.xsd"
# Encontre todas as subtags relevantes em uma única consulta XPath
subtags1 = root.findall(".//{http://tempuri.org/DSExpTurnosViagens.xsd}Viagens")
subtags2 = root.findall(".//{http://tempuri.org/DSExpTurnosViagens.xsd}Turnos")

demanda_list = []
for subtag in subtags1:
    data_dem = {
        "Demanda": subtag.find("{http://tempuri.org/DSExpTurnosViagens.xsd}PassageirosQtd").text,
        "DataIni": subtag.find("{http://tempuri.org/DSExpTurnosViagens.xsd}DataInicio").text,
        "TurnoId": subtag.find("{http://tempuri.org/DSExpTurnosViagens.xsd}TurnoID").text,
    }
    demanda_list.append(data_dem)

# Criar DataFrame a partir da lista de dicionários
demd_xml = pd.DataFrame(demanda_list)

data_list = []
for subtagdt in subtags2:
    data_data = {
        "Data_base": subtagdt.find("{http://tempuri.org/DSExpTurnosViagens.xsd}DataIni").text,
        "Data_baseF": subtagdt.find("{http://tempuri.org/DSExpTurnosViagens.xsd}DataFim").text,
        "Linha": subtagdt.find("{http://tempuri.org/DSExpTurnosViagens.xsd}Linha").text,
        "TurnoId": subtagdt.find("{http://tempuri.org/DSExpTurnosViagens.xsd}TurnoID").text,
    }
    data_list.append(data_data)

# Criar DataFrame a partir da lista de dicionários
data_xml = pd.DataFrame(data_list)

#Tratando os dados do xml
dem_data = pd.merge(data_xml, demd_xml, on='TurnoId', how='outer')
dem_data['Demanda'] = dem_data['Demanda'].astype(int)
dem_data['Data_bas'] = dem_data['Data_base'].str.split('T').str[0]
dem_data['HoraIni'] = dem_data['Data_base'].str.split('T').str[1]
dem_data['HoraIni'] = dem_data['HoraIni'].str.split('-').str[0]
dem_data['HoraFim'] = dem_data['Data_baseF'].str.split('T').str[1]
dem_data['HoraFim'] = dem_data['HoraFim'].str.split('-').str[0]
demanda_dia_xml = dem_data.groupby('Data_bas')['Demanda'].sum()
dem_data['HoraIni'] = dem_data['HoraIni'].apply(lambda x: x.split('-')[0] if '-' in x and len(x.split('-')) > 1 else x)
dem_data['HoraIni'] = dem_data['HoraIni'].apply(lambda x: x.split(':')[0] if ':' in x and len(x.split(':')) > 1 else x)
dem_data['HoraIni'] = dem_data['HoraIni'].astype(int)
dem_data['HoraFim'] = dem_data['HoraFim'].apply(lambda x: x.split('-')[0] if '-' in x and len(x.split('-')) > 1 else x)
dem_data['HoraFim'] = dem_data['HoraFim'].apply(lambda x: x.split(':')[0] if ':' in x and len(x.split(':')) > 1 else x)
dem_data['HoraFim'] = dem_data['HoraFim'].astype(int)

#Modifica a data onde há exeção de critério, baseado no número da linha
def adicionar_coluna_data(df, data_coluna, ini_coluna):
    df[data_coluna] = pd.to_datetime(df[data_coluna])
    
    def atualizar_data(row):
        if row['Linha'] not in ['0.810', '0.830']:  
            if 0 <= row[ini_coluna] <= 2:
                return row[data_coluna] - timedelta(days=1)
            else:
                return row[data_coluna]

    dem_data['Outra Dt'] = dem_data.apply(atualizar_data, axis=1)
    return df

dem_data = adicionar_coluna_data(dem_data, 'Data_bas', 'HoraIni', 'HoraFim', 'nova_data')

#Aplica exessão em linhas que não seguem a mesma regra de horário que as demais
execoes = np.where((dem_data['Linha'] == '0.810') | (dem_data['Linha'] == '0.850'),
                   dem_data['Data_bas'],
                   dem_data['Outra Dt'])
dem_data['Outra Dt'] = execoes

demanda_HrOp = dem_data.groupby('Outra Dt')['Demanda'].sum()

#Registro no log - horário normal
demanda_dia_xml.to_excel('caminho log/Meia noite.xlsx')
demanda_dia_xml = demanda_dia_xml.reset_index()

#Registro no log - horário definido pela área de negócio
demanda_HrOp.to_excel('Hora operação.xlsx')
demanda_HrOp = demanda_HrOp.reset_index()

#Visualiza o total
print("Soma da demanda total: ", sum(demanda_HrOp['Demanda']))

#Falta algum acesso de algum dia? Compara com o relatório que tem as quantidades totais
demanda_dia_xml['Data_bas'] = pd.to_datetime(demanda_dia_xml['Data_bas'])

demanda_tdm = pd.merge(demanda_dia_xml, bd_demanda_dia, left_on='Data_bas', right_on='TRUNC(TUR.DATAINI)', how='outer')
demanda_tdm['Diff'] = demanda_tdm['Demanda'] - demanda_tdm['COUNT(AC.DATA)']
demanda_tdm_dif = demanda_tdm[~(demanda_tdm['Diff'].isna() | (demanda_tdm['Diff'] == 0.0))]

novo_nome_colunas = {'Data_bas': 'Data_passagem',
                     'Demanda': 'Demanda_passagem',
                     'TRUNC(TUR.DATAINI)': 'Data_BD',
                     'COUNT(AC.DATA)': 'Demanda_BD',
                     'Diff': 'Diferença'}
demanda_tdm_dif.rename(columns=novo_nome_colunas, inplace=True)

if demanda_tdm_dif.empty:
    print("Tudo certo.")
else:
    print("Dias com demanda a menos:")
    print(" ")
    print(demanda_tdm_dif)

    outlook = win32.Dispatch('Outlook.Application')
    olNS = outlook.GetNameSpace('MAPI')

    mail = outlook.CreateItem(0)
    mail.to = 'email_responsavel_banco_+de_dados@empresa.com'
    mail.Subject = 'Dados a ser corrigidos'
    mail.BodyFormat = 1
    mail.Body = 'Prezado(a), segue em anexo alguns dados encontrados com inconsistências no banco de dados. Sinalizo-os afim de que possam ser corrigidos na fonte. Desde já grata.'
    mail.Attachments.Add(demanda_tdm_dif)

    mail._oleobj_.Invoke(*(64209, 0, 8, 0, olNS.Accounts.Item('seu_email_pessoal@outlook.com')))

    #Descomente a linha abaixo se quiser visualizar o corpo do e-mail sendo enviado
    #mail.Display()

    mail.Send()
    print("Email com correções enviado")

#Segue a rotina com os dados que se tem, até que o bug no banco de dados seja corrigido e os dados dos dias que estavam incorretos sejam corrigidos

#Identifica o tamanho do impacto da diferença entre os dados disponíveis e o que deveria ser para sinalizar as partes interessadas até que o problema seja resolvido
print("O total de diferença entre os dados puxados do banco de dados e o total do relatório é: ", sum(demanda_tdm_dif['Diferença']))

#Dias a ser atualizados
verifica_dem = verifica_dem[['DATA', 'bco']]
demanda_encaix = pd.merge(verifica_dem, demanda_HrOp, left_on='DATA', right_on='Outra Dt', how='outer')
demanda_encaix['Diff'] = demanda_encaix['bco'] - demanda_encaix['Demanda']

#Visualiza
demanda_encaix_dif = demanda_encaix[~(demanda_encaix['Diff'].isna() | (demanda_encaix['Diff'] == 0.0))]
print(" ")
print("Dias a ser atualizados:")
print(demanda_encaix_dif)

#Põe esses dias numa lita com um dia a mais do ultimo dia
dias_atualizar = demanda_encaix_dif[~demanda_encaix_dif['DATA'].isna()]
dias_atualizar = dias_atualizar['DATA'].tolist()

date_format_datasacessos = '%d-%m-%Y'
date_format_datasviagens = '%Y-%m-%d'

primeirodia = min(dias_atualizar)
ultimodia = max(dias_atualizar)

all_dates_datasacessos = [date.strftime(date_format_datasacessos) for date in
                         (primeirodia + timedelta(days=i) for i in range((ultimodia - primeirodia).days + 1))]

all_dates_datasviagens = [date.strftime(date_format_datasviagens) for date in
                         (primeirodia + timedelta(days=i) for i in range((ultimodia - primeirodia).days + 1))]

# Configurar a conexão com o banco de dados Oracle puxando os dados completos de passagem e viagem dos dias a serem atualizados
dsn = cx_Oracle.makedsn(host='172.xx.x.xxx', port=xxxx, service_name='linux.subnetdb.hpvcn.oraclevcn.com')
conexao = cx_Oracle.connect(user='xxx', password='xxxxxxxxxxxx', dsn = dsn)

# Criar um cursor para executar a consulta
cursor = conexao.cursor()

query_acessos = """
        SELECT
            ac.TURNOID,
            ac.VIAGEMID,
            ac.DATA,
            ac.EVENTOID,
            ac.PRODUTOID,
            ac.ID_VIAGEM_VIAGEM
            TRUNC(tur.DATAINI),
            tur.PREFIXO,
            tur.LINHAID,
            fam.DESCRICAO,
            va.ANOMALIAID,
            tv.LINHA,
            tur.MOTORISTAID,
            tur.COBRADORID
        FROM
            passagem.ACESSOS ac
        INNER JOIN
            passagem.TURNOS tur ON ac.TURNOID = tur.TURNOID
        LEFT JOIN
            passagem.PRODUTOS prod ON ac.PRODUTOID = prod.PRODUTOID
        LEFT JOIN
            passagem.FAMILIAS fam ON prod.FAMILIAID = fam.FAMILIAID
        LEFT JOIN
            passagem.VIAGENSANOMALIAS va ON ac.TURNOID = va.TURNOID 
            AND ac.VIAGEMID = va.VIAGEMID 
            AND NUMTODSINTERVAL(ABS((ac.DATA - va.DATA) * 86400), 'SECOND') <= NUMTODSINTERVAL(2, 'SECOND')
        LEFT JOIN
            passagem.XML_TURNOVIAGEM_TURNO tv ON ac.TURNOID = tv.TURNOID
        WHERE
            ac.EVENTOID <> 20 AND
             TO_CHAR(tur.DATAINI, 'DD-MM-YYYY') IN ({})
            """.format(', '.join([':{}'.format(i+1) for i in range(len(all_dates_datasacessos))]))

query_viagens = """
     SELECT
        vg.ID,
        vg.DATA_OPERACAO,
        vg.TIPO_DIA,
        vg.LINHA,
        vg.VEICULO,
        vg.NUMERO_VIAGEM,
        vg.DTHR_INICIO_PROGRAMADO,
        vg.DTHR_FINAL_PROGRAMADO,
        vg.DTHR_INICIO_REALIZADO,
        vg.DTHR_FINAL_REALIZADA,
        vg.TABELA_PROGRAMACAO,
        vg.CHAPA_MOTORISTA,
        vg.CHAPA_COBRADOR,
        vg.KM_PROGRAMADO,
        vg.KM_REALIZADO,
        vg.SENTIDO,
        vg.ATIVIDADE,
        loc_inicio.NOME AS Nome_ponto_inicio,
        loc_final.NOME AS Nome_ponto_final,
        vg.STATUS_SAIDA,
        vg.STATUS_CHEGADA
    FROM viagem.VIAGEM vg
    INNER JOIN viagem.LOCALIDADE loc_inicio ON loc_inicio.CODIGO = vg.CODIGO_PONTO_INICIO
    INNER JOIN viagem.LOCALIDADE loc_final ON loc_final.CODIGO = vg.CODIGO_PONTO_FINAL
    WHERE 
           TO_CHAR(vg.DATA_OPERACAO, 'YYYY-MM-DD') IN ({})
        """.format(', '.join([':{}'.format(i+1) for i in range(len(all_dates_datasviagens))]))
        
acessos_banco = pd.read_sql(query_acessos, conexao, params=all_dates_datasacessos)
viagens_banco = pd.read_sql(query_viagens, conexao, params=all_dates_datasviagens)

# Fechar o cursor e a conexão com o banco de dados
cursor.close()
conexao.close()


#Subdivide denominações de produto, conforme demanda
integracao_comum = np.where((acessos_banco['EVENTOID'] == 25) & (acessos_banco['DESCRICAO'] == 'Comum'),
                      'Integração - Comum',
                      acessos_banco['DESCRICAO'])

acessos_banco['DESCRICAO'] = integracao_comum

integracao_vale = np.where((acessos_banco['EVENTOID'] == 25) & (acessos_banco['DESCRICAO'] == 'Vale Transporte'),
                      'Integração - VT',
                      acessos_banco['DESCRICAO'])

acessos_banco['DESCRICAO'] = integracao_vale

acessos_banco['DESCRICAO'] = acessos_banco['DESCRICAO'].replace({
    'Contactless': 'Pagantes - Contactless',
    'Pagantes': 'Pagantes - Dinheiro'
})

acessos_banco['LINHA'] = acessos_banco['LINHA'].astype(float)

[...]

tecologia = tecologia[['VEICULO', 'TIPO']]

bco_esqueleto = pd.merge(acessos_tdd, tecologia, on='VEICULO', how='left')

def substituirtecnologia(df, coluna):
    mapeamento = {'CONVENCIONAL': 'CONVENCIONAL 3 PORTAS', 'Veículo EPTG': 'CONVENCIONAL 5 PORTAS', 'MIDI': 'MIDI'}
    df[coluna] = df[coluna].map(mapeamento)
substituirtecnologia(bco_esqueleto, 'TIPO')

bco_esqueleto['ANO'] = bco_esqueleto['DATA_OPERACAO'].dt.year
bco_esqueleto['MES'] = bco_esqueleto['DATA_OPERACAO'].dt.month
bco_esqueleto['FAIXA HORÁRIA'] = bco_esqueleto['DTHR_INICIO_REALIZADO'].dt.hour

bco_esqueleto['DATA DE OPERAÇÃO'] = bco_esqueleto['DATA_OPERACAO'].dt.strftime('%d/%m/%Y')

def substituir_valores(df, coluna):
    mapeamento = {'U': 'UTL', 'S': 'SAB', 'D': 'DOM'}
    df[coluna] = df[coluna].map(mapeamento)
substituir_valores(bco_esqueleto, 'TIPO_DIA')


colunas_demanda = ['Demanda', 'Vale Transporte', 'Comum', 'Escolar', 'Pagantes - Contactless', 'Pagantes - Dinheiro', 'Gratuitos', \
    'Funcionários','Integração - Comum', 'Integração - VT']
bco_esqueleto[colunas_demanda] = bco_esqueleto[colunas_demanda].fillna(0)

bco_esqueleto['Duração'] = bco_esqueleto['DTHR_FINAL_REALIZADA'] - bco_esqueleto['DTHR_INICIO_REALIZADO']
bco_esqueleto['Duração'] = bco_esqueleto['Duração'].astype(str)
bco_esqueleto['Duração'] = bco_esqueleto['Duração'].str.split('days ', expand=True)[1]

#Encaixando acessos de passageiros que ainda não subiram para o sistema, mas já foram identificados por outro setor, para que o calculo estatístico sejam fechado
encerrante = encerrante[['Data', 'Inicio', 'Término', 'Linha', 'Prefixo']]
encerrante['Famílias'] = encerrante['Inicio']
encerrante['Quantidade'] = encerrante['Prefixo']

def limpa_familia(value):
    if pd.notna(value) and any(char.isdigit() for char in str(value)):
        return np.nan
    else:
        return value

# Aplicar a função à coluna 'Famílias'
encerrante['Famílias'] = encerrante['Famílias'].apply(limpa_familia)

# Função para transformar valores com letras em NaN
def limpa_inicio(value):
    if pd.notna(value) and any(char.isalpha() for char in str(value)):
        return np.nan
    else:
        return value

# Aplicar a função à coluna 'Famílias'
encerrante['Inicio'] = encerrante['Inicio'].apply(limpa_inicio)

encerrante = encerrante[encerrante['Prefixo'] != 'Quantidade']

def limpa_quantidade(encerrante):
    for index, row in encerrante.iterrows():
        if not pd.isna(row['Data']):
            encerrante.at[index, 'Quantidade'] = np.nan

# Aplicar a função ao DataFrame
limpa_quantidade(encerrante)

def limpa_prefixo(encerrante):
    for index, row in encerrante.iterrows():
        if not pd.isna(row['Quantidade']):
            encerrante.at[index, 'Prefixo'] = np.nan

# Aplicar a função ao DataFrame
limpa_prefixo(encerrante)

total_enc = encerrante[encerrante['Data'] == 'Totais:']
encerrante = encerrante[encerrante['Data'] != 'Totais:']
encerrante = encerrante.dropna(how='all')
encerrante = encerrante.reset_index(drop=True)
encerrante = encerrante.reset_index()
demanda_encerrante = encerrante[['index', 'Famílias', 'Quantidade']]
demanda_encerrante['indice'] = np.nan

def define_index(demanda_encerrante):
    for index, row in demanda_encerrante.iterrows():
        if pd.isna(row['Famílias']):
            demanda_encerrante.at[index, 'indice'] = demanda_encerrante.at[index, 'index']

# Aplicar a função ao DataFrame
define_index(demanda_encerrante)

demanda_encerrante['indice'] = demanda_encerrante['indice'].fillna(method='ffill')
demanda_encerrante = demanda_encerrante.dropna(subset=['Famílias', 'Quantidade'])
demanda_encer = demanda_encerrante.pivot_table(values='Quantidade', index='indice', columns='Famílias', aggfunc='sum').fillna(0)
demanda_encer = demanda_encer.reset_index()

if 'Sem cartões' in demanda_encer.columns:
    demanda_encer = demanda_encer.drop(columns=['Sem cartões'])

encr_viagens = encerrante[['index', 'Data', 'Inicio', 'Término', 'Linha', 'Prefixo']]
encr_viagens = encr_viagens.dropna(subset='Data')
encr_viagens['index'] = encr_viagens['index'].astype(float)

encerrante_tratado = pd.merge(encr_viagens, demanda_encer, left_on='index', right_on='indice', how='outer')
encerrante_tratado = encerrante_tratado.drop(columns={'index', 'indice'})
encerrante_tratado['Data'] = pd.to_datetime(encerrante_tratado['Data'], format='%d/%m/%Y')
encerrante_tratado['Data mask'] = encerrante_tratado['Data'].astype(str)
encerrante_tratado['Prefixo'] = encerrante_tratado['Prefixo'].astype(float)
encerrante_tratado['d Inicio'] = encerrante_tratado['Data mask'] + ' ' + encerrante_tratado['Inicio']
encerrante_tratado['d Término'] = encerrante_tratado['Data mask'] + ' ' + encerrante_tratado['Término']
encerrante_tratado['d Inicio'] = pd.to_datetime(encerrante_tratado['d Inicio'], format='%Y-%m-%d %H:%M')
encerrante_tratado['d Término'] = pd.to_datetime(encerrante_tratado['d Término'], format='%Y-%m-%d %H:%M')
encerrante_tratado = encerrante_tratado.drop(columns={'Inicio', 'Término'})

bco_esqueleto_enc = bco_esqueleto[~bco_esqueleto['DTHR_INICIO_REALIZADO'].isna() & ((bco_esqueleto['ATIVIDADE'] == 'NOR') | (bco_esqueleto['ATIVIDADE'] == 'EXT'))]
bco_esqueleto_enc = bco_esqueleto_enc.sort_values('DTHR_INICIO_REALIZADO')
bco_esqueleto_enc = bco_esqueleto_enc.reset_index(drop=True)

encerrante_tratado = encerrante_tratado.sort_values('d Inicio')
encerrante_tratado = encerrante_tratado.reset_index(drop=True)
encerrante_tratado = encerrante_tratado.add_suffix('_enc')
columns_to_ignore = ['Data_enc', 'Linha_enc', 'Prefixo_enc', 'Data mask_enc', 'd Inicio_enc', 'd Término_enc']

def remove_commas_and_zeros(value):
    if isinstance(value, str):
        value = value.replace(',00', '')  # Remova vírgulas
        value = value.split('.')[0]  # Mantenha apenas a parte antes do ponto decimal
    return value

# Aplicar a função em todas as colunas exceto as especificadas em columns_to_ignore
for column in encerrante_tratado.columns.difference(columns_to_ignore):
    encerrante_tratado[column] = encerrante_tratado[column].apply(remove_commas_and_zeros).astype(int)

# Crie uma máscara para remover as linhas com valores zero nas colunas que não estão na lista de ignoradas
mask = (encerrante_tratado.drop(columns=columns_to_ignore) != 0).any(axis=1)

# Aplique a máscara para manter apenas as linhas que atendem ao critério
encerrante_tratado = encerrante_tratado[mask]

bco_encerrante = pd.merge_asof(bco_esqueleto_enc, encerrante_tratado,
                            left_on='DTHR_INICIO_REALIZADO',
                            right_on='d Inicio_enc', 
                            left_by=['DATA_OPERACAO', 'VEICULO'],
                            right_by=['Data_enc', 'Prefixo_enc'],
                            allow_exact_matches=True)

encerrantes = bco_encerrante[~bco_encerrante['Data_enc'].isna()]
encerrantes = encerrantes.sort_values('d Inicio_enc')

one_hour = timedelta(hours=1)
ten_minutes = timedelta(minutes=30)

# Verifica duplicatas
duplicatas = encerrantes[encerrantes.duplicated(subset=['Data_enc', 'Linha_enc', 'Prefixo_enc', 'd Inicio_enc', 'd Término_enc'], keep=False)]
duplicatas = duplicatas.sort_values('DTHR_INICIO_REALIZADO')

condicao_1 = encerrantes[((encerrantes['d Inicio_enc'] - encerrantes['DTHR_INICIO_REALIZADO']).abs() <= one_hour) | ((encerrantes['d Término_enc'] - encerrantes['DTHR_FINAL_REALIZADA']).abs() <= one_hour)]
condicao_1['Diferenca_de_Tempo'] = np.nan


# Calcule a diferença entre 'DTHR_FINAL_REALIZADA' da duplicata atual e 'DTHR_INICIO_REALIZADO' da duplicata anterior
duplicatas['Diferenca_de_Tempo'] = (duplicatas['DTHR_FINAL_REALIZADA'] - duplicatas.groupby(['Data_enc', 'Linha_enc', 'Prefixo_enc', 'd Inicio_enc', 'd Término_enc'])['DTHR_INICIO_REALIZADO'].shift(-1)).abs()
duplicatas['Diferenca_de_Tempo'] = duplicatas['Diferenca_de_Tempo'].shift(1)
duplicatas['DTHR_INICIO_REALIZADO_min'] = duplicatas['DTHR_INICIO_REALIZADO']
duplicatas['DTHR_FINAL_REALIZADA_max'] = duplicatas['DTHR_FINAL_REALIZADA']


for idx, grupo in duplicatas.groupby(['DATA_OPERACAO', 'Data_enc', 'LINHA', 'Linha_enc', 'VEICULO', 'Prefixo_enc', 'd Inicio_enc', 'd Término_enc']):
    valor_minimo = grupo['DTHR_INICIO_REALIZADO'].min()
    valor_maximo = grupo['DTHR_FINAL_REALIZADA'].max()
    
    #Substituir todas as linhas duplicadas no conjunto de duplicatas pelo valor mínimo e máximo calculados.
    duplicatas.loc[grupo.index, 'DTHR_INICIO_REALIZADO_min'] = valor_minimo
    duplicatas.loc[grupo.index, 'DTHR_FINAL_REALIZADA_max'] = valor_maximo


# Filtrar as linhas onde a diferença de tempo é menor ou igual a 'ten_minutes'
condicao_2 = duplicatas[(duplicatas['Diferenca_de_Tempo'].abs() <= ten_minutes) & ((duplicatas['DTHR_INICIO_REALIZADO_min'] > duplicatas['DTHR_INICIO_REALIZADO']) | \
    (duplicatas['DTHR_FINAL_REALIZADA'] < duplicatas['DTHR_FINAL_REALIZADA_max']) & ((duplicatas['d Inicio_enc'] - duplicatas['DTHR_INICIO_REALIZADO_min']).abs() <= one_hour) | \
        ((duplicatas['d Término_enc'] - duplicatas['DTHR_FINAL_REALIZADA_max']).abs() <= one_hour))]

encerrantes_encaixados = pd.concat([condicao_1, condicao_2])
encerrantes_encaixados = encerrantes_encaixados.drop_duplicates(subset=['DATA_OPERACAO', 'Data_enc', 'LINHA', 'Linha_enc', 'VEICULO', 'Prefixo_enc', 'DTHR_INICIO_REALIZADO',\
    'd Inicio_enc', 'DTHR_FINAL_REALIZADA', 'd Término_enc'])

encerrantes_encaixados = encerrantes_encaixados.reset_index(drop=True)
encerrantes_encaixados = encerrantes_encaixados.drop(columns={'Diferenca_de_Tempo'}, axis=1)

duplicatas_encerrantes_encaixados = encerrantes_encaixados.duplicated(subset=['Data_enc', 'Linha_enc', 'Prefixo_enc', 'd Inicio_enc', 'd Término_enc'], keep=False)

encerrantes_encaixados['VEICULO'] = encerrantes_encaixados['VEICULO'].astype(str)
# Crie uma nova coluna 'VEICULO_NOVA' com base nas duplicatas.
encerrantes_encaixados['VEICULO_NOVA'] = encerrantes_encaixados['VEICULO']
encerrantes_encaixados.loc[duplicatas_encerrantes_encaixados, 'VEICULO_NOVA'] = encerrantes_encaixados['VEICULO'] + " - encerrante ac"
encerrantes_encaixados.loc[~duplicatas_encerrantes_encaixados, 'VEICULO_NOVA'] = encerrantes_encaixados['VEICULO'] + " - encerrante"
encerrantes_encaixados['VEICULO'] = encerrantes_encaixados['VEICULO_NOVA']
encerrantes_encaixados = encerrantes_encaixados.drop(columns={'VEICULO_NOVA'}, axis=1)

encet_ac = encerrantes_encaixados[encerrantes_encaixados['VEICULO'].str.endswith(" - encerrante ac")]

for idx, grupo in encet_ac.groupby(['DATA_OPERACAO', 'Data_enc', 'LINHA', 'Linha_enc', 'VEICULO', 'Prefixo_enc', 'd Inicio_enc', 'd Término_enc']):
    valor_minimor = grupo['DTHR_INICIO_REALIZADO'].min()
    valor_minimop = grupo['DTHR_INICIO_PROGRAMADO'].min()

    valor_maximor = grupo['DTHR_FINAL_REALIZADA'].max()
    valor_maximop = grupo['DTHR_FINAL_PROGRAMADO'].max()
    
    #Substituir todas as linhas duplicadas no conjunto de duplicatas pelo valor mínimo e máximo calculados.
    encet_ac.loc[grupo.index, 'DTHR_INICIO_REALIZADO'] = valor_minimor
    encet_ac.loc[grupo.index, 'DTHR_INICIO_PROGRAMADO'] = valor_minimop
    
    encet_ac.loc[grupo.index, 'DTHR_FINAL_REALIZADA'] = valor_maximor
    encet_ac.loc[grupo.index, 'DTHR_FINAL_PROGRAMADO'] = valor_maximop

#Remover as duplicatas para reter apenas os valores únicos.
encet_ac = encet_ac.drop_duplicates(subset=['DATA_OPERACAO', 'Data_enc', 'LINHA', 'Linha_enc', 'VEICULO', 'Prefixo_enc',
                                'd Inicio_enc', 'd Término_enc'], keep='first')

#Limpa p trecho que vai ter os dados da viagem
encerrantes_enc_viagem = encerrantes_encaixados[~encerrantes_encaixados['VEICULO'].str.endswith(" - encerrante")]
colunas_a_excluir = [coluna for coluna in encerrantes_enc_viagem.columns if coluna.endswith('_enc')]
encerrantes_enc_viagem = encerrantes_enc_viagem.drop(columns=colunas_a_excluir)
encerrante_unico = encerrantes_encaixados[encerrantes_encaixados['VEICULO'].str.endswith(" - encerrante")]
colunas_demanda_encuni = ['Comum', 'Escolar', 'Funcionários', 'Gratuitos', 'Pagantes', 'Vale Transporte', 'Contactless', 'Integração']

for coluna in colunas_demanda_encuni:
    coluna_enc = coluna + '_enc'
    if coluna_enc in encerrante_unico.columns:
        if coluna == 'Pagantes':
            encerrante_unico['Pagantes - Contactless'] = encerrante_unico[coluna_enc]
        elif coluna == 'Contactless':
            encerrante_unico['Pagantes - Contactless'] = encerrante_unico[coluna_enc]
        elif coluna == 'Integração':
            encerrante_unico['Integração - Comum'] = encerrante_unico[coluna_enc]
        else:
            encerrante_unico[coluna] = encerrante_unico[coluna_enc]


colunas_existentes = [coluna + '_enc' for coluna in colunas_demanda_encuni if coluna + '_enc' in encerrante_unico.columns]

# Some as colunas e crie uma nova coluna 'Demanda'
encerrante_unico['Demanda'] = encerrante_unico[colunas_existentes].sum(axis=1)

colunas_a_excluiruni = [coluna for coluna in encerrante_unico.columns if coluna.endswith('_enc')]
encerrante_unico = encerrante_unico.drop(columns=colunas_a_excluiruni)

colunas_demanda_enc_as = ['Comum', 'Escolar', 'Funcionários', 'Gratuitos', 'Pagantes', 'Vale Transporte', 'Contactless', 'Integração']

for colunas in colunas_demanda_enc_as:
    coluna_encac = colunas + '_enc'
    if coluna_encac in encet_ac.columns:
        if colunas == 'Pagantes':
            encet_ac['Pagantes - Contactless'] = encet_ac[coluna_encac]
        elif colunas == 'Contactless':
            encet_ac['Pagantes - Contactless'] = encet_ac[coluna_encac]
        elif colunas == 'Integração':
            encet_ac['Integração - Comum'] = encet_ac[coluna_encac]
        else:
            encet_ac[colunas] = encet_ac[coluna_enc]

colunas_existentesac = [colunas + '_enc' for colunas in colunas_demanda_encuni if colunas + '_enc' in encet_ac.columns]

#Some as colunas e crie uma nova coluna 'Demanda'
encet_ac['Demanda'] = encet_ac[colunas_existentesac].sum(axis=1)

colunas_a_exclencet_ac = [coluna_encac for colunas_demanda_enc_as in encet_ac.columns if coluna_encac.endswith('_enc')]
encet_ac = encet_ac.drop(columns=colunas_a_excluiruni)

colunas_para_nan = ['KM_PROGRAMADO', 'KM_REALIZADO', 'SENTIDO', 'NOME_PONTO_INICIO', 'NOME_PONTO_FINAL', 'Duração', 'STATUS_SAIDA', 'STATUS_CHEGADA']
encet_ac[colunas_para_nan] = np.nan

tds_enc = pd.concat([encerrantes_enc_viagem, encerrante_unico, encet_ac], axis=0)
tds_enc = tds_enc.reset_index(drop=True)

filtro_ebco = tds_enc['ID']
filtro_ebco = filtro_ebco.reset_index()
filtro_ebco = filtro_ebco.add_suffix('_enc')

bco_esqueleto_esnx = pd.merge(bco_esqueleto, filtro_ebco, left_on='ID', right_on='ID_enc', how='outer')
bco_esqueletoo = bco_esqueleto_esnx[bco_esqueleto_esnx['ID_enc'].isna()]
bco_esqueletoo = pd.concat([bco_esqueletoo, tds_enc])
bco_esqueletoo = bco_esqueletoo.reset_index(drop=True)
bco_esqueletoo = bco_esqueletoo.drop(columns={'index_enc', 'ID_enc'})

#Configura o horário conforme o cliente final solicitou, para que fique no formato que ele deseja no filtro que ele escolheu no excel
bco_esqueletoo['DTHR_INICIO_PROGRAMADO_dif'] = (bco_esqueletoo['DTHR_INICIO_PROGRAMADO'] - bco_esqueletoo['DATA_OPERACAO'])
bco_esqueletoo['DTHR_FINAL_PROGRAMADO_dif'] = (bco_esqueletoo['DTHR_FINAL_PROGRAMADO'] - bco_esqueletoo['DATA_OPERACAO'])
bco_esqueletoo['DTHR_INICIO_REALIZADO_dif'] = (bco_esqueletoo['DTHR_INICIO_REALIZADO'] - bco_esqueletoo['DATA_OPERACAO'])
bco_esqueletoo['DTHR_FINAL_REALIZADA_dif'] = (bco_esqueletoo['DTHR_FINAL_REALIZADA'] - bco_esqueletoo['DATA_OPERACAO'])

bco_esqueletoo['DTHR_INICIO_PROGRAMADO_dif'] = bco_esqueletoo['DTHR_INICIO_PROGRAMADO_dif'] / pd.Timedelta(hours=24)
bco_esqueletoo['DTHR_FINAL_PROGRAMADO_dif'] = bco_esqueletoo['DTHR_FINAL_PROGRAMADO_dif'] / pd.Timedelta(hours=24)
bco_esqueletoo['DTHR_INICIO_REALIZADO_dif'] = bco_esqueletoo['DTHR_INICIO_REALIZADO_dif'] / pd.Timedelta(hours=24)
bco_esqueletoo['DTHR_FINAL_REALIZADA_dif'] = bco_esqueletoo['DTHR_FINAL_REALIZADA_dif'] / pd.Timedelta(hours=24)

bco_esqueletoo['DTHR_INICIO_PROGRAMADO'] = bco_esqueletoo['DTHR_INICIO_PROGRAMADO_dif']
bco_esqueletoo['DTHR_FINAL_PROGRAMADO'] = bco_esqueletoo['DTHR_FINAL_PROGRAMADO_dif']
bco_esqueletoo['DTHR_INICIO_REALIZADO'] = bco_esqueletoo['DTHR_INICIO_REALIZADO_dif']
bco_esqueletoo['DTHR_FINAL_REALIZADA'] = bco_esqueletoo['DTHR_FINAL_REALIZADA_dif']

#Retorna as strings pra como devem ser aprensentadas:
def substituirveiculonome(df, coluna):
    mapeamento = {10: 'NÃO REALIZADA', 0: 'FURO DE VIAGEM', 11: 'FURO DE VIAGEM', 1111111: 'GIRO DE TESTE'}
    df[coluna] = df[coluna].replace(mapeamento, regex=True)
substituirveiculonome(bco_esqueletoo, 'VEICULO')

codlinha = np.where(bco_esqueletoo['Nº da linha'].isna() == True,
                    bco_esqueletoo['LINHA'],
                    bco_esqueletoo['Nº da linha'])
bco_esqueletoo['Nº da linha'] = codlinha


#Filtra apenas as colunas escolhidas para o produto final 
bco = bco_esqueletoo[['ANO', 'MES', 'DATA DE OPERAÇÃO', 'TIPO_DIA', 'LINHA', 'Nº da linha', 'VEICULO', 'TIPO', 'Demanda', 'Vale Transporte', 'Comum', 'Escolar', \
    'Pagantes - Contactless', 'Pagantes - Dinheiro', 'Gratuitos', 'Funcionários', 'Integração - Comum', 'Integração - VT', 'FAIXA HORÁRIA', 'NUMERO_VIAGEM', \
        'DTHR_INICIO_PROGRAMADO', 'DTHR_FINAL_PROGRAMADO', 'DTHR_INICIO_REALIZADO', 'DTHR_FINAL_REALIZADA', 'TABELA_PROGRAMACAO', 'CHAPA_MOTORISTA', 'CHAPA_COBRADOR',\
            'KM_PROGRAMADO', 'KM_REALIZADO', 'SENTIDO', 'ATIVIDADE', 'NOME_PONTO_INICIO', 'NOME_PONTO_FINAL', 'Duração', 'Terminal','STATUS_SAIDA', 'STATUS_CHEGADA']]

condicoes = [
    (bco['ATIVIDADE'] == 'REC', 6),
    (bco['ATIVIDADE'] == 'SGA', 5),
    (bco['ATIVIDADE'] == 'TRA', 7),
]

# Use np.select com escolhas separadas e condicoes separadas
escolhas = [condicao[1] for condicao in condicoes]
condicoes = [condicao[0] for condicao in condicoes]

bco['SENTIDO'] = np.select(condicoes, escolhas, default=bco['SENTIDO'])

bco = bco.fillna('-')
bco = bco.drop_duplicates()
bco = bco.reset_index(drop=True)

confere_demanda = bco.groupby('DATA DE OPERAÇÃO')['Demanda'].sum()

#Visualiza
print("Demanda total, por dia: ", confere_demanda)
#Soma total
print("Demanda total: ", sum(bco['Demanda']))
#Relatório a ser enviado ao solicitante
bco.to_excel('caminho relatorio final/Dados do e-BCO.xlsx')
print('Foi atualizado desde o dia: ', demanda_encaix_dif)