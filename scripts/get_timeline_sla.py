"""
Read timeline data from the DataLake and calculate useful SLA information.
Code works as of 05.07.2021
Any changes to the timeline view and/or client processing structure might break this code

Output is an Excel spreadsheet, however it can be altered to other formats

PSC
"""


import pandas as pd
import numpy as np
import datetime
import boto3
import awswrangler as awr


session = boto3.Session(aws_access_key_id="AKIA4LFWKXXN43CG2ZLO",
                        aws_secret_access_key="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",
                        region_name='us-east-1')

PATH_SAVE_FILE = R'C:\Users\PerdoCaiafa\Downloads\Reports\\'

def get_timeline() -> pd.DataFrame:
    """Requests Timeline data from DataLake"""

    query = 'SELECT * FROM  "pontte_glue_database_data_lake_curated_prod"."info_timeline_agregada_pipefy_torre"'
    df = awr.athena.read_sql_query(query, database="pontte_glue_database_data_lake_curated_prod",
                                   s3_output="s3://pontte-s3-data-lake-athena-results-prod/",
                                   ctas_approach=True, boto3_session=session)

    return df

def calculate_sla(df) -> pd.DataFrame:
    """Calculates SLA for each macro phase

        Args:
        df (pd.DataFrame): Data frame imported from view 'info_timeline_agregada_pipefy_torre'

    Returns:
        pd.DataFrame: Same DataFrame with calculated sla columns included
        """

    df = df.set_index('contract_id')
    dtemp1 = df[["simulacao", "enviando_bacen"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_triagem"] = np.busday_count(pd.to_datetime(dtemp1['simulacao']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['enviando_bacen']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["sla_triagem"])

    dtemp1 = df[["enviando_bacen", "analise_de_credito"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_analise_inicial"] = np.busday_count(pd.to_datetime(dtemp1['enviando_bacen']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['analise_de_credito']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["sla_analise_inicial"])

    dtemp1 = df[["analise_de_credito", "envio_de_proposta_cliente"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_analise_cred"] = np.busday_count(pd.to_datetime(dtemp1['analise_de_credito']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['envio_de_proposta_cliente']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["sla_analise_cred"])

    dtemp1 = df[["envio_de_proposta_cliente", "analise_juridica"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_negociacao"] = np.busday_count(pd.to_datetime(dtemp1['envio_de_proposta_cliente']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['analise_juridica']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["sla_negociacao"])

    dtemp1 = df[["analise_juridica", "emissao_laudo"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_analise_jur"] = np.busday_count(pd.to_datetime(dtemp1['analise_juridica']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['emissao_laudo']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["sla_analise_jur"])

    dtemp1 = df[["emissao_laudo", "emissao_do_contrato"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_ajustes"] = np.busday_count(pd.to_datetime(dtemp1['emissao_laudo']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['emissao_do_contrato']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["sla_ajustes"])

    dtemp1 = df[["emissao_do_contrato", "coleta_assinatura"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_contrato"] = np.busday_count(pd.to_datetime(dtemp1['emissao_do_contrato']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['coleta_assinatura']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["sla_contrato"])

    dtemp1 = df[["simulacao", "envio_de_proposta_cliente"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["time_to_proposal"] = np.busday_count(pd.to_datetime(dtemp1['simulacao']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['envio_de_proposta_cliente']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["time_to_proposal"])

    dtemp1 = df[["simulacao", "coleta_assinatura"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["time_to_deal"] = np.busday_count(pd.to_datetime(dtemp1['simulacao']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['coleta_assinatura']).values.astype('datetime64[D]'))
    df = df.join(dtemp1["time_to_deal"])

    return df

def calculate_client_time(df) -> pd.DataFrame:
    """Calculates time client takes to answer to a request in each macro phase up to contract signing

        Args:
        df (pd.DataFrame): Data frame imported from view 'info_timeline_agregada_pipefy_torre'

    Returns:
        pd.DataFrame: Same DataFrame with calculated sla columns included
        """

    df['client_triagem'] = df['sla_triagem']

    dtemp1 = df[["enviando_bacen", "analise_bacen"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_enviando_bac"] = np.busday_count(pd.to_datetime(dtemp1['enviando_bacen']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['analise_bacen']).values.astype('datetime64[D]'))
    dtemp2 = df[['pendencia_de_documentos', 'analise_de_credito']]
    dtemp2 = dtemp2.dropna()
    dtemp2["sla_pend_doc"] = np.busday_count(pd.to_datetime(dtemp2['pendencia_de_documentos']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp2['analise_de_credito']).values.astype('datetime64[D]'))
    dtemp_join = dtemp1.join(dtemp2["sla_pend_doc"])
    dtemp_join['client_analise_inicial'] = dtemp_join['sla_enviando_bac'] + dtemp_join['sla_pend_doc']
    df = df.join(dtemp_join['client_analise_inicial'])


    dtemp1 = df[["aguardando_cliente", "pre_analise_juridica"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["client_negociacao"] = np.busday_count(pd.to_datetime(dtemp1['aguardando_cliente']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['pre_analise_juridica']).values.astype('datetime64[D]'))
    df = df.join(dtemp1['client_negociacao'])


    dtemp1 = df[["pendencias_juridicas", "reanalise_juridica"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["client_analise_jur"] = np.busday_count(pd.to_datetime(dtemp1['pendencias_juridicas']).values.astype('datetime64[D]'),
                                            pd.to_datetime(dtemp1['reanalise_juridica']).values.astype('datetime64[D]'))
    df = df.join(dtemp1['client_analise_jur'])


    return df


def calculate_aux_slas(df) -> pd.DataFrame:
    """Calculates sla from last phase update to current date and sla from simulation date to last phase update

    Args:
        df (pd.DataFrame): Data frame imported from view 'info_timeline_agregada_pipefy_torre'

    Returns:
        pd.DataFrame: Same DataFrame with calculated sla columns included
        """

    dtemp1 = df[["simulacao", "last_value"]]
    dtemp1 = dtemp1.dropna()
    dtemp1["sla_until_last_status"] = np.busday_count(
        pd.to_datetime(dtemp1['simulacao']).values.astype('datetime64[D]'),
        pd.to_datetime(dtemp1['last_value']).values.astype('datetime64[D]'))
    df = df.join(dtemp1['sla_until_last_status'])

    dtemp1 = df[['last_value']]
    dtemp1 = dtemp1.dropna()
    dtemp1['current_sla'] = np.busday_count(pd.to_datetime(dtemp1['last_value']).values.astype('datetime64[D]'),
                                            datetime.date.today())
    df = df.join(dtemp1['current_sla'])

    return df

def calculate_pontte_time(df):
    """Calculates time Pontte takes to analyze a client by subtracting client time from total processing time

     Args:
        df (pd.DataFrame): Data frame imported from view 'info_timeline_agregada_pipefy_torre'

    Returns:
        pd.DataFrame: Same DataFrame with calculated sla columns included

    """
    #TRIAGEM
    dftemp2 = df[['sla_triagem', 'client_triagem']].copy()
    dftemp2 = dftemp2.dropna(subset=['sla_triagem'])
    dftemp2['client_triagem'] = dftemp2['client_triagem'].fillna(0)
    dftemp2['pontte_triagem'] = dftemp2['sla_triagem'] - dftemp2['client_triagem']
    df = df.join(dftemp2['pontte_triagem'])

    #ANALISE INICIAL
    dftemp2 = df[['sla_analise_inicial', 'client_analise_inicial']].copy()
    dftemp2 = dftemp2.dropna(subset=['sla_analise_inicial'])
    dftemp2['client_analise_inicial'] = dftemp2['client_analise_inicial'].fillna(0)
    dftemp2['pontte_analise_inicial'] = dftemp2['sla_analise_inicial'] - dftemp2['client_analise_inicial']
    df = df.join(dftemp2['pontte_analise_inicial'])

    #NEGOCIACAO
    dftemp2 = df[['sla_negociacao', 'client_negociacao']].copy()
    dftemp2 = dftemp2.dropna(subset=['sla_negociacao'])
    dftemp2['client_negociacao'] = dftemp2['client_negociacao'].fillna(0)
    dftemp2['pontte_negociacao'] = dftemp2['sla_negociacao'] - dftemp2['client_negociacao']
    df = df.join(dftemp2['pontte_negociacao'])


    #ANALISE JURIDICA
    dftemp2 = df[['sla_analise_jur', 'client_analise_jur']].copy()
    dftemp2 = dftemp2.dropna(subset=['sla_analise_jur'])
    dftemp2['client_analise_jur'] = dftemp2['client_analise_jur'].fillna(0)
    dftemp2['pontte_analise_jur'] = dftemp2['sla_analise_jur'] - dftemp2['client_analise_jur']
    df = df.join(dftemp2['pontte_analise_jur'])

    return df


def save_file(df):
    """Save Excel spreadsheet"""
    date = str(datetime.date.today())
    file_name = 'Timeline_sla_{}'.format(date)
    print('Savig file {} ...'.format(file_name))
    df.to_excel(PATH_SAVE_FILE + file_name + '.xlsx', engine='openpyxl')


if __name__ == '__main__':
    print('Fetching data ...')
    timeline = get_timeline()
    print('Calculating SLAs ...')
    timeline = calculate_sla(timeline)
    print('Calculating Client SLAs ...')
    timeline = calculate_client_time(timeline)
    print('Calculating extra info ...')
    timeline = calculate_pontte_time(timeline)
    timeline = calculate_aux_slas(timeline)

    save_file(timeline)
