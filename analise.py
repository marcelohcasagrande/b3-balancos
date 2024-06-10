# Pacotes do Python.
import pandas as pd
import numpy as np
import wget
from zipfile import ZipFile
import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter( action = 'ignore', category = ( SettingWithCopyWarning ) )

# Vetor de anos (parâmetro para leitura).
vetorAnos = [ 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023 ]


    #                           #  
    # URL com os balanços (FRE) #
    #                           #
    
# Pasta para salvar.
pasta_arquivos = 'fre' # crie a pasta no local do código.

# Fazendo download dos zips.
for ano in vetorAnos:
    url = f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/fre_cia_aberta_{ano}.zip'
    wget.download( url, out = pasta_arquivos )
    print( f'Ano de {ano} baixado com sucesso!' )


    #                #  
    # Extraindo zips #
    #                #
 
# Extraindo.    
for ano in vetorAnos:
    ZipFile( pasta_arquivos + '/fre_cia_aberta_' + str( ano ) + '.zip' ).extractall( pasta_arquivos + '/extraidos' + '/fre_' + str( ano ) )
    print( f'Ano de {ano} extraído com sucesso!' )


    #                           #  
    # URL com os balanços (DFP) #
    #                           #

# Pasta para salvar.
pasta_arquivos = 'dfp' # crie a pasta no local do código.

# Fazendo download dos zips.
for ano in vetorAnos:
    url = f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ano}.zip'
    wget.download( url, out = pasta_arquivos )
    print( f'Ano de {ano} baixado com sucesso!' )
    
    
    #                #  
    # Extraindo zips #
    #                #
 
# Extraindo.    
for ano in vetorAnos:
    ZipFile( pasta_arquivos + '/dfp_cia_aberta_' + str( ano ) + '.zip' ).extractall( pasta_arquivos + '/extraidos' + '/dfp_' + str( ano ) )
    print( f'Ano de {ano} extraído com sucesso!' )   
    
    
    #                            #
    # Lendo de um ano específico #    
    #                            #     

# DataFrames.    
dre = pd.DataFrame()
bpa = pd.DataFrame()
bpp = pd.DataFrame()
dfc = pd.DataFrame()

# Loop.    
for ano in vetorAnos:    
    
    # Lendo DRE.    
    dreAux = pd.read_csv( 'dfp/extraidos' + '/dfp_' + str( ano ) + 
                    
                          '/dfp_cia_aberta_DRE_con_' + str( ano ) + '.csv', 
                    
                          sep = ';', encoding = 'ISO-8859-1', decimal = ',' )    
    
    # Concatenando.
    dre = pd.concat( [ dre, dreAux ], axis = 0 )
    
    # Lendo BPA (Ativos).    
    bpaAux = pd.read_csv( 'dfp/extraidos' + '/dfp_' + str( ano ) + 
                 
                          '/dfp_cia_aberta_BPA_con_' + str( ano ) + '.csv', 
                  
                          sep = ';', encoding = 'ISO-8859-1', decimal = ',' )   
    
    # Concatenando.
    bpa = pd.concat( [ bpa, bpaAux ], axis = 0 ) 
    
    # Lendo BPP (Passivos).    
    bppAux = pd.read_csv( 'dfp/extraidos' + '/dfp_' + str( ano ) + 
                 
                          '/dfp_cia_aberta_BPP_con_' + str( ano ) + '.csv', 
                  
                          sep = ';', encoding = 'ISO-8859-1', decimal = ',' )
    
    # Concatenando.
    bpp = pd.concat( [ bpp, bppAux ], axis = 0 )     
    
    # Lendo DFC (Fluxo de Caixa).
    dfcAux = pd.read_csv( 'dfp/extraidos' + '/dfp_' + str( ano ) + 
                 
                          '/dfp_cia_aberta_DFC_MI_con_' + str( ano ) + '.csv', 
                  
                          sep = ';', encoding = 'ISO-8859-1', decimal = ',' )
    
    # Concatenando.
    dfc = pd.concat( [ dfc, dfcAux ], axis = 0 )
    
    
    #            #
    # Tratativas #
    #            #

# Lista para dropar bancos e instituições com balanços com template diferente.
dropandoBancos = [ 'ITAU UNIBANCO HOLDING S.A.',
                   'BCO BRASIL S.A.',
                   'BCO BRADESCO S.A.',
                   'BCO MERCANTIL DO BRASIL S.A.',
                   'BCO ABC BRASIL S.A.',
                   'INTER & CO, INC.',
                   'BCO SANTANDER (BRASIL) S.A.',
                   'BCO PAN S.A.',
                   'BCO PINE S.A',
                   'BANCO BMG S/A',
                   'BANESTES S.A. - BCO EST ESPIRITO SANTO',
                   'BCO ALFA DE INVESTIMENTO S.A.',
                   'BCO DAYCOVAL S.A.',
                   'BCO ESTADO DO RIO GRANDE DO SUL S.A.',
                   'BANCO RCI BRASIL S.A.',
                   'BRB BANCO DE BRASILIA S.A.',
                   'COMPANHIA DE CRÉDITO FINANCIAMENTO E INVESTIMENTO RCI BRASIL',
                   'FINANCEIRA ALFA S.A.- CRED FINANC E INVS',
                   'BCO PATAGONIA S.A.',
                   'NU HOLDINGS LTD.',
                   'BRAZILIAN FINANCE E REAL ESTATE S.A.',
                   'XP INVESTIMENTOS S.A.',
                   'BANCO CRUZEIRO DO SUL SA',
                   'BCO ESTADO DE SERGIPE S.A. - BANESE',
                   'CIA SEGUROS ALIANCA DA BAHIA',
                   'BB SEGURIDADE PARTICIPAÇÕES S.A.',
                   'IRB - BRASIL RESSEGUROS S.A.',
                   'PPLA PARTICIPATIONS LTD.',
                   'BANCO INDUSTRIAL E COMERCIAL S/A'   
]    

# Pegando último exercício.   
dre = dre[ dre[ 'ORDEM_EXERC' ] == 'ÚLTIMO' ]

# Dropando colunas.
dre = dre.drop( [ 'CD_CVM', 'GRUPO_DFP', 'MOEDA', 'ORDEM_EXERC', 'DT_INI_EXERC', 'DT_FIM_EXERC', 'ST_CONTA_FIXA', 'VERSAO' ], axis = 1 )

# Transformando em data.
dre[ 'DT_REFER' ] = pd.to_datetime( dre[ 'DT_REFER' ] )

# Deixando apenas fechamento de ano fiscal normal (terminando em dezembro).
dre = dre[ dre[ 'DT_REFER' ].dt.month == 12 ]

# Transformando em numérico.
dre[ 'VL_CONTA' ] = pd.to_numeric( dre[ 'VL_CONTA' ] )

# Ajuste.
dre[ 'VL_CONTA' ] = np.where( dre[ 'ESCALA_MOEDA' ] == 'UNIDADE', dre[ 'VL_CONTA' ] / 1000, dre[ 'VL_CONTA' ] )

# Dropando colunas.
dre = dre.drop( [ 'ESCALA_MOEDA' ], axis = 1 )

# Pegando último exercício. 
bpa = bpa[ bpa[ 'ORDEM_EXERC' ] == 'ÚLTIMO' ]

# Dropando colunas.
bpa = bpa.drop( [ 'CD_CVM', 'GRUPO_DFP', 'MOEDA', 'ORDEM_EXERC', 'DT_FIM_EXERC', 'ST_CONTA_FIXA', 'VERSAO' ], axis = 1 )

# Transformando em data.
bpa[ 'DT_REFER' ] = pd.to_datetime( bpa[ 'DT_REFER' ] )

# Deixando apenas fechamento de ano fiscal normal (terminando em dezembro).
bpa = bpa[ bpa[ 'DT_REFER' ].dt.month == 12 ]

# Transformando em numérico.
bpa[ 'VL_CONTA' ] = pd.to_numeric( bpa[ 'VL_CONTA' ] )

# Ajuste.
bpa[ 'VL_CONTA' ] = np.where( bpa[ 'ESCALA_MOEDA' ] == 'UNIDADE', bpa[ 'VL_CONTA' ] / 1000, bpa[ 'VL_CONTA' ] )

# Dropando colunas.
bpa = bpa.drop( [ 'ESCALA_MOEDA' ], axis = 1 )

# Pegando último exercício. 
bpp = bpp[ bpp[ 'ORDEM_EXERC' ] == 'ÚLTIMO' ]

# Dropando colunas.
bpp = bpp.drop( [ 'CD_CVM', 'GRUPO_DFP', 'ORDEM_EXERC', 'DT_FIM_EXERC', 'ST_CONTA_FIXA', 'VERSAO' ], axis = 1 )

# Transformando em data.
bpp[ 'DT_REFER' ] = pd.to_datetime( bpp[ 'DT_REFER' ] )

# Deixando apenas fechamento de ano fiscal normal (terminando em dezembro).
bpp = bpp[ bpp[ 'DT_REFER' ].dt.month == 12 ]

# Transformando em numérico.
bpp[ 'VL_CONTA' ] = pd.to_numeric( bpp[ 'VL_CONTA' ] )

# Ajuste.
bpp[ 'VL_CONTA' ] = np.where( bpp[ 'ESCALA_MOEDA' ] == 'UNIDADE', bpp[ 'VL_CONTA' ] / 1000, bpp[ 'VL_CONTA' ] )

# Dropando colunas.
bpp = bpp.drop( [ 'ESCALA_MOEDA' ], axis = 1 )

# Pegando último exercício. 
dfc = dfc[ dfc[ 'ORDEM_EXERC' ] == 'ÚLTIMO' ]

# Dropando colunas.
dfc = dfc.drop( [ 'VERSAO', 'CD_CVM', 'GRUPO_DFP', 'MOEDA', 'ORDEM_EXERC', 'DT_INI_EXERC', 'DT_FIM_EXERC', 'ST_CONTA_FIXA', 'ST_CONTA_FIXA' ], axis = 1 )

# Transformando em data.
dfc[ 'DT_REFER' ] = pd.to_datetime( dfc[ 'DT_REFER' ] )

# Deixando apenas fechamento de ano fiscal normal (terminando em dezembro).
dfc = dfc[ dfc[ 'DT_REFER' ].dt.month == 12 ]

# Transformando em numérico.
dfc[ 'VL_CONTA' ] = pd.to_numeric( dfc[ 'VL_CONTA' ] )

# Ajuste.
dfc[ 'VL_CONTA' ] = np.where( dfc[ 'ESCALA_MOEDA' ] == 'UNIDADE', dfc[ 'VL_CONTA' ] / 1000, dfc[ 'VL_CONTA' ] )

# Dropando colunas.
dfc = dfc.drop( [ 'ESCALA_MOEDA' ], axis = 1 )


    #      #
    # Drop #
    #      #
    
# Drop.
dre = dre[ ~dre[ 'DENOM_CIA' ].isin( dropandoBancos ) ]    
bpa = bpa[ ~bpa[ 'DENOM_CIA' ].isin( dropandoBancos ) ]   
bpp = bpp[ ~bpp[ 'DENOM_CIA' ].isin( dropandoBancos ) ]   
dfc = dfc[ ~dfc[ 'DENOM_CIA' ].isin( dropandoBancos ) ]   


    #                    #     
    # Quantidades totais #
    #                    #

# Printando.
print( f'Quantidade de registros da DRE: { len( dre ) }' )
print( dre[ 'DT_REFER' ].dt.year.unique() )
print( f'Quantidade de registros da BPA: { len( bpa ) }' )
print( bpa[ 'DT_REFER' ].dt.year.unique() )
print( f'Quantidade de registros da BPP: { len( bpp ) }' )
print( bpp[ 'DT_REFER' ].dt.year.unique() )


    #                            #
    # Pegando números sintéticos #
    #                            #  

# CD CONTA.
CD_RECEITA_LIQUIDA = '3.01'             # DRE
CD_EBIT = '3.05'                        # DRE 
CD_RESULTADO_FINANCEIRO = '3.06'        # DRE
CD_IMPOSTOS = '3.08'                    # DRE  
CD_RESULTADO_LIQUIDO_OPER_DESC = '3.10' # DRE
CD_RESULTADO_LIQUIDO = '3.09'           # DRE
CD_PATRIMONIO_LIQUIDO = '2.03'          # BPP
CD_CAIXA = '1.01.01'                    # BPA
CD_APLICACOES_FINANCEIRAS = '1.01.02'   # BPA
CD_EMP_FINAN_CIRC = '2.01.04'           # BPP
CD_EMP_FINAN_N_CIRC = '2.02.01'         # BPP
CD_FCO = '6.01'                         # DFC
CD_FCI = '6.02'                         # DFC
CD_FCF = '6.03'                         # DFC


    #                               #
    # Loop para todas as companhias #
    #                               # 

# Separando companhias que ainda existem ou estão com capital aberto.
companhias = list( dre[ dre.DT_REFER == dre.DT_REFER.max() ][ 'DENOM_CIA' ].unique() )  

# DataFrame Final.
tabelaResumo = pd.DataFrame()

# Iterando.
for nome in companhias:
    try:
        # Separando demonstrações.
        dreCIA = dre[ dre[ 'DENOM_CIA' ] == nome ]
        bppCIA = bpp[ bpp[ 'DENOM_CIA' ] == nome ]
        bpaCIA = bpa[ bpa[ 'DENOM_CIA' ] == nome ]
        dfcCIA = dfc[ dfc[ 'DENOM_CIA' ] == nome ]   
        # Receita Líquida.
        receitaLiq = dreCIA[ dreCIA[ 'CD_CONTA' ] == CD_RECEITA_LIQUIDA ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        receitaLiq.set_index( 'DT_REFER', inplace = True )
        receitaLiq.columns = [ 'Receita Líquida' ] 
        # EBIT.
        EBIT = dreCIA[ dreCIA[ 'CD_CONTA' ] == CD_EBIT ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        EBIT.set_index( 'DT_REFER', inplace = True )
        EBIT.columns = [ 'EBIT' ]
        # Resultado Financeiro.
        resFin = dreCIA[ dreCIA[ 'CD_CONTA' ] == CD_RESULTADO_FINANCEIRO ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        resFin.set_index( 'DT_REFER', inplace = True )
        resFin.columns = [ 'Resultado Financeiro' ]
        # Impostos.
        impostos = dreCIA[ dreCIA[ 'CD_CONTA' ] == CD_IMPOSTOS ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        impostos.set_index( 'DT_REFER', inplace = True )
        impostos.columns = [ 'Impostos' ]
        # Lucro líquido de operações descontinuadas.
        opDesc = dreCIA[ dreCIA[ 'CD_CONTA' ] == CD_RESULTADO_LIQUIDO_OPER_DESC ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        opDesc.set_index( 'DT_REFER', inplace = True )
        opDesc.columns = [ 'Op. Desc.' ]
        # Lucro líquido.
        lucroLiq = dreCIA[ dreCIA[ 'CD_CONTA' ] == CD_RESULTADO_LIQUIDO ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        lucroLiq.set_index( 'DT_REFER', inplace = True )
        lucroLiq.columns = [ 'Lucro Líquido' ]
        # Patrimônio Líquido.
        patrimonio = bppCIA[ bppCIA[ 'CD_CONTA' ] == CD_PATRIMONIO_LIQUIDO ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        patrimonio.set_index( 'DT_REFER', inplace = True )
        patrimonio.columns = [ 'Patrimônio Líquido' ]
        # Caixa.
        caixa = bpaCIA[ bpaCIA[ 'CD_CONTA' ] == CD_CAIXA ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        caixa.set_index( 'DT_REFER', inplace = True )
        caixa.columns = [ 'Caixa' ]
        # Aplicações Financeiras.
        aplicFinan = bpaCIA[ bpaCIA[ 'CD_CONTA' ] == CD_APLICACOES_FINANCEIRAS ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        aplicFinan.set_index( 'DT_REFER', inplace = True )
        aplicFinan.columns = [ 'Aplicações Financeiras' ]
        # Empréstimos e Financiamentos Circulantes.
        empFinCirc = bppCIA[ bppCIA[ 'CD_CONTA' ] == CD_EMP_FINAN_CIRC ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        empFinCirc.set_index( 'DT_REFER', inplace = True )
        empFinCirc.columns = [ 'EmpFinCirc' ]
        # Empréstimos e Financiamentos Não Circulantes.
        empFinNCirc = bppCIA[ bppCIA[ 'CD_CONTA' ] == CD_EMP_FINAN_N_CIRC ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        empFinNCirc.set_index( 'DT_REFER', inplace = True )
        empFinNCirc.columns = [ 'EmpFinNCirc' ]
        # FCO (fluxo de caixa operacional).
        fco = dfcCIA[ dfcCIA[ 'CD_CONTA' ] == CD_FCO ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        fco.set_index( 'DT_REFER', inplace = True )
        fco.columns = [ 'FCO' ]
        # FCF (fluxo de caixa de financiamento).
        fcf = dfcCIA[ dfcCIA[ 'CD_CONTA' ] == CD_FCF ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        fcf.set_index( 'DT_REFER', inplace = True )
        fcf.columns = [ 'FCF' ]
        # FCI (fluxo de caixa de investimentos).
        fci = dfcCIA[ dfcCIA[ 'CD_CONTA' ] == CD_FCI ][ [ 'DT_REFER', 'VL_CONTA' ] ]
        fci.set_index( 'DT_REFER', inplace = True )
        fci.columns = [ 'FCI' ]
        # Merges.
        tabela = pd.merge( receitaLiq, EBIT, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, resFin, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, impostos, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, opDesc, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, lucroLiq, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, patrimonio, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, caixa, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, aplicFinan, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, empFinCirc, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, empFinNCirc, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, fco, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, fcf, left_index = True, right_index = True, how = 'left' )
        tabela = pd.merge( tabela, fci, left_index = True, right_index = True, how = 'left' )
        # Tirando NA's.
        tabela = tabela.fillna( 0 )
        # Criando novas métricas.
        tabela[ 'Caixa e Aplic Finan' ] = tabela[ 'Caixa' ] + tabela[ 'Aplicações Financeiras' ] # Caixa Geral
        tabela[ 'Dívida' ] = tabela[ 'EmpFinCirc' ] + tabela[ 'EmpFinNCirc' ] # Dívida Geral (de curto e longo prazo)
        tabela[ 'FCL' ] = tabela[ 'FCO' ] + tabela[ 'FCI' ] # Fluxo de Caixa Livre
        # Divisão para ficar mais elegante.
        tabela = tabela / 1000
        # Ajustando arredondamentos.
        tabela = tabela.round()
        # Ajustando tipos.
        tabela = tabela.astype( int )
        # Reset Index.
        tabela = tabela.reset_index()
        # Tabela Resumo.
        tabelaResumoAux = tabela[ [ 'DT_REFER', 'Receita Líquida', 'EBIT', 'Resultado Financeiro', 'Impostos', 'Op. Desc.', 
                                    'Lucro Líquido', 'Patrimônio Líquido', 'Caixa e Aplic Finan', 'Dívida', 'FCO', 'FCI', 'FCF', 'FCL' ] ]
        # Renomeando.
        tabelaResumoAux.rename( columns = { 'DT_REFER': 'Ano' }, inplace = True )
        # Adicionando nome da empresa.
        tabelaResumoAux[ 'Empresa' ] = nome
        # Appendando.
        tabelaResumo = pd.concat( [ tabelaResumo, tabelaResumoAux ], ignore_index = True, axis = 0 )
    except:
        continue       
    

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'WEG S.A.' ] 

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'ENGIE BRASIL ENERGIA S.A.' ] 

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'PORTO SEGURO S.A.' ] 

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'RAIA DROGASIL S.A.' ] 

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'MULTIPLAN - EMPREEND IMOBILIARIOS S.A.' ] 

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'PRIO S.A.' ] 

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'FLEURY S.A.' ] 

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'ITAÚSA S.A.' ] 

tabelaResumo[ tabelaResumo[ 'Empresa' ] == 'VIVARA PARTICIPAÇÕES S.A.' ] 


