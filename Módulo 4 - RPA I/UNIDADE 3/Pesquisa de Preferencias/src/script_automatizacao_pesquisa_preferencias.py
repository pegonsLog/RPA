# %% [markdown]
# ### Pesquisa de Preferências: Coleta e Armazenamento de Dados
# ***
# 
# ##### Objetivo da Pesquisa
# 
# Esta pesquisa fictícia foi desenvolvida com o propósito de treinar e aprimorar todo o processo de Automação de Processos Robóticos (RPA) desde a coleta de dados, passando pelo tratamento, integração e armazenamento de informações. A simulação envolve a coleta de dados sobre preferências pessoais, hábitos e características demográficas dos participantes, fornecendo uma base prática para o desenvolvimento de habilidades em RPA.
# 
# #### Atividades de Coleta e Armazenamento
# 
# A principal atividade deste projeto é a coleta de dados por meio de questionários estruturados e a subsequente armazenagem dessas informações em um banco de dados. O processo é dividido em várias etapas essenciais para garantir a qualidade e a integridade dos dados, além de proporcionar uma experiência completa de automação:
# 
# 1. **Desenvolvimento do Questionário**
#    - Elaboramos um questionário detalhado para capturar informações sobre preferências pessoais (como bebida favorita, hobbies), características demográficas (como gênero e data de nascimento) e outros aspectos relevantes (como presença de animais de estimação e percepções sobre o clima).
# 
# 2. **Coleta de Dados**
#    - A coleta de dados é realizada por meio de entrevistas ou preenchimento de formulários online. Os participantes fornecem suas respostas, que são então capturadas automaticamente por bots de RPA.
# 
# 3. **Tratamento de Dados**
#    - **Verificação de Dados Duplicados**: Utilizando scripts automatizados, removemos registros duplicados para evitar redundâncias e garantir a precisão dos dados.
#    - **Tratamento de Dados Ausentes**: Identificamos e lidamos com valores ausentes, utilizando métodos automáticos para preenchimento com valores padrão ou imputação baseada em algoritmos.
#    - **Validação e Limpeza de Dados**: Bots de RPA validam automaticamente que todos os dados estão no formato adequado e dentro dos intervalos válidos, corrigindo inconsistências ou erros de entrada.
# 
# 4. **Integração de Dados**
#    - Os dados coletados e tratados são integrados de maneira automática com sistemas existentes, garantindo que todas as informações estejam disponíveis para análise e uso imediato.
# 
# 5. **Armazenamento de Dados**
#    - Após o tratamento e a integração, os dados são armazenados em um banco de dados estruturado. Este processo inclui a criação automatizada de tabelas e esquemas que organizam os dados de forma eficiente e acessível, além de implementar medidas de integridade referencial para assegurar a consistência dos dados entre diferentes tabelas.
# 
# <Center><h5><font>Fluxograma da Automatização Coleta e Armazenamento de Dados de Pesquisa de Preferências </font></h5></center>
# 
# <center>
# <img src="../img/Pesquisa de Prefências.png" width="70%">
# </center>
# 
# 
# 
# #### Objetivo Geral
# 
# A realização desta pesquisa fictícia oferece uma oportunidade valiosa para treinar e desenvolver habilidades em Automação de Processos Robóticos (RPA), abrangendo todas as etapas desde a coleta até o armazenamento de dados. O uso de bots de RPA para automatizar tarefas repetitivas e garantir a precisão dos dados permite uma experiência prática e completa. Com a prática adquirida, é possível aplicar esses conhecimentos em projetos reais, otimizando processos e tomando decisões mais informadas e eficazes.
# 

# %% [markdown]
# ### Importação das bibliotecas

# %%
import pandas as pd
import os 
from sqlalchemy import create_engine, text
import dotenv
dotenv.load_dotenv('../config/.env')

# %% [markdown]
# #### Coletando Dados da Pesquisa
# ***

# %%
PATH = '../datasets/dados_pesquisa_preferencias/'
list_path = os.listdir(PATH)

lista_df = []

for arquivo in list_path:
    local_arquivo = os.path.join(PATH, arquivo)
    if arquivo.endswith('.csv'):
        df = pd.read_csv(local_arquivo, sep=';', encoding='latin1')
        lista_df.append(df)
df_pesquisa = pd.concat(lista_df)

# %%
df_pesquisa.head()

# %%
df_pesquisa.shape

# %% [markdown]
# #### Coletando Dados dos Respondentes
# ***

# %%
df_respondentes = pd.read_csv('../datasets/dados_pessoas_respondentes/respondentes_pesquisa.csv', 
                              sep='|', encoding='latin1')

# %%
df_respondentes.head()

# %%
df_respondentes.shape

# %% [markdown]
# #### Coletando Dados de Estados
# ***

# %%
df_estado = pd.read_csv('../datasets/dados_estado_regiao/estado_regiao.csv', 
                        sep=';', encoding='latin1')

# %%
df_estado.head()

# %%
df_estado.shape

# %% [markdown]
# #### Analisando Dados Coletados

# %%
def verifica_dados_duplicados(nome_datraframe, dataframe):
    qtd = dataframe.duplicated().sum()
    print(f'O dataframe {nome_datraframe} possui {qtd} dados duplicados.')

# %%
verifica_dados_duplicados('df_pesquisa', df_pesquisa)

# %%
verifica_dados_duplicados('df_respondentes', df_respondentes)

# %%
lista_dataframes = [
                    ('df_pesquisa', df_pesquisa),
                    ('df_respondentes', df_respondentes), 
                    ('df_estados', df_estado)
]

for nome_dataframe, dataframe in lista_dataframes:
    verifica_dados_duplicados(nome_dataframe, dataframe)

# %% [markdown]
# #### Avaliando Dados Ausentes

# %%
def verifica_dados_ausentes(nome_dataframe, dataframe):
    dados_ausentes = dataframe.isna().sum()
    colunas_com_ausentes = dados_ausentes[dados_ausentes>0]
    print('--' * 50)

    if not colunas_com_ausentes.empty:
        print(f'O dataframe {nome_dataframe} possui dados ausentes.\nLista de colunas:' )
        return print(colunas_com_ausentes)
    else:
        print(f'O dataframe {nome_dataframe} não possui dados ausentes.')

# %%
for nome_dataframe, dataframe in lista_dataframes:
    verifica_dados_ausentes(nome_dataframe, dataframe)

# %%
df_respondentes.isna().sum()

# %% [markdown]
# ### Tratamento de Dados (Data Cleaning/Preprocessing)
# ***

# %% [markdown]
# ##### Corrigindo Dados Duplicados

# %%
df_respondentes.drop_duplicates(inplace=True)

# %%
verifica_dados_duplicados('df_respondentes', df_respondentes)

# %% [markdown]
# #### Analisando Dados Ausentes da Variável - Peso
# ***

# %%
filtro = df_respondentes['peso'].isna()
index_aunsentes_peso = df_respondentes[filtro].index
df_respondentes.loc[index_aunsentes_peso]

# %% [markdown]
# ##### Correção dos Dados de Peso

# %%
media_peso = df_respondentes['peso'].mean().round(2)
print(media_peso)
df_respondentes['peso'] = df_respondentes['peso'].fillna(value=media_peso)

# %% [markdown]
# ##### Verificando Dados Corrigidos

# %%
df_respondentes.loc[index_aunsentes_peso]

# %% [markdown]
# #### Analisando Dados Ausentes da Variável - Estado Civil
# ***

# %%
filtro = df_respondentes['estado_civil'].isna()
index_ausentes_estado_civil = df_respondentes[filtro].index
df_respondentes.loc[index_ausentes_estado_civil]

# %% [markdown]
# ##### Correção dos Dados de Estado Civil

# %%
moda_estado_civil = df_respondentes['estado_civil'].mode()[0]
print(moda_estado_civil)
df_respondentes['estado_civil'] = df_respondentes['estado_civil'].fillna(value=moda_estado_civil)

# %% [markdown]
# ##### Verificando Dados Corrigidos

# %%
df_respondentes.loc[index_ausentes_estado_civil]

# %% [markdown]
# ## Modelo de Entidade e Relacionamento 
# ***
# 
# <font color="yellow">Conceito</font>
# 
# 
# Um modelo de entidade e relacionamento (ER) é uma representação visual e conceitual usada para descrever a estrutura lógica de uma base de dados. Ele serve como um plano detalhado que mapeia como os dados se relacionam entre si dentro do sistema, facilitando a organização e a gestão de informações.
# 
# Componentes do Modelo ER
# Entidades: Representam objetos ou coisas do mundo real sobre os quais se deseja armazenar informações. Uma entidade pode ser uma pessoa, lugar, evento ou qualquer outro elemento relevante para o sistema. Por exemplo, em um sistema de gerenciamento de pesquisa de preferências, entidades podem incluir "participante", "pesquisa" e "estado".
# 
# Atributos: São propriedades ou características das entidades. Cada entidade possui um conjunto de atributos que descrevem suas propriedades. Por exemplo, a entidade "participantes" pode ter atributos como "genero", "data_nascimento" e "nome".
# 
# Relacionamentos: Descrevem como as entidades interagem entre si. Um relacionamento indica a associação entre uma ou mais entidades. Por exemplo, o relacionamento "realizado por" pode conectar as entidades "participante" e "pesquisa", indicando quais autores realizam a pesquisa.
# 
# <center>
# 
# <img src="../img/MER_pesquisa_referencias.png" width="70%">
# </center>
# 

# %% [markdown]
# ### Conexão com o Banco de Dados
# ***

# %%
def conectar_banco_mysql():
    try:
        user = os.environ['user_db']        
        password = os.environ['password_db']
        host = os.environ['host']           
        conexao =  f'mysql://{user}:{password}@{host}'
        engine = create_engine(conexao)
        conn = engine.connect() 
        print('conexão realizada com sucesso!')
        return conn
    except Exception as e:
        print(f'Não foi possível conectar ao Banco de dados. Erro: {e}')

# %% [markdown]
# #### Validação da Conexão com o Servidor de Banco de Dados

# %%
conn = conectar_banco_mysql()

# %% [markdown]
# #### Criação do Banco de Dados
# ***

# %%
try:
    database = 'pesquisa_preferencias'
    query = f'create schema if not exists {database}'
    conn.execute(text(query))
    print('Banco de Dados Criado com sucesso')
    conn.commit()

except Exception as e:
    print(f'Não foi possível criar o banco de dados. Erro:{e}')

# %% [markdown]
# #### Executa o Script de Criação das Tabelas e Seus Relacionamentos

# %%
with open ('../script_DB/create_database_pesquisa_preferencias.sql' , 'r') as file:
    query = file.read()

try:
    conn.execute(text(query))
    conn.commit()
    print('Tabelas Criadas com sucesso!')
except Exception as e:
    print('Não foi possível criar as tabelas do Banco de Dados')
    

# %% [markdown]
# #### Feature Engineering
# ***

# %% [markdown]
# #### Criação da Variável de Idade

# %%
df_respondentes.info()

# %%
data_hoje = pd.to_datetime('today')
df_respondentes['data_nascimento'] = pd.to_datetime(df_respondentes['data_nascimento'])
df_respondentes['idade'] = ((data_hoje - df_respondentes['data_nascimento']).dt.days / 365.25).astype(int)

# %%
df_respondentes.head()

# %% [markdown]
# #### Correção Variáveis Peso e Colesterol

# %%
df_respondentes['peso'] = df_respondentes['peso'].round(2)
df_respondentes['colesterol'] = df_respondentes['colesterol'].round(2)

# %%
df_respondentes.head()

# %% [markdown]
# #### Integração de Dados
# ***

# %%
df_pesquisa.head()

# %%
df_respondentes.head()

# %%
df = pd.merge(left=df_pesquisa, 
              right=df_respondentes, 
              left_on='cod_pessoa', 
              right_on='cod_pessoa', 
              how='inner')

# %%
df.head()

# %%
df_estado.head()

# %%
df = pd.merge(left=df,
              right=df_estado, 
              left_on='id_estado', 
              right_on='cod_estado', 
              how= 'inner')

# %%
df.head()

# %% [markdown]
# #### Praparação dos Dados
# ***

# %%
df.columns

# %%
cols_pessoa = ['cod_pessoa', 'genero', 'data_nascimento','idade','peso','colesterol','renda_mensal']
df_pessoas = df[cols_pessoa].drop_duplicates()

# %%
cols_estado = ['cod_estado','sigla', 'estado', 'regiao', 'pais']
df_estado = df[cols_estado].drop_duplicates()

# %%
lista_animais = list(df['animal_estimacao'].unique())
lista_animais

# %%
lista_climas = list(df['clima'].unique())
lista_climas

# %%
lista_bebidas = list(df['bebida_favorita'].unique())
lista_bebidas

# %%
lista_hobbies = list(df['hobby'].unique())
lista_hobbies

# %%
lista_preferencias_musicais = list(df['preferencia_musical'].unique())
lista_preferencias_musicais

# %%
lista_atividades_fisicas = list(df['atividade_fisica'].unique())
lista_atividades_fisicas

# %%
lsita_educacao = list(df['educacao'].unique())
lsita_educacao

# %%
lista_estado_civil = list(df['estado_civil'].unique())
lista_estado_civil

# %%
tabela = 'tb_bebida'
print(tabela[3:])

# %% [markdown]
# #### Inserindo Registros no Banco de Dados
# ***

# %%
def insert_dados_banco(lista_dados, tabela):
    coluna_tabela = tabela[3:]

    for registro in lista_dados:
        try:
            query = f'''Insert into {tabela} ({coluna_tabela})
                        values ("{registro}")
            '''
            conn.execute(text(query))
            conn.commit()

            print(f'Registro inserido com sucesso: {registro}')
        except Exception as e:
            print(f'Não foi possível inserir o registro: {registro}. Erro: {e}')

# %% [markdown]
# #### Inserção de Dados

# %%
query = 'use pesquisa_preferencias'
conn.execute(text(query))
conn.commit()

# %%
tabela_banco = 'tb_preferencia_musical'
insert_dados_banco(lista_dados=lista_preferencias_musicais, tabela=tabela_banco)

# %%
tabela_banco = 'tb_animal_estimacao'
insert_dados_banco(lista_dados=lista_animais, tabela=tabela_banco)

# %%
tabela_banco = 'tb_clima'
insert_dados_banco(lista_dados=lista_climas, tabela=tabela_banco)

# %%
tabela_banco = 'tb_hobby'
insert_dados_banco(lista_dados=lista_hobbies, tabela=tabela_banco)

# %%
tabela_banco = 'tb_bebida'
insert_dados_banco(lista_dados=lista_bebidas, tabela=tabela_banco)

# %%
tabela_banco = 'tb_atividade_fisica'
insert_dados_banco(lista_dados=lista_atividades_fisicas, tabela=tabela_banco)

# %%
tabela_banco = 'tb_educacao'
insert_dados_banco(lista_dados=lsita_educacao, tabela=tabela_banco)

# %%
tabela_banco = 'tb_estado_civil'
insert_dados_banco(lista_dados=lista_estado_civil, tabela=tabela_banco)

# %%


# %% [markdown]
# #### Inserção de Dados Tabelas: Estado - Pessoa e Pesquisa
# ***

# %%
for registro in df_estado.itertuples():
    cod_estado = registro.cod_estado
    estado = registro.estado
    sigla_estado = registro.sigla
    regiao = registro.regiao
    pais = registro.pais

    try:
        query = f'''
                    insert into tb_estado (cod_estado, estado, sigla_estado, regiao, pais)
                    values({cod_estado}, "{estado}", "{sigla_estado}", "{regiao}", "{pais}")'''
        conn.execute(text(query))
        conn.commit()
        print(f'O registro inserido com sucesso. Estado: {estado}')
    except Exception as e:
        print(f'Não foi possível inserir o registro: {estado}. Erro: {e}')

# %% [markdown]
# #### Criação da Tabela Stage
# ***

# %% [markdown]
# A tabela "stage" em um banco de dados relacional é frequentemente utilizada como uma área temporária para armazenar dados que estão em processo de carregamento, transformação ou validação antes de serem movidos para as tabelas finais de destino. Essa abordagem é comum em processos de ETL (Extração, Transformação e Carga) onde dados brutos são extraídos de várias fontes, carregados na tabela de stage para processamento intermediário, e posteriormente transformados e carregados nas tabelas definitivas do banco de dados.

# %%
df.head()

# %%
try:
    df.to_sql('stg_pesquisa', 
              schema='pesquisa_preferencias', 
              index=False, 
              con=conn)
    print('Tabela Stage Criada com Sucesso!')
except Exception as e:
    print(f'Não foi possível criar a tabela de stage. Erro: {e}')

# %%
conn.commit()

# %% [markdown]
# #### Inserção de Dados na Tabela de Pessoa

# %%
query = '''

insert into tb_pessoa( cod_pessoa,
                       cod_estado_civil, 
                       cod_estado, 
                       cod_educacao,
                       genero,
                       data_nascimento, 
                       idade, 
                       peso,
                       colesterol,
                       renda_mensal
                     )
(
    select cod_pessoa, 
        eci.cod_estado_civil,
        est.cod_estado,
        edu.cod_educacao, 
        pesq.genero, 
        pesq.data_nascimento,
        pesq.idade, 
        pesq.peso,
        pesq.colesterol, 
        pesq.renda_mensal
    from stg_pesquisa as pesq
    inner join tb_estado as est on pesq.cod_estado = est.cod_estado
    inner join tb_estado_civil as eci on eci.estado_civil = pesq.estado_civil
    inner join tb_educacao as edu on edu.educacao = pesq.educacao
)
'''

try:
    conn.execute(text(query))
    conn.commit()
    print('Dados Inseridos com sucesso')
except Exception as e:
    print(f'Não foi possível inserir dados na tabela de Pessoa. Erro: {e}')

# %% [markdown]
# #### Inserção de Dados na Tabela de pesquisa

# %%
query = '''
insert into tb_pesquisa( 
                        data_pesquisa,
                        cod_pessoa,
                        cod_hobby,
                        cod_bebida,
                        cod_clima,
                        cod_animal_estimacao,
                        cod_atividade_fisica,
                        cod_preferencia_musical
)

(
    select pesq.data_coleta as data_pesquisa, 
        pesq.cod_pessoa,
        hob.cod_hobby,
        beb.cod_bebida, 
        cli.cod_clima,
        aes.cod_animal_estimacao,
        afi.cod_atividade_fisica,
        pmu.cod_preferencia_musical
    from stg_pesquisa as pesq
    inner join tb_hobby as hob on hob.hobby = pesq.hobby
    inner join tb_bebida as beb on beb.bebida = pesq.bebida_favorita
    inner join tb_clima as cli on cli.clima = pesq.clima
    inner join tb_animal_estimacao as aes on aes.animal_estimacao = pesq.animal_estimacao
    inner join tb_atividade_fisica as afi on afi.atividade_fisica = pesq.atividade_fisica
    inner join tb_preferencia_musical as pmu on pmu.preferencia_musical = pesq.preferencia_musical
)
'''

try:
    conn.execute(text(query))
    conn.commit()
    print('Dados Inseridos com sucesso')
except Exception as e:
    print(f'Não foi possível inserir dados na tabela de Pesquisa. Erro: {e}')

# %%
query = '''select pes.genero,  
       est.estado, 
       pes.idade, 
       pes.renda_mensal
from tb_pessoa as pes 
inner join tb_estado est on est.cod_estado = pes.cod_estado
where pes.genero = "Masculino"
and est.estado = "Minas Gerais"'''

pd.read_sql_query(query, con=conn)

# %%



