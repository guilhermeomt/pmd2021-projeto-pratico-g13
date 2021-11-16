# 2021/1 Processamento Massivo de Dados - Projeto Prático

Este projeto consiste em uma aplicação ETL (Extract, Transform, Load) utilizando [Apache Spark](https://spark.apache.org/) e um SGBD NoSQL, o [MongoDB](https://www.mongodb.com/). O objetivo é realizar um processamento de dados de um arquivo CSV para um banco de dados NoSQL, promovendo transformações nos dados originais. A aplicação foi desenvolvida como forma de avaliação do projeto prático da disciplina de Processamento Massivo de Dados.

## Fonte de Dados
Para a execução do projeto, foi escolhida uma fonte de dados com informações relacionadas a classificação (ranking) de universidades ao redor do mundo. O dataset, que tem por nome [World University Rankings](https://www.kaggle.com/mylesoneill/world-university-rankings), foi obtido na plataforma [Kaggle](https://www.kaggle.com). 


## Pipeline ETL

O processo ETL é composto por três etapas:
  - **Extract**: Extração dos dados do arquivo CSV. A extração é feita utilizando o Spark DataFrame API. O arquivo `timesData.csv` é carregado e convertido para um DataFrame.  
  - **Transform**: Transformação dos dados extraídos.
      - **Transformação 1**: Cálculo da média das notas de cada universidade.
      - **Transformação 2**: Categoriação das universidades, de acordo com a média calculada.
      - **Transformação 3**: Divisão da coluna de gênero em duas colunas, uma para o gênero masculino e outra para o gênero feminino.
      - **Transformação 4**: Criação de uma coluna com array de pontuações do ranking de cada universidade.
      - **Transformação Adicionais**: Formatação das colunas de número de estudantes e taxa de estudantes estrangeiros
  - **Load**: Carregamento dos dados transformados no MongoDB. A carga dos dados é feita utilizando o conector do Apache Spark para o MongoDB. Os dados são carregados no banco de dados `PMD2021` na coleção *universities*. 

## Consultas

Após a realização de todas as etapas do pipeline ETL, se torna possível executar as seguintes consultas:
<ol>
  <li>País que possui a maior quantidade de universidades com conceito A.</li>
  <li>Obter as 10 universidades com maior quantidade de alunos estrangeiros.</li>
  <li>Total de universidades por país.</li>
</ol>

## Como executar

Inicialmente, é necessário configurar ambiente de desenvolvimento local, que será utilizado para executar o projeto. Para isto, é necessário instalar o [Apache Spark](https://spark.apache.org/downloads.html), o [MongoDB](https://www.mongodb.com/try/download/community), e o [Python](https://www.python.org/downloads/). Após a instalação, siga os passos a seguir: 


Inicialize um servidor de banco de dados MongoDB localmente, como o exemplo abaixo:
```bash
mongod --dbpath /data/db
```

Em seguida, é possível executar a aplicação utilizando o comando abaixo:
```bash
python3 etl.py
```

Alternativamente, pode-se executar as células do notebook [etl.ipynb](etl.ipynb) para executar a aplicação.

Uma vez que os dados transformados sejam carregados no banco de dados, é possível executar as consultas. Para isso, basta acessar a pasta `consultas` e executar o comando abaixo:
```bash
mongo PMD2021 < consultaX.js # onde X é o número da consulta
```

## Colaboradores

<table>
  <tr>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/44469392?s=100&v=4" width="100px;" alt=""/><br /><sub><b>Guilherme Tavares</b></sub><br /></td>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/58371716?s=60&v=4" width="100px;" alt=""/><br /><sub><b>Javier Ernesto</b></sub><br /></td>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/86381712?s=60&v=4" width="100px;" alt=""/><br /><sub><b>Maria Luiza Stellet</b></sub><br /></td>
  </tr>
</table>