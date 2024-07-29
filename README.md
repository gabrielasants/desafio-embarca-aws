# Análise de Ocorrências de Trânsito

Este projeto realiza a análise de dados de ocorrências de trânsito, fornecendo insights sobre diferentes tipos de acidentes, seus impactos e características.

## Índice

- [Introdução](#introdução)
- [Arquitetura do Projeto](#arquitetura-do-projeto)
- [Instalação](#instalação)
- [Uso](#uso)
- [Funcionalidades](#funcionalidades)
- [Contribuição](#contribuição)
- [Licença](#licença)
- [Contato](#contato)

## Introdução

Este projeto foi desenvolvido para analisar dados de ocorrências de trânsito a partir de um conjunto de dados fornecido em formato CSV. Ele visa fornecer insights sobre o número de acidentes, tipos de veículos envolvidos, severidade das lesões e muito mais.

## Arquitetura do Projeto

O projeto está dividido em duas funções Lambda principais, que realizam as seguintes tarefas:

- **`downloadCSV`**: Esta função baixa o arquivo CSV de uma fonte externa e o armazena em um bucket S3.
- **`processCSV`**: Esta função lê o arquivo CSV do bucket S3, processa os dados e os armazena em um banco de dados Amazon RDS utilizando PostgreSQL.

### Estrutura de Dados

O arquivo CSV contém as seguintes colunas:

- `data`: Data da ocorrência
- `horario`: Horário da ocorrência
- `n_da_ocorrencia`: Número da ocorrência
- `tipo_de_ocorrencia`: Tipo de ocorrência
- `km`: Quilometragem
- `trecho`: Trecho da rodovia
- `sentido`: Sentido da via
- `tipo_de_acidente`: Tipo de acidente
- `automovel`: Contagem de automóveis envolvidos
- `bicicleta`: Contagem de bicicletas envolvidas
- `caminhao`: Contagem de caminhões envolvidos
- `moto`: Contagem de motos envolvidas
- `onibus`: Contagem de ônibus envolvidos
- `outros`: Contagem de outros veículos
- `tracao_animal`: Contagem de veículos de tração animal
- `transporte_de_cargas_especiais`: Contagem de transporte de cargas especiais
- `trator_maquinas`: Contagem de tratores e máquinas
- `utilitarios`: Contagem de veículos utilitários
- `ilesos`: Número de pessoas ilesas
- `levemente_feridos`: Número de pessoas levemente feridas
- `moderadamente_feridos`: Número de pessoas moderadamente feridas
- `gravemente_feridos`: Número de pessoas gravemente feridas
- `mortos`: Número de pessoas mortas

## Instalação

Para rodar o projeto localmente, siga as instruções abaixo:

### Pré-requisitos

- Python 3.9
- AWS CLI configurado
- Conta na AWS com permissões para Lambda, S3 e RDS
- Instalação de requirements.txt

### Passos de Instalação

1. Clone o repositório para o seu ambiente local:

   ```bash
   git clone https://github.com/seu-usuario/nome-do-repositorio.git
   cd nome-do-repositorio
