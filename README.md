# Desafio de Data Engineer - EMD

Repositório de instrução para o desafio técnico para vaga de Pessoa Engenheira de Dados no Escritório de Dados do Rio de Janeiro

## Descrição do desafio

Neste desafio você deverá capturar, estruturar, armazenar e transformar dados de Terceirizados de Órgãos Federais, disponíveis no site [Dados Abertos - Terceirizados de Órgãos Federais](https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados).

Para o desafio, será necessário construir uma pipeline que realiza a extração, processamento e transformação dos dados. Salve os dados de cada mes em um arquivo CSV (estruture os dados da maneira que achar mais conveniente, você tem liberdade para criar novas colunas ou particionar os dados), então carregue os dados para uma tabela no Postgres. Por fim, crie uma tabela derivada usando o DBT. A tabela derivada deverá seguir a padronização especificada no [manual de estilo do Escritorio de Dados](https://docs.dados.rio/guia-desenvolvedores/manual-estilo/#nome-e-ordem-das-colunas). A solução devera contemplar o surgimento de novos dados a cada 4 meses.


## O que iremos avaliar

- **Completude**: A solução proposta atende a todos os requisitos do desafio?
- **Simplicidade**: A solução proposta é simples e direta? É fácil de entender e trabalhar?
- **Organização**: A solução proposta é organizada e bem documentada? É fácil de navegar e encontrar o que se procura?
- **Criatividade**: A solução proposta é criativa? Apresenta uma abordagem inovadora para o problema proposto?
- **Boas práticas**: A solução proposta segue boas práticas de Python, Git, Docker, etc.?

## Atenção

- A solução desse desafio deve ser publicada em um fork deste repositório no GitHub.
- O link do repositório deve ser enviado até às 23:59, horário de Brasília, do dia 26 de julho de 2024, para o e-mail utilizado para contato com o assunto "Desafio Data Engineer - EMD".
- Você deve ser capaz de apresentar sua solução, explicando como a idealizou, caso seja aprovado(a) para a próxima etapa.

## Links de referência / utilidades

- Documentação [Prefect](https://docs-v1.prefect.io/)
- Documentação [DBT](https://docs.getdbt.com/docs/introduction)
- Instalar e configurar o
   [Prefect Server](https://docs.prefect.io/orchestration/getting-started/install.html)
   locamente com um [Docker Agent](https://docs.prefect.io/orchestration/agents/docker.html)
- [Dados Abertos - Terceirizados de Órgãos Federais](https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados)
- Repositório pipelines do [Escritorio de Dados](https://github.com/prefeitura-rio/pipelines)
- Repositório de modelos DBT do [Escritorio de Dados](https://github.com/prefeitura-rio/queries-datario)
- [Manual de estilo do Escritório de Dados](https://docs.dados.rio/guia-desenvolvedores/manual-estilo/#nome-e-ordem-das-colunas)
  
## Dúvidas?

Fale conosco pelo e-mail que foi utilizado para o envio desse desafio.
