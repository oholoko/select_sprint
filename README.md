# Repositorio contendo o source do projeto 'nyc_data'

Para conseguir executar a questão bonus eu decidi provisionar todo o projeto
direto na cloud utilizando AWS numa conta publica, como ja tive experiencia
com o sistema num serviço anterior.

## Funcionamento 
O codigo foi feito utilizando python na versão 3.7 os requerimentos se encontram no arquivo requirements.txt 
dentro da pasta data_import.
Pode os instalar facilmente utilizando 
```
pip install requirements.txt
```

## Como replicar meus estudos:

A biblioteca ETL faz a coleta de todos os dados e os salva no formato JSON, o fato dos dados serem bem estruturados ajudou bastante nesse processo. Não havia motivo para esse tipo de dado ser salvo num formato no-sql ao invez de um sql ja que todos possuem a mesma estrutura. 
Eu salvei o processo em multiplos arquivos csv pois ia tentar usar uma instancia da AWS para resolver o problema mas a instancia em si era pior que um notebook bem simples então terminei por não o fazer. Não fiz o uso de um banco de dados pois o carregamento que estava fazendo na AWS tambem se provou bem lento comparado com a leitura em disco. 
A biblioteca analisys toma os resultados gerados pela ETL e os analisa gerando o que foi pedido nos exercicios. 
