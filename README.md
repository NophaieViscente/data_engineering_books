# Practice Data Engineering

Este projeto visa efetuar a prática de aquisição de dados via Scrapping. 

Os dados foram retirados do site http://books.toscrape.com/, este é um site especifico para uso de webscrapping.

### Execução da aquisição

Para a execução é necessária a instalação das libs. A instalação se dá através do arquivo requirements.txt
* pip install -r requirements.txt

Para a execução é necessária a prévia instalação do docker em sua máquina. Após o docker instalado basta rodar os comandos:
Para criação da instância do airflow em um container. 
* docker-compose up airflow-init 
Para a execução da instância.
* docker-compose up