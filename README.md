## Introdução

Este repositório é voltado ao armazenamento de códigos e informações relativas à configuração de um datalake básico para resolução de um desafio voltado à engenharia de dados, fazendo uso das ferramentas databricks e serviço em nuvem Microsoft Azure.

#### Contexto e Objetivos

Uma empresa de análise esportiva FutData Insights, especialista em análise de dados para times profissionais de futebol, precisa que um conjunto de dados em formatos variados seja processado para análise. O objetivo é realizar as transformações necessárias em dados voltados a futebol e carregá-los nas camadas adequadas e em um database.

Observação: **Cada anotação, descrição, código ou comentário foi escrito por mim, autor do repositório, com base no meu entendimento sobre os temas abordados. Modelos de IA generativos podem ter sido usados para auxílio nos estudos, respondendo eventuais dúvidas.**

Observação II: Infelizmente, o acesso à plataforma databricks via azure foi perdido durante a confecção deste repositório. Portanto, nem todos os notebooks não puderam ser executados em ambiente databricks, mas servem como exercício para trabalhar com pyspark.

## Databricks

O databricks é uma plataforma baseada em nuvem que unifica ferramentas para trabalhar com grandes volumes de dados por meio das tecnologias e arquitetura do Apache Spark.

#### Serviço em Nuvem

O serviço em nuvem consiste em alocar parte da capacidade computacional de servidores remotos objetivando processar e armazenar dados, delegando a manutenção da infraestrutura a terceiros. Nesse contexto, as tarefas são divididas em partes e distribuídas em um conjunto de computadores - também conhecido como cluster - para processá-las simultaneamente, provendo redução considerável no tempo necessário para que uma tarefa seja concluída.

#### Assinaturas Databricks

Existem dois planos que trabalham em conjunto no databricks:

- **Plano de controle (Control Plane)**: Oferece a interface para comunicação entre os recursos do databricks e o usuário final e armazena metadados sobre configurações.
- **Plano de dados (Data Plane)**: Fornece ambiente para execução de máquinas virtuais e tráfego de dados exclusivos à assinatura do cliente.

#### Apache Spark: Tecnologias e Arquitetura 

O Apache Spark é um framework voltado ao trabalho com dados, envolvendo bibliotecas em linguagens de programação e uma arquitetura de processamento distribuído, dispondo das tecnologias apresentadas nas imagens 1 e 2.


<div align="center"><h4>Imagem 1 - Tecnologias do Apache Spark</h4></div>
<div align="center"><img title="Imagem 1" src="https://github.com/guilhermyandrade/Datalake-Azure/blob/main/Imagens/Estrutura_spark.png" ></div>

- **Bibliotecas de alto nível**: O Spark dispõe de bibliotecas para trabalhar com diferentes segmentos de dados. Na imagem 1, temos como exemplo bibliotecas para SQL, Streamming (dados em tempo real), ML (machine learning) e Graph (para trabalhar com gráficos).
- **Spark SQL Engine:** APIs para lidar com dados estruturados.
- **Spark Core Engine:** O RDD (Resilient Distributed Dataset) é uma abstração de dados de uma aplicação, definindo as intruções para que possa ser dividida em *Jobs*, *Stages* e *Tasks*, com APIs para as linguagens Python, Scala, R, Java, etc.
- **Gerenciador de Recursos:** Um gerenciador de recursos é o que, efetivamente, realiza a distribuição das Tasks de uma aplicação entre diferentes computadores em um cluster.


<div align="center"><h4>Imagem 2 - Arquitetura de Processamento Distribuído</h4></div>
<div align="center"><img title="Imagem 1" src="https://github.com/guilhermyandrade/Datalake-Azure/blob/main/Imagens/Processamento_distribuido.png" ></div>

- **Driver**: Máquina que fica responsável pelo tráfego de informações entre workers e o usuário final, dividindo, distribuindo e juntando as informações.
- **Worker**: O worker é a máquina que realiza as tarefas.
- **Job**: Um job é uma unidade de trabalho para ser realizada.
- **Stage**: Stages são um conjunto de operações que podem ser realizados sem a necessidade de tráfego de dados.
- **Task**: As tasks são as menores divisões de um job, que efetivamente serão distribuídas entre computadores de um cluster.

#### Hive Metastore

o Hive Metastore é um software da Apache usado pelo Databricks para armazenar metadados como esquemas de tabelas, localização física dos arquivos, etc. e trabalhar com ambientes de processamento distribuído utilizando SQL. Dessa forma, é possível criar bancos de dados, tabelas, views, etc.

##### Tabelas Externas e Gerenciadas

Ao lidar com SQL, existem duas formas de armazenar os dados que variam de acordo com o projeto.

- Tabelas gerenciadas: são tabelas criadas, armazenadas e gerenciadas pelo hive/dbfs (databricks file system)
- Tabelas externas: são tabelas que podem ser registradas e gerenciadas pelo hive, mas localizadas fisicamente nas contas de armazenamento da azure

#### Delta Lake

Um problema recorrente em ambientes como data lakes é a falta de ferramentas que auxiliem na segurança da manipulação dos dados por conta da grande variedade e volume de arquivos. Para contornar o problema, a Databricks desenvolveu uma tecnologia open-source baseando-se nas propriedades ACID, originárias de bancos de dados transacionais.

##### O que é uma transação?
Uma transação consiste num conjunto de operações que devem ser realizadas dentro de um banco de dados, funcionando de acordo com as propriedades ACID, que garantem a segurança nas modificações de conjuntos de dados.

As propriedades ACID podem ser explicadas da seguinte maneira:

- **(A) Atomicidade**: Cada transação é composta por um conjunto de operações que não pode ser dividido, fazendo com que a conclusão da transação dependa do sucesso de cada uma de suas operações.
- **(C) Consistência**: A consistência se refere a manter uma versão válida do conjunto de dados antes e depois de uma transação, bem como validando possíveis problemas de integridade - a integridade consiste num conjunto de metadados de uma entidade que definem certas regras para que os dados sejam armazenados.
- **(I) Isolamento**: Cada transação deve ser única e isolada, o que significa que elas podem ser executadas simultâneamente e não vão interferir umas com as outras, previnindo que os dados sejam corrompidos.
- **(D) Durabilidade**: Uma transação deve ser durável, isto é, se uma transação está sendo executada, mas for interrompida devido a falhas como problemas de hardware ou falta de energia durante o processo de execução, por exemplo, ela ainda será aplicada após o retorno do funcionamento adequado do sistema. Isso é possível graças a metadados armazenados em logs relacionados às operações realizadas, dados afetados e outras informações relevantes sobre a execução da transação.

##### Aplicação Prática

Na prática, os arquivos parquet são o que efetivamente armazenam os dados e os arquivos log são gerados pelo Delta Engine para aplicação das propriedades ACID. O Delta Engine, por sua vez, é manipulado pelo Spark, que possui API's para diferentes linguagens - python, SQL, etc.

## O que é um Datalake?

Um datalake é uma forma de armazenamento de dados que está diretamente relacionada ao Big Data.

#### Big Data

O big data é tido como um fenômeno derivado da conectividade provida pela internet, onde milhões de usuários geram cerca de 2.5 quintilhões de bytes diariamente em todos os setores, sendo comumente caracterizado por três V's: 

- **Volume**: Quantidade de dados gerada
- **Velocidade**: Velocidade de geração de dados
- **Variedade**: Diferentes formatos de dados (textos, imagens, pdfs, vídeos, áudios, etc.)

O valor dos dados do big data podem ser extraídos e usados para impulsionar negócios, mas os métodos convencionais de armazenamento de dados exigem o processo de transformação antes do carregamento, o que se mostra inadequado nesse cenário já que a velocidade de geração de dados é muito alta e a transformação exige tempo. Sendo assim, originou-se o formato de armazenamento datalake, onde os dados são extraídos, armazenados no formato bruto e processados quando necessário.

#### Organização dos Dados em Camadas

Os dados de um datalake normalmente são divididos em camadas de processamento:

- **Raw data**: Camada de dados brutos, sem nenhum tipo de transformação.
- **Bronze**: Camada de dados ainda brutos, minimamente filtrados e sem transformação significativa.
- **Silver**: Camada de dados processados, mas não mesclados, provenientes da camada bronze. Geralmente usada por cientistas de dados para obter dados para treinamento de modelos de aprendizado de máquina.
- **Gold**: Camada onde os dados processados podem ser agregados/mesclados. Usada para conexão de ferramentas de visualização de dados, como Power BI, Tableau, etc.

##### Aplicação na Azure
As camadas são criadas em uma conta de armazenamento na azure conectada ao databricks por meio de registros de aplicativo. Na prática, são diretórios onde os dados são carregados e tratados.