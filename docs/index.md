# Correção Monetária de Débitos Judiciais

O [Tribunal de Justiça do Estado de São Paulo](https://www.tjsp.jus.br/) disponibiliza mensalmente as taxas para calcular a correção monetária de multas e débitos judiciais. As taxas atualizadas são divulgadas por meio de um arquivo em formato _.pdf_, intitulado [TabelaDebitosJudiciais.pdf](https://api.tjsp.jus.br/Handlers/Handler/FileFetch.ashx?codigo=177683).

![Photo by [**Sasun Bughdaryan**](https://unsplash.com/@sasun1990?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText) on [Unsplash](https://unsplash.com)](./assets/imgs/sasun-bughdaryan.jpg)

<br>

No início do projeto, em novembro de 2021, a tabela estava disponível na _url_:

- [https://www.tjsp.jus.br/Download/Tabelas/TabelaDebitosJudiciais.pdf](https://www.tjsp.jus.br/Download/Tabelas/TabelaDebitosJudiciais.pdf)

<br>

Em janeiro de 2025, refatorando o projeto, ajustei para a mudança para a _url_ [Tabelas Práticas para Atualização Monetária (Publicado DJE)](https://www.tjsp.jus.br/PrimeiraInstancia/CalculosJudiciais/Comunicado?codigoComunicado=2524&pagina=1)

Especificamente, na [Tabela Prática de Atualização Monetária dos Débitos Judiciais - Lei nº 14.905/24](https://api.tjsp.jus.br/Handlers/Handler/FileFetch.ashx?codigo=178456)

<br>

O repositório [tjsp_correcao_monetaria](https://github.com/michelmetran/tjsp_correcao_monetaria) objetivou criar uma função para converter esse arquivo _.pdf_ em formato tabular (_.csv_) e disponibilizar isso de maneira facilitada, por meio de um servidor, com atualização periódica!
