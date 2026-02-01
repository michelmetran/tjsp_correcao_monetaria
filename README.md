# Correção Monetária de Débitos Judiciais

O [Tribunal de Justiça do Estado de São Paulo](https://www.tjsp.jus.br/) disponibiliza mensalmente as taxas para
calcular a correção monetária de multas e débitos judiciais. As taxas atualizadas são divulgadas por meio de um arquivo
em formato _.pdf_,
intitulado [TabelaDebitosJudiciais.pdf](https://www.tjsp.jus.br/Download/Tabelas/TabelaDebitosJudiciais.pdf).

O repositório [gaemapiracicaba/correcao_monetaria](https://github.com/gaemapiracicaba/sp_tjsp_correcao_monetaria)
objetivou criar uma função para converter esse arquivo _.pdf_ em formato tabular (_.csv_) e disponibilizar isso de
maneira facilitada, por meio de um servidor, com atualização periódica!

<br>

---

## Como Usar?

O arquivo _.csv_ disponível no servidor é atualizado todas as terças-feiras e quintas, as 4h00 e fica disponível no
endereço a seguir:

- <a href="https://gaemapiracicaba.github.io/assets/correcao_monetaria/data/tabela_debitos_judiciais.csv" target="_blank">https://gaemapiracicaba.github.io/assets/correcao_monetaria/data/tabela_debitos_judiciais.csv</a>

<br>

---

### _Google Spreadsheets_

Uma vez que o arquivo _.csv_ está disponível em um servidor, é possível utilizar a função **_=IMPORTDATA()_** do _Google
Spreadsheets_ para acessa-lo diretamente na tabela, possibilitando cálculos etc.

![Google Spreadsheets](https://i.imgur.com/oFdGGbA.png)

<br>

Visando auxiliar essa etapa, **já foi criada uma tabela com a função**, bastando criar uma cópia da tabela para sua
conta _Google_.

<a href="https://docs.google.com/spreadsheets/d/1xOH1QN8qsZ3-_u6p1dbhIZ2N4IvSBbMJucM1BhXf8Sw/edit?usp=sharing" class="btn btn--primary">_Google Spreadsheets_</a>

<br>

---

### _Microsoft Excel_

No _Microsoft Excel_ é possível também manter o arquivo atualizado em uma aba, por meio dos passos abaixo:

![](./docs/imgs/excel.gif)

<br>

---

### Outros Formatos

É possível também acessar a tabela em formatos _.csv_ e _.pdf_ nos botões abaixo:

<a href="https://gaemapiracicaba.github.io/assets/correcao_monetaria/data/tabela_debitos_judiciais.csv" class="btn btn--primary">
Download *csv*</a>  
<a href="https://gaemapiracicaba.github.io/assets/correcao_monetaria/data/tabela_debitos_judiciais.pdf" class="btn btn--primary" target="_blank">
Download *pdf*</a>

<br>

---

### Python

```python
# Instala
!pip3 install tjsp --upgrade

# Pega Tabela (Local)
df_tjsp = tjsp.get_local_table()

# Pega Taxa TJSP para um dado dia
tjsp.get_tjsp_from_date(date='2018-09-15', update_table=False)
```

<br>

Para testar fiz um [Google Colab](https://colab.research.google.com/drive/1IiHtNCmdtiq18npCNX4VBy__P2LO1NvZ?usp=sharing)
