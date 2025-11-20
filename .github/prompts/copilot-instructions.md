# Instruções gerais

- O projeto se trata de parser de arquivos de dados financeiros que utiliza ocr e llm para interpretar os dados.
- O projeto segue as melhores práticas de desenvolvimento de software.
- O código deve ser limpo, legível e bem documentado.
- Utilize tipagem estática com mypy para garantir a segurança de tipos.
- Inclua docstrings detalhadas para todas as funções e classes.
- Exiba exemplos de uso para componentes complexos.
- Para datas, sempre use o fuso horário UTC.
- Evite duplicação de código, utilize funções auxiliares.
- Quando pedir para escrever um texto do commit, analise o staged area do git para entender o contexto, e escreva uma mensagem clara e concisa. use conventional changelog.

## Tech stack and conventions

- Linguagens: Python 3.13+
- Testes: Pytest com cobertura mínima de 80%. Mocks com MagicMock.

## Coding guidelines

- Use PascalCase para nomear tipos e constantes.
- Use snake case para nomear funcoes
- Prefira funções puras e sem efeitos colaterais.
- O estilo de programação é funcional, evitando mutações de estado, e criação de classes.
- Prefira funções puras e programação orientada a composição.
- Sempre trate erros de I/O com logging estruturado.
- Respeite as regras definidas do ruff.
- Sempre que possive substitua prints por logging.
- list comprehensions são preferidas em relação a loops for tradicionais.

## Security and reliability

- Nunca registre dados sensíveis (senhas, tokens, dados de cartão).
