# Instruções gerais

- Use PascalCase para nomear tipos e constantes.
- Use snake case para nomear funcoes
- Prefira funções puras e sem efeitos colaterais.
- O estilo de programação é funcional, evitando mutações de estado, e criação de classes.
- Exiba exemplos de uso para componentes complexos.
- Para datas, sempre use o fuso horário UTC.
- Evite duplicação de código, utilize funções auxiliares.
- Quando pedir para escrever um texto do commit, analise o staged area do git para entender o contexto, e escreva uma mensagem clara e concisa. use conventional changelog.

## Tech stack and conventions

- Linguagens: Python 3.13+
- Testes: Pytest com cobertura mínima de 80%. Mocks com MagicMock.

## Coding guidelines

- Use TypeScript estrito com tipos explícitos em funções públicas.
- Nomeie funções e variáveis em camelCase; classes em PascalCase.
- Prefira funções puras e programação orientada a composição.
- Sempre trate erros de I/O com logging estruturado.
- Respeite as regras definidas do ruff.

## Security and reliability

- Nunca registre dados sensíveis (senhas, tokens, dados de cartão).
