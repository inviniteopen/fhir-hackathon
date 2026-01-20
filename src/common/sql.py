"""Small SQL identifier helpers shared across layers."""


def quote_ident(ident: str) -> str:
    return f'"{ident.replace(chr(34), chr(34) * 2)}"'


def qualified_table(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"
