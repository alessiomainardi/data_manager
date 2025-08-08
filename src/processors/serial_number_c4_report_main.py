from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, Optional
import unicodedata

import pandas as pd


# =============================================================================
# Helpers gerais
# =============================================================================
def _is_na(x: Any) -> bool:
    try:
        return pd.isna(x)
    except Exception:
        return x is None

def _to_str(x: Any) -> str:
    return "" if _is_na(x) else str(x)

def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

def _norm_spaces(s: str) -> str:
    return " ".join(_to_str(s).split()).strip()

def _norm_cat(cat: Any) -> str:
    # categorias da tabela são minúsculas: bronze, prata, ouro, ouro duplo, diamante, black
    return _norm_spaces(_to_str(cat)).lower()

def _norm_desc(val: Any) -> str:
    s = _to_str(val).lower().strip()
    s = _strip_accents(s)  # "fácil" -> "facil"
    return f" {s} "  # bordas para facilitar match de ' pro '


# =============================================================================
# 1) TIPO_USUARIO
# =============================================================================
def compute_user_type(row) -> str:
    """
    USUARIO FINAL se RAZAO_SOCIAL_REVENDA estiver vazia/NaN; caso contrário USUARIO DE REVENDA.
    """
    v = row.get("RAZAO_SOCIAL_REVENDA")
    return "USUARIO FINAL" if pd.isna(v) or str(v).strip() == "" else "USUARIO DE REVENDA"


# =============================================================================
# 2) ANO_MES_VCTO (YYYY/MM a partir de DATA_VENCIMENTO_SERIAL)
# =============================================================================
def compute_ano_mes_vcto(row) -> Optional[str]:
    val = row.get("DATA_VENCIMENTO_SERIAL")
    if pd.isna(val) or str(val).strip() == "":
        return None
    ts = pd.to_datetime(val, errors="coerce")
    if pd.isna(ts):
        return None
    return f"{ts.year:04d}/{ts.month:02d}"


# =============================================================================
# 3) PRODUTO_CALCULADO (pela descrição)
# =============================================================================
def calcular_produto(desc: Any) -> str:
    """
    Implementa a lógica fornecida:
      essencial -> ZWeb Essencial
      premium   -> ZWeb Premium
      standard  -> ZWeb Standard
      mei cpf   -> Clipp MEI CPF
      mei       -> Clipp MEI
      360       -> Clipp360
      small commerce -> Small Commerce
      small go  -> Small Go
      facil/fácil/fàcil -> ClippFacil
      ' pro ' ou 'pro' ou 'renovação pro' -> ClippPRO
      else -> Produto não encontrado
    """
    d = _norm_desc(desc)

    if "essencial" in d:
        return "ZWeb Essencial"
    elif "premium" in d:
        return "ZWeb Premium"
    elif "standard" in d:
        return "ZWeb Standard"
    elif "mei cpf" in d:
        return "Clipp MEI CPF"
    elif " mei " in d or d.strip() == "mei":
        return "Clipp MEI"
    elif "360" in d:
        return "Clipp360"
    elif "small commerce" in d:
        return "Small Commerce"
    elif "small go" in d:
        return "Small Go"
    elif any(x in d for x in ["facil", "fácil", "fàcil"]):
        return "ClippFacil"
    elif " renovacao pro" in d or " renovação pro" in d or " pro " in d or d.strip() == "pro":
        return "ClippPRO"
    else:
        return "Produto não encontrado"

def compute_produto(row) -> str:
    # O input traz "Product description"; o output renomeia para DESCRICAO_DO_PRODUTO.
    desc = row.get("Product description")
    if desc is None:
        desc = row.get("DESCRICAO_DO_PRODUTO")
    return calcular_produto(desc)


# =============================================================================
# 4) BRINDE: 'Sim' se DESCRICAO_DO_PRODUTO contiver 'brinde' (case-insensitive)
# =============================================================================
def compute_brinde(row) -> str:
    desc = row.get("DESCRICAO_DO_PRODUTO")
    if desc is None:
        desc = row.get("Product description")
    if pd.isna(desc):
        return "Não"
    return "Sim" if "brinde" in str(desc).lower() else "Não"


# =============================================================================
# 5) MENSAL: 'Sim' se PERIODICIDADE_DO_PRODUTO == '1'
# =============================================================================
def compute_mensal(row) -> str:
    val = row.get("PERIODICIDADE_DO_PRODUTO")
    return "Sim" if str(val).strip() == "1" else "Não"


# =============================================================================
# 6) SITUACAO_SERIAL: ESTOQUE/ATIVO/VENCIDO
# =============================================================================
def compute_situacao_serial(row) -> str:
    today = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))

    ativ = row.get("DATA_ATIVACAO_SERIAL")
    if pd.isna(ativ):
        return "ESTOQUE"

    venc = row.get("DATA_VENCIMENTO_SERIAL")
    venc_dt = pd.to_datetime(venc, errors="coerce")
    if pd.isna(venc_dt):
        return "VENCIDO"
    return "ATIVO" if venc_dt > today else "VENCIDO"


# =============================================================================
# 7) DELIVERY (mantido para o seu layout)
# =============================================================================
def compute_delivery(row) -> str:
    return "REVENDA" if compute_user_type(row) == "USUARIO DE REVENDA" else "DIRETO"


# =============================================================================
# 8) PREÇOS embutidos e cálculo (mensal divide por 10)
# =============================================================================
precos_final_c4: Dict[str, float] = {
    'ClippPRO': 1669.00,
    'Clipp MEI': 679.00,
    'Clipp MEI CPF': 949.00,
    'Clipp360': 949.00,
    'ClippFacil': 679.00,
    'ZWeb Essencial': 599.80,
    'ZWeb Standard': 859.80,
    'ZWeb Premium': 1319.80,
    'Small Commerce': 0.0,
    'Small Go': 0.0,
}
precos_revenda_c4: Dict[str, Dict[str, float]] = {
    'ClippPRO':      {'bronze': 759.00, 'prata': 759.00, 'ouro': 659.00, 'ouro duplo': 639.00, 'diamante': 579.00, 'black': 569.00},
    'Clipp MEI':     {'bronze': 349.00, 'prata': 349.00, 'ouro': 349.00, 'ouro duplo': 349.00, 'diamante': 349.00, 'black': 349.00},
    'Clipp MEI CPF': {'bronze': 499.00, 'prata': 499.00, 'ouro': 499.00, 'ouro duplo': 499.00, 'diamante': 499.00, 'black': 499.00},
    'Clipp360':      {'bronze': 539.00, 'prata': 539.00, 'ouro': 539.00, 'ouro duplo': 539.00, 'diamante': 539.00, 'black': 539.00},
    'ClippFacil':    {'bronze': 409.00, 'prata': 409.00, 'ouro': 409.00, 'ouro duplo': 409.00, 'diamante': 409.00, 'black': 409.00},
    'ZWeb Essencial':{'bronze': 299.90, 'prata': 299.90, 'ouro': 299.90, 'ouro duplo': 299.90, 'diamante': 299.90, 'black': 299.90},
    'ZWeb Standard': {'bronze': 429.90, 'prata': 429.90, 'ouro': 429.90, 'ouro duplo': 429.90, 'diamante': 429.90, 'black': 429.90},
    'ZWeb Premium':  {'bronze': 659.90, 'prata': 659.90, 'ouro': 659.90, 'ouro duplo': 659.90, 'diamante': 659.90, 'black': 659.90},
    'Small Commerce':{'bronze': 0.0, 'prata': 0.0, 'ouro': 0.0, 'ouro duplo': 0.0, 'diamante': 0.0, 'black': 0.0},
    'Small Go':      {'bronze': 0.0, 'prata': 0.0, 'ouro': 0.0, 'ouro duplo': 0.0, 'diamante': 0.0, 'black': 0.0},
}
# Cópias para "small" (como você pediu)
precos_final_small = precos_final_c4.copy()
precos_revenda_small = {k: v.copy() for k, v in precos_revenda_c4.items()}

# Tabelas "mensais" (preço dividido por 10)
def _divide_dict_values(d: Dict[str, float], div: float) -> Dict[str, float]:
    return {k: (float(v) / div) for k, v in d.items()}

def _divide_nested_dict_values(d: Dict[str, Dict[str, float]], div: float) -> Dict[str, Dict[str, float]]:
    return {k: {kk: (float(vv) / div) for kk, vv in inner.items()} for k, inner in d.items()}

precos_final_c4_mensal   = _divide_dict_values(precos_final_c4, 10.0)
precos_revenda_c4_mensal = _divide_nested_dict_values(precos_revenda_c4, 10.0)

precos_final_small_mensal   = _divide_dict_values(precos_final_small, 10.0)
precos_revenda_small_mensal = _divide_nested_dict_values(precos_revenda_small, 10.0)


def _format_price_brl(value: float) -> str:
    # 2 casas, vírgula decimal, sem milhar
    try:
        return f"{float(value):.2f}".replace(".", ",")
    except Exception:
        return "0,00"


def compute_preco(row) -> str:
    """
    Regra:
      - Produto calculado via compute_produto.
      - MENSAL = 'Sim' => usar tabelas *_mensal (preço/10).
      - TIPO_USUARIO:
          * 'USUARIO FINAL'      => usar precos_final_*
          * 'USUARIO DE REVENDA' => usar precos_revenda_* pela CATEGORIA_REVENDA
      - Caso produto não exista na tabela => 0,00
      - Caso categoria não exista => tenta match por lowercase; se não achar => 0,00
    """
    produto   = compute_produto(row)
    mensal    = compute_mensal(row) == "Sim"
    user_type = compute_user_type(row)
    categoria = _norm_cat(row.get("CATEGORIA_REVENDA"))

    # Escolha da família de tabelas (c4 vs small)
    # -> Pelo nome do produto: se começar com 'Small ' usa small, senão c4
    is_small = produto.startswith("Small ")
    if user_type == "USUARIO FINAL":
        if mensal:
            tabela = precos_final_small_mensal if is_small else precos_final_c4_mensal
        else:
            tabela = precos_final_small if is_small else precos_final_c4
        preco = tabela.get(produto, 0.0)
        return _format_price_brl(preco)

    # USUARIO DE REVENDA
    if mensal:
        tabela = precos_revenda_small_mensal if is_small else precos_revenda_c4_mensal
    else:
        tabela = precos_revenda_small if is_small else precos_revenda_c4

    cat_table = tabela.get(produto, {})
    if not isinstance(cat_table, dict):
        # caso algum produto seja número direto (não esperado aqui)
        return _format_price_brl(cat_table)

    # tenta categoria exata (lower)
    if categoria in cat_table:
        return _format_price_brl(cat_table[categoria])

    # fallback: tenta por igualdade case-insensitive “solta”
    for k, v in cat_table.items():
        if _norm_cat(k) == categoria:
            return _format_price_brl(v)

    return _format_price_brl(0.0)
