import sys
import re
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timedelta
from calendar import monthrange
from typing import Optional, Dict, Tuple, List

import pandas as pd

from PySide6.QtGui import QDoubleValidator
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QGridLayout,
    QGroupBox, QLabel, QLineEdit, QPushButton,
    QFileDialog, QMessageBox, QSpinBox, QComboBox,
    QRadioButton, QButtonGroup, QTextEdit
)


# ==========================================================
# Helpers: parse dinero / fechas
# ==========================================================
def last_day_of_month(year: int, month: int) -> pd.Timestamp:
    return pd.Timestamp(datetime(year, month, monthrange(year, month)[1])).normalize()


def parse_date_any(x) -> pd.Timestamp:
    if pd.isna(x):
        return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.Timestamp(x).normalize()
    s = str(x).strip()
    if not s:
        return pd.NaT

    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return pd.NaT
    return pd.Timestamp(dt).normalize()


def to_float_money(x) -> float:
    """
    Convierte strings tipo:
    'COP -$ 9.651.966,00'  o  '$ 510.000.000,00'  o  '-17'
    """
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s == "":
        return 0.0

    s = s.replace("COP", "").replace("$", "").replace("\xa0", " ").strip()
    neg = "-" in s
    s2 = re.sub(r"[^0-9\.,]", "", s)

    # Colombia: . miles , decimales
    if "," in s2 and "." in s2:
        s2 = s2.replace(".", "").replace(",", ".")
    else:
        if "," in s2 and "." not in s2:
            s2 = s2.replace(",", ".")

    try:
        val = float(s2) if s2 else 0.0
    except:
        val = 0.0
    if neg:
        val = -abs(val)
    return float(val)


def norm_upper(s) -> str:
    return str(s).upper().strip() if s is not None else ""


# ==========================================================
# Modelos de configuración
# ==========================================================
@dataclass
class BankFiles:
    bancolombia_csv: Optional[str] = None
    fiducia_csv: Optional[str] = None
    fiducia_rendimientos: Optional[float] = None

    bogota_mov_xls: Optional[str] = None
    bogota_inf_csv: Optional[str] = None
    bogota_inf_year: Optional[int] = None  # si informe trae mm/dd sin año

    davivienda_xls: Optional[str] = None

    agrario_mov_xls: Optional[str] = None
    agrario_inf_xls: Optional[str] = None

    bbva_xls: Optional[str] = None


@dataclass
class AccountingFiles:
    balance_prueba_xlsx: Optional[str] = None
    reporte_comprobantes_xlsx: Optional[str] = None
    libro_auxiliar_xlsx: Optional[str] = None


@dataclass
class OtherFiles:
    aplicativo_xlsx: Optional[str] = None
    criterios_bancarios_xlsx: Optional[str] = None
    reglas_xlsx: Optional[str] = None


@dataclass
class RunConfig:
    mes: int
    anio: int
    tol_nomina_days: int = 5
    tol_general_days: int = 3
    tol_bbva_sum_days: int = 5

    bancos: BankFiles = field(default_factory=BankFiles)
    contables: AccountingFiles = field(default_factory=AccountingFiles)
    otros: OtherFiles = field(default_factory=OtherFiles)
    salida_xlsx: str = "Cruce_Flujo_Caja.xlsx"


# ==========================================================
# Mapeo cuentas -> banco (Auxiliar)
# ==========================================================
CUENTA_A_BANCO = {
    "11100504": "Bancolombia",
    "11100505": "Davivienda",
    "11100507": "Agrario",
    "11200505": "BBVA",
    "124505": "Fiducia",
    "11100503": "Bogotá",
}


# ==========================================================
# Reglas embebidas (sin archivo Reglas.xlsx)
# ==========================================================
def apply_embedded_rules_to_bancos(bancos: pd.DataFrame) -> pd.DataFrame:
    b = bancos.copy()

    b["_banco_norm"] = b["Banco"].astype(str).str.upper().str.strip()
    b["_detalle_norm"] = b["Detalle"].fillna("").astype(str).str.upper().str.strip()

    # Asegurar columnas
    for col in ["Tipo", "Cuenta", "Concepto"]:
        if col not in b.columns:
            b[col] = ""
        b[col] = b[col].fillna("").astype(str)

    # ----------------------------------------------------------
    # 0) Cuenta por banco (solo si está vacía)
    # ----------------------------------------------------------
    empty_cta = b["Cuenta"].astype(str).str.strip().eq("")
    b.loc[empty_cta, "Cuenta"] = b.loc[empty_cta, "_banco_norm"].map(CUENTA_A_BANCO).fillna("")

    # ----------------------------------------------------------
    # 1) REGLAS PRIORITARIAS: "INICIA CON" (sobrescriben)
    # ----------------------------------------------------------
    STARTS_RULES = [
        # AGRARIO
        ("AGRARIO", "TRASLADO INTERBANCARIO", "Egreso", "", "Proveedores"),
        ("AGRARIO", "RECAUDOS DE CONVENIOS", "Ingreso", "", "Agrario"),
        ("AGRARIO", "INTERNET TRANSFERENCIAS ENTRE TERCEROS INTERNET", "Ingreso", "", "Agrario"),

        # BANCOLOMBIA - EGRESO (conceptos específicos)
        ("BANCOLOMBIA", "PAGO PSE IMPUESTO DIAN", "Egreso", "", "Impuestos"),
        ("BANCOLOMBIA", "PAGO PSE MUNICIPIO DE YUMBO", "Egreso", "", "Impuestos"),
        ("BANCOLOMBIA", "PAGO A PROVE LOPEZ MORALES J", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "CARGUE TARJETA PREPAGO PROPIA", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO A PROV JGN BBVA AHORRO", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO A PROV UPEGUI NARANJO ADR", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO PSE ASOPAGOS", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO PSE BANCOLOMBIA", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO A PROVE OPERADORA DE SE", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO A PROVE COMFAMA", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO A PROVE FIDEICOMISO MAS", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO PSE BANCOOMEVA", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO PSE APORTES EN LINEA", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO A PROV RIO CLARO AGRO BCO", "Egreso", "", "Traslado"),

        # BANCOLOMBIA - reglas generales controladas
        ("BANCOLOMBIA", "PAGO A NOMIN", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO PROGRAMADO", "Egreso", "", "Proveedores"),
        ("BANCOLOMBIA", "TRASLADO A FONDO DE INVERSION", "Egreso", "", "Traslado"),
        ("BANCOLOMBIA", "PAGO CUOTA CREDITO BANCOL", "Egreso", "", "Credito"),
        ("BANCOLOMBIA", "TRANSF INTERNACIONAL ENVIADA", "Egreso", "", "Proveedores"),
        ("BANCOLOMBIA", "PAGO SUC VIRT TC VISA", "Egreso", "", "Credito"),
        ("BANCOLOMBIA", "PAGO AUTOM TC VISA", "Egreso", "", "Credito"),
        ("BANCOLOMBIA", "PAGO CREDITO SUC VIRTUAL", "Egreso", "", "Proveedores"),
        ("BANCOLOMBIA", "PAGO SEGUROS GENERALES", "Egreso", "", "Nomina"),
        ("BANCOLOMBIA", "PAGO SURAMERICANA DE SEGUROS", "Egreso", "", "Proveedores"),
        ("BANCOLOMBIA", "PAGO SV", "Egreso", "", "Proveedores"),

        # BANCOLOMBIA (INGRESO)
        ("BANCOLOMBIA", "PAGO DE PROV", "Ingreso", "", "Bancolombia"),
        ("BANCOLOMBIA", "PAGO INTERBANC", "Ingreso", "", "Bancolombia"),
        ("BANCOLOMBIA", "CONSIG LOCAL REFEREN EFECTIVO", "Ingreso", "", "Bancolombia"),
        ("BANCOLOMBIA", "ABONO CREDIPAGO SUC VIRTUAL", "Ingreso", "", "Credipago Bancolombia"),
        ("BANCOLOMBIA", "REC CREDIPA", "Ingreso", "", "Bancolombia"),
        ("BANCOLOMBIA", "TRANSF INTERNACIONAL RECIBIDA", "Ingreso", "", "Bancolombia"),
        ("BANCOLOMBIA", "TRANSFERENCIA DESDE NEQUI", "Ingreso", "", "Bancolombia"),
        ("BANCOLOMBIA", "TRASLADO DE FONDO DE INVERS", "Ingreso", "", "Traslado"),

        # FIDUCIA
        ("FIDUCIA", "TRASLADO DESDE", "Ingreso", "", "Traslado"),
        ("FIDUCIA", "TRASLADO HACIA", "Egreso", "", "Traslado"),

        # BBVA
        ("BBVA", "CARGO POR TRASP", "Egreso", "", "Nomina"),
        ("BBVA", "CARGO DOMICILIA", "Egreso", "", "Proveedores"),
        ("BBVA", "CARGO CUENTA TR", "Egreso", "", "Nomina"),
        ("BBVA", "DEPOSITO EFECTI", "Ingreso", "", "BBVA"),
        ("BBVA", "DEPOSITO EN EFE", "Ingreso", "", "BBVA"),
        ("BBVA", "ABONO POR DOMIC", "Ingreso", "", "BBVA"),
        ("BBVA", "RECIBISTE DINER", "Ingreso", "", "BBVA"),

        # BOGOTÁ
        ("BOGOTÁ", "CR ACH BANCOLOMBIA RIO CLARO TECNOL NIT890927624 FAC RIO CLARO TECNOLOGIA", "Ingreso", "", "Traslado"),
        ("BOGOTÁ", "PAGO AUTOMATICO CUOTA DE CREDITO", "Egreso", "", "Credito"),
        ("BOGOTÁ", "PAGO TARJETA", "Egreso", "", "Credito"),
        ("BOGOTÁ", "ABONO DISPERSION PAGO A PROVEEDORES", "Ingreso", "", "Bogotá"),
        ("BOGOTÁ", "ABONO POR DEPOSITO EN CORRESPONSAL", "Ingreso", "", "Bogotá"),
        ("BOGOTÁ", "ABONO TRANSFERENCIA POR BUSINESS", "Ingreso", "", "Bogotá"),
        ("BOGOTÁ", "CONSIGNACION NACIONAL", "Ingreso", "", "Bogotá"),
        ("BOGOTÁ", "CR ACH BANDAVIVIENDA", "Ingreso", "", "Bogotá"),

        # DAVIVIENDA
        ("DAVIVIENDA", "DESCUENTO TRANSFERENCIA", "Egreso", "", "Proveedores"),
        ("DAVIVIENDA", "TRANF DE CONTINGEN A CTA AFC", "Egreso", "", "Nomina"),
        ("DAVIVIENDA", "DESCUENTO POR PAGO A PROVEEDORES 8909276244", "Egreso", "", "Nomina"),
        ("DAVIVIENDA", "PAGO CREDITO N", "Egreso", "", "Credito"),
        ("DAVIVIENDA", "ABONO", "Ingreso", "", "Davivienda"),
        ("DAVIVIENDA", "CONSIGNACION EFECTIVO EN OFICINA", "Ingreso", "", "Davivienda"),
    ]

    # --- REGLA ESPECIAL: BANCOLOMBIA "TRANSFERENCIA CTA SUC VIRTUAL" depende del signo ---
    m_suc = (b["_banco_norm"] == "BANCOLOMBIA") & (b["_detalle_norm"].str.startswith("TRANSFERENCIA CTA SUC VIRTUAL", na=False))
    if m_suc.any():
        v_suc = pd.to_numeric(b.loc[m_suc, "Valor"], errors="coerce").fillna(0.0)
        m_pos = m_suc.copy(); m_pos[m_suc] = v_suc > 0
        m_neg = m_suc.copy(); m_neg[m_suc] = v_suc < 0

        b.loc[m_pos, "Tipo"] = "Ingreso"
        b.loc[m_pos, "Cuenta"] = ""
        b.loc[m_pos, "Concepto"] = "Bancolombia"

        b.loc[m_neg, "Tipo"] = "Egreso"
        b.loc[m_neg, "Cuenta"] = ""
        b.loc[m_neg, "Concepto"] = "Proveedores"

    # Aplica reglas "inicia con"
    for banco, patron, tipo, cuenta, concepto in STARTS_RULES:
        m = (b["_banco_norm"] == banco) & (b["_detalle_norm"].str.startswith(patron, na=False))
        if m.any():
            b.loc[m, "Tipo"] = tipo
            b.loc[m, "Cuenta"] = cuenta
            b.loc[m, "Concepto"] = concepto

    # Marcar cuáles ya quedaron tipificados por reglas fuertes
    tipificado = b["Tipo"].astype(str).str.strip().ne("")

    # ----------------------------------------------------------
    # 2) REGLAS SECUNDARIAS: "CONTIENTE" SOLO donde NO tipificado
    # ----------------------------------------------------------
# IVA: SOLO si realmente es un movimiento de IVA (no pagos a proveedores)
    empty_tipo = ~tipificado
    
    m = empty_tipo & (
        b["_detalle_norm"].str.startswith("IVA ", na=False) |
        b["_detalle_norm"].str.startswith("COBRO IVA SERVICIOS FINANCIEROS", na=False) |
        b["_detalle_norm"].str.contains("CARGO IVA", na=False) |
        b["_detalle_norm"].str.startswith("IVA CUOTA MANEJO", na=False) |
        b["_detalle_norm"].str.startswith("IVA SUC VIRT", na=False) |
        b["_detalle_norm"].str.startswith("COBRO IVA PAGOS AUTOMATICOS", na=False) |
        b["_detalle_norm"].str.startswith("IVA SUC VIRT EMP", na=False)
    )
    b.loc[m, "Tipo"] = "GB"
    b.loc[m, "Cuenta"] = "24080213"
    b.loc[m, "Concepto"] = "IVA"

    # IMPORTANTE: recalcular para que no se pisen reglas después
    empty_tipo = b["Tipo"].astype(str).str.strip().eq("")
    # Davivienda - Cobro Servicio Manejo Portal => GB 530515
    m = (
        empty_tipo &
        (b["_banco_norm"] == "DAVIVIENDA") &
        (b["_detalle_norm"].str.contains("COBRO SERVICIO MANEJO PORTAL", na=False))
    )
    b.loc[m, "Tipo"] = "GB"
    b.loc[m, "Cuenta"] = "530505"
    b.loc[m, "Concepto"] = "GASTOS BANCARIOS"

    # Rendimientos Fiducia
    m = empty_tipo & (b["_banco_norm"] == "FIDUCIA") & (b["_detalle_norm"].str.startswith("RENDIMIENTOS FIDUCIA", na=False))
    b.loc[m, "Tipo"] = "Ingreso"
    b.loc[m, "Cuenta"] = "421005"
    b.loc[m, "Concepto"] = "FIDUCIA"

    # BBVA ABONO POR INTER
    m = empty_tipo & (b["_banco_norm"] == "BBVA") & (b["_detalle_norm"].str.startswith("ABONO POR INTER", na=False))
    b.loc[m, "Tipo"] = "Ingreso"
    b.loc[m, "Cuenta"] = "421005"
    b.loc[m, "Concepto"] = "BBVA"
    
    # Gastos bancarios: cobros / comisiones
    m = empty_tipo & (
        b["_detalle_norm"].str.contains("COBRO PAGO", na=False) |
        b["_detalle_norm"].str.contains("SERVICIO POR PAGOS", na=False) |
        b["_detalle_norm"].str.contains("SERVICIO PAGO", na=False) |
        b["_detalle_norm"].str.contains("DESCUENTO COBRO SERVICIOS ENTRE CIUD", na=False) |
        b["_detalle_norm"].str.contains("COMISION", na=False) |
        b["_detalle_norm"].str.contains(r"\bCOMIS\b", na=False)
    )
    b.loc[m, "Tipo"] = "GB"
    b.loc[m, "Cuenta"] = "530515"
    b.loc[m, "Concepto"] = "GASTOS BANCARIOS"
    
    m = empty_tipo & (
        b["_detalle_norm"].str.contains("CUOTA MANEJO", na=False)
    )
    b.loc[m, "Tipo"] = "GB"
    b.loc[m, "Cuenta"] = "530505"
    b.loc[m, "Concepto"] = "GASTOS BANCARIOS"
    
    m = empty_tipo & (
        b["_detalle_norm"].str.contains("RETENCIÓN EN LA FUENTE", na=False)
    )
    b.loc[m, "Tipo"] = "GB"
    b.loc[m, "Cuenta"] = "13551506"
    b.loc[m, "Concepto"] = "RTEFTE"

    # GMF / 4x1000
    m = empty_tipo & (
        b["_detalle_norm"].str.contains(r"\bGMF\b", na=False) |
        b["_detalle_norm"].str.contains("IMPTO GOBIERNO 4X1000", na=False) |
        b["_detalle_norm"].str.contains("CARGO POR IMPUE", na=False) |
        b["_detalle_norm"].str.contains("GRAVAMEN MOVIMIENTOS FINANCIEROS", na=False)
    )
    b.loc[m, "Tipo"] = "GB"
    b.loc[m, "Cuenta"] = "53059501"
    b.loc[m, "Concepto"] = "GMF"

    # ----------------------------------------------------------
    # 3) Fallback: si aún quedó Tipo vacío, por signo
    # ----------------------------------------------------------
    v = pd.to_numeric(b["Valor"], errors="coerce").fillna(0.0)
    empty_tipo2 = b["Tipo"].astype(str).str.strip().eq("")
    b.loc[empty_tipo2 & (v > 0), "Tipo"] = "Ingreso"
    b.loc[empty_tipo2 & (v < 0), "Tipo"] = "Egreso"

    # ----------------------------------------------------------
    # 4) ÚLTIMO recurso: Proveedores solo si NO quedó concepto (para no pisar)
    # ----------------------------------------------------------
    empty_con = b["Concepto"].astype(str).str.strip().eq("")
    m = empty_con & (b["_banco_norm"] == "BANCOLOMBIA") & (b["_detalle_norm"].str.startswith("PAGO PSE", na=False))
    b.loc[m, "Concepto"] = "Proveedores"

    m = empty_con & (b["_banco_norm"] == "BANCOLOMBIA") & (b["_detalle_norm"].str.startswith("PAGO A PROVE", na=False))
    b.loc[m, "Concepto"] = "Proveedores"

    b.drop(columns=["_banco_norm", "_detalle_norm"], inplace=True, errors="ignore")
    return b


# ==========================================================
# Carga Bancos
# ==========================================================
def load_bancolombia_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        header=None,
        dtype=str,
        sep=",",
        engine="python",
        skipinitialspace=True,
        on_bad_lines="skip",
        encoding="latin1"
    )

    s_fecha = df.iloc[:, 3].astype(str).str.strip()
    fecha = pd.to_datetime(s_fecha, format="%Y%m%d", errors="coerce").dt.normalize()

    detalle = df.iloc[:, 7].astype(str).str.strip()

    s_valor = df.iloc[:, 5].astype(str).str.strip()
    valor = pd.to_numeric(s_valor, errors="coerce").fillna(0.0)

    out = pd.DataFrame({
        "Banco": "Bancolombia",
        "Fecha": fecha,
        "Detalle": detalle,
        "Valor": valor
    })

    out["Detalle"] = out["Detalle"].replace({"nan": None, "NaN": None, "": None})
    return out.dropna(subset=["Fecha", "Detalle"])


def load_fiducia_csv(path: str, anio: int, mes: int, rendimientos: float) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", dtype=str, encoding_errors="ignore")
    df.columns = [c.strip() for c in df.columns]
    cols = list(df.columns)
    fecha_col = cols[0]
    valor_col = "VALOR" if "VALOR" in cols else cols[-1]
    det_col = "DESCRIPCION" if "DESCRIPCION" in cols else (cols[1] if len(cols) > 1 else cols[0])

    df["Fecha"] = pd.to_datetime(df[fecha_col], errors="coerce", dayfirst=True).dt.normalize()
    df["Detalle"] = df[det_col].astype(str).str.strip()
    df["Valor"] = df[valor_col].apply(to_float_money)

    out = df[["Fecha", "Detalle", "Valor"]].copy()
    out["Banco"] = "Fiducia"
    out = out[["Banco", "Fecha", "Detalle", "Valor"]].dropna(subset=["Fecha"])

    rend_row = pd.DataFrame([{
        "Banco": "Fiducia",
        "Fecha": last_day_of_month(anio, mes),
        "Detalle": "Rendimientos Fiducia",
        "Valor": float(rendimientos)
    }])
    return pd.concat([out, rend_row], ignore_index=True)


def load_bogota_mov_xls(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, skiprows=26, dtype=object)
    cols = list(df.columns)

    fecha = pd.to_datetime(df.iloc[:, 1], errors="coerce").dt.normalize()
    detalle = df.iloc[:, 4].astype(str).str.strip()
    detalle = detalle.replace({"nan": None, "NaN": None}).ffill()

    credit_cols = [c for c in cols if "CRED" in norm_upper(c)]
    debit_cols = [c for c in cols if "DEB" in norm_upper(c)]

    def clean_series(s):
        return (
            s.astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .str.strip()
            .replace({"nan": None, "NaN": None, "": None})
            .ffill()
        )

    if credit_cols:
        cred = pd.to_numeric(clean_series(df[credit_cols[0]]), errors="coerce").fillna(0.0)
    else:
        cred = pd.to_numeric(clean_series(df.iloc[:, 14]), errors="coerce").fillna(0.0) if len(cols) > 14 else 0.0

    if debit_cols:
        deb = pd.to_numeric(clean_series(df[debit_cols[0]]), errors="coerce").fillna(0.0)
    else:
        d1 = pd.to_numeric(clean_series(df.iloc[:, 12]), errors="coerce").fillna(0.0) if len(cols) > 12 else 0.0
        d2 = pd.to_numeric(clean_series(df.iloc[:, 13]), errors="coerce").fillna(0.0) if len(cols) > 13 else 0.0
        deb = d1 + d2

    valor = (cred - deb).astype(float)

    out = pd.DataFrame({"Banco": "Bogotá", "Fecha": fecha, "Detalle": detalle, "Valor": valor})
    return out.dropna(subset=["Fecha"])


def load_bogota_informe_csv(path: str, year: int) -> pd.DataFrame:
    df = pd.read_csv(path, skiprows=1, dtype=str, encoding_errors="ignore")
    fecha_raw = df.iloc[:, 0].astype(str).str.strip()
    fecha = pd.to_datetime(fecha_raw + f"/{year}", errors="coerce", dayfirst=False).dt.normalize()

    detalle = df.iloc[:, 1].astype(str).str.strip()
    deb = df.iloc[:, 4].apply(to_float_money)
    cred = df.iloc[:, 5].apply(to_float_money)
    valor = (cred - deb).astype(float)

    out = pd.DataFrame({"Banco": "Bogotá", "Fecha": fecha, "Detalle": detalle, "Valor": valor})
    return out.dropna(subset=["Fecha"])


def load_davivienda_xls(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, skiprows=2, dtype=object)
    fecha = pd.to_datetime(df.iloc[:, 0], errors="coerce", dayfirst=True).dt.normalize()

    detalle = df.iloc[:, 7].astype(str).str.strip()
    valor_base = df.iloc[:, 8].apply(to_float_money)

    tran = df.iloc[:, 2].astype(str).str.upper()
    sign = pd.Series([1] * len(df))
    sign[tran.str.contains("NOTAS DEBITO", na=False)] = -1

    valor = valor_base.abs() * sign
    out = pd.DataFrame({"Banco": "Davivienda", "Fecha": fecha, "Detalle": detalle, "Valor": valor})
    return out.dropna(subset=["Fecha"])


def _agrario_split_iva(df_in: pd.DataFrame, base_text: str) -> pd.DataFrame:
    mask = df_in["Detalle"].astype(str).str.contains(base_text, na=False)
    if not mask.any():
        return df_in
    rows = df_in[mask].copy()
    rest = df_in[~mask].copy()

    new_rows = []
    for _, r in rows.iterrows():
        v = float(r["Valor"])
        base = round(v / 1.19)
        iva = round(v - base)

        r_base = r.copy()
        r_base["Valor"] = base
        new_rows.append(r_base)

        r_iva = r.copy()
        r_iva["Detalle"] = "IVA " + base_text
        r_iva["Valor"] = iva
        new_rows.append(r_iva)

    return pd.concat([rest, pd.DataFrame(new_rows)], ignore_index=True)


def load_agrario_mov_xls(path: str, anio: int, mes: int) -> pd.DataFrame:
    df = pd.read_excel(path, skiprows=10, dtype=object)
    fecha = pd.to_datetime(df.iloc[:, 0], errors="coerce", dayfirst=False).dt.normalize()
    detalle = df.iloc[:, 2].astype(str).str.strip()

    credito = pd.to_numeric(df.iloc[:, 3], errors="coerce").fillna(0.0)
    debito = pd.to_numeric(df.iloc[:, 4], errors="coerce").fillna(0.0)
    valor = (debito - credito).astype(float)

    out = pd.DataFrame({"Banco": "Agrario", "Fecha": fecha, "Detalle": detalle, "Valor": valor}).dropna(subset=["Fecha"])

    gmf_col = df.iloc[:, 5] if df.shape[1] > 5 else pd.Series([0] * len(df))
    gmf_total = pd.to_numeric(gmf_col, errors="coerce").fillna(0.0).sum()
    if gmf_total != 0:
        out = pd.concat([out, pd.DataFrame([{
            "Banco": "Agrario",
            "Fecha": last_day_of_month(anio, mes),
            "Detalle": "GMF",
            "Valor": -abs(float(gmf_total))
        }])], ignore_index=True)

    out = _agrario_split_iva(out, "CNV COBRO COMISION PAGO CONVENIOS")
    out = _agrario_split_iva(out, "DB CTA CTE COMISION INTERBANCARIA")

    return out


def load_agrario_informe_xls(path: str, anio: int, mes: int) -> pd.DataFrame:
    df = pd.read_excel(path, skiprows=15, dtype=object)
    dia = pd.to_numeric(df.iloc[:, 1], errors="coerce").fillna(0).astype(int)
    fecha = pd.to_datetime([f"{anio}-{mes:02d}-{d:02d}" if d > 0 else None for d in dia], errors="coerce").normalize()

    detalle = df.iloc[:, 2].astype(str).str.strip()
    valor = df.iloc[:, 12].apply(to_float_money) if df.shape[1] > 12 else 0.0

    out = pd.DataFrame({"Banco": "Agrario", "Fecha": fecha, "Detalle": detalle, "Valor": valor})
    return out.dropna(subset=["Fecha"])


def load_bbva_xls(path: str) -> pd.DataFrame:
    raw = pd.read_excel(path, engine="openpyxl", header=None, dtype=object)
    mask = raw.apply(lambda r: r.astype(str).str.contains("FECHA DE OPER", case=False, na=False).any(), axis=1)
    if not mask.any():
        raise ValueError("No se encontró el encabezado 'FECHA DE OPERACIÓN' en el archivo BBVA.")
    start = int(mask.idxmax())

    df = pd.read_excel(path, engine="openpyxl", skiprows=start, dtype=object)

    fecha = pd.to_datetime(df.iloc[:, 1], errors="coerce", dayfirst=True).dt.normalize()
    fecha = fecha.ffill()

    detalle = df.iloc[:, 5].astype(str).str.strip()
    valor = df.iloc[:, 7].apply(to_float_money)

    out = pd.DataFrame({"Banco": "BBVA", "Fecha": fecha, "Detalle": detalle, "Valor": valor})
    out["Detalle"] = out["Detalle"].replace({"nan": None, "NaN": None, "": None})
    return out.dropna(subset=["Fecha", "Valor", "Detalle"])


def build_bancos(cfg: RunConfig, log) -> pd.DataFrame:
    parts = []

    if cfg.bancos.bancolombia_csv:
        log("Cargando Bancolombia…")
        parts.append(load_bancolombia_csv(cfg.bancos.bancolombia_csv))

    if cfg.bancos.fiducia_csv:
        log("Cargando Fiducia…")
        if cfg.bancos.fiducia_rendimientos is None:
            raise ValueError("Fiducia cargada pero falta Rendimientos.")
        parts.append(load_fiducia_csv(cfg.bancos.fiducia_csv, cfg.anio, cfg.mes, cfg.bancos.fiducia_rendimientos))

    if cfg.bancos.bogota_mov_xls:
        log("Cargando Bogotá Movimientos…")
        parts.append(load_bogota_mov_xls(cfg.bancos.bogota_mov_xls))
    elif cfg.bancos.bogota_inf_csv:
        log("Cargando Bogotá Informe…")
        if not cfg.bancos.bogota_inf_year:
            raise ValueError("Bogotá Informe requiere año.")
        parts.append(load_bogota_informe_csv(cfg.bancos.bogota_inf_csv, cfg.bancos.bogota_inf_year))

    if cfg.bancos.davivienda_xls:
        log("Cargando Davivienda…")
        parts.append(load_davivienda_xls(cfg.bancos.davivienda_xls))

    if cfg.bancos.agrario_mov_xls:
        log("Cargando Agrario Movimientos…")
        parts.append(load_agrario_mov_xls(cfg.bancos.agrario_mov_xls, cfg.anio, cfg.mes))
    elif cfg.bancos.agrario_inf_xls:
        log("Cargando Agrario Informe…")
        parts.append(load_agrario_informe_xls(cfg.bancos.agrario_inf_xls, cfg.anio, cfg.mes))

    if cfg.bancos.bbva_xls:
        log("Cargando BBVA…")
        parts.append(load_bbva_xls(cfg.bancos.bbva_xls))

    if not parts:
        raise ValueError("No se cargó ningún banco.")

    bancos = pd.concat(parts, ignore_index=True)
    bancos["Fecha"] = pd.to_datetime(bancos["Fecha"], errors="coerce").dt.normalize()
    bancos = bancos.dropna(subset=["Fecha"])

    bancos = bancos[(bancos["Fecha"].dt.year == cfg.anio) & (bancos["Fecha"].dt.month == cfg.mes)].copy()

    bancos["Detalle"] = bancos["Detalle"].fillna("").astype(str).str.strip()
    bancos["Valor"] = pd.to_numeric(bancos["Valor"], errors="coerce").fillna(0.0).astype(float)

    bancos["Tipo"] = ""
    bancos["Cuenta"] = ""
    bancos["Concepto"] = ""
    bancos = bancos.reset_index(drop=True)
    return bancos[["Banco", "Fecha", "Detalle", "Valor", "Tipo", "Cuenta", "Concepto"]]


# ==========================================================
# Carga Auxiliar + Balance
# ==========================================================
def build_auxiliar(libro_aux_path: str, cfg: RunConfig, log) -> pd.DataFrame:
    log("Cargando Libro Auxiliar…")
    df = pd.read_excel(libro_aux_path, skiprows=2, dtype=object)

    cuenta = df.iloc[:, 0].astype(str).str.replace(".0", "", regex=False).str.strip()
    fecha = pd.to_datetime(df.iloc[:, 5], errors="coerce").dt.normalize()
    detalle = df.iloc[:, 10].astype(str).str.strip()

    deb = pd.to_numeric(df.iloc[:, 15], errors="coerce").fillna(0.0)
    cred = pd.to_numeric(df.iloc[:, 16], errors="coerce").fillna(0.0)
    valor = (deb - cred).astype(float)

    comp = pd.to_numeric(df.iloc[:, 7], errors="coerce").fillna(0).astype(int).astype(str)
    doc = pd.to_numeric(df.iloc[:, 9], errors="coerce").fillna(0).astype(int).astype(str)
    mes = fecha.dt.month.fillna(0).astype(int).astype(str)
    documento = comp + "-" + doc + "-" + mes

    out = pd.DataFrame({
        "Cuenta": cuenta,
        "Fecha": fecha,
        "Detalle": detalle,
        "Valor": valor,
        "Documento": documento
    })
    out["Banco"] = out["Cuenta"].map(CUENTA_A_BANCO)
    out = out.dropna(subset=["Banco", "Fecha"])
    out = out[(out["Fecha"].dt.year == cfg.anio) & (out["Fecha"].dt.month == cfg.mes)].copy()

    out["Tipo"] = ""
    doc_ser = out["Documento"].astype(str).fillna("")
    val_ser = pd.to_numeric(out["Valor"], errors="coerce").fillna(0.0)

    out.loc[doc_ser.str.startswith("1-"), "Tipo"] = "Ingreso"
    out.loc[doc_ser.str.startswith("2-"), "Tipo"] = "Egreso"
    out.loc[doc_ser.str.startswith("5-"), "Tipo"] = "GB"
    out.loc[doc_ser.str.startswith("2-") & (val_ser > 0), "Tipo"] = "Ingreso"  # regla especial

    return out[["Banco", "Cuenta", "Documento", "Fecha", "Detalle", "Valor", "Tipo"]]


def verify_balance(balance_path: str, auxiliar: pd.DataFrame, log) -> None:
    if not balance_path:
        return
    log("Verificando Balance de prueba vs Auxiliar…")
    df = pd.read_excel(balance_path, skiprows=2, dtype=object)
    cuenta = df.iloc[:, 0].astype(str).str.replace(".0", "", regex=False).str.strip()

    allowed = set(CUENTA_A_BANCO.keys())
    df = df[cuenta.isin(allowed)].copy()
    df["Cuenta"] = cuenta[cuenta.isin(allowed)].values

    mov = pd.to_numeric(df.iloc[:, 4], errors="coerce").fillna(0.0) - pd.to_numeric(df.iloc[:, 5], errors="coerce").fillna(0.0)
    df["Movimientos"] = mov.astype(float)

    difs = []
    for c in sorted(allowed):
        bal_mov = float(df.loc[df["Cuenta"] == c, "Movimientos"].sum())
        aux_mov = float(auxiliar.loc[auxiliar["Cuenta"] == c, "Valor"].sum())
        diff = round(bal_mov - aux_mov, 2)
        if abs(diff) >= 0.01:
            difs.append(f"Cuenta {c}: Balance={bal_mov} Auxiliar={aux_mov} Dif={diff}")

    if not difs:
        log("✅ Ok verificado Libro auxiliar y Balance de prueba")
    else:
        log("❌ Diferencias Balance vs Auxiliar:")
        for d in difs:
            log(" - " + d)


# ==========================================================
# Cruces
# ==========================================================
def cruzar(bancos: pd.DataFrame, auxiliar: pd.DataFrame, cfg: RunConfig, log) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    b = bancos.reset_index(drop=True).reset_index().rename(columns={"index": "idx_banco"}).copy()
    a = auxiliar.reset_index().rename(columns={"index": "idx_aux"}).copy()

    b["Fecha"] = pd.to_datetime(b["Fecha"], errors="coerce").dt.normalize()
    a["Fecha"] = pd.to_datetime(a["Fecha"], errors="coerce").dt.normalize()
    b["Valor"] = pd.to_numeric(b["Valor"], errors="coerce").fillna(0.0).round(2)
    a["Valor"] = pd.to_numeric(a["Valor"], errors="coerce").fillna(0.0).round(2)

    matched_b = set()
    matched_a = set()
    links = []

    # 1) Nómina Bancolombia: agrupar por día en bancos
    tol_nom = timedelta(days=cfg.tol_nomina_days)
    nom_mask = (b["Banco"].str.upper() == "BANCOLOMBIA") & (b["Detalle"].astype(str).str.upper().str.startswith("PAGO A NOMIN"))
    b_nom = b[nom_mask].copy()

    if not b_nom.empty:
        log("Cruce Nómina Bancolombia por día…")
        grouped = b_nom.groupby("Fecha").agg(
            Valor_total=("Valor", "sum"),
            idxs=("idx_banco", lambda s: list(s))
        ).reset_index()

        for _, r in grouped.iterrows():
            f = r["Fecha"]
            v = round(float(r["Valor_total"]), 2)

            cand = a[
                (a["Banco"].str.upper() == "BANCOLOMBIA") &
                (a["Tipo"].str.upper() == "EGRESO") &
                (~a["idx_aux"].isin(matched_a)) &
                (a["Valor"].round(2) == v) &
                (a["Fecha"].between(f - tol_nom, f + tol_nom))
            ]
            if cand.empty:
                continue

            ia = int(cand.iloc[0]["idx_aux"])
            for ib in r["idxs"]:
                links.append((int(ib), ia, "NOMINA_DIA_TOL"))
                matched_b.add(int(ib))
            matched_a.add(ia)

    # 2) BBVA CARGO DOMICILIA vs suma diaria auxiliar egresos
    tol_bbva = timedelta(days=cfg.tol_bbva_sum_days)
    bbva_cargo = b[
        (b["Banco"].str.upper() == "BBVA") &
        (b["Tipo"].str.upper() == "EGRESO") &
        (b["Detalle"].astype(str).str.upper().str.contains("CARGO DOMICILIA", na=False)) &
        (~b["idx_banco"].isin(matched_b))
    ].copy()

    if not bbva_cargo.empty:
        log("Cruce BBVA CARGO DOMICILIA vs suma diaria auxiliar…")
        aux_bbva = a[(a["Banco"].str.upper() == "BBVA") & (a["Tipo"].str.upper() == "EGRESO") & (~a["idx_aux"].isin(matched_a))].copy()
        sum_by_day = aux_bbva.groupby("Fecha")["Valor"].sum().round(2)
        day_to_idxs = aux_bbva.groupby("Fecha")["idx_aux"].apply(list).to_dict()

        for _, rb in bbva_cargo.iterrows():
            fb = rb["Fecha"]
            vb = float(rb["Valor"])

            window = sum_by_day.loc[(sum_by_day.index >= fb - tol_bbva) & (sum_by_day.index <= fb + tol_bbva)]
            hit = window[window.round(2) == round(vb, 2)]
            if hit.empty:
                continue

            fa = hit.index[0]
            idxs_aux = day_to_idxs.get(fa, [])
            if not idxs_aux:
                continue

            for ia in idxs_aux:
                links.append((int(rb["idx_banco"]), int(ia), "BBVA_CARGO_DOMICILIA_SUMDIA"))
                matched_a.add(int(ia))
            matched_b.add(int(rb["idx_banco"]))

    # 3) Cruce general: por valor, banco y tipo con tolerancia ± tol_general
    tol_gen = timedelta(days=cfg.tol_general_days)

    def cross_one_to_one(tipo: str):
        nonlocal matched_b, matched_a, links
        bb = b[(b["Tipo"].str.upper() == tipo.upper()) & (~b["idx_banco"].isin(matched_b))].copy()
        for _, rb in bb.iterrows():
            fb = rb["Fecha"]
            vb = float(rb["Valor"])
            banco = rb["Banco"].upper()

            cand = a[
                (a["Banco"].str.upper() == banco) &
                (~a["idx_aux"].isin(matched_a)) &
                (a["Tipo"].str.upper() == tipo.upper()) &
                (a["Valor"].round(2) == round(vb, 2)) &
                (a["Fecha"].between(fb - tol_gen, fb + tol_gen))
            ]
            if cand.empty:
                continue
            ia = int(cand.iloc[0]["idx_aux"])
            links.append((int(rb["idx_banco"]), ia, f"{tipo.upper()}_TOL"))
            matched_b.add(int(rb["idx_banco"]))
            matched_a.add(ia)

    cross_one_to_one("Ingreso")
    cross_one_to_one("Egreso")

    cruce_df = pd.DataFrame(links, columns=["idx_banco", "idx_aux", "ReglaCruce"])
    pend_b = b[~b["idx_banco"].isin(matched_b)].drop(columns=["idx_banco"])
    pend_a = a[~a["idx_aux"].isin(matched_a)].drop(columns=["idx_aux"])

    return cruce_df, pend_b, pend_a


# ==========================================================
# documento en Bancos desde Auxiliar usando Cruce
# ==========================================================
def add_documento_column(bancos: pd.DataFrame, auxiliar: pd.DataFrame, cruce_df: pd.DataFrame) -> pd.DataFrame:
    b = bancos.reset_index(drop=True).copy()
    a = auxiliar.reset_index(drop=True).copy()

    if "documento" in b.columns:
        b = b.drop(columns=["documento"])
    b.insert(7, "documento", "")

    if cruce_df is None or cruce_df.empty:
        return b
    if not {"idx_banco", "idx_aux"}.issubset(cruce_df.columns):
        return b

    aux_doc_col = a.columns[2]  # Documento
    aux_idx = a.reset_index().rename(columns={"index": "idx_aux"})[["idx_aux", aux_doc_col]].rename(columns={aux_doc_col: "documento"})
    merged = cruce_df.merge(aux_idx, on="idx_aux", how="left")

    map_docs = (
        merged.groupby("idx_banco")["documento"]
        .apply(lambda s: ",".join(sorted(set([x for x in s.dropna().astype(str) if x and x.lower() != "nan"]))))
        .to_dict()
    )

    for idx_b, doc in map_docs.items():
        try:
            b.loc[int(idx_b), "documento"] = doc
        except:
            pass
    return b


# ==========================================================
# Pipeline completo
# ==========================================================
def run_pipeline(cfg: RunConfig, log) -> Dict[str, pd.DataFrame]:
    if not cfg.contables.libro_auxiliar_xlsx:
        raise ValueError("Debe cargar Libro Auxiliar.")
    if not cfg.contables.balance_prueba_xlsx:
        raise ValueError("Debe cargar Balance de prueba.")

    bancos = build_bancos(cfg, log)
    auxiliar = build_auxiliar(cfg.contables.libro_auxiliar_xlsx, cfg, log)
    verify_balance(cfg.contables.balance_prueba_xlsx, auxiliar, log)

    bancos = apply_embedded_rules_to_bancos(bancos)

    cruce_df, pend_b, pend_a = cruzar(bancos, auxiliar, cfg, log)
    bancos_final = add_documento_column(bancos, auxiliar, cruce_df)

    return {
        "Bancos": bancos_final,
        "Auxiliar": auxiliar,
        "Cruce Bancos-Aux": cruce_df,
        "Pendientes Bancos": pend_b,
        "Pendientes Auxiliar": pend_a,
    }


def save_excel(out_path: str, sheets: Dict[str, pd.DataFrame]):
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)


# ==========================================================
# GUI Components
# ==========================================================
class FilePicker(QWidget):
    def __init__(self, title: str, filter_str: str):
        super().__init__()
        self.filter_str = filter_str
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.label = QLabel(title)
        self.label.setMinimumWidth(220)
        self.edit = QLineEdit()
        self.edit.setPlaceholderText("Seleccione archivo…")
        self.btn = QPushButton("Buscar")
        self.btn.clicked.connect(self.pick)

        layout.addWidget(self.label)
        layout.addWidget(self.edit, 1)
        layout.addWidget(self.btn)

    def pick(self):
        path, _ = QFileDialog.getOpenFileName(self, "Seleccionar archivo", "", self.filter_str)
        if path:
            self.edit.setText(path)

    def get(self) -> Optional[str]:
        p = self.edit.text().strip()
        return p or None


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Conciliación Bancaria - Todo en Uno (PySide6)")
        self.setMinimumWidth(1050)

        root = QWidget()
        L = QVBoxLayout(root)

        # Periodo
        gb_periodo = QGroupBox("Periodo a conciliar")
        lp = QHBoxLayout(gb_periodo)

        self.cmb_mes = QComboBox()
        for m, n in [(1, "Enero"), (2, "Febrero"), (3, "Marzo"), (4, "Abril"), (5, "Mayo"), (6, "Junio"),
                     (7, "Julio"), (8, "Agosto"), (9, "Septiembre"), (10, "Octubre"), (11, "Noviembre"), (12, "Diciembre")]:
            self.cmb_mes.addItem(n, m)
        self.cmb_mes.setCurrentIndex(10)  # Nov

        self.sp_anio = QSpinBox()
        self.sp_anio.setRange(2000, 2100)
        self.sp_anio.setValue(2025)

        self.sp_tol_nom = QSpinBox(); self.sp_tol_nom.setRange(0, 30); self.sp_tol_nom.setValue(5)
        self.sp_tol_gen = QSpinBox(); self.sp_tol_gen.setRange(0, 30); self.sp_tol_gen.setValue(3)
        self.sp_tol_bbva = QSpinBox(); self.sp_tol_bbva.setRange(0, 30); self.sp_tol_bbva.setValue(5)

        lp.addWidget(QLabel("Mes:")); lp.addWidget(self.cmb_mes)
        lp.addSpacing(10)
        lp.addWidget(QLabel("Año:")); lp.addWidget(self.sp_anio)
        lp.addSpacing(20)
        lp.addWidget(QLabel("Tol Nómina (días):")); lp.addWidget(self.sp_tol_nom)
        lp.addWidget(QLabel("Tol General (días):")); lp.addWidget(self.sp_tol_gen)
        lp.addWidget(QLabel("Tol BBVA Suma (días):")); lp.addWidget(self.sp_tol_bbva)
        lp.addStretch(1)

        # Bancos
        gb_bancos = QGroupBox("Cargar Bancos")
        lb = QGridLayout(gb_bancos)

        self.fp_bancolombia = FilePicker("Bancolombia (CSV sin títulos)", "CSV (*.csv);;Todos (*.*)")
        self.fp_fiducia = FilePicker("Fiducia (CSV ;)", "CSV (*.csv);;Todos (*.*)")
        self.ed_fid_rend = QLineEdit()
        self.ed_fid_rend.setPlaceholderText("Rendimientos Fiducia (ej: 41664.9)")
        self.ed_fid_rend.setValidator(QDoubleValidator(0.0, 1e15, 6))

        self.fp_davivienda = FilePicker("Davivienda (XLS/XLSX)", "Excel (*.xls *.xlsx);;Todos (*.*)")
        self.fp_bbva = FilePicker("BBVA (XLS/XLSX)", "Excel (*.xls *.xlsx);;Todos (*.*)")

        # Bogotá: Movimiento/Informe
        self.rb_bog_mov = QRadioButton("Bogotá Movimiento"); self.rb_bog_inf = QRadioButton("Bogotá Informe")
        self.bg_bog = QButtonGroup(self)
        self.bg_bog.addButton(self.rb_bog_mov); self.bg_bog.addButton(self.rb_bog_inf)
        self.rb_bog_mov.setChecked(True)

        self.fp_bog_mov = FilePicker("Bogotá - Movimientos (XLS)", "Excel (*.xls *.xlsx);;Todos (*.*)")
        self.fp_bog_inf = FilePicker("Bogotá - Informe (CSV)", "CSV (*.csv);;Todos (*.*)")
        self.sp_bog_inf_year = QSpinBox(); self.sp_bog_inf_year.setRange(2000, 2100); self.sp_bog_inf_year.setValue(2025)

        # Agrario: Movimiento/Informe
        self.rb_agr_mov = QRadioButton("Agrario Movimiento"); self.rb_agr_inf = QRadioButton("Agrario Informe")
        self.bg_agr = QButtonGroup(self)
        self.bg_agr.addButton(self.rb_agr_mov); self.bg_agr.addButton(self.rb_agr_inf)
        self.rb_agr_mov.setChecked(True)

        self.fp_agr_mov = FilePicker("Agrario - Movimientos (XLS)", "Excel (*.xls *.xlsx);;Todos (*.*)")
        self.fp_agr_inf = FilePicker("Agrario - Informe (XLS)", "Excel (*.xls *.xlsx);;Todos (*.*)")

        # Layout bancos
        r = 0
        lb.addWidget(self.fp_bancolombia, r, 0, 1, 2); r += 1
        lb.addWidget(self.fp_bbva, r, 0, 1, 2); r += 1
        lb.addWidget(self.fp_davivienda, r, 0, 1, 2); r += 1
        lb.addWidget(self.fp_fiducia, r, 0, 1, 2); r += 1
        lb.addWidget(QLabel("Rendimientos Fiducia:"), r, 0); lb.addWidget(self.ed_fid_rend, r, 1); r += 1

        bog_row = QWidget()
        hb = QHBoxLayout(bog_row); hb.setContentsMargins(0, 0, 0, 0)
        hb.addWidget(self.rb_bog_mov); hb.addWidget(self.rb_bog_inf); hb.addStretch(1)
        hb.addWidget(QLabel("Año Informe Bogotá:")); hb.addWidget(self.sp_bog_inf_year)
        lb.addWidget(bog_row, r, 0, 1, 2); r += 1
        lb.addWidget(self.fp_bog_mov, r, 0, 1, 2); r += 1
        lb.addWidget(self.fp_bog_inf, r, 0, 1, 2); r += 1

        agr_row = QWidget()
        ha = QHBoxLayout(agr_row); ha.setContentsMargins(0, 0, 0, 0)
        ha.addWidget(self.rb_agr_mov); ha.addWidget(self.rb_agr_inf); ha.addStretch(1)
        lb.addWidget(agr_row, r, 0, 1, 2); r += 1
        lb.addWidget(self.fp_agr_mov, r, 0, 1, 2); r += 1
        lb.addWidget(self.fp_agr_inf, r, 0, 1, 2); r += 1

        # Contables
        gb_cont = QGroupBox("Archivos Contables")
        lc = QVBoxLayout(gb_cont)
        self.fp_balance = FilePicker("Balance de prueba", "Excel (*.xls *.xlsx);;Todos (*.*)")
        self.fp_reporte = FilePicker("Reporte movimiento por comprobante", "Excel (*.xls *.xlsx);;Todos (*.*)")
        self.fp_libro = FilePicker("Libro auxiliar", "Excel (*.xls *.xlsx);;Todos (*.*)")
        lc.addWidget(self.fp_balance)
        lc.addWidget(self.fp_reporte)
        lc.addWidget(self.fp_libro)

        # Otros
        gb_otros = QGroupBox("OTROS")
        lo = QVBoxLayout(gb_otros)
        self.fp_aplicativo = FilePicker("Aplicativo", "Excel (*.xls *.xlsx);;Todos (*.*)")
        self.fp_criterios = FilePicker("Criterios bancarios", "Excel (*.xls *.xlsx);;Todos (*.*)")
        lo.addWidget(self.fp_aplicativo)
        lo.addWidget(self.fp_criterios)

        # Salida + botón
        gb_out = QGroupBox("Salida")
        lout = QHBoxLayout(gb_out)
        self.ed_out = QLineEdit()
        self.ed_out.setPlaceholderText("Ruta de salida .xlsx")
        self.btn_out = QPushButton("Elegir…")
        self.btn_out.clicked.connect(self.pick_output)

        self.btn_run = QPushButton("Generar Excel Conciliación")
        self.btn_run.setMinimumHeight(40)
        self.btn_run.clicked.connect(self.on_run)

        lout.addWidget(QLabel("Archivo Excel:"))
        lout.addWidget(self.ed_out, 1)
        lout.addWidget(self.btn_out)
        lout.addSpacing(10)
        lout.addWidget(self.btn_run)

        # Log
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setMinimumHeight(180)

        L.addWidget(gb_periodo)
        L.addWidget(gb_bancos)
        L.addWidget(gb_cont)
        L.addWidget(gb_otros)
        L.addWidget(gb_out)
        L.addWidget(QLabel("Log:"))
        L.addWidget(self.log)

        self.setCentralWidget(root)
        self.set_default_output()

    def set_default_output(self):
        mes = int(self.cmb_mes.currentData())
        anio = int(self.sp_anio.value())
        self.ed_out.setText(str(Path.cwd() / f"Cruce_Flujo_Caja_{anio}_{mes:02d}_v40_final.xlsx"))

    def pick_output(self):
        path, _ = QFileDialog.getSaveFileName(self, "Guardar Excel", "", "Excel (*.xlsx)")
        if path:
            if not path.lower().endswith(".xlsx"):
                path += ".xlsx"
            self.ed_out.setText(path)

    def log_line(self, msg: str):
        self.log.append(msg)

    def build_config(self) -> RunConfig:
        mes = int(self.cmb_mes.currentData())
        anio = int(self.sp_anio.value())

        banc = BankFiles()
        banc.bancolombia_csv = self.fp_bancolombia.get()
        banc.fiducia_csv = self.fp_fiducia.get()
        banc.davivienda_xls = self.fp_davivienda.get()
        banc.bbva_xls = self.fp_bbva.get()

        rend = self.ed_fid_rend.text().strip()
        banc.fiducia_rendimientos = float(rend) if rend else None

        if self.rb_bog_mov.isChecked():
            banc.bogota_mov_xls = self.fp_bog_mov.get()
        else:
            banc.bogota_inf_csv = self.fp_bog_inf.get()
            banc.bogota_inf_year = int(self.sp_bog_inf_year.value())

        if self.rb_agr_mov.isChecked():
            banc.agrario_mov_xls = self.fp_agr_mov.get()
        else:
            banc.agrario_inf_xls = self.fp_agr_inf.get()

        cont = AccountingFiles(
            balance_prueba_xlsx=self.fp_balance.get(),
            reporte_comprobantes_xlsx=self.fp_reporte.get(),
            libro_auxiliar_xlsx=self.fp_libro.get()
        )

        otros = OtherFiles(
            aplicativo_xlsx=self.fp_aplicativo.get(),
            criterios_bancarios_xlsx=self.fp_criterios.get()
        )

        cfg = RunConfig(
            mes=mes,
            anio=anio,
            tol_nomina_days=int(self.sp_tol_nom.value()),
            tol_general_days=int(self.sp_tol_gen.value()),
            tol_bbva_sum_days=int(self.sp_tol_bbva.value()),
            bancos=banc,
            contables=cont,
            otros=otros,
            salida_xlsx=self.ed_out.text().strip()
        )
        return cfg

    def validate(self, cfg: RunConfig):
        if not cfg.salida_xlsx:
            raise ValueError("Debe seleccionar salida .xlsx")

        if self.rb_bog_mov.isChecked() and not cfg.bancos.bogota_mov_xls:
            raise ValueError("Bogotá Movimiento seleccionado pero sin archivo.")
        if self.rb_bog_inf.isChecked() and (not cfg.bancos.bogota_inf_csv or not cfg.bancos.bogota_inf_year):
            raise ValueError("Bogotá Informe requiere archivo y año.")

        if self.rb_agr_mov.isChecked() and not cfg.bancos.agrario_mov_xls:
            raise ValueError("Agrario Movimiento seleccionado pero sin archivo.")
        if self.rb_agr_inf.isChecked() and not cfg.bancos.agrario_inf_xls:
            raise ValueError("Agrario Informe seleccionado pero sin archivo.")

        if cfg.bancos.fiducia_csv and cfg.bancos.fiducia_rendimientos is None:
            raise ValueError("Fiducia cargada pero falta rendimientos.")

        if not cfg.contables.libro_auxiliar_xlsx:
            raise ValueError("Falta Libro Auxiliar.")
        if not cfg.contables.balance_prueba_xlsx:
            raise ValueError("Falta Balance de prueba.")

    def on_run(self):
        self.log.clear()
        try:
            cfg = self.build_config()
            self.validate(cfg)

            self.log_line("Iniciando conciliación…")
            sheets = run_pipeline(cfg, self.log_line)
            save_excel(cfg.salida_xlsx, sheets)

            self.log_line(f"✅ Excel generado: {cfg.salida_xlsx}")
            QMessageBox.information(self, "OK", f"Excel generado:\n{cfg.salida_xlsx}")

        except Exception as e:
            self.log_line(f"❌ Error: {e}")
            QMessageBox.critical(self, "Error", str(e))


def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

