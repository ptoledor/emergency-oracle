"""
Carga feriados chilenos desde el Excel y agrega Semana Santa con calculo correcto de Pascua.
"""
import re
import pandas as pd
from dateutil.easter import easter

MESES_ES = {
    'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
    'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12
}

# Feriados cuya fecha cambia por año — se excluyen del parseo regex y se calculan aparte
FERIADOS_MOVILES_KEYWORDS = {'viernes santo', 'sabado santo', 'sábado santo'}


def cargar_feriados(xlsx_path, anios=range(2021, 2027), log_fn=None):
    """
    Retorna un set de datetime.date con todos los feriados chilenos
    para los años indicados.
    """
    fechas = set()

    # ── 1. Feriados de fecha fija (leídos del Excel) ──────────────────────────
    try:
        df = pd.read_excel(xlsx_path, header=1)
        fecha_col = df.columns[0]
        nombre_col = df.columns[1] if len(df.columns) > 1 else None

        for idx, row in df.iterrows():
            texto = str(row[fecha_col]).lower().strip()
            nombre = str(row[nombre_col]).lower().strip() if nombre_col else ''

            # Saltar filas de cabecera secundaria o feriados móviles
            if any(kw in nombre for kw in FERIADOS_MOVILES_KEYWORDS):
                continue
            if any(kw in texto for kw in FERIADOS_MOVILES_KEYWORDS):
                continue
            # Saltar filas sin fecha parseable
            if texto in ('nan', 'todos los días domingos', 'todos los dias domingos',
                         'día', 'dia', 'feriados específicos (aplican a un grupo de personas o región)',
                         'feriados especificos (aplican a un grupo de personas o region)'):
                continue

            m = re.search(r'(\d{1,2})\s+de\s+(\w+)', texto)
            if m:
                dia = int(m.group(1))
                mes = MESES_ES.get(m.group(2).strip())
                if mes:
                    for anio in anios:
                        try:
                            fechas.add(pd.Timestamp(anio, mes, dia).date())
                        except ValueError:
                            pass

    except Exception as e:
        if log_fn:
            log_fn(f"Error leyendo feriados Excel: {e}")

    # ── 2. Semana Santa (Viernes y Sábado Santo) — cálculo correcto ──────────
    for anio in anios:
        pascua = easter(anio)                          # Domingo de Pascua
        viernes_santo = pascua - pd.Timedelta(days=2)
        sabado_santo  = pascua - pd.Timedelta(days=1)
        fechas.add(viernes_santo.date() if hasattr(viernes_santo, 'date') else viernes_santo)
        fechas.add(sabado_santo.date()  if hasattr(sabado_santo,  'date') else sabado_santo)

    if log_fn:
        log_fn(f"Feriados cargados: {len(fechas)} dias "
               f"({len(fechas)//len(list(anios))} aprox/año, Semana Santa calculada correctamente)")
        # Mostrar Viernes Santos como verificacion
        log_fn("  Viernes Santos: " + ", ".join(
            str(easter(a) - pd.Timedelta(days=2))[:10] for a in anios
        ))

    return fechas
