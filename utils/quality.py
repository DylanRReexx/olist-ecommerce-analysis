import pandas as pd
from utils.logger import get_logger

logger = get_logger("quality")


def check_nulls(df: pd.DataFrame, columnas: list, tabla: str):
    """Verifica que no haya nulos en columnas clave."""
    for col in columnas:
        nulls = df[col].isnull().sum()
        if nulls > 0:
            logger.error(f"{tabla} | {col} tiene {nulls} valores nulos")
        else:
            logger.info(f"{tabla} | {col} — sin nulos ✓")


def check_duplicates(df: pd.DataFrame, columna: str, tabla: str):
    """Verifica que no haya duplicados en una columna clave."""
    duplicados = df[columna].duplicated().sum()
    if duplicados > 0:
        logger.error(f"{tabla} | {columna} tiene {duplicados} duplicados")
    else:
        logger.info(f"{tabla} | {columna} — sin duplicados ✓")


def check_range(df: pd.DataFrame, columna: str, tabla: str, min_val=None, max_val=None):
    """Verifica que los valores de una columna estén dentro de un rango válido."""
    if min_val is not None:
        fuera = (df[columna] < min_val).sum()
        if fuera > 0:
            logger.error(f"{tabla} | {columna} tiene {fuera} valores menores a {min_val}")
        else:
            logger.info(f"{tabla} | {columna} >= {min_val} ✓")

    if max_val is not None:
        fuera = (df[columna] > max_val).sum()
        if fuera > 0:
            logger.error(f"{tabla} | {columna} tiene {fuera} valores mayores a {max_val}")
        else:
            logger.info(f"{tabla} | {columna} <= {max_val} ✓")


def check_row_count(df: pd.DataFrame, tabla: str, min_rows: int):
    """Verifica que el DataFrame tenga al menos un mínimo de filas."""
    total = len(df)
    if total < min_rows:
        logger.error(f"{tabla} | solo {total} filas, esperaba al menos {min_rows}")
    else:
        logger.info(f"{tabla} | {total} filas cargadas ✓")