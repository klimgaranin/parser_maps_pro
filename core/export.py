import os
import pandas as pd


def export_xlsx(db, out_path: str, sheet: str, sql_text: str) -> str:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    con = db.connect()
    try:
        df = pd.read_sql_query(sql_text, con)
    finally:
        try:
            con.close()
        except Exception:
            pass

    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as w:
        df.to_excel(w, sheet_name=sheet, index=False)

    return out_path
