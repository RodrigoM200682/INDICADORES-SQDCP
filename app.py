import sqlite3
import unicodedata
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="SQDCP | FMDS", page_icon="📊", layout="wide")

DB_DIR = Path("data")
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "sqdcp_fmds.db"

MESES = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
]

CSS = """
<style>
.block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
.main-title {font-size: 2rem; font-weight: 800; color: #111827; margin-bottom: 0.2rem;}
.subtitle {color: #4b5563; margin-bottom: 1.0rem;}
.kpi-card {background: #ffffff; border: 1px solid #e5e7eb; border-radius: 18px; padding: 14px; box-shadow: 0 6px 18px rgba(15,23,42,.06);}
.kpi-title {font-weight: 800; font-size: 1.0rem; color: #111827; text-align: center; margin-bottom: 0.2rem;}
.kpi-unit {font-size: .82rem; color: #6b7280; text-align: center; margin-top: -0.4rem; margin-bottom: .2rem;}
.section-title {font-size: 1.15rem; font-weight: 800; margin-top: 1.2rem; margin-bottom: .4rem;}
.small-note {color:#6b7280; font-size: .85rem;}
.stButton button {border-radius: 10px; font-weight: 700;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


def norm_col(text):
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.replace("%", "pct").replace("/", "_").replace("-", "_")
    for ch in [" ", ".", "(", ")"]:
        text = text.replace(ch, "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")

COLUMN_ALIASES = {
    "data": "data",
    "dia": "data",
    "mes": "mes",
    "mês": "mes",
    "semana": "semana",
    "turno": "turno",
    "acidente": "acidentes",
    "acidentes": "acidentes",
    "reclamacao": "reclamacoes",
    "reclamacoes": "reclamacoes",
    "reclamações": "reclamacoes",
    "perda_prensas": "perda_prensas_ton",
    "perda_prensas_ton": "perda_prensas_ton",
    "perda_prensas_tonelada": "perda_prensas_ton",
    "perda_litografia": "perda_litografia_ton",
    "perda_litografia_ton": "perda_litografia_ton",
    "perda_litografia_tonelada": "perda_litografia_ton",
    "perda_montagem": "perda_montagem_ton",
    "perda_montagem_ton": "perda_montagem_ton",
    "perda_montagem_tonelada": "perda_montagem_ton",
    "atendimento_no_prazo": "atendimento_prazo_pct",
    "atendimento_prazo": "atendimento_prazo_pct",
    "atendimento_prazo_pct": "atendimento_prazo_pct",
    "eficiencia_prensas": "eficiencia_prensas_pct",
    "eficiencia_prensas_pct": "eficiencia_prensas_pct",
    "eficiencia_litografia": "eficiencia_litografia_pct",
    "eficiencia_litografia_pct": "eficiencia_litografia_pct",
    "eficiencia_montagem": "eficiencia_montagem_pct",
    "eficiencia_montagem_pct": "eficiencia_montagem_pct",
}

DATA_COLS = [
    "data", "mes", "semana", "turno", "acidentes", "reclamacoes",
    "perda_prensas_ton", "perda_litografia_ton", "perda_montagem_ton",
    "atendimento_prazo_pct", "eficiencia_prensas_pct", "eficiencia_litografia_pct", "eficiencia_montagem_pct"
]

NUMERIC_COLS = [
    "semana", "acidentes", "reclamacoes", "perda_prensas_ton", "perda_litografia_ton", "perda_montagem_ton",
    "atendimento_prazo_pct", "eficiencia_prensas_pct", "eficiencia_litografia_pct", "eficiencia_montagem_pct"
]


def connect():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    with connect() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS lancamentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT,
                mes TEXT,
                semana INTEGER,
                turno TEXT,
                acidentes REAL DEFAULT 0,
                reclamacoes REAL DEFAULT 0,
                perda_prensas_ton REAL DEFAULT 0,
                perda_litografia_ton REAL DEFAULT 0,
                perda_montagem_ton REAL DEFAULT 0,
                atendimento_prazo_pct REAL DEFAULT 0,
                eficiencia_prensas_pct REAL DEFAULT 0,
                eficiencia_litografia_pct REAL DEFAULT 0,
                eficiencia_montagem_pct REAL DEFAULT 0,
                criado_em TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS acoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indicador TEXT NOT NULL,
                descricao TEXT,
                responsavel TEXT,
                prazo TEXT,
                status TEXT DEFAULT 'Aberta',
                criado_em TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def load_data():
    with connect() as con:
        return pd.read_sql_query("SELECT * FROM lancamentos ORDER BY data DESC, id DESC", con)


def load_actions(indicador=None):
    with connect() as con:
        if indicador:
            return pd.read_sql_query("SELECT id, descricao, responsavel, prazo, status FROM acoes WHERE indicador=? ORDER BY prazo, id DESC", con, params=(indicador,))
        return pd.read_sql_query("SELECT * FROM acoes ORDER BY indicador, prazo", con)


def insert_rows(df):
    df = df.copy()
    for col in DATA_COLS:
        if col not in df.columns:
            df[col] = 0 if col in NUMERIC_COLS else ""
    df = df[DATA_COLS]
    df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    df["data"] = df["data"].fillna(date.today().isoformat())
    df["mes"] = df["mes"].replace("", pd.NA)
    df["mes"] = df["mes"].fillna(pd.to_datetime(df["data"], errors="coerce").dt.month.map(lambda m: MESES[int(m)-1] if pd.notna(m) else ""))
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["atendimento_prazo_pct", "eficiencia_prensas_pct", "eficiencia_litografia_pct", "eficiencia_montagem_pct"]:
        df[col] = df[col].clip(0, 100)
    with connect() as con:
        df.to_sql("lancamentos", con, if_exists="append", index=False)


def clear_database():
    with connect() as con:
        con.execute("DELETE FROM lancamentos")
        con.execute("DELETE FROM acoes")
        con.execute("DELETE FROM sqlite_sequence WHERE name IN ('lancamentos','acoes')")


def normalize_import(df):
    rename = {}
    for col in df.columns:
        key = norm_col(col)
        rename[col] = COLUMN_ALIASES.get(key, key)
    df = df.rename(columns=rename)
    keep = [c for c in DATA_COLS if c in df.columns]
    return df[keep].copy()


def make_template():
    return pd.DataFrame([{c: "" for c in DATA_COLS}])


def gauge(title, value, max_value, suffix="", danger_high=False):
    if pd.isna(value):
        value = 0
    if danger_high:
        steps = [
            {"range": [0, max_value * 0.33], "color": "#dcfce7"},
            {"range": [max_value * 0.33, max_value * 0.66], "color": "#fef9c3"},
            {"range": [max_value * 0.66, max_value], "color": "#fee2e2"},
        ]
    else:
        steps = [
            {"range": [0, max_value * 0.60], "color": "#fee2e2"},
            {"range": [max_value * 0.60, max_value * 0.85], "color": "#fef9c3"},
            {"range": [max_value * 0.85, max_value], "color": "#dcfce7"},
        ]
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=float(value),
        number={"suffix": suffix, "font": {"size": 28}},
        gauge={
            "axis": {"range": [0, max_value], "tickwidth": 1},
            "bar": {"color": "#1f2937"},
            "bgcolor": "white",
            "borderwidth": 1,
            "bordercolor": "#e5e7eb",
            "steps": steps,
        },
        title={"text": title, "font": {"size": 16}},
    ))
    fig.update_layout(height=230, margin=dict(l=8, r=8, t=44, b=8), paper_bgcolor="rgba(0,0,0,0)")
    return fig


def filtered_data(df, mes, semana):
    out = df.copy()
    if mes != "Todos":
        out = out[out["mes"] == mes]
    if semana != "Todas":
        out = out[out["semana"] == int(semana)]
    return out


def calc_indicators(df):
    if df.empty:
        return {
            "acidentes": 0, "reclamacoes": 0, "perda_total": 0,
            "atendimento": 0, "eficiencia": 0,
            "perdas_area": {"Prensas": 0, "Litografia": 0, "Montagem": 0},
            "ef_area": {"Prensas": 0, "Litografia": 0, "Montagem": 0},
        }
    return {
        "acidentes": df["acidentes"].sum(),
        "reclamacoes": df["reclamacoes"].sum(),
        "perda_total": df[["perda_prensas_ton", "perda_litografia_ton", "perda_montagem_ton"]].sum().sum(),
        "atendimento": df["atendimento_prazo_pct"].mean(),
        "eficiencia": df[["eficiencia_prensas_pct", "eficiencia_litografia_pct", "eficiencia_montagem_pct"]].mean().mean(),
        "perdas_area": {
            "Prensas": df["perda_prensas_ton"].sum(),
            "Litografia": df["perda_litografia_ton"].sum(),
            "Montagem": df["perda_montagem_ton"].sum(),
        },
        "ef_area": {
            "Prensas": df["eficiencia_prensas_pct"].mean(),
            "Litografia": df["eficiencia_litografia_pct"].mean(),
            "Montagem": df["eficiencia_montagem_pct"].mean(),
        },
    }


def actions_editor(indicador):
    st.markdown(f"<div class='small-note'>Ações — {indicador}</div>", unsafe_allow_html=True)
    existing = load_actions(indicador)
    display = existing[["descricao", "responsavel", "prazo", "status"]].copy() if not existing.empty else pd.DataFrame(columns=["descricao", "responsavel", "prazo", "status"])
    edited = st.data_editor(
        display,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        key=f"acoes_{indicador}",
        column_config={
            "descricao": st.column_config.TextColumn("Descrição", width="large"),
            "responsavel": st.column_config.TextColumn("Responsável"),
            "prazo": st.column_config.DateColumn("Prazo", format="DD/MM/YYYY"),
            "status": st.column_config.SelectboxColumn("Status", options=["Aberta", "Em andamento", "Concluída"]),
        },
    )
    if st.button("Salvar ações", key=f"salvar_{indicador}"):
        with connect() as con:
            con.execute("DELETE FROM acoes WHERE indicador=?", (indicador,))
            for _, row in edited.dropna(how="all").iterrows():
                if str(row.get("descricao", "")).strip() or str(row.get("responsavel", "")).strip():
                    prazo = row.get("prazo", "")
                    if pd.notna(prazo) and prazo != "":
                        prazo = pd.to_datetime(prazo).date().isoformat()
                    else:
                        prazo = ""
                    con.execute(
                        "INSERT INTO acoes (indicador, descricao, responsavel, prazo, status) VALUES (?, ?, ?, ?, ?)",
                        (indicador, row.get("descricao", ""), row.get("responsavel", ""), prazo, row.get("status", "Aberta")),
                    )
        st.success("Ações salvas.")


init_db()

st.markdown("<div class='main-title'>Painel SQDCP / FMDS</div>", unsafe_allow_html=True)
st.markdown("<div class='subtitle'>Gestão visual simples: indicador, desvio e ação no mesmo painel.</div>", unsafe_allow_html=True)

with st.sidebar:
    st.header("Filtros")
    data_all = load_data()
    meses_disponiveis = [m for m in MESES if not data_all.empty and m in data_all["mes"].dropna().unique().tolist()]
    mes = st.selectbox("Mês", ["Todos"] + meses_disponiveis if meses_disponiveis else ["Todos"] + MESES)
    semanas = sorted([int(x) for x in data_all["semana"].dropna().unique().tolist()]) if not data_all.empty else list(range(1, 7))
    semana = st.selectbox("Semana", ["Todas"] + semanas)
    st.divider()
    st.header("Importar Excel")
    template = make_template()
    st.download_button(
        "Baixar modelo Excel",
        data=template.to_excel(index=False, engine="openpyxl"),
        file_name="modelo_importacao_sqdcp.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    uploaded = st.file_uploader("Selecionar arquivo .xlsx", type=["xlsx"])
    if uploaded is not None:
        try:
            imp = pd.read_excel(uploaded)
            imp = normalize_import(imp)
            st.caption(f"Linhas identificadas: {len(imp)}")
            st.dataframe(imp.head(10), use_container_width=True, hide_index=True)
            if st.button("Importar para a base"):
                insert_rows(imp)
                st.success("Dados importados com sucesso.")
                st.rerun()
        except Exception as e:
            st.error(f"Não foi possível importar o arquivo: {e}")
    st.divider()
    st.header("Base de dados")
    st.caption("Exclusão sem senha. Marque a confirmação antes de apagar.")
    confirmar = st.checkbox("Confirmo que desejo excluir toda a base")
    if st.button("Excluir toda a base", type="primary", disabled=not confirmar):
        clear_database()
        st.success("Base excluída.")
        st.rerun()

with st.expander("Lançamento em massa manual", expanded=False):
    base = pd.DataFrame([
        {
            "data": date.today(), "mes": MESES[date.today().month - 1], "semana": 1, "turno": "1º",
            "acidentes": 0, "reclamacoes": 0, "perda_prensas_ton": 0.0, "perda_litografia_ton": 0.0, "perda_montagem_ton": 0.0,
            "atendimento_prazo_pct": 100, "eficiencia_prensas_pct": 0, "eficiencia_litografia_pct": 0, "eficiencia_montagem_pct": 0,
        }
    ])
    edited = st.data_editor(
        base,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
            "mes": st.column_config.SelectboxColumn("Mês", options=MESES),
            "semana": st.column_config.NumberColumn("Semana", min_value=1, max_value=6, step=1),
            "turno": st.column_config.SelectboxColumn("Turno", options=["1º", "2º", "3º"]),
            "acidentes": st.column_config.NumberColumn("Acidentes", min_value=0, step=1),
            "reclamacoes": st.column_config.NumberColumn("Reclamações", min_value=0, step=1),
            "perda_prensas_ton": st.column_config.NumberColumn("Perda Prensas t", min_value=0.0, step=0.01, format="%.3f"),
            "perda_litografia_ton": st.column_config.NumberColumn("Perda Litografia t", min_value=0.0, step=0.01, format="%.3f"),
            "perda_montagem_ton": st.column_config.NumberColumn("Perda Montagem t", min_value=0.0, step=0.01, format="%.3f"),
            "atendimento_prazo_pct": st.column_config.NumberColumn("Atend. prazo %", min_value=0, max_value=100, step=1),
            "eficiencia_prensas_pct": st.column_config.NumberColumn("Ef. Prensas %", min_value=0, max_value=100, step=1),
            "eficiencia_litografia_pct": st.column_config.NumberColumn("Ef. Litografia %", min_value=0, max_value=100, step=1),
            "eficiencia_montagem_pct": st.column_config.NumberColumn("Ef. Montagem %", min_value=0, max_value=100, step=1),
        },
    )
    if st.button("Salvar lançamentos"):
        insert_rows(edited)
        st.success("Lançamentos salvos.")
        st.rerun()

df = load_data()
dff = filtered_data(df, mes, semana) if not df.empty else df
ind = calc_indicators(dff)

st.markdown("<div class='section-title'>Indicadores principais</div>", unsafe_allow_html=True)
cols = st.columns(5)
max_acidentes = max(5, ind["acidentes"] * 1.2)
max_reclamacoes = max(10, ind["reclamacoes"] * 1.2)
max_perda = max(1, ind["perda_total"] * 1.2)
with cols[0]:
    st.plotly_chart(gauge("Acidentes", ind["acidentes"], max_acidentes, " un", True), use_container_width=True)
with cols[1]:
    st.plotly_chart(gauge("Reclamações", ind["reclamacoes"], max_reclamacoes, " un", True), use_container_width=True)
with cols[2]:
    st.plotly_chart(gauge("Perda", ind["perda_total"], max_perda, " t", True), use_container_width=True)
with cols[3]:
    st.plotly_chart(gauge("Atend. Prazo", ind["atendimento"], 100, "%", False), use_container_width=True)
with cols[4]:
    st.plotly_chart(gauge("Eficiência", ind["eficiencia"], 100, "%", False), use_container_width=True)

st.markdown("<div class='section-title'>Ações por indicador</div>", unsafe_allow_html=True)
tabs = st.tabs(["Acidentes", "Reclamações", "Perda", "Atendimento no Prazo", "Eficiência"])
for tab, indicador in zip(tabs, ["Acidentes", "Reclamações", "Perda", "Atendimento no Prazo", "Eficiência"]):
    with tab:
        actions_editor(indicador)

st.markdown("<div class='section-title'>Detalhamento por área</div>", unsafe_allow_html=True)
c1, c2 = st.columns(2)
with c1:
    perda_area = pd.DataFrame({"Área": list(ind["perdas_area"].keys()), "Perda t": list(ind["perdas_area"].values())})
    st.bar_chart(perda_area.set_index("Área"), use_container_width=True)
with c2:
    ef_area = pd.DataFrame({"Área": list(ind["ef_area"].keys()), "Eficiência %": list(ind["ef_area"].values())}).fillna(0)
    st.bar_chart(ef_area.set_index("Área"), use_container_width=True)

with st.expander("Ver base filtrada", expanded=False):
    st.dataframe(dff, use_container_width=True, hide_index=True)
    csv = dff.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Baixar base filtrada em CSV", csv, "base_sqdcp_filtrada.csv", "text/csv")
