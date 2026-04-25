from __future__ import annotations

import sqlite3
from io import BytesIO
from pathlib import Path
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

APP_TITLE = "SQDCP | FMDS"
DB_PATH = Path("data") / "sqdcp.db"
DB_PATH.parent.mkdir(exist_ok=True)

INDICADORES = {
    "Acidentes": {"unidade": "un", "tipo": "menor_melhor", "meta": 0, "max": 5},
    "Reclamações": {"unidade": "un", "tipo": "menor_melhor", "meta": 0, "max": 10},
    "Perda": {"unidade": "ton", "tipo": "menor_melhor", "meta": 0, "max": 10},
    "Atendimento no prazo": {"unidade": "%", "tipo": "maior_melhor", "meta": 95, "max": 100},
    "Eficiência": {"unidade": "%", "tipo": "maior_melhor", "meta": 85, "max": 100},
}

COLUNAS_MODELO = [
    "data",
    "acidentes",
    "reclamacoes",
    "perda_prensas",
    "perda_litografia",
    "perda_montagem",
    "atendimento_prazo",
    "eficiencia_prensas",
    "eficiencia_litografia",
    "eficiencia_montagem",
]

st.set_page_config(page_title=APP_TITLE, layout="wide", initial_sidebar_state="collapsed")

st.markdown(
    """
    <style>
    .main .block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1500px;}
    .titulo {font-size: 34px; font-weight: 800; color: #111827; margin-bottom: 0px;}
    .subtitulo {font-size: 15px; color: #6b7280; margin-bottom: 16px;}
    .card {border: 1px solid #e5e7eb; border-radius: 18px; padding: 14px; background: #ffffff; box-shadow: 0 1px 5px rgba(0,0,0,0.04);}
    .secao {font-size: 18px; font-weight: 800; color: #111827; margin-top: 10px; margin-bottom: 8px;}
    .indicador-label {font-size: 16px; font-weight: 800; text-align:center; margin-top: -8px;}
    .indicador-meta {font-size: 12px; color: #6b7280; text-align:center; margin-bottom: 8px;}
    div[data-testid="stMetricValue"] {font-size: 26px;}
    </style>
    """,
    unsafe_allow_html=True,
)


def conectar() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lancamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            acidentes REAL DEFAULT 0,
            reclamacoes REAL DEFAULT 0,
            perda_prensas REAL DEFAULT 0,
            perda_litografia REAL DEFAULT 0,
            perda_montagem REAL DEFAULT 0,
            atendimento_prazo REAL DEFAULT 0,
            eficiencia_prensas REAL DEFAULT 0,
            eficiencia_litografia REAL DEFAULT 0,
            eficiencia_montagem REAL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS acoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicador TEXT NOT NULL,
            descricao TEXT,
            responsavel TEXT,
            prazo TEXT,
            status TEXT DEFAULT 'Aberta'
        )
        """
    )
    conn.commit()
    return conn


def carregar_lancamentos() -> pd.DataFrame:
    with conectar() as conn:
        df = pd.read_sql_query("SELECT * FROM lancamentos", conn)
    if df.empty:
        return pd.DataFrame(columns=["id"] + COLUNAS_MODELO)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df.dropna(subset=["data"])


def carregar_acoes(indicador: str) -> pd.DataFrame:
    with conectar() as conn:
        df = pd.read_sql_query(
            "SELECT id, descricao, responsavel, prazo, status FROM acoes WHERE indicador = ? ORDER BY id DESC",
            conn,
            params=(indicador,),
        )
    if not df.empty:
        df["prazo"] = pd.to_datetime(df["prazo"], errors="coerce").dt.date
    return df


def salvar_lancamentos(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = padronizar_planilha(df)
    with conectar() as conn:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO lancamentos (
                    data, acidentes, reclamacoes, perda_prensas, perda_litografia, perda_montagem,
                    atendimento_prazo, eficiencia_prensas, eficiencia_litografia, eficiencia_montagem
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(row[col] for col in COLUNAS_MODELO),
            )
        conn.commit()
    return len(df)


def padronizar_planilha(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    faltantes = [c for c in COLUNAS_MODELO if c not in df.columns]
    if faltantes:
        raise ValueError("Colunas obrigatórias ausentes: " + ", ".join(faltantes))
    df = df[COLUNAS_MODELO]
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    if df["data"].isna().any():
        raise ValueError("Existem datas inválidas na coluna 'data'.")
    for col in COLUNAS_MODELO[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["data"] = df["data"].dt.strftime("%Y-%m-%d")
    return df


def gerar_modelo_excel() -> bytes:
    template = pd.DataFrame(
        {
            "data": [date.today()],
            "acidentes": [0],
            "reclamacoes": [0],
            "perda_prensas": [0.0],
            "perda_litografia": [0.0],
            "perda_montagem": [0.0],
            "atendimento_prazo": [95.0],
            "eficiencia_prensas": [85.0],
            "eficiencia_litografia": [85.0],
            "eficiencia_montagem": [85.0],
        }
    )
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        template.to_excel(writer, index=False, sheet_name="LANÇAMENTOS")
    buffer.seek(0)
    return buffer.getvalue()


def criar_gauge(titulo: str, valor: float, unidade: str, maximo: float, meta: float, tipo: str) -> go.Figure:
    if tipo == "maior_melhor":
        cor = "#16a34a" if valor >= meta else "#dc2626"
        steps = [{"range": [0, meta], "color": "#fee2e2"}, {"range": [meta, maximo], "color": "#dcfce7"}]
    else:
        limite_alerta = max(meta + 1, maximo * 0.25)
        cor = "#16a34a" if valor <= meta else "#dc2626"
        steps = [{"range": [0, limite_alerta], "color": "#dcfce7"}, {"range": [limite_alerta, maximo], "color": "#fee2e2"}]
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=float(valor),
            number={"suffix": f" {unidade}", "font": {"size": 26}},
            gauge={
                "axis": {"range": [0, maximo], "tickwidth": 1},
                "bar": {"color": cor, "thickness": 0.28},
                "bgcolor": "white",
                "borderwidth": 0,
                "steps": steps,
                "threshold": {"line": {"color": "#111827", "width": 3}, "thickness": 0.8, "value": meta},
            },
        )
    )
    fig.update_layout(height=210, margin=dict(l=12, r=12, t=20, b=8), paper_bgcolor="white", font={"color": "#111827"})
    return fig


def resumo_indicadores(df: pd.DataFrame) -> dict[str, float]:
    if df.empty:
        return {"Acidentes": 0, "Reclamações": 0, "Perda": 0, "Atendimento no prazo": 0, "Eficiência": 0}
    perda = df[["perda_prensas", "perda_litografia", "perda_montagem"]].sum().sum()
    eficiencia = df[["eficiencia_prensas", "eficiencia_litografia", "eficiencia_montagem"]].mean().mean()
    return {
        "Acidentes": df["acidentes"].sum(),
        "Reclamações": df["reclamacoes"].sum(),
        "Perda": perda,
        "Atendimento no prazo": df["atendimento_prazo"].mean(),
        "Eficiência": eficiencia,
    }


def filtro_mes_semana(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["mes"] = df["data"].dt.month
    df["semana"] = df["data"].dt.isocalendar().week.astype(int)
    meses = sorted(df["mes"].unique().tolist())
    mes_sel = st.selectbox("Mês", meses, format_func=lambda m: f"{m:02d}")
    semanas = sorted(df.loc[df["mes"] == mes_sel, "semana"].unique().tolist())
    semana_opts = ["Todas"] + semanas
    semana_sel = st.selectbox("Semana", semana_opts)
    filtrado = df[df["mes"] == mes_sel]
    if semana_sel != "Todas":
        filtrado = filtrado[filtrado["semana"] == semana_sel]
    return filtrado


def painel_acoes(indicador: str) -> None:
    st.markdown("<div class='indicador-meta'>Ações do indicador</div>", unsafe_allow_html=True)
    acoes = carregar_acoes(indicador)
    if acoes.empty:
        base = pd.DataFrame([{"descricao": "", "responsavel": "", "prazo": date.today(), "status": "Aberta"}])
    else:
        base = acoes[["descricao", "responsavel", "prazo", "status"]]
    editado = st.data_editor(
        base,
        key=f"acoes_{indicador}",
        hide_index=True,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "descricao": st.column_config.TextColumn("Descrição"),
            "responsavel": st.column_config.TextColumn("Responsável"),
            "prazo": st.column_config.DateColumn("Prazo", format="DD/MM/YYYY"),
            "status": st.column_config.SelectboxColumn("Status", options=["Aberta", "Em andamento", "Concluída"]),
        },
    )
    if st.button("Salvar ações", key=f"salvar_{indicador}", use_container_width=True):
        editado = editado.fillna("")
        with conectar() as conn:
            conn.execute("DELETE FROM acoes WHERE indicador = ?", (indicador,))
            for _, row in editado.iterrows():
                if str(row.get("descricao", "")).strip():
                    prazo = row.get("prazo", "")
                    conn.execute(
                        "INSERT INTO acoes (indicador, descricao, responsavel, prazo, status) VALUES (?, ?, ?, ?, ?)",
                        (indicador, row.get("descricao", ""), row.get("responsavel", ""), str(prazo), row.get("status", "Aberta")),
                    )
            conn.commit()
        st.success("Ações salvas.")
        st.rerun()


st.markdown(f"<div class='titulo'>{APP_TITLE}</div>", unsafe_allow_html=True)
st.markdown("<div class='subtitulo'>Gestão visual simples para rotina FMDS: ver o desvio, registrar ação e acompanhar o fechamento.</div>", unsafe_allow_html=True)

with st.expander("Lançamento em massa e importação Excel", expanded=False):
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        st.download_button(
            "Baixar modelo Excel",
            data=gerar_modelo_excel(),
            file_name="modelo_importacao_sqdcp.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with c2:
        upload = st.file_uploader("Importar planilha .xlsx", type=["xlsx"])
    with c3:
        st.write("")
        st.write("")
        apagar = st.checkbox("Confirmo a exclusão total da base")
        if st.button("Excluir base de dados", disabled=not apagar, use_container_width=True):
            with conectar() as conn:
                conn.execute("DELETE FROM lancamentos")
                conn.execute("DELETE FROM acoes")
                conn.commit()
            st.success("Base excluída.")
            st.rerun()
    if upload is not None:
        try:
            df_import = pd.read_excel(upload, engine="openpyxl")
            qtd = salvar_lancamentos(df_import)
            st.success(f"Importação concluída: {qtd} linhas incluídas.")
            st.rerun()
        except Exception as e:
            st.error(f"Erro na importação: {e}")

    st.markdown("**Lançamento manual em massa**")
    base_manual = pd.DataFrame(
        [{
            "data": date.today(), "acidentes": 0, "reclamacoes": 0, "perda_prensas": 0.0,
            "perda_litografia": 0.0, "perda_montagem": 0.0, "atendimento_prazo": 95.0,
            "eficiencia_prensas": 85.0, "eficiencia_litografia": 85.0, "eficiencia_montagem": 85.0,
        }]
    )
    manual = st.data_editor(
        base_manual,
        key="manual",
        hide_index=True,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
            "acidentes": st.column_config.NumberColumn("Acidentes", min_value=0, step=1),
            "reclamacoes": st.column_config.NumberColumn("Reclamações", min_value=0, step=1),
            "perda_prensas": st.column_config.NumberColumn("Perda Prensas", min_value=0.0, step=0.01, format="%.2f"),
            "perda_litografia": st.column_config.NumberColumn("Perda Litografia", min_value=0.0, step=0.01, format="%.2f"),
            "perda_montagem": st.column_config.NumberColumn("Perda Montagem", min_value=0.0, step=0.01, format="%.2f"),
            "atendimento_prazo": st.column_config.NumberColumn("Atendimento %", min_value=0.0, max_value=100.0, step=0.1, format="%.1f"),
            "eficiencia_prensas": st.column_config.NumberColumn("Ef. Prensas %", min_value=0.0, max_value=100.0, step=0.1, format="%.1f"),
            "eficiencia_litografia": st.column_config.NumberColumn("Ef. Litografia %", min_value=0.0, max_value=100.0, step=0.1, format="%.1f"),
            "eficiencia_montagem": st.column_config.NumberColumn("Ef. Montagem %", min_value=0.0, max_value=100.0, step=0.1, format="%.1f"),
        },
    )
    if st.button("Salvar lançamentos manuais", use_container_width=True):
        try:
            qtd = salvar_lancamentos(manual)
            st.success(f"{qtd} lançamentos salvos.")
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")

dados = carregar_lancamentos()
if dados.empty:
    st.info("Ainda não existem dados. Faça um lançamento manual ou importe o modelo Excel.")
    st.stop()

f1, f2 = st.columns([1, 5])
with f1:
    dados_filtrados = filtro_mes_semana(dados)

resumo = resumo_indicadores(dados_filtrados)

st.markdown("<div class='secao'>Painel principal</div>", unsafe_allow_html=True)
cols = st.columns(5)
for col, indicador in zip(cols, INDICADORES.keys()):
    cfg = INDICADORES[indicador]
    valor = resumo[indicador]
    maximo = max(cfg["max"], valor * 1.2 if valor > 0 else cfg["max"])
    with col:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.plotly_chart(criar_gauge(indicador, valor, cfg["unidade"], maximo, cfg["meta"], cfg["tipo"]), use_container_width=True)
        st.markdown(f"<div class='indicador-label'>{indicador}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='indicador-meta'>Meta: {cfg['meta']} {cfg['unidade']}</div>", unsafe_allow_html=True)
        painel_acoes(indicador)
        st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div class='secao'>Detalhamento por área</div>", unsafe_allow_html=True)
c1, c2 = st.columns(2)
with c1:
    perdas = pd.DataFrame({
        "Área": ["Prensas", "Litografia", "Montagem"],
        "Perda (ton)": [
            dados_filtrados["perda_prensas"].sum(),
            dados_filtrados["perda_litografia"].sum(),
            dados_filtrados["perda_montagem"].sum(),
        ],
    })
    st.dataframe(perdas, hide_index=True, use_container_width=True)
with c2:
    efic = pd.DataFrame({
        "Área": ["Prensas", "Litografia", "Montagem"],
        "Eficiência (%)": [
            dados_filtrados["eficiencia_prensas"].mean(),
            dados_filtrados["eficiencia_litografia"].mean(),
            dados_filtrados["eficiencia_montagem"].mean(),
        ],
    }).fillna(0)
    st.dataframe(efic, hide_index=True, use_container_width=True)
