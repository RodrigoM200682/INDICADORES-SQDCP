from __future__ import annotations

import base64
import os
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

st.set_page_config(page_title="FMDS SQDCP", page_icon="📊", layout="wide")

# =====================================================
# Configuração de persistência
# =====================================================
LOCAL_DATA_DIR = Path("data")
LOCAL_DATA_DIR.mkdir(exist_ok=True)
LOCAL_DB_FILE = LOCAL_DATA_DIR / "sqdcp_base.xlsx"

DATA_SHEET = "dados"
ACTIONS_SHEET = "acoes"
METAS_SHEET = "metas"

INDICADORES = [
    "Acidentes",
    "Reclamações",
    "Perda",
    "Atendimento no prazo",
    "Eficiência",
]

COLS_DADOS = [
    "data",
    "semana",
    "acidentes_un",
    "reclamacoes_un",
    "perda_prensas_t",
    "perda_litografia_t",
    "perda_montagem_t",
    "atendimento_prazo_pct",
    "eficiencia_prensas_pct",
    "eficiencia_litografia_pct",
    "eficiencia_montagem_pct",
]

COLS_ACOES = [
    "indicador",
    "descricao",
    "responsavel",
    "prazo",
    "status",
]

COLS_METAS = [
    "indicador",
    "meta",
    "unidade",
    "tipo",
]

METAS_PADRAO = pd.DataFrame([
    {"indicador": "Acidentes", "meta": 0.0, "unidade": "un", "tipo": "menor_melhor"},
    {"indicador": "Reclamações", "meta": 0.0, "unidade": "un", "tipo": "menor_melhor"},
    {"indicador": "Perda", "meta": 0.0, "unidade": "t", "tipo": "menor_melhor"},
    {"indicador": "Atendimento no prazo", "meta": 100.0, "unidade": "%", "tipo": "maior_melhor"},
    {"indicador": "Eficiência", "meta": 100.0, "unidade": "%", "tipo": "maior_melhor"},
])


def get_secret(name: str, default: str = "") -> str:
    try:
        return str(st.secrets.get(name, default))
    except Exception:
        return os.getenv(name, default)


GITHUB_TOKEN = get_secret("GITHUB_TOKEN")
GITHUB_REPO = get_secret("GITHUB_REPO")  # exemplo: usuario/repositorio
GITHUB_BRANCH = get_secret("GITHUB_BRANCH", "main")
GITHUB_FILE_PATH = get_secret("GITHUB_FILE_PATH", "data/sqdcp_base.xlsx")


def github_enabled() -> bool:
    return bool(GITHUB_TOKEN and GITHUB_REPO and GITHUB_BRANCH and GITHUB_FILE_PATH)


def github_headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def github_api_url() -> str:
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"


def read_github_file() -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    if not github_enabled():
        return None, None, "Persistência GitHub não configurada. Usando arquivo local temporário."
    try:
        r = requests.get(
            github_api_url(),
            headers=github_headers(),
            params={"ref": GITHUB_BRANCH},
            timeout=20,
        )
        if r.status_code == 404:
            return None, None, None
        r.raise_for_status()
        payload = r.json()
        content = base64.b64decode(payload["content"])
        return content, payload.get("sha"), None
    except Exception as exc:
        return None, None, f"Não foi possível ler a base no GitHub: {exc}"


def write_github_file(file_bytes: bytes, message: str = "Atualiza base SQDCP FMDS") -> Optional[str]:
    if not github_enabled():
        return "Persistência GitHub não configurada. Dados salvos apenas no arquivo local da sessão."
    _, sha, _ = read_github_file()
    payload = {
        "message": message,
        "content": base64.b64encode(file_bytes).decode("utf-8"),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    try:
        r = requests.put(github_api_url(), headers=github_headers(), json=payload, timeout=25)
        r.raise_for_status()
        return None
    except Exception as exc:
        return f"Não foi possível gravar a base no GitHub: {exc}"


def empty_dados() -> pd.DataFrame:
    df = pd.DataFrame(columns=COLS_DADOS)
    df["data"] = pd.to_datetime(df["data"])
    return df


def empty_acoes() -> pd.DataFrame:
    return pd.DataFrame(columns=COLS_ACOES)


def empty_metas() -> pd.DataFrame:
    return METAS_PADRAO.copy()


def normalize_dados(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in COLS_DADOS:
        if col not in df.columns:
            df[col] = 0 if col != "data" else pd.NaT
    df = df[COLS_DADOS]
    df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["data"])
    df["semana"] = pd.to_numeric(df["semana"], errors="coerce").fillna(df["data"].dt.isocalendar().week).astype(int)
    numeric_cols = [c for c in COLS_DADOS if c not in ["data", "semana"]]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def normalize_acoes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in COLS_ACOES:
        if col not in df.columns:
            df[col] = ""
    df = df[COLS_ACOES]
    df["indicador"] = df["indicador"].fillna("").astype(str)
    df["descricao"] = df["descricao"].fillna("").astype(str)
    df["responsavel"] = df["responsavel"].fillna("").astype(str)
    df["prazo"] = pd.to_datetime(df["prazo"], errors="coerce", dayfirst=True)
    df["status"] = df["status"].fillna("Aberta").astype(str)
    return df


def normalize_metas(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return empty_metas()
    df = df.copy()
    for col in COLS_METAS:
        if col not in df.columns:
            df[col] = ""
    df = df[COLS_METAS]
    df["indicador"] = df["indicador"].fillna("").astype(str)
    df["meta"] = pd.to_numeric(df["meta"], errors="coerce").fillna(0.0)
    df["unidade"] = df["unidade"].fillna("").astype(str)
    df["tipo"] = df["tipo"].fillna("").astype(str)

    base = empty_metas()
    atual = df[df["indicador"].isin(INDICADORES)].copy()
    if not atual.empty:
        base = base[~base["indicador"].isin(atual["indicador"])]
        base = pd.concat([base, atual], ignore_index=True)
    base = base.drop_duplicates(subset=["indicador"], keep="last")
    return base[COLS_METAS]


def get_meta(metas: pd.DataFrame, indicador: str, default: float = 0.0) -> float:
    metas = normalize_metas(metas)
    linha = metas[metas["indicador"] == indicador]
    if linha.empty:
        return default
    return float(linha["meta"].iloc[0])


def read_workbook(file_bytes: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio, engine="openpyxl")
    dados = pd.read_excel(xls, DATA_SHEET) if DATA_SHEET in xls.sheet_names else empty_dados()
    acoes = pd.read_excel(xls, ACTIONS_SHEET) if ACTIONS_SHEET in xls.sheet_names else empty_acoes()
    metas = pd.read_excel(xls, METAS_SHEET) if METAS_SHEET in xls.sheet_names else empty_metas()
    return normalize_dados(dados), normalize_acoes(acoes), normalize_metas(metas)


def to_workbook_bytes(dados: pd.DataFrame, acoes: pd.DataFrame, metas: Optional[pd.DataFrame] = None) -> bytes:
    dados = normalize_dados(dados)
    acoes = normalize_acoes(acoes)
    metas = normalize_metas(metas if metas is not None else empty_metas())
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        dados.to_excel(writer, sheet_name=DATA_SHEET, index=False)
        acoes.to_excel(writer, sheet_name=ACTIONS_SHEET, index=False)
        metas.to_excel(writer, sheet_name=METAS_SHEET, index=False)
    output.seek(0)
    return output.getvalue()


def load_base() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    avisos = []
    file_bytes, _, warn = read_github_file()
    if warn:
        avisos.append(warn)
    if file_bytes:
        try:
            LOCAL_DB_FILE.write_bytes(file_bytes)
            dados, acoes, metas = read_workbook(file_bytes)
            return dados, acoes, metas, avisos
        except Exception as exc:
            avisos.append(f"A base no GitHub existe, mas não pôde ser lida: {exc}")
    if LOCAL_DB_FILE.exists():
        try:
            dados, acoes, metas = read_workbook(LOCAL_DB_FILE.read_bytes())
            return dados, acoes, metas, avisos
        except Exception as exc:
            avisos.append(f"A base local não pôde ser lida: {exc}")
    return empty_dados(), empty_acoes(), empty_metas(), avisos


def save_base(dados: pd.DataFrame, acoes: pd.DataFrame, metas: Optional[pd.DataFrame] = None) -> Optional[str]:
    file_bytes = to_workbook_bytes(dados, acoes, metas)
    LOCAL_DB_FILE.parent.mkdir(exist_ok=True)
    LOCAL_DB_FILE.write_bytes(file_bytes)
    return write_github_file(file_bytes)


def month_name(n: int) -> str:
    nomes = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    return nomes[n - 1]


def gauge_status_color(value: float, meta: float, tipo: str) -> str:
    """Retorna a cor principal do relógio conforme a meta cadastrada."""
    value = float(value or 0)
    meta = float(meta or 0)
    if tipo == "menor_melhor":
        if value <= meta:
            return "#2ca02c"
        if (meta > 0 and value <= meta * 1.2) or (meta <= 0 and value <= 1):
            return "#ffbf00"
        return "#d62728"
    if meta <= 0:
        return "#2ca02c" if value > 0 else "#d62728"
    if value >= meta:
        return "#2ca02c"
    if value >= meta * 0.9:
        return "#ffbf00"
    return "#d62728"


def gauge(title: str, value: float, unit: str, min_value: float, max_value: float, meta: float, tipo: str) -> go.Figure:
    """Relógio com faixas visuais calibradas pela meta do indicador."""
    value = float(value or 0)
    meta = float(meta or 0)
    if max_value <= min_value:
        max_value = min_value + 1

    if tipo == "menor_melhor":
        limite_verde = max(min_value, min(max_value, meta))
        limite_amarelo = meta * 1.2 if meta > 0 else min(max_value, 1)
        limite_amarelo = min(max_value, max(limite_verde, limite_amarelo))
        steps = [
            {"range": [min_value, limite_verde], "color": "#2ca02c"},
            {"range": [limite_verde, limite_amarelo], "color": "#ffbf00"},
            {"range": [limite_amarelo, max_value], "color": "#d62728"},
        ]
    else:
        limite_vermelho = max(min_value, min(max_value, meta * 0.9))
        limite_meta = min(max_value, max(limite_vermelho, meta))
        steps = [
            {"range": [min_value, limite_vermelho], "color": "#d62728"},
            {"range": [limite_vermelho, limite_meta], "color": "#ffbf00"},
            {"range": [limite_meta, max_value], "color": "#2ca02c"},
        ]

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number={"suffix": f" {unit}"},
        title={"text": title, "font": {"size": 18}},
        gauge={
            "axis": {"range": [min_value, max_value]},
            "bar": {"color": gauge_status_color(value, meta, tipo)},
            "threshold": {
                "line": {"color": "#111111", "width": 3},
                "thickness": 0.75,
                "value": min(max_value, max(min_value, meta)),
            },
            "steps": steps,
        },
    ))
    fig.update_layout(height=260, margin=dict(l=10, r=10, t=40, b=10))
    return fig


def status_sinaleira(status: str) -> str:
    status = str(status or "").strip().lower()
    if status in ["concluída", "concluida"]:
        return "🟢 Concluída"
    if status == "em andamento":
        return "🟡 Em andamento"
    return "🔴 Aberta"


def build_template() -> bytes:
    modelo_dados = pd.DataFrame([{
        "data": date.today(),
        "semana": int(date.today().isocalendar().week),
        "acidentes_un": 0,
        "reclamacoes_un": 0,
        "perda_prensas_t": 0.0,
        "perda_litografia_t": 0.0,
        "perda_montagem_t": 0.0,
        "atendimento_prazo_pct": 100.0,
        "eficiencia_prensas_pct": 0.0,
        "eficiencia_litografia_pct": 0.0,
        "eficiencia_montagem_pct": 0.0,
    }])
    modelo_acoes = pd.DataFrame(columns=COLS_ACOES)
    modelo_metas = empty_metas()
    return to_workbook_bytes(modelo_dados, modelo_acoes, modelo_metas)


# =====================================================
# Interface
# =====================================================
st.title("📊 FMDS SQDCP - Painel Industrial")
st.caption("Visualização simples, lançamento em massa e base persistente via GitHub.")

if "loaded" not in st.session_state:
    dados, acoes, metas, avisos = load_base()
    st.session_state["dados"] = dados
    st.session_state["acoes"] = acoes
    st.session_state["metas"] = metas
    st.session_state["avisos"] = avisos
    st.session_state["loaded"] = True

for aviso in st.session_state.get("avisos", []):
    st.warning(aviso)

if github_enabled():
    st.success(f"Persistência GitHub ativa: {GITHUB_REPO}/{GITHUB_FILE_PATH}")
else:
    st.info("Persistência GitHub ainda não configurada. Configure os Secrets no Streamlit Cloud para salvar a base definitivamente.")

with st.sidebar:
    st.header("Filtros")
    dados_base = normalize_dados(st.session_state["dados"])
    if dados_base.empty:
        meses_disp = list(range(1, 13))
        semanas_disp = list(range(1, 54))
    else:
        meses_disp = sorted(dados_base["data"].dt.month.dropna().unique().astype(int).tolist())
        semanas_disp = sorted(dados_base["semana"].dropna().unique().astype(int).tolist())
    mes_sel = st.selectbox("Mês", meses_disp, format_func=month_name)
    semana_opcoes = ["Todas"] + semanas_disp
    semana_sel = st.selectbox("Semana", semana_opcoes)

    st.divider()
    st.header("Base")
    st.download_button(
        "Baixar modelo Excel",
        data=build_template(),
        file_name="modelo_sqdcp_fmds.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.download_button(
        "Baixar base atual",
        data=to_workbook_bytes(st.session_state["dados"], st.session_state["acoes"], st.session_state.get("metas", empty_metas())),
        file_name="base_sqdcp_fmds.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    upload = st.file_uploader("Importar Excel .xlsx", type=["xlsx"])
    if upload is not None:
        try:
            imp_dados, imp_acoes, imp_metas = read_workbook(upload.getvalue())
            st.session_state["dados"] = imp_dados
            if not imp_acoes.empty:
                st.session_state["acoes"] = imp_acoes
            st.session_state["metas"] = imp_metas
            erro = save_base(st.session_state["dados"], st.session_state["acoes"], st.session_state.get("metas", empty_metas()))
            if erro:
                st.warning(erro)
            else:
                st.success("Base importada e salva.")
            st.rerun()
        except Exception as exc:
            st.error(f"Erro ao importar arquivo: {exc}")

    st.divider()
    confirmar = st.checkbox("Confirmo que desejo excluir toda a base")
    if st.button("Excluir base de dados", disabled=not confirmar, type="secondary"):
        st.session_state["dados"] = empty_dados()
        st.session_state["acoes"] = empty_acoes()
        st.session_state["metas"] = empty_metas()
        erro = save_base(st.session_state["dados"], st.session_state["acoes"], st.session_state.get("metas", empty_metas()))
        if erro:
            st.warning(erro)
        else:
            st.success("Base excluída e sincronizada.")
        st.rerun()

# Metas dos indicadores
with st.expander("Metas dos indicadores", expanded=False):
    st.caption("Defina a referência dos relógios. Para acidentes, reclamações e perdas, quanto menor melhor. Para atendimento no prazo e eficiência, quanto maior melhor.")
    metas_edit = normalize_metas(st.session_state.get("metas", empty_metas()))
    edited_metas = st.data_editor(
        metas_edit,
        num_rows="fixed",
        use_container_width=True,
        hide_index=True,
        column_config={
            "indicador": st.column_config.SelectboxColumn("Indicador", options=INDICADORES, required=True),
            "meta": st.column_config.NumberColumn("Meta", min_value=0.0, step=0.1),
            "unidade": st.column_config.TextColumn("Unidade"),
            "tipo": st.column_config.SelectboxColumn("Critério", options=["menor_melhor", "maior_melhor"], required=True),
        },
        disabled=["indicador", "unidade", "tipo"],
        key="metas_editor",
    )
    if st.button("Salvar metas", type="primary"):
        st.session_state["metas"] = normalize_metas(edited_metas)
        erro = save_base(st.session_state["dados"], st.session_state["acoes"], st.session_state["metas"])
        if erro:
            st.warning(erro)
        else:
            st.success("Metas salvas e sincronizadas no GitHub.")
        st.rerun()

# Lançamento em massa
with st.expander("Lançamento em massa", expanded=False):
    base_edit = normalize_dados(st.session_state["dados"])
    if base_edit.empty:
        base_edit = pd.DataFrame([{
            "data": pd.to_datetime(date.today()),
            "semana": int(date.today().isocalendar().week),
            "acidentes_un": 0,
            "reclamacoes_un": 0,
            "perda_prensas_t": 0.0,
            "perda_litografia_t": 0.0,
            "perda_montagem_t": 0.0,
            "atendimento_prazo_pct": 100.0,
            "eficiencia_prensas_pct": 0.0,
            "eficiencia_litografia_pct": 0.0,
            "eficiencia_montagem_pct": 0.0,
        }])
    edited = st.data_editor(
        base_edit,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
            "semana": st.column_config.NumberColumn("Semana", min_value=1, max_value=53, step=1),
            "acidentes_un": st.column_config.NumberColumn("Acidentes", min_value=0, step=1),
            "reclamacoes_un": st.column_config.NumberColumn("Reclamações", min_value=0, step=1),
            "perda_prensas_t": st.column_config.NumberColumn("Perda Prensas t", min_value=0.0, step=0.01),
            "perda_litografia_t": st.column_config.NumberColumn("Perda Litografia t", min_value=0.0, step=0.01),
            "perda_montagem_t": st.column_config.NumberColumn("Perda Montagem t", min_value=0.0, step=0.01),
            "atendimento_prazo_pct": st.column_config.NumberColumn("Atendimento %", min_value=0.0, max_value=100.0, step=0.1),
            "eficiencia_prensas_pct": st.column_config.NumberColumn("Eficiência Prensas %", min_value=0.0, max_value=100.0, step=0.1),
            "eficiencia_litografia_pct": st.column_config.NumberColumn("Eficiência Litografia %", min_value=0.0, max_value=100.0, step=0.1),
            "eficiencia_montagem_pct": st.column_config.NumberColumn("Eficiência Montagem %", min_value=0.0, max_value=100.0, step=0.1),
        },
    )
    if st.button("Salvar lançamentos", type="primary"):
        st.session_state["dados"] = normalize_dados(edited)
        erro = save_base(st.session_state["dados"], st.session_state["acoes"], st.session_state.get("metas", empty_metas()))
        if erro:
            st.warning(erro)
        else:
            st.success("Lançamentos salvos e sincronizados no GitHub.")
        st.rerun()

# Filtro
filtro = normalize_dados(st.session_state["dados"])
if not filtro.empty:
    filtro = filtro[filtro["data"].dt.month == mes_sel]
    if semana_sel != "Todas":
        filtro = filtro[filtro["semana"] == int(semana_sel)]

if filtro.empty:
    st.warning("Nenhum dado encontrado para o filtro selecionado.")
else:
    metas = normalize_metas(st.session_state.get("metas", empty_metas()))
    acidentes = filtro["acidentes_un"].sum()
    reclamacoes = filtro["reclamacoes_un"].sum()
    perda = filtro[["perda_prensas_t", "perda_litografia_t", "perda_montagem_t"]].sum().sum()
    atendimento = filtro["atendimento_prazo_pct"].mean()
    eficiencia = filtro[["eficiencia_prensas_pct", "eficiencia_litografia_pct", "eficiencia_montagem_pct"]].mean(axis=1).mean()

    meta_acidentes = get_meta(metas, "Acidentes", 0.0)
    meta_reclamacoes = get_meta(metas, "Reclamações", 0.0)
    meta_perda = get_meta(metas, "Perda", 0.0)
    meta_atendimento = get_meta(metas, "Atendimento no prazo", 100.0)
    meta_eficiencia = get_meta(metas, "Eficiência", 100.0)

    c1, c2, c3, c4, c5 = st.columns(5)
    max_acidentes = max(5, float(acidentes) * 1.5, meta_acidentes * 1.5)
    max_reclamacoes = max(10, float(reclamacoes) * 1.5, meta_reclamacoes * 1.5)
    max_perda = max(5, float(perda) * 1.5, meta_perda * 1.5)
    with c1:
        st.plotly_chart(gauge("Acidentes", acidentes, "un", 0, max_acidentes, meta_acidentes, "menor_melhor"), use_container_width=True)
        st.caption(f"Meta: {meta_acidentes:g} un")
    with c2:
        st.plotly_chart(gauge("Reclamações", reclamacoes, "un", 0, max_reclamacoes, meta_reclamacoes, "menor_melhor"), use_container_width=True)
        st.caption(f"Meta: {meta_reclamacoes:g} un")
    with c3:
        st.plotly_chart(gauge("Perda", perda, "t", 0, max_perda, meta_perda, "menor_melhor"), use_container_width=True)
        st.caption(f"Meta: {meta_perda:g} t")
    with c4:
        st.plotly_chart(gauge("Atendimento", atendimento, "%", 0, 100, meta_atendimento, "maior_melhor"), use_container_width=True)
        st.caption(f"Meta: {meta_atendimento:g}%")
    with c5:
        st.plotly_chart(gauge("Eficiência", eficiencia, "%", 0, 100, meta_eficiencia, "maior_melhor"), use_container_width=True)
        st.caption(f"Meta: {meta_eficiencia:g}%")

    st.subheader("Detalhamento por área")
    d1, d2 = st.columns(2)
    with d1:
        perdas_area = pd.DataFrame({
            "Área": ["Prensas", "Litografia", "Montagem"],
            "Perda t": [
                filtro["perda_prensas_t"].sum(),
                filtro["perda_litografia_t"].sum(),
                filtro["perda_montagem_t"].sum(),
            ],
        })
        fig_perdas = go.Figure(go.Bar(
            x=perdas_area["Área"],
            y=perdas_area["Perda t"],
            text=perdas_area["Perda t"].round(2),
            textposition="outside",
        ))
        fig_perdas.update_layout(
            title="Perdas por área — toneladas",
            xaxis_title="Área",
            yaxis_title="Perda (t)",
            yaxis=dict(range=[0, max(0.01, float(perdas_area["Perda t"].max()) * 1.25)]),
            height=360,
            margin=dict(l=10, r=10, t=60, b=10),
        )
        st.plotly_chart(fig_perdas, use_container_width=True)
    with d2:
        efic_area = pd.DataFrame({
            "Área": ["Prensas", "Litografia", "Montagem"],
            "Eficiência %": [
                filtro["eficiencia_prensas_pct"].mean(),
                filtro["eficiencia_litografia_pct"].mean(),
                filtro["eficiencia_montagem_pct"].mean(),
            ],
        }).fillna(0)
        fig_efic = go.Figure(go.Bar(
            x=efic_area["Área"],
            y=efic_area["Eficiência %"],
            text=efic_area["Eficiência %"].round(1).astype(str) + "%",
            textposition="outside",
        ))
        fig_efic.update_layout(
            title="Eficiência por área — percentual",
            xaxis_title="Área",
            yaxis_title="Eficiência (%)",
            yaxis=dict(range=[0, 100]),
            height=360,
            margin=dict(l=10, r=10, t=60, b=10),
        )
        st.plotly_chart(fig_efic, use_container_width=True)

st.divider()
st.subheader("Ações por indicador")
acoes_edit = normalize_acoes(st.session_state["acoes"])
for indicador in INDICADORES:
    with st.expander(f"Ações - {indicador}", expanded=True):
        subset = acoes_edit[acoes_edit["indicador"] == indicador].copy()
        if subset.empty:
            subset = pd.DataFrame([{
                "indicador": indicador,
                "descricao": "",
                "responsavel": "",
                "prazo": pd.NaT,
                "status": "Aberta",
            }])
        subset["sinaleira"] = subset["status"].apply(status_sinaleira)
        subset = subset[["sinaleira", "indicador", "descricao", "responsavel", "prazo", "status"]]
        edited_acoes = st.data_editor(
            subset,
            num_rows="dynamic",
            hide_index=True,
            use_container_width=True,
            key=f"acoes_{indicador}",
            column_config={
                "sinaleira": st.column_config.TextColumn("Sinaleira"),
                "indicador": st.column_config.SelectboxColumn("Indicador", options=INDICADORES, required=True),
                "descricao": st.column_config.TextColumn("Descrição"),
                "responsavel": st.column_config.TextColumn("Responsável"),
                "prazo": st.column_config.DateColumn("Prazo", format="DD/MM/YYYY"),
                "status": st.column_config.SelectboxColumn("Status", options=["Aberta", "Em andamento", "Concluída"], required=True),
            },
            disabled=["sinaleira"],
        )
        if st.button(f"Salvar ações - {indicador}", key=f"salvar_{indicador}"):
            outras = acoes_edit[acoes_edit["indicador"] != indicador]
            novas = normalize_acoes(edited_acoes)
            novas = novas[(novas["descricao"].str.strip() != "") | (novas["responsavel"].str.strip() != "")]
            st.session_state["acoes"] = normalize_acoes(pd.concat([outras, novas], ignore_index=True))
            erro = save_base(st.session_state["dados"], st.session_state["acoes"], st.session_state.get("metas", empty_metas()))
            if erro:
                st.warning(erro)
            else:
                st.success("Ações salvas e sincronizadas no GitHub.")
            st.rerun()
