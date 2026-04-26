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


def read_workbook(file_bytes: bytes) -> Tuple[pd.DataFrame, pd.DataFrame]:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio, engine="openpyxl")
    dados = pd.read_excel(xls, DATA_SHEET) if DATA_SHEET in xls.sheet_names else empty_dados()
    acoes = pd.read_excel(xls, ACTIONS_SHEET) if ACTIONS_SHEET in xls.sheet_names else empty_acoes()
    return normalize_dados(dados), normalize_acoes(acoes)


def to_workbook_bytes(dados: pd.DataFrame, acoes: pd.DataFrame) -> bytes:
    dados = normalize_dados(dados)
    acoes = normalize_acoes(acoes)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        dados.to_excel(writer, sheet_name=DATA_SHEET, index=False)
        acoes.to_excel(writer, sheet_name=ACTIONS_SHEET, index=False)
    output.seek(0)
    return output.getvalue()


def load_base() -> Tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    avisos = []
    file_bytes, _, warn = read_github_file()
    if warn:
        avisos.append(warn)
    if file_bytes:
        try:
            LOCAL_DB_FILE.write_bytes(file_bytes)
            dados, acoes = read_workbook(file_bytes)
            return dados, acoes, avisos
        except Exception as exc:
            avisos.append(f"A base no GitHub existe, mas não pôde ser lida: {exc}")
    if LOCAL_DB_FILE.exists():
        try:
            return (*read_workbook(LOCAL_DB_FILE.read_bytes()), avisos)
        except Exception as exc:
            avisos.append(f"A base local não pôde ser lida: {exc}")
    return empty_dados(), empty_acoes(), avisos


def save_base(dados: pd.DataFrame, acoes: pd.DataFrame) -> Optional[str]:
    file_bytes = to_workbook_bytes(dados, acoes)
    LOCAL_DB_FILE.parent.mkdir(exist_ok=True)
    LOCAL_DB_FILE.write_bytes(file_bytes)
    return write_github_file(file_bytes)


def month_name(n: int) -> str:
    nomes = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    return nomes[n - 1]


def gauge(title: str, value: float, unit: str, min_value: float, max_value: float, inverse: bool = False) -> go.Figure:
    if max_value <= min_value:
        max_value = min_value + 1
    if inverse:
        steps = [
            {"range": [min_value, max_value * 0.33], "color": "#2ca02c"},
            {"range": [max_value * 0.33, max_value * 0.66], "color": "#ffbf00"},
            {"range": [max_value * 0.66, max_value], "color": "#d62728"},
        ]
    else:
        steps = [
            {"range": [min_value, max_value * 0.6], "color": "#d62728"},
            {"range": [max_value * 0.6, max_value * 0.85], "color": "#ffbf00"},
            {"range": [max_value * 0.85, max_value], "color": "#2ca02c"},
        ]
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=float(value),
        number={"suffix": f" {unit}"},
        title={"text": title, "font": {"size": 18}},
        gauge={"axis": {"range": [min_value, max_value]}, "bar": {"color": "#1f77b4"}, "steps": steps},
    ))
    fig.update_layout(height=260, margin=dict(l=10, r=10, t=40, b=10))
    return fig


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
    return to_workbook_bytes(modelo_dados, modelo_acoes)


# =====================================================
# Interface
# =====================================================
st.title("📊 FMDS SQDCP - Painel Industrial")
st.caption("Visualização simples, lançamento em massa e base persistente via GitHub.")

if "loaded" not in st.session_state:
    dados, acoes, avisos = load_base()
    st.session_state["dados"] = dados
    st.session_state["acoes"] = acoes
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
        data=to_workbook_bytes(st.session_state["dados"], st.session_state["acoes"]),
        file_name="base_sqdcp_fmds.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    upload = st.file_uploader("Importar Excel .xlsx", type=["xlsx"])
    if upload is not None:
        try:
            imp_dados, imp_acoes = read_workbook(upload.getvalue())
            st.session_state["dados"] = imp_dados
            if not imp_acoes.empty:
                st.session_state["acoes"] = imp_acoes
            erro = save_base(st.session_state["dados"], st.session_state["acoes"])
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
        erro = save_base(st.session_state["dados"], st.session_state["acoes"])
        if erro:
            st.warning(erro)
        else:
            st.success("Base excluída e sincronizada.")
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
        erro = save_base(st.session_state["dados"], st.session_state["acoes"])
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
    acidentes = filtro["acidentes_un"].sum()
    reclamacoes = filtro["reclamacoes_un"].sum()
    perda = filtro[["perda_prensas_t", "perda_litografia_t", "perda_montagem_t"]].sum().sum()
    atendimento = filtro["atendimento_prazo_pct"].mean()
    eficiencia = filtro[["eficiencia_prensas_pct", "eficiencia_litografia_pct", "eficiencia_montagem_pct"]].mean(axis=1).mean()

    c1, c2, c3, c4, c5 = st.columns(5)
    max_acidentes = max(5, float(acidentes) * 1.5)
    max_reclamacoes = max(10, float(reclamacoes) * 1.5)
    max_perda = max(5, float(perda) * 1.5)
    with c1:
        st.plotly_chart(gauge("Acidentes", acidentes, "un", 0, max_acidentes, inverse=True), use_container_width=True)
    with c2:
        st.plotly_chart(gauge("Reclamações", reclamacoes, "un", 0, max_reclamacoes, inverse=True), use_container_width=True)
    with c3:
        st.plotly_chart(gauge("Perda", perda, "t", 0, max_perda, inverse=True), use_container_width=True)
    with c4:
        st.plotly_chart(gauge("Atendimento", atendimento, "%", 0, 100, inverse=False), use_container_width=True)
    with c5:
        st.plotly_chart(gauge("Eficiência", eficiencia, "%", 0, 100, inverse=False), use_container_width=True)

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
        st.bar_chart(perdas_area.set_index("Área"))
    with d2:
        efic_area = pd.DataFrame({
            "Área": ["Prensas", "Litografia", "Montagem"],
            "Eficiência %": [
                filtro["eficiencia_prensas_pct"].mean(),
                filtro["eficiencia_litografia_pct"].mean(),
                filtro["eficiencia_montagem_pct"].mean(),
            ],
        })
        st.bar_chart(efic_area.set_index("Área"))

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
        edited_acoes = st.data_editor(
            subset,
            num_rows="dynamic",
            hide_index=True,
            use_container_width=True,
            key=f"acoes_{indicador}",
            column_config={
                "indicador": st.column_config.SelectboxColumn("Indicador", options=INDICADORES, required=True),
                "descricao": st.column_config.TextColumn("Descrição"),
                "responsavel": st.column_config.TextColumn("Responsável"),
                "prazo": st.column_config.DateColumn("Prazo", format="DD/MM/YYYY"),
                "status": st.column_config.SelectboxColumn("Status", options=["Aberta", "Em andamento", "Concluída"], required=True),
            },
        )
        if st.button(f"Salvar ações - {indicador}", key=f"salvar_{indicador}"):
            outras = acoes_edit[acoes_edit["indicador"] != indicador]
            novas = normalize_acoes(edited_acoes)
            novas = novas[(novas["descricao"].str.strip() != "") | (novas["responsavel"].str.strip() != "")]
            st.session_state["acoes"] = normalize_acoes(pd.concat([outras, novas], ignore_index=True))
            erro = save_base(st.session_state["dados"], st.session_state["acoes"])
            if erro:
                st.warning(erro)
            else:
                st.success("Ações salvas e sincronizadas no GitHub.")
            st.rerun()
