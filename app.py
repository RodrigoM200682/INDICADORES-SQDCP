import os
import sqlite3
from datetime import date, datetime

import pandas as pd
import streamlit as st

APP_TITLE = "SQCDP"
DB_PATH = os.path.join("data", "sqdcp.db")
MESES = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
}
INDICADORES = ["S", "Q", "C", "D", "P"]

METAS = {
    "S": {"titulo": "Acidentes", "meta": 0, "tipo": "menor", "unidade": ""},
    "Q": {"titulo": "Reclamações de Clientes", "meta": 2, "tipo": "menor", "unidade": ""},
    "C": {"titulo": "Perda / Ton Processada", "meta": 1.0, "tipo": "menor", "unidade": "%"},
    "D": {"titulo": "Atendimento no Prazo", "meta": 98.0, "tipo": "maior", "unidade": "%"},
    "P": {"titulo": "Eficiência", "meta": 75.0, "tipo": "maior", "unidade": "%"},
}

st.set_page_config(page_title="SQCDP - Unidade RS", layout="wide", initial_sidebar_state="collapsed")

CSS = """
<style>
.main .block-container {padding-top: 1.2rem; padding-bottom: 1rem; max-width: 1400px;}
#MainMenu, footer, header {visibility: hidden;}
.titulo-wrap {display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:8px;}
.logo {font-size:32px; font-weight:800; color:#5aa05a; letter-spacing:-1px; margin-bottom:0;}
.sublogo {font-size:12px; font-weight:700; color:#222; margin-top:-6px;}
.unidade {font-size:30px; font-weight:500; color:#17212b; padding-top:8px;}
.filter-card {border:1px solid #e5e7eb; border-radius:10px; padding:8px 12px; background:#fff; margin-bottom:12px;}
.fake-nav {display:flex; gap:8px; justify-content:center; margin:6px 0 12px 0;}
.fake-btn {border:1px solid #d5d9df; padding:8px 22px; font-size:11px; border-radius:2px; background:#fff; color:#222; min-width:105px; text-align:center;}
.fake-btn.active {background:#1f2933; color:white; font-weight:700;}
.card {border:3px solid #1e3437; border-radius:38px; padding:16px 16px 18px 16px; height:540px; background:white; display:flex; flex-direction:column; align-items:center;}
.card-letter {font-size:34px; font-weight:800; color:#192b32; margin-bottom:4px;}
.card-title {font-size:14px; min-height:36px; text-align:center; color:#000; margin-bottom:8px;}
.gauge {width:150px; height:75px; border-radius:150px 150px 0 0; background: conic-gradient(from 270deg, var(--gauge-color) 0deg, var(--gauge-color) var(--angle), #e9e9e9 var(--angle), #e9e9e9 180deg, transparent 180deg); position:relative; margin-top:4px; overflow:hidden;}
.gauge::after {content:""; position:absolute; left:26px; top:26px; width:98px; height:98px; border-radius:50%; background:white;}
.gauge-value {font-size:22px; font-weight:500; margin-top:-34px; z-index:2; color:#20303a;}
.meta {font-size:11px; color:#555; margin-top:4px; min-height:16px;}
.bars {height:115px; width:100%; display:flex; align-items:flex-end; gap:5px; justify-content:center; margin-top:28px; border-bottom:1px dashed #b5b5b5;}
.bar {width:13px; border-radius:2px 2px 0 0; background:#ff5c58;}
.bar.ok {background:#58a857;}
.note {font-size:12px; color:#667085; margin-top:12px; text-align:center;}
.metric-line {width:100%; border-top:1px solid #f0f0f0; margin-top:14px; padding-top:10px; font-size:12px; color:#4b5563; text-align:center;}
.stTabs [data-baseweb="tab-list"] {gap: 10px;}
.stTabs [data-baseweb="tab"] {height: 42px; border:1px solid #e5e7eb; border-radius:8px; padding:8px 18px; background:white;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


def init_db():
    os.makedirs("data", exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lancamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            ano INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            semana INTEGER NOT NULL,
            turno TEXT,
            indicador TEXT NOT NULL,
            processo TEXT,
            valor REAL NOT NULL,
            observacao TEXT,
            criado_em TEXT NOT NULL
        )
        """
    )
    con.commit()
    con.close()


def load_data():
    init_db()
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM lancamentos", con)
    con.close()
    if not df.empty:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df


def save_long(df_long):
    init_db()
    con = sqlite3.connect(DB_PATH)
    df_long.to_sql("lancamentos", con, if_exists="append", index=False)
    con.close()


def format_val(ind, val):
    if pd.isna(val):
        val = 0
    if ind in ["C", "D", "P"]:
        return f"{val:.1f}%".replace(".", ",")
    return f"{int(round(val))}"


def calc_status(ind, val):
    meta = METAS[ind]["meta"]
    tipo = METAS[ind]["tipo"]
    if ind == "S":
        return "ok" if val <= meta else "bad"
    if tipo == "menor":
        return "ok" if val <= meta else "bad"
    return "ok" if val >= meta else "bad"


def aggregate_indicator(df, ind):
    aux = df[df["indicador"] == ind]
    if aux.empty:
        return 0.0
    if ind in ["S", "Q"]:
        return float(aux["valor"].sum())
    return float(aux["valor"].mean())


def series_by_day(df, ind):
    aux = df[df["indicador"] == ind].copy()
    if aux.empty:
        return pd.Series(dtype=float)
    aux["dia"] = aux["data"].dt.day
    if ind in ["S", "Q"]:
        return aux.groupby("dia")["valor"].sum().sort_index()
    return aux.groupby("dia")["valor"].mean().sort_index()


def card_html(ind, val, serie):
    meta = METAS[ind]["meta"]
    status = calc_status(ind, val)
    color = "#58a857" if status == "ok" else "#ff5c58"
    # escala visual do gauge
    if ind in ["S", "Q", "C"]:
        max_ref = max(meta * 2, val, 1)
        pct = 1 - min(val / max_ref, 1)
    else:
        pct = min(val / 100, 1)
    angle = int(max(8, pct * 180))

    if len(serie) == 0:
        bars = "".join(["<div class='bar' style='height:8px; opacity:.25'></div>" for _ in range(12)])
    else:
        maxv = max(float(serie.max()), 1.0)
        bars = ""
        for _, v in serie.tail(16).items():
            h = max(8, int((float(v) / maxv) * 105))
            bstatus = calc_status(ind, float(v))
            cls = "bar ok" if bstatus == "ok" else "bar"
            bars += f"<div class='{cls}' style='height:{h}px'></div>"

    meta_txt = f"Meta: {format_val(ind, meta)}{METAS[ind]['unidade'] if ind in ['S','Q'] else ''}"
    if ind in ["C", "D", "P"]:
        meta_txt = f"Meta: {str(meta).replace('.', ',')}%"

    return f"""
    <div class='card'>
        <div class='card-letter'>{ind}</div>
        <div class='card-title'>{METAS[ind]['titulo']}</div>
        <div class='gauge' style='--gauge-color:{color}; --angle:{angle}deg;'></div>
        <div class='gauge-value'>{format_val(ind, val)}</div>
        <div class='meta'>{meta_txt}</div>
        <div class='bars'>{bars}</div>
        <div class='metric-line'>Visão consolidada por dia conforme mês/semana selecionados</div>
    </div>
    """


def expand_mass_table(df_wide):
    registros = []
    now = datetime.now().isoformat(timespec="seconds")
    mapa = {
        "S_Acidentes": "S",
        "Q_Reclamacoes": "Q",
        "C_Perdas_Ton_pct": "C",
        "D_AtendPrazo_pct": "D",
        "P_Eficiencia_pct": "P",
    }
    for _, row in df_wide.iterrows():
        data = pd.to_datetime(row.get("Data"), errors="coerce")
        if pd.isna(data):
            continue
        semana = row.get("Semana")
        if pd.isna(semana) or semana == "":
            semana = int(data.isocalendar().week)
        else:
            semana = int(semana)
        turno = str(row.get("Turno", "")).strip()
        processo = str(row.get("Processo", "GERAL")).strip() or "GERAL"
        obs = str(row.get("Observacao", "")).strip()
        for col, ind in mapa.items():
            v = row.get(col)
            if pd.isna(v) or v == "":
                continue
            try:
                valor = float(str(v).replace(",", "."))
            except Exception:
                continue
            registros.append({
                "data": data.date().isoformat(),
                "ano": int(data.year),
                "mes": int(data.month),
                "semana": semana,
                "turno": turno,
                "indicador": ind,
                "processo": processo if ind == "P" else "GERAL",
                "valor": valor,
                "observacao": obs,
                "criado_em": now,
            })
    return pd.DataFrame(registros)


def template_df():
    hoje = date.today()
    semana = int(pd.Timestamp(hoje).isocalendar().week)
    return pd.DataFrame([
        {"Data": hoje, "Semana": semana, "Turno": "1º", "Processo": "GERAL", "S_Acidentes": 0, "Q_Reclamacoes": 0, "C_Perdas_Ton_pct": 0.0, "D_AtendPrazo_pct": 98.0, "P_Eficiencia_pct": 75.0, "Observacao": ""},
        {"Data": hoje, "Semana": semana, "Turno": "2º", "Processo": "LITOGRAFIA", "S_Acidentes": 0, "Q_Reclamacoes": 0, "C_Perdas_Ton_pct": 0.0, "D_AtendPrazo_pct": 98.0, "P_Eficiencia_pct": 75.0, "Observacao": ""},
        {"Data": hoje, "Semana": semana, "Turno": "3º", "Processo": "PRENSAS", "S_Acidentes": 0, "Q_Reclamacoes": 0, "C_Perdas_Ton_pct": 0.0, "D_AtendPrazo_pct": 98.0, "P_Eficiencia_pct": 75.0, "Observacao": ""},
    ])


def prepare_editor_df(df):
    colunas = ["Data", "Semana", "Turno", "Processo", "S_Acidentes", "Q_Reclamacoes", "C_Perdas_Ton_pct", "D_AtendPrazo_pct", "P_Eficiencia_pct", "Observacao"]
    df = df.copy()
    for col in colunas:
        if col not in df.columns:
            df[col] = None
    df = df[colunas]
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    df["Semana"] = pd.to_numeric(df["Semana"], errors="coerce").fillna(0).astype(int)
    for col in ["S_Acidentes", "Q_Reclamacoes", "C_Perdas_Ton_pct", "D_AtendPrazo_pct", "P_Eficiencia_pct"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ".", regex=False), errors="coerce").fillna(0.0)
    df["Turno"] = df["Turno"].fillna("1º").astype(str)
    df["Processo"] = df["Processo"].fillna("GERAL").astype(str)
    df["Observacao"] = df["Observacao"].fillna("").astype(str)
    return df


init_db()
df_all = load_data()

st.markdown(
    """
    <div class='titulo-wrap'>
      <div><div class='logo'>SQCDP</div><div class='sublogo'>Segurança | Qualidade | Custos | Entregas | Produtividade</div></div>
      <div class='unidade'>UNIDADE RS</div>
    </div>
    <div class='fake-nav'><span class='fake-btn'>SP</span><span class='fake-btn active'>RS</span><span class='fake-btn'>RJ</span><span class='fake-btn'>PE</span><span class='fake-btn'>GO</span></div>
    <div class='fake-nav'><span class='fake-btn'>CORTE</span><span class='fake-btn'>LITOGRAFIA</span><span class='fake-btn'>MONTAGEM</span><span class='fake-btn'>PRENSAS</span></div>
    """,
    unsafe_allow_html=True,
)

tab_dash, tab_lanc, tab_export = st.tabs(["Painel SQCDP", "Lançamentos em massa", "Exportar / Base"])

with tab_dash:
    if df_all.empty:
        ano_default = date.today().year
        meses_op = list(MESES.keys())
    else:
        ano_default = int(df_all["ano"].max())
        meses_op = sorted(df_all[df_all["ano"] == ano_default]["mes"].dropna().unique().astype(int).tolist()) or list(MESES.keys())

    st.markdown("<div class='filter-card'>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 5])
    with c1:
        mes_sel = st.selectbox("Mês", options=meses_op, format_func=lambda x: MESES.get(int(x), str(x)))
    df_mes_base = df_all[(df_all["ano"] == ano_default) & (df_all["mes"] == int(mes_sel))] if not df_all.empty else pd.DataFrame()
    semanas = ["Todas"] + (sorted(df_mes_base["semana"].dropna().unique().astype(int).tolist()) if not df_mes_base.empty else [])
    with c2:
        semana_sel = st.selectbox("Semana", options=semanas)
    with c3:
        st.caption("Filtros simplificados: o painel usa somente mês e semana. Os dados são consolidados, sem consulta individual por ocorrência.")
    st.markdown("</div>", unsafe_allow_html=True)

    if df_all.empty:
        df_f = df_all.copy()
    else:
        df_f = df_all[(df_all["ano"] == ano_default) & (df_all["mes"] == int(mes_sel))].copy()
        if semana_sel != "Todas":
            df_f = df_f[df_f["semana"] == int(semana_sel)]

    cols = st.columns(5, gap="medium")
    for col, ind in zip(cols, INDICADORES):
        val = aggregate_indicator(df_f, ind)
        serie = series_by_day(df_f, ind)
        col.markdown(card_html(ind, val, serie), unsafe_allow_html=True)

with tab_lanc:
    st.subheader("Lançamentos em massa")
    st.write("Cole/edite várias linhas na tabela ou importe um arquivo Excel com as mesmas colunas. Ao salvar, o app grava os dados no banco SQLite.")

    up = st.file_uploader("Importar Excel de lançamentos em massa", type=["xlsx"])
    if up is not None:
        try:
            base_edit = pd.read_excel(up)
        except Exception as e:
            st.error(f"Não consegui ler o Excel: {e}")
            base_edit = template_df()
    else:
        base_edit = template_df()

    base_edit = prepare_editor_df(base_edit)

    edited = st.data_editor(
        base_edit,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
            "Semana": st.column_config.NumberColumn("Semana", min_value=1, max_value=53, step=1),
            "Turno": st.column_config.SelectboxColumn("Turno", options=["1º", "2º", "3º", "ADM"]),
            "Processo": st.column_config.SelectboxColumn("Processo", options=["GERAL", "CORTE", "LITOGRAFIA", "MONTAGEM", "PRENSAS"]),
        },
    )

    c1, c2 = st.columns([1, 4])
    with c1:
        if st.button("Salvar lançamentos", type="primary", use_container_width=True):
            long_df = expand_mass_table(edited)
            if long_df.empty:
                st.warning("Nenhum dado válido para salvar. Verifique se a coluna Data e os valores foram preenchidos.")
            else:
                save_long(long_df)
                st.success(f"{len(long_df)} registros gravados com sucesso.")
                st.rerun()
    with c2:
        st.info("Colunas principais: S e Q em quantidade; C, D e P em percentual. Para P, o campo Processo permite separar eficiência de CORTE, LITOGRAFIA, MONTAGEM e PRENSAS na base.")

with tab_export:
    st.subheader("Base consolidada")
    df_view = load_data()
    if df_view.empty:
        st.warning("Ainda não existem lançamentos gravados.")
    else:
        st.dataframe(df_view.sort_values(["data", "indicador"], ascending=[False, True]), use_container_width=True, hide_index=True)
        csv = df_view.to_csv(index=False, sep=";").encode("utf-8-sig")
        st.download_button("Baixar base em CSV", csv, file_name="base_sqdcp.csv", mime="text/csv", use_container_width=True)

        with st.expander("Limpeza da base"):
            st.warning("Esta ação exclui todos os lançamentos do banco de dados deste app.")
            senha = st.text_input("Senha para excluir", type="password")
            if st.button("Excluir todos os dados"):
                if senha == "QualidadeRS":
                    con = sqlite3.connect(DB_PATH)
                    con.execute("DELETE FROM lancamentos")
                    con.commit()
                    con.close()
                    st.success("Base excluída.")
                    st.rerun()
                else:
                    st.error("Senha incorreta.")
