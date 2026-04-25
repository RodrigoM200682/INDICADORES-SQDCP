import os
import sqlite3
from datetime import date, datetime

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# =========================================================
# APP SQDCP - FMDS SIMPLIFICADO
# Segurança | Qualidade | Custos | Delivery | Processo
# =========================================================

st.set_page_config(
    page_title="SQDCP FMDS - Unidade RS",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

DB_PATH = os.path.join("data", "sqdcp_fmds.db")
os.makedirs("data", exist_ok=True)

AREAS = ["Prensas", "Litografia", "Montagem"]
MESES = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
}
MESES_INV = {v: k for k, v in MESES.items()}
TURNOS = ["1º", "2º", "3º"]

# Metas padrão. Ajuste aqui conforme regra da unidade.
METAS = {
    "acidentes_max": 0,
    "reclamacoes_max_mes": 8,
    "perda_pct_max": 0.80,
    "prazo_min": 98.0,
    "eficiencia_min": 75.0,
}

CSS = """
<style>
.block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
#MainMenu, footer, header {visibility: hidden;}
.titulo-sqdcp {font-size: 34px; font-weight: 800; color: #4b9b59; line-height: 1;}
.subtitulo {font-size: 13px; color: #263238; font-weight: 700; margin-top: -6px;}
.unidade {font-size: 30px; font-weight: 800; color: #1e2b2d; text-align: right;}
.fmds-bar {border: 1px solid #e2e8e8; border-radius: 12px; padding: 10px 14px; background: #fff; margin-bottom: 10px;}
.card {border: 3px solid #1e3336; border-radius: 36px; min-height: 570px; background: #ffffff; padding: 12px 14px 8px 14px;}
.card-title {font-size: 36px; font-weight: 800; text-align: center; color: #273133; line-height: 1; margin-top: 4px;}
.card-subtitle {font-size: 13px; font-weight: 700; text-align: center; color: #111; min-height: 34px; margin-bottom: 4px;}
.status-ok {background: #57a85a; color: white; padding: 5px 12px; border-radius: 20px; font-size: 13px; font-weight: 800; display: inline-block;}
.status-nok {background: #ff5b57; color: white; padding: 5px 12px; border-radius: 20px; font-size: 13px; font-weight: 800; display: inline-block;}
.status-alerta {background: #f2b84b; color: #111; padding: 5px 12px; border-radius: 20px; font-size: 13px; font-weight: 800; display: inline-block;}
.kpi-num {font-size: 26px; font-weight: 800; text-align: center; margin: 0px; color: #1f2933;}
.kpi-label {font-size: 12px; text-align: center; color: #4b5563; margin-top: -6px;}
.area-row {display: flex; justify-content: space-between; gap: 8px; margin: 6px 0px; align-items:center;}
.area-name {font-size: 12px; font-weight: 700; width: 78px;}
.area-value {font-size: 12px; font-weight: 800; min-width: 54px; text-align:right;}
.small-note {font-size: 12px; color: #566; text-align: center;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


def conectar():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    con = conectar()
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lancamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            mes INTEGER NOT NULL,
            semana INTEGER NOT NULL,
            turno TEXT NOT NULL,
            area TEXT NOT NULL,
            acidentes INTEGER DEFAULT 0,
            reclamacoes INTEGER DEFAULT 0,
            perda_ton REAL DEFAULT 0,
            ton_processada REAL DEFAULT 0,
            entregas_no_prazo INTEGER DEFAULT 0,
            entregas_total INTEGER DEFAULT 0,
            eficiencia_pct REAL DEFAULT 0,
            observacao TEXT DEFAULT '',
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.commit()
    con.close()


def carregar_dados():
    con = conectar()
    df = pd.read_sql_query("SELECT * FROM lancamentos", con)
    con.close()
    if not df.empty:
        for col in ["mes", "semana", "acidentes", "reclamacoes", "entregas_no_prazo", "entregas_total"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in ["perda_ton", "ton_processada", "eficiencia_pct"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df


def salvar_lancamentos(df_novo):
    if df_novo.empty:
        return 0
    df = df_novo.copy()
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df.dropna(subset=["data"])
    df["mes"] = df["data"].dt.month.astype(int)
    df["semana"] = pd.to_numeric(df["semana"], errors="coerce").fillna(1).astype(int)
    df["turno"] = df["turno"].fillna("1º").astype(str)
    df["area"] = df["area"].fillna("Prensas").astype(str)

    colunas = [
        "data", "mes", "semana", "turno", "area", "acidentes", "reclamacoes",
        "perda_ton", "ton_processada", "entregas_no_prazo", "entregas_total",
        "eficiencia_pct", "observacao"
    ]
    for c in colunas:
        if c not in df.columns:
            df[c] = 0 if c not in ["data", "turno", "area", "observacao"] else ""
    for c in ["acidentes", "reclamacoes", "entregas_no_prazo", "entregas_total"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    for c in ["perda_ton", "ton_processada", "eficiencia_pct"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["data"] = df["data"].dt.strftime("%Y-%m-%d")
    df = df[colunas]

    con = conectar()
    df.to_sql("lancamentos", con, if_exists="append", index=False)
    con.close()
    return len(df)


def excluir_tudo():
    con = conectar()
    con.execute("DELETE FROM lancamentos")
    con.commit()
    con.close()


def status_bool(valor, meta, sentido="max"):
    if sentido == "max":
        return valor <= meta
    return valor >= meta


def status_html(ok, alerta=False):
    if alerta:
        return '<span class="status-alerta">ATENÇÃO</span>'
    return '<span class="status-ok">OK</span>' if ok else '<span class="status-nok">FORA</span>'


def gauge(valor, meta, titulo, modo="max", sufixo="", altura=155):
    if modo == "max":
        cor = "#57a85a" if valor <= meta else "#ff5b57"
        eixo_max = max(meta * 1.6, valor * 1.25, 1)
    else:
        cor = "#57a85a" if valor >= meta else "#ff5b57"
        eixo_max = 100 if valor <= 100 and meta <= 100 else max(valor * 1.2, meta * 1.2, 1)
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=float(valor),
        number={"suffix": sufixo, "font": {"size": 24}},
        title={"text": titulo, "font": {"size": 12}},
        gauge={
            "axis": {"range": [0, eixo_max], "tickwidth": 0, "tickfont": {"size": 9}},
            "bar": {"color": cor, "thickness": 0.38},
            "bgcolor": "#eeeeee",
            "borderwidth": 0,
            "threshold": {"line": {"color": "#263238", "width": 3}, "thickness": 0.75, "value": meta},
        }
    ))
    fig.update_layout(height=altura, margin=dict(l=5, r=5, t=24, b=0), paper_bgcolor="rgba(0,0,0,0)")
    return fig


def barras_serie(df, coluna, titulo, meta=None, modo="max", sufixo=""):
    if df.empty:
        serie = pd.DataFrame({"dia": [], coluna: []})
    else:
        tmp = df.copy()
        tmp["dia"] = pd.to_datetime(tmp["data"], errors="coerce").dt.day
        serie = tmp.groupby("dia", as_index=False)[coluna].sum()
    cores = []
    for v in serie[coluna].tolist():
        if meta is None:
            cores.append("#ff5b57")
        elif modo == "max":
            cores.append("#57a85a" if v <= meta else "#ff5b57")
        else:
            cores.append("#57a85a" if v >= meta else "#ff5b57")
    fig = go.Figure()
    fig.add_bar(x=serie["dia"], y=serie[coluna], marker_color=cores, text=serie[coluna], textposition="outside")
    if meta is not None:
        fig.add_hline(y=meta, line_dash="dot", line_color="#9aa0a6")
    fig.update_layout(
        title={"text": titulo, "font": {"size": 12}}, height=170,
        margin=dict(l=4, r=4, t=28, b=4), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="", showgrid=False, tickfont=dict(size=9)),
        yaxis=dict(title="", showgrid=False, visible=False),
        showlegend=False,
    )
    return fig


def barras_area(valores, meta, titulo, modo="max", sufixo=""):
    xs = list(valores.keys())
    ys = [valores[a] for a in xs]
    cores = [("#57a85a" if (v <= meta if modo == "max" else v >= meta) else "#ff5b57") for v in ys]
    textos = [f"{v:.2f}{sufixo}" if isinstance(v, float) else f"{v}{sufixo}" for v in ys]
    fig = go.Figure()
    fig.add_bar(x=xs, y=ys, marker_color=cores, text=textos, textposition="outside")
    fig.add_hline(y=meta, line_dash="dot", line_color="#9aa0a6")
    fig.update_layout(
        title={"text": titulo, "font": {"size": 12}}, height=185,
        margin=dict(l=4, r=4, t=28, b=4), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="", showgrid=False, tickfont=dict(size=9)),
        yaxis=dict(title="", showgrid=False, visible=False),
        showlegend=False,
    )
    return fig


def calcular_indicadores(df):
    total_acidentes = int(df["acidentes"].sum()) if not df.empty else 0
    total_reclamacoes = int(df["reclamacoes"].sum()) if not df.empty else 0
    perda_ton = float(df["perda_ton"].sum()) if not df.empty else 0.0
    ton = float(df["ton_processada"].sum()) if not df.empty else 0.0
    perda_pct = (perda_ton / ton * 100) if ton > 0 else 0.0
    entregas_ok = int(df["entregas_no_prazo"].sum()) if not df.empty else 0
    entregas_total = int(df["entregas_total"].sum()) if not df.empty else 0
    prazo_pct = (entregas_ok / entregas_total * 100) if entregas_total > 0 else 100.0

    perdas_area = {}
    efic_area = {}
    for area in AREAS:
        dfa = df[df["area"] == area] if not df.empty else pd.DataFrame()
        perda = float(dfa["perda_ton"].sum()) if not dfa.empty else 0.0
        ton_area = float(dfa["ton_processada"].sum()) if not dfa.empty else 0.0
        perdas_area[area] = (perda / ton_area * 100) if ton_area > 0 else 0.0
        # média ponderada simples pela tonelada processada; se não houver tonelada, média aritmética dos registros.
        if not dfa.empty and ton_area > 0:
            efic_area[area] = float((dfa["eficiencia_pct"] * dfa["ton_processada"]).sum() / ton_area)
        elif not dfa.empty:
            efic_area[area] = float(dfa["eficiencia_pct"].mean())
        else:
            efic_area[area] = 0.0
    eficiencia_geral = sum(efic_area.values()) / len(AREAS) if AREAS else 0.0

    return {
        "acidentes": total_acidentes,
        "reclamacoes": total_reclamacoes,
        "perda_pct": perda_pct,
        "prazo_pct": prazo_pct,
        "eficiencia_geral": eficiencia_geral,
        "perdas_area": perdas_area,
        "efic_area": efic_area,
        "perda_ton": perda_ton,
        "ton": ton,
        "entregas_ok": entregas_ok,
        "entregas_total": entregas_total,
    }


def cabecalho():
    c1, c2 = st.columns([1.2, 1])
    with c1:
        st.markdown('<div class="titulo-sqdcp">SQDCP</div>', unsafe_allow_html=True)
        st.markdown('<div class="subtitulo">Segurança | Qualidade | Custos | Delivery | Processo</div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="unidade">UNIDADE RS</div>', unsafe_allow_html=True)


def filtro_mes_semana(df):
    ano_atual = date.today().year
    col1, col2, col3, col4 = st.columns([1, 1, 1, 5])
    anos_disponiveis = sorted(pd.to_datetime(df["data"], errors="coerce").dt.year.dropna().unique().astype(int).tolist()) if not df.empty else [ano_atual]
    if ano_atual not in anos_disponiveis:
        anos_disponiveis.append(ano_atual)
        anos_disponiveis = sorted(anos_disponiveis)
    with col1:
        ano = st.selectbox("Ano", anos_disponiveis, index=len(anos_disponiveis)-1)
    with col2:
        mes_nome = st.selectbox("Mês", list(MESES.values()), index=date.today().month-1)
    with col3:
        semana = st.selectbox("Semana", ["Todas"] + list(range(1, 7)), index=0)
    with col4:
        st.markdown('<div class="fmds-bar"><b>FMDS:</b> foco em gestão visual, desvio destacado por cor e ação rápida na rotina diária.</div>', unsafe_allow_html=True)
    mes = MESES_INV[mes_nome]
    dff = df[(pd.to_datetime(df["data"], errors="coerce").dt.year == ano) & (df["mes"] == mes)] if not df.empty else df
    if semana != "Todas" and not dff.empty:
        dff = dff[dff["semana"] == int(semana)]
    return ano, mes, mes_nome, semana, dff


def card_segurança(df, ind):
    ok = status_bool(ind["acidentes"], METAS["acidentes_max"], "max")
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">S</div><div class="card-subtitle">Acidentes</div>', unsafe_allow_html=True)
    st.plotly_chart(gauge(ind["acidentes"], METAS["acidentes_max"], "Meta: zero acidente", "max", ""), use_container_width=True, config={"displayModeBar": False})
    st.markdown(f'<p class="kpi-num">{ind["acidentes"]}</p><p class="kpi-label">acidentes no período</p><div style="text-align:center">{status_html(ok)}</div>', unsafe_allow_html=True)
    st.plotly_chart(barras_serie(df, "acidentes", "Evolução diária", meta=0, modo="max"), use_container_width=True, config={"displayModeBar": False})
    st.markdown('<p class="small-note">Regra FMDS: acidente gera desvio vermelho e tratativa imediata.</p>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def card_qualidade(df, ind):
    ok = status_bool(ind["reclamacoes"], METAS["reclamacoes_max_mes"], "max")
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">Q</div><div class="card-subtitle">Reclamações de Clientes</div>', unsafe_allow_html=True)
    st.plotly_chart(gauge(ind["reclamacoes"], METAS["reclamacoes_max_mes"], "Meta mensal", "max", ""), use_container_width=True, config={"displayModeBar": False})
    st.markdown(f'<p class="kpi-num">{ind["reclamacoes"]}</p><p class="kpi-label">reclamações no período</p><div style="text-align:center">{status_html(ok)}</div>', unsafe_allow_html=True)
    st.plotly_chart(barras_serie(df, "reclamacoes", "Evolução diária", meta=METAS["reclamacoes_max_mes"] / 4, modo="max"), use_container_width=True, config={"displayModeBar": False})
    st.markdown('<p class="small-note">Acompanhar tendência e priorizar maiores desvios de qualidade.</p>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def card_custos(df, ind):
    ok = status_bool(ind["perda_pct"], METAS["perda_pct_max"], "max")
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">C</div><div class="card-subtitle">Perdas / Ton Processada</div>', unsafe_allow_html=True)
    st.plotly_chart(gauge(ind["perda_pct"], METAS["perda_pct_max"], "Perda total", "max", "%"), use_container_width=True, config={"displayModeBar": False})
    st.markdown(f'<p class="kpi-num">{ind["perda_pct"]:.2f}%</p><p class="kpi-label">{ind["perda_ton"]:.2f} ton perdidas / {ind["ton"]:.2f} ton processadas</p><div style="text-align:center">{status_html(ok)}</div>', unsafe_allow_html=True)
    st.plotly_chart(barras_area(ind["perdas_area"], METAS["perda_pct_max"], "Perdas por área", "max", "%"), use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)


def card_delivery(df, ind):
    ok = status_bool(ind["prazo_pct"], METAS["prazo_min"], "min")
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">D</div><div class="card-subtitle">Atendimento no Prazo</div>', unsafe_allow_html=True)
    st.plotly_chart(gauge(ind["prazo_pct"], METAS["prazo_min"], "Meta mínima", "min", "%"), use_container_width=True, config={"displayModeBar": False})
    st.markdown(f'<p class="kpi-num">{ind["prazo_pct"]:.1f}%</p><p class="kpi-label">{ind["entregas_ok"]} no prazo / {ind["entregas_total"]} entregas</p><div style="text-align:center">{status_html(ok)}</div>', unsafe_allow_html=True)
    # Calcula % diário para gráfico
    if df.empty:
        dfd = pd.DataFrame(columns=["data", "prazo_pct_dia"])
    else:
        dfd = df.copy()
        dfd["dia"] = pd.to_datetime(dfd["data"], errors="coerce").dt.day
        dfd = dfd.groupby("dia", as_index=False).agg({"entregas_no_prazo": "sum", "entregas_total": "sum"})
        dfd["prazo_pct_dia"] = dfd.apply(lambda r: (r["entregas_no_prazo"] / r["entregas_total"] * 100) if r["entregas_total"] else 100, axis=1)
    fig = go.Figure()
    cores = ["#57a85a" if v >= METAS["prazo_min"] else "#ff5b57" for v in dfd.get("prazo_pct_dia", pd.Series(dtype=float)).tolist()]
    fig.add_bar(x=dfd.get("dia", []), y=dfd.get("prazo_pct_dia", []), marker_color=cores, text=[f"{v:.0f}%" for v in dfd.get("prazo_pct_dia", [])], textposition="outside")
    fig.add_hline(y=METAS["prazo_min"], line_dash="dot", line_color="#9aa0a6")
    fig.update_layout(title={"text": "Evolução diária", "font": {"size": 12}}, height=170, margin=dict(l=4, r=4, t=28, b=4), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", yaxis=dict(visible=False), xaxis=dict(showgrid=False), showlegend=False)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)


def card_processo(df, ind):
    ok = status_bool(ind["eficiencia_geral"], METAS["eficiencia_min"], "min")
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">P</div><div class="card-subtitle">Eficiência por Área</div>', unsafe_allow_html=True)
    st.plotly_chart(gauge(ind["eficiencia_geral"], METAS["eficiencia_min"], "Eficiência média", "min", "%"), use_container_width=True, config={"displayModeBar": False})
    st.markdown(f'<p class="kpi-num">{ind["eficiencia_geral"]:.1f}%</p><p class="kpi-label">média das áreas produtivas</p><div style="text-align:center">{status_html(ok)}</div>', unsafe_allow_html=True)
    st.plotly_chart(barras_area(ind["efic_area"], METAS["eficiencia_min"], "Eficiência: Prensas, Litografia e Montagem", "min", "%"), use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)


def dashboard(df_filtrado):
    ind = calcular_indicadores(df_filtrado)
    cols = st.columns(5)
    with cols[0]: card_segurança(df_filtrado, ind)
    with cols[1]: card_qualidade(df_filtrado, ind)
    with cols[2]: card_custos(df_filtrado, ind)
    with cols[3]: card_delivery(df_filtrado, ind)
    with cols[4]: card_processo(df_filtrado, ind)


def tela_lancamentos():
    st.subheader("Lançamento em massa")
    st.caption("Preencha várias linhas e salve de uma única vez. O mês é calculado automaticamente pela data.")
    hoje = date.today()
    base = pd.DataFrame([
        {
            "data": hoje,
            "semana": 1,
            "turno": "1º",
            "area": "Prensas",
            "acidentes": 0,
            "reclamacoes": 0,
            "perda_ton": 0.0,
            "ton_processada": 0.0,
            "entregas_no_prazo": 0,
            "entregas_total": 0,
            "eficiencia_pct": 0.0,
            "observacao": "",
        }
    ])

    edited = st.data_editor(
        base,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY", required=True),
            "semana": st.column_config.NumberColumn("Semana", min_value=1, max_value=6, step=1),
            "turno": st.column_config.SelectboxColumn("Turno", options=TURNOS, required=True),
            "area": st.column_config.SelectboxColumn("Área", options=AREAS, required=True),
            "acidentes": st.column_config.NumberColumn("Acidentes", min_value=0, step=1),
            "reclamacoes": st.column_config.NumberColumn("Reclamações", min_value=0, step=1),
            "perda_ton": st.column_config.NumberColumn("Perda ton", min_value=0.0, step=0.01, format="%.2f"),
            "ton_processada": st.column_config.NumberColumn("Ton processada", min_value=0.0, step=0.01, format="%.2f"),
            "entregas_no_prazo": st.column_config.NumberColumn("Entregas no prazo", min_value=0, step=1),
            "entregas_total": st.column_config.NumberColumn("Entregas total", min_value=0, step=1),
            "eficiencia_pct": st.column_config.NumberColumn("Eficiência %", min_value=0.0, max_value=150.0, step=0.1, format="%.1f"),
            "observacao": st.column_config.TextColumn("Observação"),
        },
        key="editor_lancamento_massivo",
    )

    c1, c2 = st.columns([1, 4])
    with c1:
        if st.button("Salvar lançamentos", type="primary", use_container_width=True):
            qtd = salvar_lancamentos(edited)
            st.success(f"{qtd} lançamento(s) salvo(s).")
            st.cache_data.clear()
            st.rerun()
    with c2:
        modelo = base.copy()
        modelo["data"] = pd.to_datetime(modelo["data"]).dt.strftime("%d/%m/%Y")
        st.download_button(
            "Baixar modelo CSV",
            data=modelo.to_csv(index=False, sep=";", decimal=","),
            file_name="modelo_lancamento_sqdcp.csv",
            mime="text/csv",
            use_container_width=False,
        )

    st.divider()
    st.subheader("Importação por CSV")
    arq = st.file_uploader("Importar arquivo CSV no padrão do modelo", type=["csv"])
    if arq is not None:
        try:
            df_imp = pd.read_csv(arq, sep=None, engine="python")
            # Normalização de nomes usuais
            df_imp.columns = [str(c).strip() for c in df_imp.columns]
            if "data" not in df_imp.columns and "Data" in df_imp.columns:
                df_imp = df_imp.rename(columns={"Data": "data"})
            if st.button("Confirmar importação CSV", type="primary"):
                qtd = salvar_lancamentos(df_imp)
                st.success(f"{qtd} linha(s) importada(s).")
                st.cache_data.clear()
                st.rerun()
            st.dataframe(df_imp.head(50), use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Não foi possível ler o CSV: {e}")


def tela_base(df):
    st.subheader("Base consolidada")
    st.caption("Visão consolidada, sem consulta individual. Use apenas para conferência da massa de dados lançada.")
    if df.empty:
        st.info("Ainda não existem lançamentos salvos.")
    else:
        vis = df.copy()
        vis["data"] = pd.to_datetime(vis["data"], errors="coerce").dt.strftime("%d/%m/%Y")
        st.dataframe(vis.drop(columns=["id", "criado_em"], errors="ignore"), use_container_width=True, hide_index=True)
        st.download_button("Exportar base CSV", vis.to_csv(index=False, sep=";", decimal=","), "base_sqdcp.csv", "text/csv")

    with st.expander("Área administrativa - limpar base"):
        senha = st.text_input("Senha", type="password")
        if st.button("Excluir todos os lançamentos", type="secondary"):
            if senha == "QualidadeRS":
                excluir_tudo()
                st.success("Base excluída.")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("Senha incorreta.")


def main():
    init_db()
    df = carregar_dados()
    cabecalho()
    ano, mes, mes_nome, semana, df_filtrado = filtro_mes_semana(df)

    aba1, aba2, aba3 = st.tabs(["Painel FMDS", "Lançamentos em massa", "Base consolidada"])
    with aba1:
        if df.empty:
            st.info("Inclua os primeiros dados na aba 'Lançamentos em massa'.")
        dashboard(df_filtrado)
    with aba2:
        tela_lancamentos()
    with aba3:
        tela_base(df)


if __name__ == "__main__":
    main()
