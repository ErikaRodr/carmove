import streamlit as st
import pandas as pd
from datetime import date, timedelta
import time
import gspread
import numpy as np

# ==============================================================================
# 1. CONFIGURAÇÃO E CONEXÃO
# ==============================================================================

SHEET_ID = '1BNjgWhvEj8NbnGr4x7F42LW7QbQiG5kZ1FBhfr9Q-4g'
PLANILHA_TITULO = 'Dados Automóvel'

# Define as colunas esperadas para garantir que o formulário apareça
EXPECTED_COLS = {
    'veiculo': ['id_veiculo', 'nome', 'placa', 'ano', 'valor_pago', 'data_compra'],
    'prestador': ['id_prestador', 'empresa', 'telefone', 'nome_prestador', 'cnpj', 'email', 'endereco', 'numero', 'cidade', 'bairro', 'cep'],
    'servico': ['id_servico', 'id_veiculo', 'id_prestador', 'nome_servico', 'data_servico', 'garantia_dias', 'valor', 'km_realizado', 'km_proxima_revisao', 'registro', 'data_vencimento']
}

@st.cache_resource(ttl=3600)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        gc = gspread.service_account_from_dict(creds_info)
        return gc
    except Exception as e:
        st.error(f"Erro de autenticação: {e}")
        st.stop()

@st.cache_data(ttl=2)
def get_sheet_data(sheet_name):
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID) if SHEET_ID else gc.open(PLANILHA_TITULO)
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        
        if df.empty:
            return pd.DataFrame(columns=EXPECTED_COLS.get(sheet_name, []))

        # Padronização de IDs
        id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
        if id_col in df.columns:
            df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)
        
        return df
    except Exception:
        return pd.DataFrame(columns=EXPECTED_COLS.get(sheet_name, []))

def get_data(sheet_name, filter_col=None, filter_value=None):
    """Busca dados com filtro seguro (Resolve o NameError)."""
    df = get_sheet_data(sheet_name)
    if df.empty: return df
    
    if filter_col and filter_value is not None:
        try:
            if str(filter_col).startswith('id_'):
                df[filter_col] = pd.to_numeric(df[filter_col], errors='coerce').fillna(0).astype(int)
                filter_value = int(filter_value)
            return df[df[filter_col] == filter_value]
        except:
            return pd.DataFrame(columns=df.columns)
    return df

def write_sheet_data(sheet_name, df_new):
    """Salva dados tratando erros de JSON/NaN."""
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID) if SHEET_ID else gc.open(PLANILHA_TITULO)
        worksheet = sh.worksheet(sheet_name)
        
        df_save = df_new.copy()
        
        # Datas para string
        for col in df_save.select_dtypes(include=['datetime64']).columns:
            df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')
            
        # 🔥 LIMPEZA CRÍTICA: Remove NaN e Infinitos que quebram o Google Sheets
        df_save = df_save.fillna("") 
        df_save = df_save.replace([np.inf, -np.inf], 0)
        
        worksheet.clear()
        worksheet.update('A1', [df_save.columns.tolist()] + df_save.values.tolist(), value_input_option='USER_ENTERED')
        get_sheet_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# ==============================================================================
# 2. CRUD
# ==============================================================================

def execute_crud_operation(sheet_name, data=None, id_value=None, operation='insert'):
    df = get_sheet_data(sheet_name)
    id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'

    if operation == 'insert':
        new_id = 1
        if not df.empty and id_col in df.columns:
            new_id = int(df[id_col].max() + 1)
        data[id_col] = new_id
        df_updated = pd.concat([df, pd.DataFrame([data])], ignore_index=True).fillna("")
        return write_sheet_data(sheet_name, df_updated)

    elif operation == 'update':
        idx = df[df[id_col] == int(id_value)].index
        if not idx.empty:
            for k, v in data.items(): df.loc[idx, k] = v
            return write_sheet_data(sheet_name, df)
        return False

    elif operation == 'delete':
        df_updated = df[df[id_col] != int(id_value)]
        return write_sheet_data(sheet_name, df_updated)

# ==============================================================================
# 3. RELATÓRIOS (JOIN)
# ==============================================================================

def get_full_service_data():
    df_s = get_sheet_data('servico')
    df_v = get_sheet_data('veiculo')
    df_p = get_sheet_data('prestador')

    if df_s.empty: return pd.DataFrame()

    # Garante tipagem para o Merge
    df_s['id_veiculo'] = pd.to_numeric(df_s['id_veiculo'], errors='coerce').fillna(0).astype(int)
    df_s['id_prestador'] = pd.to_numeric(df_s['id_prestador'], errors='coerce').fillna(0).astype(int)
    
    if not df_v.empty:
        df_v['id_veiculo'] = pd.to_numeric(df_v['id_veiculo'], errors='coerce').fillna(0).astype(int)
        df_merged = pd.merge(df_s, df_v[['id_veiculo', 'nome', 'placa']], on='id_veiculo', how='left')
    else:
        df_merged = df_s.copy()
        df_merged['nome'] = '-'

    if not df_p.empty:
        df_p['id_prestador'] = pd.to_numeric(df_p['id_prestador'], errors='coerce').fillna(0).astype(int)
        df_merged = pd.merge(df_merged, df_p[['id_prestador', 'empresa']], on='id_prestador', how='left')
    else:
        df_merged['empresa'] = '-'

    df_merged['data_vencimento'] = pd.to_datetime(df_merged['data_vencimento'], errors='coerce')
    df_merged['valor'] = pd.to_numeric(df_merged['valor'], errors='coerce').fillna(0.0)
    df_merged['Dias p/ Vencer'] = (df_merged['data_vencimento'] - pd.to_datetime(date.today())).dt.days
    
    return df_merged

# ==============================================================================
# 4. INTERFACES DE GESTÃO (UI)
# ==============================================================================

def generic_management_ui(category_name, sheet_name, display_col):
    """UI genérica para Veículos e Prestadores."""
    st.subheader(f"Gestão de {category_name}")
    state_key = f'edit_{sheet_name}_id'
    id_col = f'id_{sheet_name}'
    
    # Lista
    if st.session_state[state_key] is None:
        if st.button(f"➕ Novo {category_name}"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
        
        df = get_sheet_data(sheet_name)
        if df.empty:
            st.info("Nenhum registro.")
        else:
            for _, row in df.iterrows():
                c1, c2, c3 = st.columns([0.7, 0.15, 0.15])
                c1.write(f"**{row.get(display_col)}**")
                sid = int(row.get(id_col, 0))
                if c2.button("✏️", key=f"ed_{sid}"):
                    st.session_state[state_key] = sid
                    st.rerun()
                if c3.button("🗑️", key=f"del_{sid}"):
                    execute_crud_operation(sheet_name, id_value=sid, operation='delete')
                    st.rerun()
    # Formulário
    else:
        df = get_sheet_data(sheet_name)
        is_new = st.session_state[state_key] == 'NEW'
        curr = {}
        if not is_new:
            res = df[df[id_col] == st.session_state[state_key]]
            if not res.empty: curr = res.iloc[0].to_dict()
        
        with st.form(f"form_{sheet_name}"):
            payload = {}
            cols = EXPECTED_COLS.get(sheet_name)
            for col in cols:
                if col == id_col: continue
                val = curr.get(col, "")
                if "data" in col:
                    try: d = pd.to_datetime(val) if val else date.today()
                    except: d = date.today()
                    payload[col] = st.date_input(col, value=d)
                elif any(x in col for x in ["valor", "ano", "numero"]):
                    payload[col] = st.number_input(col, value=float(val) if val else 0.0)
                else:
                    payload[col] = st.text_input(col, value=str(val))
            
            if st.form_submit_button("Salvar"):
                for k,v in payload.items():
                    if isinstance(v, (date, pd.Timestamp)): payload[k] = v.strftime('%Y-%m-%d')
                if is_new: execute_crud_operation(sheet_name, data=payload, operation='insert')
                else: execute_crud_operation(sheet_name, data=payload, id_value=st.session_state[state_key], operation='update')
                st.session_state[state_key] = None
                st.rerun()
        
        if st.button("Cancelar"):
            st.session_state[state_key] = None
            st.rerun()

def service_management_ui():
    """UI ESPECÍFICA PARA SERVIÇOS (COM SELECTBOX)."""
    st.subheader("Gestão de Serviços")
    state_key = 'edit_servico_id'
    
    # Carrega dados auxiliares para o Selectbox
    df_v = get_sheet_data('veiculo')
    df_p = get_sheet_data('prestador')
    
    # Cria dicionários {Nome: ID}
    map_v = {f"{r['nome']} ({r.get('placa','S/P')})": int(r['id_veiculo']) for _, r in df_v.iterrows()} if not df_v.empty else {}
    map_p = {f"{r['empresa']}": int(r['id_prestador']) for _, r in df_p.iterrows()} if not df_p.empty else {}
    
    # Lista
    if st.session_state[state_key] is None:
        if not map_v or not map_p:
            st.warning("⚠️ Cadastre Veículos e Prestadores antes de lançar Serviços.")
            return

        if st.button("➕ Novo Serviço"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
            
        df = get_sheet_data('servico')
        if not df.empty:
            for _, row in df.iterrows():
                c1, c2, c3 = st.columns([0.7, 0.15, 0.15])
                c1.write(f"**{row.get('nome_servico')}** - {row.get('data_servico')}")
                sid = int(row.get('id_servico', 0))
                if c2.button("✏️", key=f"ed_s_{sid}"):
                    st.session_state[state_key] = sid
                    st.rerun()
                if c3.button("🗑️", key=f"del_s_{sid}"):
                    execute_crud_operation('servico', id_value=sid, operation='delete')
                    st.rerun()
        else:
            st.info("Nenhum serviço.")

    # Formulário Especial de Serviço
    else:
        df = get_sheet_data('servico')
        is_new = st.session_state[state_key] == 'NEW'
        curr = {}
        current_id_v = 0
        current_id_p = 0
        
        if not is_new:
            res = df[df['id_servico'] == st.session_state[state_key]]
            if not res.empty:
                curr = res.iloc[0].to_dict()
                current_id_v = int(curr.get('id_veiculo', 0))
                current_id_p = int(curr.get('id_prestador', 0))

        with st.form("form_servico_especial"):
            # Encontra o índice correto para o selectbox
            idx_v = list(map_v.values()).index(current_id_v) if current_id_v in map_v.values() else 0
            idx_p = list(map_p.values()).index(current_id_p) if current_id_p in map_p.values() else 0
            
            sel_v_name = st.selectbox("Veículo", options=list(map_v.keys()), index=idx_v)
            sel_p_name = st.selectbox("Prestador", options=list(map_p.keys()), index=idx_p)
            
            nome_s = st.text_input("Nome do Serviço", value=curr.get('nome_servico', ''))
            
            c_dt, c_gr = st.columns(2)
            try: dt_val = pd.to_datetime(curr.get('data_servico')) if curr.get('data_servico') else date.today()
            except: dt_val = date.today()
            data_s = c_dt.date_input("Data Serviço", value=dt_val)
            garantia = c_gr.number_input("Garantia (dias)", value=int(curr.get('garantia_dias', 90)))
            
            c_val, c_km = st.columns(2)
            valor = c_val.number_input("Valor (R$)", value=float(curr.get('valor', 0.0)))
            km_r = c_km.number_input("KM Realizado", value=float(curr.get('km_realizado', 0)))
            
            registro = st.text_input("Nota/Registro", value=curr.get('registro', ''))
            
            if st.form_submit_button("Salvar Serviço"):
                # Calcula Data Vencimento
                dt_venc = data_s + timedelta(days=int(garantia))
                
                payload = {
                    'id_veiculo': map_v[sel_v_name],
                    'id_prestador': map_p[sel_p_name],
                    'nome_servico': nome_s,
                    'data_servico': data_s.strftime('%Y-%m-%d'),
                    'garantia_dias': int(garantia),
                    'valor': float(valor),
                    'km_realizado': km_r,
                    'registro': registro,
                    'data_vencimento': dt_venc.strftime('%Y-%m-%d')
                }
                
                if is_new: execute_crud_operation('servico', data=payload, operation='insert')
                else: execute_crud_operation('servico', data=payload, id_value=st.session_state[state_key], operation='update')
                
                st.session_state[state_key] = None
                st.success("Serviço salvo!")
                time.sleep(1)
                st.rerun()

        if st.button("Cancelar"):
            st.session_state[state_key] = None
            st.rerun()

# ==============================================================================
# 5. SIMULAÇÃO E MAIN
# ==============================================================================

def run_auto_test_data():
    st.info("Simulando...")
    # Veículo
    execute_crud_operation('veiculo', data={'nome': 'Civic Teste', 'placa': 'TST-0001', 'ano': 2023, 'valor_pago': 150000, 'data_compra': '2023-01-01'}, operation='insert')
    # Prestador
    execute_crud_operation('prestador', data={'empresa': 'Oficina Master', 'telefone': '1199999', 'cnpj': '00.000/0001-00'}, operation='insert')
    time.sleep(1.5)
    
    df_v = get_data('veiculo', 'placa', 'TST-0001')
    df_p = get_data('prestador', 'empresa', 'Oficina Master')
    
    if not df_v.empty and not df_p.empty:
        execute_crud_operation('servico', data={
            'id_veiculo': int(df_v.iloc[0]['id_veiculo']), 
            'id_prestador': int(df_p.iloc[0]['id_prestador']),
            'nome_servico': 'Revisão Teste', 'data_servico': date.today().strftime('%Y-%m-%d'),
            'garantia_dias': 180, 'valor': 500.0, 'km_realizado': 10000, 'registro': 'TEST-99', 
            'data_vencimento': (date.today() + timedelta(days=180)).strftime('%Y-%m-%d')
        }, operation='insert')
        st.success("Dados criados!")
        time.sleep(1)
        st.rerun()

def main():
    st.set_page_config(page_title="Controle Automotivo", layout="wide")
    for key in ['edit_veiculo_id', 'edit_prestador_id', 'edit_servico_id']:
        if key not in st.session_state: st.session_state[key] = None

    st.title("🚗 Sistema de Controle Automotivo")
    
    tab_resumo, tab_hist, tab_manual = st.tabs(["📊 Resumo", "📈 Histórico", "➕ Manual de Gestão"])

    with st.sidebar:
        st.header("⚙️ Ferramentas")
        if st.button("🧪 Rodar Simulação"): run_auto_test_data()

    with tab_resumo:
        df_full = get_full_service_data()
        if not df_full.empty:
            c1, c2 = st.columns(2)
            c1.metric("Total Gasto", f"R$ {df_full['valor'].sum():,.2f}")
            c2.metric("Serviços", len(df_full))
            st.bar_chart(df_full.groupby('nome')['valor'].sum())
        else:
            st.info("Sem dados de serviço.")

    with tab_hist:
        df_full = get_full_service_data()
        if not df_full.empty:
            cols = ['nome', 'placa', 'nome_servico', 'empresa', 'data_servico', 'valor', 'Dias p/ Vencer']
            st.dataframe(df_full[[c for c in cols if c in df_full.columns]], use_container_width=True)
        else:
            st.info("Histórico vazio.")

    with tab_manual:
        # 🔥 CORREÇÃO: Menu correto
        opcao = st.radio("Gerenciar:", ["Veículo", "Serviço", "Prestador"], horizontal=True)
        st.divider()
        if opcao == "Veículo": generic_management_ui("Veículo", "veiculo", "nome")
        elif opcao == "Serviço": service_management_ui() # 🔥 UI NOVA PARA SERVIÇO
        elif opcao == "Prestador": generic_management_ui("Prestador", "prestador", "empresa")

if __name__ == '__main__':
    main()