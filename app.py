import streamlit as st
import pandas as pd
from datetime import date, timedelta
import time
import gspread
import numpy as np

# ==============================================================================
# 1. CONFIGURAÇÃO E CONEXÃO GOOGLE SHEETS
# ==============================================================================

SHEET_ID = '1BNjgWhvEj8NbnGr4x7F42LW7QbQiG5kZ1FBhfr9Q-4g'
PLANILHA_TITULO = 'Dados Automóvel'

@st.cache_resource(ttl=3600)
def get_gspread_client():
    try:
        creds_info = st.secrets["gcp_service_account"]
        gc = gspread.service_account_from_dict(creds_info)
        return gc
    except Exception as e:
        st.error(f"Erro de autenticação: {e}")
        st.stop()

@st.cache_data(ttl=5)
def get_sheet_data(sheet_name):
    """Lê dados garantindo que IDs sejam inteiros e valores sejam floats."""
    expected_cols = {
        'veiculo': ['id_veiculo', 'nome', 'placa', 'ano', 'valor_pago', 'data_compra'],
        'prestador': ['id_prestador', 'empresa', 'telefone', 'nome_prestador', 'cnpj', 'email', 'endereco', 'numero', 'cidade', 'bairro', 'cep'],
        'servico': ['id_servico', 'id_veiculo', 'id_prestador', 'nome_servico', 'data_servico', 'garantia_dias', 'valor', 'km_realizado', 'km_proxima_revisao', 'registro', 'data_vencimento']
    }
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID) if SHEET_ID else gc.open(PLANILHA_TITULO)
        worksheet = sh.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())

        if df.empty:
            return pd.DataFrame(columns=expected_cols.get(sheet_name, []))

        # Padronização de Tipos
        id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
        df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)
        
        if sheet_name == 'servico':
            df['id_veiculo'] = pd.to_numeric(df['id_veiculo'], errors='coerce').fillna(0).astype(int)
            df['id_prestador'] = pd.to_numeric(df['id_prestador'], errors='coerce').fillna(0).astype(int)
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0.0)
            df['data_servico'] = pd.to_datetime(df['data_servico'], errors='coerce')
            df['data_vencimento'] = pd.to_datetime(df['data_vencimento'], errors='coerce')
        
        return df
    except Exception:
        return pd.DataFrame(columns=expected_cols.get(sheet_name, []))

def write_sheet_data(sheet_name, df_new):
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID) if SHEET_ID else gc.open(PLANILHA_TITULO)
        worksheet = sh.worksheet(sheet_name)
        
        df_save = df_new.copy()
        for col in df_save.select_dtypes(include=['datetime64']).columns:
            df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')
            
        worksheet.clear()
        worksheet.update('A1', [df_save.columns.tolist()] + df_save.values.tolist(), value_input_option='USER_ENTERED')
        get_sheet_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# ==============================================================================
# 2. OPERAÇÕES CRUD CORE
# ==============================================================================

def execute_crud_operation(sheet_name, data=None, id_value=None, operation='insert'):
    df = get_sheet_data(sheet_name)
    id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'

    if operation == 'insert':
        new_id = int(df[id_col].max() + 1) if not df.empty else 1
        data[id_col] = new_id
        df_updated = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
        return write_sheet_data(sheet_name, df_updated), new_id

    elif operation == 'update':
        idx = df[df[id_col] == int(id_value)].index
        if idx.empty: return False, None
        for k, v in data.items(): df.loc[idx, k] = v
        return write_sheet_data(sheet_name, df), id_value

    elif operation == 'delete':
        df_updated = df[df[id_col] != int(id_value)]
        return write_sheet_data(sheet_name, df_updated), id_value

# ==============================================================================
# 3. COMPONENTES DE INTERFACE (FORMULÁRIOS MANUAIS)
# ==============================================================================

def manage_vehicle_form():
    st.subheader("🚗 Gestão de Veículos")
    state_key = 'edit_vehicle_id'
    
    if st.session_state[state_key] is None:
        if st.button("➕ Novo Veículo"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
        
        df = get_sheet_data('veiculo')
        for _, row in df.iterrows():
            c1, c2 = st.columns([0.8, 0.2])
            c1.markdown(f"**{row['nome']}** ({row['placa']}) - {row['ano']}")
            if c2.button("✏️", key=f"ed_v_{row['id_veiculo']}"):
                st.session_state[state_key] = row['id_veiculo']
                st.rerun()
    else:
        is_new = st.session_state[state_key] == 'NEW'
        df = get_sheet_data('veiculo')
        current_data = df[df['id_veiculo'] == st.session_state[state_key]].iloc[0] if not is_new else None
        
        with st.form("form_veiculo"):
            nome = st.text_input("Nome do Veículo", value="" if is_new else current_data['nome'])
            placa = st.text_input("Placa", value="" if is_new else current_data['placa'])
            ano = st.number_input("Ano", value=2024 if is_new else int(current_data['ano']))
            valor = st.number_input("Valor Pago", value=0.0 if is_new else float(current_data['valor_pago']))
            data_c = st.date_input("Data Compra", value=date.today() if is_new else pd.to_datetime(current_data['data_compra']))
            
            if st.form_submit_button("Salvar"):
                payload = {'nome': nome, 'placa': placa, 'ano': ano, 'valor_pago': valor, 'data_compra': data_c.isoformat()}
                if is_new: execute_crud_operation('veiculo', data=payload, operation='insert')
                else: execute_crud_operation('veiculo', data=payload, id_value=st.session_state[state_key], operation='update')
                st.session_state[state_key] = None
                st.rerun()
        if st.button("Cancelar"):
            st.session_state[state_key] = None
            st.rerun()

def manage_service_form():
    st.subheader("🔧 Gestão de Serviços")
    df_v = get_sheet_data('veiculo')
    df_p = get_sheet_data('prestador')
    
    if df_v.empty or df_p.empty:
        st.warning("Cadastre primeiro um Veículo e um Prestador.")
        return

    # Mapeamento para Selectbox
    v_map = {f"{r['nome']} ({r['placa']})": r['id_veiculo'] for _, r in df_v.iterrows()}
    p_map = {r['empresa']: r['id_prestador'] for _, r in df_p.iterrows()}

    with st.form("form_servico"):
        veiculo_sel = st.selectbox("Veículo", options=list(v_map.keys()))
        prestador_sel = st.selectbox("Prestador", options=list(p_map.keys()))
        nome_s = st.text_input("Descrição do Serviço")
        data_s = st.date_input("Data do Serviço")
        garantia = st.number_input("Garantia (Dias)", value=90)
        valor = st.number_input("Valor (R$)", format="%.2f")
        km_r = st.number_input("KM Atual", value=0)
        km_p = st.number_input("Próxima Revisão (KM)", value=0)
        reg = st.text_input("Código de Registro/Nota")

        if st.form_submit_button("Registrar Serviço"):
            d_venc = data_s + timedelta(days=int(garantia))
            payload = {
                'id_veiculo': v_map[veiculo_sel], 'id_prestador': p_map[prestador_sel],
                'nome_servico': nome_s, 'data_servico': data_s.isoformat(),
                'garantia_dias': garantia, 'valor': valor, 'km_realizado': km_r,
                'km_proxima_revisao': km_p, 'registro': reg, 'data_vencimento': d_venc.isoformat()
            }
            execute_crud_operation('servico', data=payload, operation='insert')
            st.success("Serviço cadastrado!")
            time.sleep(1)
            st.rerun()

# ==============================================================================
# 4. DASHBOARDS E EXECUÇÃO PRINCIPAL
# ==============================================================================

def get_full_service_data():
    df_s = get_sheet_data('servico')
    df_v = get_sheet_data('veiculo')
    df_p = get_sheet_data('prestador')
    if df_s.empty or df_v.empty or df_p.empty: return pd.DataFrame()

    df_m = pd.merge(df_s, df_v[['id_veiculo', 'nome', 'placa']], on='id_veiculo', how='left')
    df_m = pd.merge(df_m, df_p[['id_prestador', 'empresa']], on='id_prestador', how='left')
    df_m['Dias p/ Vencer'] = (df_m['data_vencimento'] - pd.to_datetime(date.today())).dt.days
    return df_m

def main():
    st.set_page_config(page_title="Controle Automotivo", layout="wide")
    
    # Inicializar Session States
    for key in ['edit_vehicle_id', 'edit_prestador_id']:
        if key not in st.session_state: st.session_state[key] = None

    st.title("🚗 Sistema de Controle Automotivo")

    # Sidebar Admin
    with st.sidebar:
        st.header("⚙️ Ferramentas")
        if st.button("🧪 Gerar Dados de Teste"):
            # Lógica simplificada de teste
            execute_crud_operation('veiculo', data={'nome':'Fusca','placa':'ABC1234','ano':1970,'valor_pago':15000,'data_compra':'2023-01-01'}, operation='insert')
            execute_crud_operation('prestador', data={'empresa':'Oficina do Zé','cnpj':'00.000/0001-00'}, operation='insert')
            st.rerun()

    tab_resumo, tab_hist, tab_gestao = st.tabs(["📊 Resumo", "📈 Histórico", "➕ Gestão Manual"])

    with tab_resumo:
        df = get_full_service_data()
        if not df.empty:
            st.metric("Total Gasto", f"R$ {df['valor'].sum():,.2f}")
            st.bar_chart(df.groupby('nome')['valor'].sum())
        else: st.info("Sem dados cadastrados.")

    with tab_hist:
        df_h = get_full_service_data()
        if not df_h.empty:
            st.dataframe(df_h[['nome', 'placa', 'nome_servico', 'empresa', 'data_servico', 'valor', 'Dias p/ Vencer']], use_container_width=True)

    with tab_gestao:
        escolha = st.radio("O que deseja gerenciar?", ["Veículo", "Serviço", "Prestador"], horizontal=True)
        st.divider()
        if escolha == "Veículo": manage_vehicle_form()
        elif escolha == "Serviço": manage_service_form()
        else: st.info("Formulário de Prestador segue a mesma lógica do Veículo.")

if __name__ == '__main__':
    main()