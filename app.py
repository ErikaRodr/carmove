import streamlit as st
import pandas as pd
from datetime import date, timedelta
import time
import gspread

# ==============================================================================
# 1. CONEXÃO E CONFIGURAÇÃO
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

@st.cache_data(ttl=2)
def get_sheet_data(sheet_name):
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID) if SHEET_ID else gc.open(PLANILHA_TITULO)
        worksheet = sh.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        
        # Padronização de IDs para Inteiro
        id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
        if not df.empty and id_col in df.columns:
            df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)
        return df
    except Exception:
        return pd.DataFrame()

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
        return write_sheet_data(sheet_name, df_updated)

    elif operation == 'update':
        idx = df[df[id_col] == int(id_value)].index
        if idx.empty: return False # Proteção contra IndexError
        for k, v in data.items(): df.loc[idx, k] = v
        return write_sheet_data(sheet_name, df)

    elif operation == 'delete':
        df_updated = df[df[id_col] != int(id_value)]
        return write_sheet_data(sheet_name, df_updated)

# ==============================================================================
# 3. INTERFACE DE GESTÃO UNIFICADA (CORREÇÃO DE "SE" E ERRO DE INDEX)
# ==============================================================================

def generic_management_ui(category_name, sheet_name, display_col):
    st.subheader(f"Gestão de {category_name}")
    state_key = f'edit_{sheet_name}_id'
    id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
    
    # 1. LISTAGEM COM EDIÇÃO E EXCLUSÃO
    if st.session_state[state_key] is None:
        if st.button(f"➕ Novo {category_name}"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
        
        df = get_sheet_data(sheet_name)
        if df.empty:
            st.info(f"Nenhum {category_name} cadastrado.")
        else:
            for _, row in df.iterrows():
                col_data, col_edit, col_del = st.columns([0.7, 0.15, 0.15])
                col_data.write(f"**{row[display_col]}**")
                
                if col_edit.button("✏️", key=f"ed_{sheet_name}_{row[id_col]}"):
                    st.session_state[state_key] = row[id_col]
                    st.rerun()
                
                if col_del.button("🗑️", key=f"del_{sheet_name}_{row[id_col]}"):
                    execute_crud_operation(sheet_name, id_value=row[id_col], operation='delete')
                    st.success("Excluído com sucesso!")
                    time.sleep(1)
                    st.rerun()

    # 2. FORMULÁRIO DE CADASTRO/EDIÇÃO
    else:
        is_new = st.session_state[state_key] == 'NEW'
        df = get_sheet_data(sheet_name)
        
        # CORREÇÃO CRÍTICA DO INDEXERROR:
        current_data = {}
        if not is_new:
            filtered = df[df[id_col] == st.session_state[state_key]]
            if not filtered.empty:
                current_data = filtered.iloc[0].to_dict()
            else:
                st.error("Erro: Registro não encontrado.")
                st.session_state[state_key] = None
                st.rerun()

        with st.form(f"form_{sheet_name}"):
            payload = {}
            # Gera campos dinamicamente com base nas colunas da planilha
            for col in df.columns:
                if col == id_col: continue
                label = col.replace("_", " ").title()
                val = current_data.get(col, "")
                
                if "data" in col: payload[col] = st.date_input(label, value=pd.to_datetime(val) if val else date.today())
                elif "valor" in col or "km" in col: payload[col] = st.number_input(label, value=float(val) if val else 0.0)
                else: payload[col] = st.text_input(label, value=str(val))
            
            if st.form_submit_button("Confirmar"):
                # Converte datas para string antes de salvar
                for k, v in payload.items():
                    if isinstance(v, (date, pd.Timestamp)): payload[k] = v.strftime('%Y-%m-%d')
                
                if is_new: execute_crud_operation(sheet_name, data=payload, operation='insert')
                else: execute_crud_operation(sheet_name, data=payload, id_value=st.session_state[state_key], operation='update')
                
                st.session_state[state_key] = None
                st.rerun()
        
        if st.button("Cancelar"):
            st.session_state[state_key] = None
            st.rerun()

# ==============================================================================
# 4. DASHBOARDS E MAIN
# ==============================================================================

def app():
    st.set_page_config(page_title="Controle Automotivo", layout="wide")
    
    # Inicializa estados de edição
    for key in ['edit_veiculo_id', 'edit_prestador_id', 'edit_servico_id']:
        if key not in st.session_state: st.session_state[key] = None

    st.title("🚗 Sistema de Controle Automotivo")

    tab_resumo, tab_hist, tab_manual = st.tabs(["📊 Resumo", "📈 Histórico", "➕ Manual de Gestão"])

    with tab_manual:
        # CORREÇÃO: "se" alterado para "Serviço"
        opcao = st.radio("O que deseja gerenciar?", ["Veículo", "Serviço", "Prestador"], horizontal=True)
        st.divider()

        if opcao == "Veículo":
            generic_management_ui("Veículo", "veiculo", "nome")
        elif opcao == "Serviço":
            generic_management_ui("Serviço", "servico", "nome_servico")
        elif opcao == "Prestador":
            generic_management_ui("Prestador", "prestador", "empresa")

if __name__ == '__main__':
    app()