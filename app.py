import streamlit as st
import pandas as pd
from datetime import date, timedelta
import time
import gspread
import numpy as np

# ==============================================================================
# 1. CONFIGURAÇÃO E DEFINIÇÃO DE ESTRUTURA
# ==============================================================================

SHEET_ID = '1BNjgWhvEj8NbnGr4x7F42LW7QbQiG5kZ1FBhfr9Q-4g'
PLANILHA_TITULO = 'Dados Automóvel'

# 🟢 SOLUÇÃO: Define as colunas fixas para que os campos apareçam
# mesmo que a planilha esteja vazia ou nova.
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
        
        # Se vazio, retorna DataFrame com colunas esperadas
        if df.empty:
            return pd.DataFrame(columns=EXPECTED_COLS.get(sheet_name, []))

        # Padroniza IDs para int
        id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
        if id_col in df.columns:
            df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)
        return df
    except Exception:
        # Retorna vazio seguro em caso de erro de aba
        return pd.DataFrame(columns=EXPECTED_COLS.get(sheet_name, []))

# 🟢 SOLUÇÃO: Função auxiliar para filtros (Resolve o NameError)
def get_data(sheet_name, filter_col=None, filter_value=None):
    df = get_sheet_data(sheet_name)
    if df.empty:
        return df
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
    """Salva os dados tratando erros de JSON/NaN."""
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID) if SHEET_ID else gc.open(PLANILHA_TITULO)
        worksheet = sh.worksheet(sheet_name)
        
        df_save = df_new.copy()
        
        # Converte datas para string
        for col in df_save.select_dtypes(include=['datetime64']).columns:
            df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')
            
        # 🟢 SOLUÇÃO: Remove NaN e Infinitos que quebram o JSON do Google
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
# 2. OPERAÇÕES CRUD
# ==============================================================================

def execute_crud_operation(sheet_name, data=None, id_value=None, operation='insert'):
    df = get_sheet_data(sheet_name)
    id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'

    if operation == 'insert':
        # Garante ID sequencial mesmo se vazio
        new_id = 1
        if not df.empty and id_col in df.columns:
            new_id = int(df[id_col].max() + 1)
            
        data[id_col] = new_id
        
        # Cria nova linha e garante colunas
        new_row = pd.DataFrame([data])
        df_updated = pd.concat([df, new_row], ignore_index=True).fillna("")
        
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
# 3. INTERFACE DE GESTÃO UNIFICADA
# ==============================================================================

def generic_management_ui(category_name, sheet_name, display_col):
    st.subheader(f"Gestão de {category_name}")
    state_key = f'edit_{sheet_name}_id'
    id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
    
    # --- MODO LISTAGEM ---
    if st.session_state[state_key] is None:
        if st.button(f"➕ Novo {category_name}"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
        
        df = get_sheet_data(sheet_name)
        if df.empty or len(df) == 0:
            st.info(f"Nenhum {category_name} cadastrado. Clique em Novo para começar.")
        else:
            for _, row in df.iterrows():
                col_data, col_edit, col_del = st.columns([0.7, 0.15, 0.15])
                
                # Tratamento para display seguro
                display_val = str(row.get(display_col, 'Sem Nome'))
                col_data.write(f"**{display_val}**")
                
                # ID seguro
                safe_id = int(row.get(id_col, 0))
                
                if col_edit.button("✏️", key=f"ed_{sheet_name}_{safe_id}"):
                    st.session_state[state_key] = safe_id
                    st.rerun()
                
                if col_del.button("🗑️", key=f"del_{sheet_name}_{safe_id}"):
                    execute_crud_operation(sheet_name, id_value=safe_id, operation='delete')
                    st.rerun()
    
    # --- MODO FORMULÁRIO ---
    else:
        is_new = st.session_state[state_key] == 'NEW'
        df = get_sheet_data(sheet_name)
        current_data = {}
        
        if not is_new:
            # Busca dados para edição
            res = df[df[id_col] == st.session_state[state_key]]
            if not res.empty: 
                current_data = res.iloc[0].to_dict()
            else:
                st.warning("Registro não encontrado.")
                st.session_state[state_key] = None
                st.rerun()

        with st.form(f"form_{sheet_name}"):
            payload = {}
            # 🟢 SOLUÇÃO: Garante que os campos apareçam usando EXPECTED_COLS
            cols_to_render = EXPECTED_COLS.get(sheet_name, df.columns.tolist())
            
            for col in cols_to_render:
                if col == id_col: continue # Pula ID
                
                label = col.replace("_", " ").title()
                val = current_data.get(col, "")
                
                # Lógica de tipos de campo
                if "data" in col:
                    try:
                        d_val = pd.to_datetime(val) if val else date.today()
                    except:
                        d_val = date.today()
                    payload[col] = st.date_input(label, value=d_val)
                
                elif any(x in col for x in ["valor", "km", "ano", "telefone", "numero"]):
                    try:
                        n_val = float(val) if val else 0.0
                    except:
                        n_val = 0.0
                    payload[col] = st.number_input(label, value=n_val)
                
                else:
                    payload[col] = st.text_input(label, value=str(val))
            
            c1, c2 = st.columns(2)
            if c1.form_submit_button("💾 Salvar"):
                # Formata datas
                for k, v in payload.items():
                    if isinstance(v, (date, pd.Timestamp)): payload[k] = v.strftime('%Y-%m-%d')
                
                if is_new: 
                    execute_crud_operation(sheet_name, data=payload, operation='insert')
                else: 
                    execute_crud_operation(sheet_name, data=payload, id_value=st.session_state[state_key], operation='update')
                
                st.success("Salvo com sucesso!")
                st.session_state[state_key] = None
                time.sleep(1)
                st.rerun()
            
            if c2.form_submit_button("❌ Cancelar"):
                st.session_state[state_key] = None
                st.rerun()

# ==============================================================================
# 4. SIMULAÇÃO E MAIN
# ==============================================================================

def run_auto_test_data():
    st.info("Iniciando simulação de dados...")
    # Veículo
    execute_crud_operation('veiculo', data={'nome': 'Civic Teste', 'placa': 'TST-0001', 'ano': 2023, 'valor_pago': 150000, 'data_compra': '2023-01-01'}, operation='insert')
    # Prestador
    execute_crud_operation('prestador', data={'empresa': 'Oficina Master', 'telefone': '1199999', 'cnpj': '00.000/0001-00'}, operation='insert')
    
    time.sleep(1.5) # Aguarda propagação
    
    # Busca IDs gerados usando get_data
    df_v = get_data('veiculo', 'placa', 'TST-0001')
    df_p = get_data('prestador', 'empresa', 'Oficina Master')
    
    if not df_v.empty and not df_p.empty:
        execute_crud_operation('servico', data={
            'id_veiculo': int(df_v.iloc[0]['id_veiculo']), 
            'id_prestador': int(df_p.iloc[0]['id_prestador']),
            'nome_servico': 'Revisão Teste', 'data_servico': date.today().strftime('%Y-%m-%d'),
            'garantia_dias': 180, 'valor': 500.0, 'km_realizado': 10000, 'km_proxima_revisao': 20000, 
            'registro': 'TEST-99', 'data_vencimento': (date.today() + timedelta(days=180)).strftime('%Y-%m-%d')
        }, operation='insert')
        st.success("Dados simulados com sucesso!")
        time.sleep(1)
        st.rerun()
    else:
        st.warning("IDs não encontrados. Tente rodar novamente em instantes.")

def main():
    st.set_page_config(page_title="Controle Automotivo", layout="wide")
    for key in ['edit_veiculo_id', 'edit_prestador_id', 'edit_servico_id']:
        if key not in st.session_state: st.session_state[key] = None

    st.title("🚗 Sistema de Controle Automotivo")

    tab_resumo, tab_hist, tab_manual = st.tabs(["📊 Resumo", "📈 Histórico", "➕ Manual de Gestão"])

    with st.sidebar:
        st.header("⚙️ Ferramentas")
        if st.button("🧪 Rodar Simulação (Dados Teste)"):
            run_auto_test_data()

    with tab_manual:
        # 🟢 SOLUÇÃO: Nome corrigido de "se" para "Serviço"
        opcao = st.radio("O que deseja gerenciar?", ["Veículo", "Serviço", "Prestador"], horizontal=True)
        st.divider()

        if opcao == "Veículo":
            generic_management_ui("Veículo", "veiculo", "nome")
        elif opcao == "Serviço":
            generic_management_ui("Serviço", "servico", "nome_servico")
        elif opcao == "Prestador":
            generic_management_ui("Prestador", "prestador", "empresa")

if __name__ == '__main__':
    main()