import streamlit as st
import pandas as pd
from datetime import date, timedelta
import time
import gspread
import numpy as np

# ==============================================================================
# 🚨 CONFIGURAÇÃO GOOGLE SHEETS E CONEXÃO
# ==============================================================================

SHEET_ID = '1BNjgWhvEj8NbnGr4x7F42LW7QbQiG5kZ1FBhfr9Q-4g'
PLANILHA_TITULO = 'Dados Automóvel'

@st.cache_resource(ttl=3600)
def get_gspread_client():
    """Retorna o cliente Gspread autenticado."""
    try:
        creds_info = st.secrets["gcp_service_account"]
        gc = gspread.service_account_from_dict(creds_info)
        return gc
    except Exception as e:
        st.error(f"Erro de autenticação: {e}")
        st.stop()

@st.cache_data(ttl=5)
def get_sheet_data(sheet_name):
    """Lê os dados e garante a tipagem correta para evitar erros de Join."""
    expected_cols = {
        'veiculo': ['id_veiculo', 'nome', 'placa', 'ano', 'valor_pago', 'data_compra'],
        'prestador': ['id_prestador', 'empresa', 'telefone', 'nome_prestador', 'cnpj', 'email', 'endereco', 'numero', 'cidade', 'bairro', 'cep'],
        'servico': ['id_servico', 'id_veiculo', 'id_prestador', 'nome_servico', 'data_servico', 'garantia_dias', 'valor', 'km_realizado', 'km_proxima_revisao', 'registro', 'data_vencimento']
    }

    try:
        gc = get_gspread_client()
        sh = None
        try:
            sh = gc.open_by_key(SHEET_ID)
        except:
            sh = gc.open(PLANILHA_TITULO)
        
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        if df.empty:
            return pd.DataFrame(columns=expected_cols.get(sheet_name, []))

        # --- ESTABILIZAÇÃO DE TIPOS CRÍTICOS ---
        id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
        if id_col in df.columns:
            df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)

        if sheet_name == 'veiculo':
            df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce').fillna(0.0)
            df['data_compra'] = pd.to_datetime(df['data_compra'], errors='coerce')

        if sheet_name == 'servico':
            for col in ['valor', 'garantia_dias', 'km_realizado', 'km_proxima_revisao']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            for col in ['data_servico', 'data_vencimento']:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            # IDs de serviço devem ser int para o merge funcionar
            df['id_veiculo'] = pd.to_numeric(df['id_veiculo'], errors='coerce').fillna(0).astype(int)
            df['id_prestador'] = pd.to_numeric(df['id_prestador'], errors='coerce').fillna(0).astype(int)

        return df
    except Exception as e:
        return pd.DataFrame(columns=expected_cols.get(sheet_name, []))

def write_sheet_data(sheet_name, df_new):
    """Sobrescreve a aba com tratamento de datas para JSON."""
    try:
        gc = get_gspread_client()
        try:
            sh = gc.open_by_key(SHEET_ID)
        except:
            sh = gc.open(PLANILHA_TITULO)
        
        worksheet = sh.worksheet(sheet_name)
        
        # Converte datas para string ISO para o Google Sheets
        df_save = df_new.copy()
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')
            
        data_to_write = [df_save.columns.tolist()] + df_save.values.tolist()
        worksheet.clear()
        worksheet.update('A1', data_to_write, value_input_option='USER_ENTERED')
        get_sheet_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# ==============================================================================
# 🚨 OPERAÇÕES CRUD CORE
# ==============================================================================

def execute_crud_operation(sheet_name, data=None, id_col=None, id_value=None, operation='insert'):
    df = get_sheet_data(sheet_name)
    id_col = f'id_{sheet_name}' if id_col is None else id_col

    if operation == 'insert':
        new_id = int(df[id_col].max() + 1) if not df.empty else 1
        data[id_col] = new_id
        df_new_row = pd.DataFrame([data])
        df_updated = pd.concat([df, df_new_row], ignore_index=True)
        success = write_sheet_data(sheet_name, df_updated)
        return success, new_id

    elif operation in ['update', 'delete']:
        if df.empty or id_value is None: return False, None
        index_to_modify = df[df[id_col] == int(id_value)].index
        if index_to_modify.empty: return False, None

        if operation == 'update':
            for key, value in data.items():
                if key in df.columns:
                    df.loc[index_to_modify, key] = value
            df_updated = df
        else:
            df_updated = df.drop(index_to_modify).reset_index(drop=True)

        success = write_sheet_data(sheet_name, df_updated)
        return success, id_value

    return False, None

# ==============================================================================
# 🚨 FUNÇÕES AUXILIARES E SIMULAÇÃO (JOIN SQL)
# ==============================================================================

def get_full_service_data(date_start=None, date_end=None):
    df_servicos = get_sheet_data('servico')
    df_veiculos = get_sheet_data('veiculo')
    df_prestadores = get_sheet_data('prestador')

    if df_servicos.empty or df_veiculos.empty or df_prestadores.empty:
        return pd.DataFrame()

    # Join Pandas (Simulando SQL)
    df_merged = pd.merge(df_servicos, df_veiculos[['id_veiculo', 'nome', 'placa']], on='id_veiculo', how='left')
    df_merged = pd.merge(df_merged, df_prestadores[['id_prestador', 'empresa', 'cidade']], on='id_prestador', how='left')

    df_merged = df_merged.rename(columns={
        'nome': 'Veículo', 'placa': 'Placa', 'empresa': 'Empresa', 
        'cidade': 'Cidade', 'nome_servico': 'Serviço', 'data_servico': 'Data', 'valor': 'Valor'
    })

    # Cálculo de Dias para Vencer
    df_merged['Dias para Vencer'] = (df_merged['data_vencimento'] - pd.to_datetime(date.today())).dt.days

    if date_start and date_end:
        mask = (df_merged['Data'].dt.date >= date_start) & (df_merged['Data'].dt.date <= date_end)
        df_merged = df_merged.loc[mask]

    return df_merged.sort_values(by='Data', ascending=False)

# ==============================================================================
# 🚨 FERRAMENTAS DE DESENVOLVEDOR (TESTE E RESET)
# ==============================================================================

def run_auto_test_data():
    """Insere dados fictícios para teste funcional."""
    with st.spinner("Gerando dados de teste..."):
        # Veículos
        insert_vehicle("Civic Teste", "TST-0001", 2023, 150000, date.today())
        # Prestador
        insert_new_prestador("Oficina Master", "1199999", "Mestre", "00.000/0001-00", "e@e.com", "Rua 1", "1", "SP", "Centro", "000")
        
        # Pega IDs criados
        df_v = get_data('veiculo', 'placa', 'TST-0001')
        df_p = get_data('prestador', 'empresa', 'Oficina Master')
        
        if not df_v.empty and not df_p.empty:
            insert_service(df_v.iloc[0]['id_veiculo'], df_p.iloc[0]['id_prestador'], "Troca Óleo Teste", date.today(), 180, 450.0, 1000, 10000, "TEST-REG-99")
            
        st.success("Simulação concluída!")
        time.sleep(1)
        st.rerun()

def reset_system_data():
    """Limpa todas as tabelas."""
    for tab in ['veiculo', 'prestador', 'servico']:
        write_sheet_data(tab, pd.DataFrame(columns=get_sheet_data(tab).columns))
    st.cache_data.clear()
    st.rerun()

# ==============================================================================
# 🚨 UI E FRONT-END (CSS E FORMS) - RESUMIDO PARA ESPAÇO
# ==============================================================================

CUSTOM_CSS = """
.st-emotion-cache-12fmwza, .st-emotion-cache-n2e28m { display: flex; flex-wrap: nowrap !important; gap: 5px; align-items: center; }
"""

# [Aqui entrariam as funções de insert_vehicle, manage_vehicle_form etc do seu código original]
# [Por brevidade, incluirei as funções CRUD de serviço que ajustamos]

def insert_vehicle(nome, placa, ano, valor_pago, data_compra):
    if not placa: return False
    data = {'id_veiculo': 0, 'nome': nome, 'placa': placa, 'ano': ano, 'valor_pago': float(valor_pago), 'data_compra': data_compra.isoformat()}
    success, _ = execute_crud_operation('veiculo', data=data)
    return success

def insert_new_prestador(*args):
    data = dict(zip(['empresa', 'telefone', 'nome_prestador', 'cnpj', 'email', 'endereco', 'numero', 'cidade', 'bairro', 'cep'], args))
    data['id_prestador'] = 0
    success, _ = execute_crud_operation('prestador', data=data)
    return success

def insert_service(id_v, id_p, nome, d_serv, gar, val, km_r, km_p, reg):
    d_venc = pd.to_datetime(d_serv) + timedelta(days=int(gar))
    data = {
        'id_servico': 0, 'id_veiculo': int(id_v), 'id_prestador': int(id_p),
        'nome_servico': nome, 'data_servico': d_serv.isoformat() if hasattr(d_serv, 'isoformat') else d_serv,
        'garantia_dias': gar, 'valor': float(val), 'km_realizado': km_r,
        'km_proxima_revisao': km_p, 'registro': reg, 'data_vencimento': d_venc.date().isoformat()
    }
    success, _ = execute_crud_operation('servico', data=data)
    return success

# [Funções de Delete com proteção]
def delete_vehicle(id_veiculo):
    df_s = get_sheet_data('servico')
    if not df_s[df_s['id_veiculo'] == int(id_veiculo)].empty:
        st.error("Erro: Veículo possui serviços vinculados.")
        return False
    execute_crud_operation('veiculo', id_value=id_veiculo, operation='delete')
    st.rerun()

# ==============================================================================
# 🚨 FUNÇÃO PRINCIPAL (MAIN)
# ==============================================================================

def main():
    st.set_page_config(page_title="Controle Automotivo", layout="wide")
    st.markdown(f"<style>{CUSTOM_CSS}</style>", unsafe_allow_html=True)
    
    # State Init
    for key in ['edit_service_id', 'edit_vehicle_id', 'edit_prestador_id']:
        if key not in st.session_state: st.session_state[key] = None

    st.title("🚗 Sistema de Controle Automotivo")

    # Sidebar Tools
    with st.sidebar:
        st.header("🛠️ Admin")
        if st.button("🧪 Rodar Simulação (Dados Teste)"): run_auto_test_data()
        if st.checkbox("⚠️ Modo Reset"):
            if st.button("💥 APAGAR TUDO"): reset_system_data()

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Resumo", "📈 Histórico", "➕ Gestão"])

    with tab1:
        df = get_full_service_data()
        if not df.empty:
            resumo = df.groupby('Veículo')['Valor'].sum().reset_index()
            st.dataframe(resumo, use_container_width=True)
        else:
            st.info("Sem dados.")

    with tab2:
        df_h = get_full_service_data()
        st.dataframe(df_h, use_container_width=True)

    with tab3:
        # Aqui você chama suas funções de formulário (manage_vehicle_form, etc)
        st.write("Use os botões de edição/cadastro originais aqui.")
        # Exemplo simplificado de escolha:
        escolha = st.radio("Gerenciar:", ["Veículo", "Prestador", "Serviço"], horizontal=True)
        st.info(f"Interface de {escolha} ativa.")

if __name__ == '__main__':
    main()