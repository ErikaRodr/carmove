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

        # --- ESTABILIZAÇÃO DE TIPOS ---
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
            df['id_veiculo'] = pd.to_numeric(df['id_veiculo'], errors='coerce').fillna(0).astype(int)
            df['id_prestador'] = pd.to_numeric(df['id_prestador'], errors='coerce').fillna(0).astype(int)

        return df
    except Exception:
        return pd.DataFrame(columns=expected_cols.get(sheet_name, []))

def write_sheet_data(sheet_name, df_new):
    try:
        gc = get_gspread_client()
        try:
            sh = gc.open_by_key(SHEET_ID)
        except:
            sh = gc.open(PLANILHA_TITULO)
        
        worksheet = sh.worksheet(sheet_name)
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
# 2. FUNÇÕES DE ACESSO E FILTRO (O QUE CAUSOU O ERRO)
# ==============================================================================

def get_data(sheet_name, filter_col=None, filter_value=None):
    df = get_sheet_data(sheet_name)
    if df.empty:
        return df
    if filter_col and filter_value is not None:
        try:
            # Se for filtro de ID, garante que ambos são inteiros
            if str(filter_col).startswith('id_'):
                df[filter_col] = pd.to_numeric(df[filter_col], errors='coerce').fillna(0).astype(int)
                filter_value = int(filter_value)
            
            df_filtered = df[df[filter_col] == filter_value]
            return df_filtered
        except:
            return pd.DataFrame()
    return df

# ==============================================================================
# 3. OPERAÇÕES CRUD CORE
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
# 4. LÓGICA DE NEGÓCIO (VEÍCULOS, PRESTADORES, SERVIÇOS)
# ==============================================================================

def insert_vehicle(nome, placa, ano, valor_pago, data_compra):
    if not placa: return False
    # Evitar duplicados
    if not get_data('veiculo', 'placa', placa).empty:
        return False
    data = {'id_veiculo': 0, 'nome': nome, 'placa': placa, 'ano': ano, 'valor_pago': float(valor_pago), 'data_compra': data_compra.isoformat() if hasattr(data_compra, 'isoformat') else data_compra}
    return execute_crud_operation('veiculo', data=data)[0]

def insert_new_prestador(empresa, telefone, nome_p, cnpj, email, end, num, cid, bai, cep):
    if not cnpj: return False
    if not get_data('prestador', 'cnpj', cnpj).empty:
        return False
    data = {'id_prestador': 0, 'empresa': empresa, 'telefone': telefone, 'nome_prestador': nome_p, 'cnpj': cnpj, 'email': email, 'endereco': end, 'numero': num, 'cidade': cid, 'bairro': bai, 'cep': cep}
    return execute_crud_operation('prestador', data=data)[0]

def insert_service(id_v, id_p, nome, d_serv, gar, val, km_r, km_p, reg):
    if not reg: return False
    d_serv_dt = pd.to_datetime(d_serv)
    d_venc = d_serv_dt + timedelta(days=int(gar))
    data = {
        'id_servico': 0, 'id_veiculo': int(id_v), 'id_prestador': int(id_p),
        'nome_servico': nome, 'data_servico': d_serv_dt.date().isoformat(),
        'garantia_dias': int(gar), 'valor': float(val), 'km_realizado': int(km_r),
        'km_proxima_revisao': int(km_p), 'registro': reg, 'data_vencimento': d_venc.date().isoformat()
    }
    return execute_crud_operation('servico', data=data)[0]

def get_full_service_data(date_start=None, date_end=None):
    df_s = get_sheet_data('servico')
    df_v = get_sheet_data('veiculo')
    df_p = get_sheet_data('prestador')
    if df_s.empty or df_v.empty or df_p.empty: return pd.DataFrame()

    df_m = pd.merge(df_s, df_v[['id_veiculo', 'nome', 'placa']], on='id_veiculo', how='left')
    df_m = pd.merge(df_m, df_p[['id_prestador', 'empresa', 'cidade']], on='id_prestador', how='left')

    df_m['Dias para Vencer'] = (df_m['data_vencimento'] - pd.to_datetime(date.today())).dt.days
    
    # Renomear para exibição
    df_m = df_m.rename(columns={'nome': 'Veículo', 'placa': 'Placa', 'empresa': 'Empresa', 'nome_servico': 'Serviço', 'data_servico': 'Data', 'valor': 'Valor'})
    
    if date_start and date_end:
        mask = (df_m['Data'].dt.date >= date_start) & (df_m['Data'].dt.date <= date_end)
        df_m = df_m.loc[mask]
    return df_m.sort_values(by='Data', ascending=False)

# ==============================================================================
# 5. FERRAMENTAS DE SIMULAÇÃO E RESET
# ==============================================================================

def run_auto_test_data():
    st.info("Iniciando simulação...")
    # 1. Veículo Teste
    insert_vehicle("Civic Teste", "TST-0001", 2023, 150000, date.today())
    # 2. Prestador Teste
    insert_new_prestador("Oficina Master", "1199999", "Mestre", "00.000/0001-00", "e@e.com", "Rua 1", "1", "SP", "Centro", "000")
    
    # Busca IDs para vincular o serviço
    df_v = get_data('veiculo', 'placa', 'TST-0001')
    df_p = get_data('prestador', 'empresa', 'Oficina Master')
    
    if not df_v.empty and not df_p.empty:
        insert_service(df_v.iloc[0]['id_veiculo'], df_p.iloc[0]['id_prestador'], "Troca Óleo Teste", date.today(), 180, 450.0, 1000, 10000, "TEST-REG-99")
        st.success("Dados de teste inseridos!")
        time.sleep(1)
        st.rerun()

def reset_system_data():
    for tab in ['veiculo', 'prestador', 'servico']:
        cols = get_sheet_data(tab).columns.tolist()
        write_sheet_data(tab, pd.DataFrame(columns=cols))
    st.cache_data.clear()
    st.rerun()

# ==============================================================================
# 6. INTERFACE (MAIN)
# ==============================================================================

def main():
    st.set_page_config(page_title="Controle Automotivo", layout="wide")
    st.markdown("<style>.st-emotion-cache-12fmwza { display: flex; flex-wrap: nowrap; gap: 5px; }</style>", unsafe_allow_html=True)
    st.title("🚗 Sistema de Controle Automotivo")

    # Sidebar
    with st.sidebar:
        st.header("🛠️ Administrador")
        if st.button("🧪 Rodar Simulação (Dados Teste)"):
            run_auto_test_data()
        if st.checkbox("⚠️ Ativar Reset Total"):
            if st.button("💥 APAGAR TUDO"):
                reset_system_data()

    # Conteúdo Principal
    tab_resumo, tab_hist, tab_cad = st.tabs(["📊 Resumo", "📈 Histórico", "➕ Gestão"])

    with tab_resumo:
        df = get_full_service_data()
        if not df.empty:
            res = df.groupby('Veículo')['Valor'].sum().reset_index()
            st.write("### Total Gasto por Veículo")
            st.dataframe(res, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum dado para exibir.")

    with tab_hist:
        df_h = get_full_service_data()
        if not df_h.empty:
            st.dataframe(df_h[['Veículo', 'Placa', 'Serviço', 'Empresa', 'Data', 'Valor', 'Dias para Vencer']], use_container_width=True, hide_index=True)
        else:
            st.info("Histórico vazio.")

    with tab_cad:
        st.write("Selecione uma aba acima para ver os dados. Use os formulários originais para novos cadastros.")

if __name__ == '__main__':
    main()