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

# Define as colunas esperadas para cada aba
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

        # Padronização de IDs para garantir que sejam números
        id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
        if id_col in df.columns:
            # Remove pontos decimais e converte para int
            df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)
        
        return df
    except Exception:
        return pd.DataFrame(columns=EXPECTED_COLS.get(sheet_name, []))

def get_data(sheet_name, filter_col=None, filter_value=None):
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
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID) if SHEET_ID else gc.open(PLANILHA_TITULO)
        worksheet = sh.worksheet(sheet_name)
        
        df_save = df_new.copy()
        for col in df_save.select_dtypes(include=['datetime64']).columns:
            df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')
            
        # Limpeza para evitar erros de JSON
        df_save = df_save.fillna("") 
        df_save = df_save.replace([np.inf, -np.inf], 0)
        
        worksheet.clear()
        worksheet.update('A1', [df_save.columns.tolist()] + df_save.values.tolist(), value_input_option='USER_ENTERED')
        get_sheet_data.clear() # Limpa cache para atualizar relatório imediatamente
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
# 3. LÓGICA DE RELATÓRIO (CORRIGIDA)
# ==============================================================================

def get_full_service_data():
    """Busca e unifica os dados para os relatórios."""
    df_s = get_sheet_data('servico')
    df_v = get_sheet_data('veiculo')
    df_p = get_sheet_data('prestador')

    # Se não houver serviços, retorna vazio
    if df_s.empty:
        return pd.DataFrame()

    # 🛑 CORREÇÃO DE TIPAGEM: Garante que os IDs sejam do mesmo tipo (int) antes do merge
    # Isso resolve o problema do relatório vazio
    df_s['id_veiculo'] = pd.to_numeric(df_s['id_veiculo'], errors='coerce').fillna(0).astype(int)
    df_s['id_prestador'] = pd.to_numeric(df_s['id_prestador'], errors='coerce').fillna(0).astype(int)
    
    if not df_v.empty:
        df_v['id_veiculo'] = pd.to_numeric(df_v['id_veiculo'], errors='coerce').fillna(0).astype(int)
        # Merge Serviços + Veículos
        df_merged = pd.merge(df_s, df_v[['id_veiculo', 'nome', 'placa']], on='id_veiculo', how='left')
    else:
        df_merged = df_s.copy()
        df_merged['nome'] = 'Desconhecido'
        df_merged['placa'] = '-'

    if not df_p.empty:
        df_p['id_prestador'] = pd.to_numeric(df_p['id_prestador'], errors='coerce').fillna(0).astype(int)
        # Merge + Prestadores
        df_merged = pd.merge(df_merged, df_p[['id_prestador', 'empresa']], on='id_prestador', how='left')
    else:
        df_merged['empresa'] = 'Desconhecido'

    # Tratamento de Data e Valor
    df_merged['data_vencimento'] = pd.to_datetime(df_merged['data_vencimento'], errors='coerce')
    df_merged['data_servico'] = pd.to_datetime(df_merged['data_servico'], errors='coerce')
    df_merged['valor'] = pd.to_numeric(df_merged['valor'], errors='coerce').fillna(0.0)

    # Cálculo
    df_merged['Dias p/ Vencer'] = (df_merged['data_vencimento'] - pd.to_datetime(date.today())).dt.days
    
    # Preenche nomes vazios caso o merge não tenha encontrado correspondência
    df_merged['nome'] = df_merged['nome'].fillna('Veículo Removido')
    df_merged['empresa'] = df_merged['empresa'].fillna('Prestador Removido')

    return df_merged

# ==============================================================================
# 4. INTERFACE
# ==============================================================================

def generic_management_ui(category_name, sheet_name, display_col):
    st.subheader(f"Gestão de {category_name}")
    state_key = f'edit_{sheet_name}_id'
    id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
    
    # LISTA
    if st.session_state[state_key] is None:
        if st.button(f"➕ Novo {category_name}"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
        
        df = get_sheet_data(sheet_name)
        if df.empty or len(df) == 0:
            st.info(f"Nenhum registro encontrado.")
        else:
            for _, row in df.iterrows():
                col_data, col_edit, col_del = st.columns([0.7, 0.15, 0.15])
                val_disp = str(row.get(display_col, 'Sem Nome'))
                col_data.write(f"**{val_disp}**")
                
                sid = int(row.get(id_col, 0))
                if col_edit.button("✏️", key=f"ed_{sheet_name}_{sid}"):
                    st.session_state[state_key] = sid
                    st.rerun()
                if col_del.button("🗑️", key=f"del_{sheet_name}_{sid}"):
                    execute_crud_operation(sheet_name, id_value=sid, operation='delete')
                    st.rerun()
    
    # FORMULÁRIO
    else:
        is_new = st.session_state[state_key] == 'NEW'
        df = get_sheet_data(sheet_name)
        current_data = {}
        if not is_new:
            res = df[df[id_col] == st.session_state[state_key]]
            if not res.empty: current_data = res.iloc[0].to_dict()

        with st.form(f"form_{sheet_name}"):
            payload = {}
            cols = EXPECTED_COLS.get(sheet_name, df.columns.tolist())
            
            for col in cols:
                if col == id_col: continue
                label = col.replace("_", " ").title()
                val = current_data.get(col, "")
                
                if "data" in col:
                    try: d_val = pd.to_datetime(val) if val else date.today()
                    except: d_val = date.today()
                    payload[col] = st.date_input(label, value=d_val)
                elif any(x in col for x in ["valor", "km", "ano", "telefone"]):
                    try: n_val = float(val) if val else 0.0
                    except: n_val = 0.0
                    payload[col] = st.number_input(label, value=n_val)
                else:
                    payload[col] = st.text_input(label, value=str(val))
            
            if st.form_submit_button("💾 Salvar"):
                for k, v in payload.items():
                    if isinstance(v, (date, pd.Timestamp)): payload[k] = v.strftime('%Y-%m-%d')
                
                if is_new: execute_crud_operation(sheet_name, data=payload, operation='insert')
                else: execute_crud_operation(sheet_name, data=payload, id_value=st.session_state[state_key], operation='update')
                
                st.session_state[state_key] = None
                st.success("Salvo!")
                time.sleep(1)
                st.rerun()
                
        if st.button("Cancelar"):
            st.session_state[state_key] = None
            st.rerun()

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
            'garantia_dias': 180, 'valor': 500.0, 'km_realizado': 10000, 'km_proxima_revisao': 20000, 
            'registro': 'TEST-99', 'data_vencimento': (date.today() + timedelta(days=180)).strftime('%Y-%m-%d')
        }, operation='insert')
        st.success("Feito!")
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

    # ----------------------------------------
    # ABA RESUMO (RELATÓRIOS)
    # ----------------------------------------
    with tab_resumo:
        df_full = get_full_service_data()
        
        if not df_full.empty:
            # Métricas
            total_gasto = df_full['valor'].sum()
            total_servicos = len(df_full)
            c1, c2 = st.columns(2)
            c1.metric("Total Gasto (Geral)", f"R$ {total_gasto:,.2f}")
            c2.metric("Serviços Realizados", total_servicos)
            
            st.divider()
            
            # Gráfico de Gastos por Veículo
            st.subheader("Gastos por Veículo")
            if 'nome' in df_full.columns and 'valor' in df_full.columns:
                df_chart = df_full.groupby('nome')['valor'].sum().reset_index()
                st.bar_chart(df_chart.set_index('nome'))
            else:
                st.warning("Dados incompletos para gerar gráfico.")
        else:
            st.info("Nenhum serviço registrado para exibir no resumo.")

    # ----------------------------------------
    # ABA HISTÓRICO
    # ----------------------------------------
    with tab_hist:
        df_full = get_full_service_data()
        if not df_full.empty:
            # Seleciona apenas colunas úteis para exibição
            cols_show = ['nome', 'placa', 'nome_servico', 'empresa', 'data_servico', 'valor', 'Dias p/ Vencer']
            # Renomeia para ficar bonito
            rename_map = {
                'nome': 'Veículo', 'placa': 'Placa', 'nome_servico': 'Serviço', 
                'empresa': 'Prestador', 'data_servico': 'Data', 'valor': 'Valor (R$)'
            }
            
            # Garante que as colunas existem antes de mostrar
            final_cols = [c for c in cols_show if c in df_full.columns]
            st.dataframe(df_full[final_cols].rename(columns=rename_map), use_container_width=True)
        else:
            st.info("Histórico vazio.")

    # ----------------------------------------
    # ABA MANUAL
    # ----------------------------------------
    with tab_manual:
        opcao = st.radio("Gerenciar:", ["Veículo", "Serviço", "Prestador"], horizontal=True)
        st.divider()
        if opcao == "Veículo": generic_management_ui("Veículo", "veiculo", "nome")
        elif opcao == "Serviço": generic_management_ui("Serviço", "servico", "nome_servico")
        elif opcao == "Prestador": generic_management_ui("Prestador", "prestador", "empresa")

if __name__ == '__main__':
    main()