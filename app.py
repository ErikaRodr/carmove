import streamlit as st
import pandas as pd
from datetime import date, timedelta
import time
import gspread
import numpy as np
import altair as alt

# ==============================================================================
# 1. CONFIGURAÇÃO E CONEXÃO
# ==============================================================================

SHEET_ID = '1BNjgWhvEj8NbnGr4x7F42LW7QbQiG5kZ1FBhfr9Q-4g'
PLANILHA_TITULO = 'Dados Automóvel'

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

@st.cache_data(ttl=0)
def get_sheet_data(sheet_name):
    # Tenta ler até 3 vezes para evitar erros de API ou leitura vazia falsa
    for _ in range(3):
        try:
            gc = get_gspread_client()
            sh = gc.open_by_key(SHEET_ID) if SHEET_ID else gc.open(PLANILHA_TITULO)
            worksheet = sh.worksheet(sheet_name)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            
            if df.empty:
                return pd.DataFrame(columns=EXPECTED_COLS.get(sheet_name, []))

            id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'
            if id_col in df.columns:
                df[id_col] = pd.to_numeric(df[id_col], errors='coerce').fillna(0).astype(int)
            
            return df
        except Exception:
            time.sleep(0.5) 
            
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
            
        df_save = df_save.fillna("") 
        df_save = df_save.replace([np.inf, -np.inf], 0)
        
        worksheet.clear()
        worksheet.update('A1', [df_save.columns.tolist()] + df_save.values.tolist(), value_input_option='USER_ENTERED')
        
        # Limpa cache imediatamente
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
        # Exclusão direta sem verificação de dependência (solicitado pelo usuário)
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

    # Tratamento de Strings e Datas
    df_merged['nome'] = df_merged['nome'].fillna('Desconhecido').astype(str)
    df_merged['empresa'] = df_merged['empresa'].fillna('Desconhecido').astype(str)
    
    df_merged['data_vencimento'] = pd.to_datetime(df_merged['data_vencimento'], errors='coerce')
    df_merged['data_servico'] = pd.to_datetime(df_merged['data_servico'], errors='coerce')
    df_merged['valor'] = pd.to_numeric(df_merged['valor'], errors='coerce').fillna(0.0)
    df_merged['Dias p/ Vencer'] = (df_merged['data_vencimento'] - pd.to_datetime(date.today())).dt.days
    
    return df_merged.sort_values(by='data_servico', ascending=False)

# ==============================================================================
# 4. INTERFACES DE GESTÃO (UI)
# ==============================================================================

def generic_management_ui(category_name, sheet_name, display_col):
    st.subheader(f"Gestão de {category_name}")
    state_key = f'edit_{sheet_name}_id'
    id_col = f'id_{sheet_name}'
    
    # MODO LISTA
    if st.session_state[state_key] is None:
        if st.button(f"➕ Novo {category_name}"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
        
        df = get_sheet_data(sheet_name)
        if df.empty:
            st.info("Nenhum registro.")
        else:
            for _, row in df.iterrows():
                # Layout de colunas seguro
                c1, c2, c3 = st.columns([0.7, 0.15, 0.15])
                val_display = str(row.get(display_col, 'Sem Nome'))
                c1.write(f"**{val_display}**")
                
                sid = int(row.get(id_col, 0))
                
                # Chaves únicas para evitar NodeError
                if c2.button("✏️", key=f"btn_edit_{sheet_name}_{sid}"):
                    st.session_state[state_key] = sid
                    st.rerun()
                
                if c3.button("🗑️", key=f"btn_del_{sheet_name}_{sid}"):
                    execute_crud_operation(sheet_name, id_value=sid, operation='delete')
                    st.success("Excluído!")
                    time.sleep(1) # Aguarda propagação
                    st.rerun()
    
    # MODO FORMULÁRIO
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
                label = col.replace("_", " ").title()

                if "data" in col:
                    try: d = pd.to_datetime(val) if val else date.today()
                    except: d = date.today()
                    payload[col] = st.date_input(label, value=d, format="DD/MM/YYYY")
                elif any(x in col for x in ["telefone", "numero", "ano", "km"]):
                    try: n_val = int(float(val)) if val else 0
                    except: n_val = 0
                    payload[col] = st.number_input(label, value=n_val, step=1, format="%d")
                elif "valor" in col:
                    try: n_val = float(val) if val else 0.0
                    except: n_val = 0.0
                    payload[col] = st.number_input(label, value=n_val, format="%.2f")
                else:
                    payload[col] = st.text_input(label, value=str(val))
            
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
    st.subheader("Gestão de Serviços")
    state_key = 'edit_servico_id'
    
    # Carrega dados
    df_v = get_sheet_data('veiculo')
    df_p = get_sheet_data('prestador')
    
    # Mapeamentos para Selectbox
    map_v = {f"{r['nome']} ({r.get('placa','S/P')})": int(r['id_veiculo']) for _, r in df_v.iterrows()} if not df_v.empty else {}
    map_p = {f"{r['empresa']}": int(r['id_prestador']) for _, r in df_p.iterrows()} if not df_p.empty else {}
    
    # LÓGICA DE LISTA
    if st.session_state[state_key] is None:
        
        # Botão de Novo
        if st.button("➕ Novo Serviço"):
            # Só bloqueia a criação se não tiver dependências
            if not map_v or not map_p:
                st.error("Para cadastrar um serviço, você precisa ter pelo menos um Veículo e um Prestador.")
            else:
                st.session_state[state_key] = 'NEW'
                st.rerun()
        
        # Mostra aviso APENAS se as tabelas estiverem vazias, não ao excluir um serviço
        if df_v.empty or df_p.empty:
            st.warning("⚠️ Atenção: Não foram encontrados Veículos ou Prestadores. Cadastre-os para habilitar novos serviços.")

        # Listagem de Serviços (independente de veículos existirem ou não)
        df_serv = get_sheet_data('servico')
        if not df_serv.empty:
            if 'data_servico' in df_serv.columns:
                df_serv['data_servico_dt'] = pd.to_datetime(df_serv['data_servico'], errors='coerce')
            
            for _, row in df_serv.iterrows():
                c1, c2, c3 = st.columns([0.7, 0.15, 0.15])
                
                data_str = ""
                if 'data_servico_dt' in row and pd.notna(row['data_servico_dt']):
                    data_str = row['data_servico_dt'].strftime('%d/%m/%Y')
                
                val_display = str(row.get('nome_servico', 'Serviço'))
                c1.write(f"**{val_display}** - {data_str}")
                
                sid = int(row.get('id_servico', 0))
                
                # Botão Editar
                if c2.button("✏️", key=f"btn_ed_serv_{sid}"):
                    st.session_state[state_key] = sid
                    st.rerun()
                
                # Botão Excluir
                if c3.button("🗑️", key=f"btn_del_serv_{sid}"):
                    execute_crud_operation('servico', id_value=sid, operation='delete')
                    st.success("Serviço excluído!")
                    time.sleep(1)
                    st.rerun()
        else:
            st.info("Nenhum serviço cadastrado.")

    # LÓGICA DE FORMULÁRIO
    else:
        df_serv = get_sheet_data('servico')
        is_new = st.session_state[state_key] == 'NEW'
        curr = {}
        current_id_v = 0
        current_id_p = 0
        
        if not is_new:
            res = df_serv[df_serv['id_servico'] == st.session_state[state_key]]
            if not res.empty:
                curr = res.iloc[0].to_dict()
                current_id_v = int(curr.get('id_veiculo', 0))
                current_id_p = int(curr.get('id_prestador', 0))

        with st.form("form_servico_especial"):
            # Tenta encontrar índice atual no selectbox
            idx_v = 0
            if current_id_v in map_v.values():
                idx_v = list(map_v.values()).index(current_id_v)
                
            idx_p = 0
            if current_id_p in map_p.values():
                idx_p = list(map_p.values()).index(current_id_p)
            
            # Se não houver itens nos mapas (apagados), evita erro no selectbox
            opts_v = list(map_v.keys()) if map_v else ["Nenhum Veículo"]
            opts_p = list(map_p.keys()) if map_p else ["Nenhum Prestador"]
            
            sel_v_name = st.selectbox("Veículo", options=opts_v, index=min(idx_v, len(opts_v)-1))
            sel_p_name = st.selectbox("Prestador", options=opts_p, index=min(idx_p, len(opts_p)-1))
            
            nome_s = st.text_input("Nome do Serviço", value=curr.get('nome_servico', ''))
            
            c_dt, c_gr = st.columns(2)
            try: d_val = pd.to_datetime(curr.get('data_servico')) if curr.get('data_servico') else date.today()
            except: d_val = date.today()
            
            data_s = c_dt.date_input("Data Serviço", value=d_val, format="DD/MM/YYYY")
            garantia = c_gr.number_input("Garantia (dias)", value=int(curr.get('garantia_dias', 90)))
            
            c_val, c_km = st.columns(2)
            valor = c_val.number_input("Valor (R$)", value=float(curr.get('valor', 0.0)), format="%.2f")
            
            try: km_val = int(float(curr.get('km_realizado', 0)))
            except: km_val = 0
            km_r = c_km.number_input("KM Realizado", value=km_val, step=1, format="%d")
            
            registro = st.text_input("Nota/Registro", value=curr.get('registro', ''))
            
            if st.form_submit_button("Salvar Serviço"):
                # Validação básica
                if not map_v or not map_p:
                    st.error("Impossível salvar sem Veículo e Prestador cadastrados.")
                else:
                    dt_venc = data_s + timedelta(days=int(garantia))
                    payload = {
                        'id_veiculo': map_v.get(sel_v_name, 0),
                        'id_prestador': map_p.get(sel_p_name, 0),
                        'nome_servico': nome_s,
                        'data_servico': data_s.strftime('%Y-%m-%d'),
                        'garantia_dias': int(garantia),
                        'valor': float(valor),
                        'km_realizado': int(km_r),
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
    execute_crud_operation('veiculo', data={'nome': 'Civic Teste', 'placa': 'TST-0001', 'ano': 2023, 'valor_pago': 150000, 'data_compra': '2023-01-01'}, operation='insert')
    execute_crud_operation('prestador', data={'empresa': 'Oficina Master', 'telefone': 1199999, 'cnpj': '00.000/0001-00'}, operation='insert')
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

    # ABA RESUMO
    with tab_resumo:
        df_full = get_full_service_data()
        
        if not df_full.empty:
            df_full['Ano'] = df_full['data_servico'].dt.year
            
            st.subheader("Filtros do Dashboard")
            c_filt1, c_filt2 = st.columns(2)
            
            anos_disponiveis = sorted(df_full['Ano'].dropna().unique().astype(int).tolist(), reverse=True)
            sel_ano = c_filt1.selectbox("Filtrar por Ano", ["Todos"] + anos_disponiveis)
            
            veiculos_disp = sorted(df_full['nome'].astype(str).unique().tolist())
            sel_veiculo = c_filt2.selectbox("Filtrar por Veículo", ["Todos"] + veiculos_disp)
            
            df_filtered = df_full.copy()
            if sel_ano != "Todos":
                df_filtered = df_filtered[df_filtered['Ano'] == sel_ano]
            
            if sel_veiculo != "Todos":
                df_filtered = df_filtered[df_filtered['nome'] == sel_veiculo]
            
            st.divider()

            if not df_filtered.empty:
                c1, c2 = st.columns(2)
                c1.metric("Total Gasto (Filtro)", f"R$ {df_filtered['valor'].sum():,.2f}")
                c2.metric("Serviços Realizados", len(df_filtered))
                
                st.subheader("Gastos Detalhados")
                
                df_chart = df_filtered.groupby('nome', as_index=False)['valor'].sum()
                
                base = alt.Chart(df_chart).encode(
                    x=alt.X('nome', sort='-y', title='Veículo'),
                    y=alt.Y('valor', title='Total Gasto (R$)')
                )
                barras = base.mark_bar(color='#FF4B4B')
                rotulos = base.mark_text(align='center', baseline='bottom', dy=-5, fontSize=12).encode(text=alt.Text('valor', format=',.2f'))
                
                st.altair_chart((barras + rotulos).properties(height=400).interactive(), use_container_width=True)
            else:
                st.warning("Nenhum dado encontrado.")
        else:
            st.info("Sem dados de serviço.")

    # ABA HISTÓRICO
    with tab_hist:
        df_full = get_full_service_data()
        if not df_full.empty:
            df_full['Ano'] = df_full['data_servico'].dt.year
            
            st.subheader("Filtros")
            c_hf1, c_hf2 = st.columns(2)
            
            v_list = ["Todos"] + sorted(list(df_full['nome'].astype(str).unique()))
            v_sel = c_hf1.selectbox("Veículo:", v_list, key="hist_veiculo")
            
            years_list = ["Todos"] + sorted(list(df_full['Ano'].unique().astype(int)), reverse=True)
            y_sel = c_hf2.selectbox("Ano:", years_list, key="hist_ano")
            
            df_hist_final = df_full.copy()
            if v_sel != "Todos":
                df_hist_final = df_hist_final[df_hist_final['nome'] == v_sel]
            if y_sel != "Todos":
                df_hist_final = df_hist_final[df_hist_final['Ano'] == y_sel]

            cols = ['nome', 'placa', 'nome_servico', 'empresa', 'data_servico', 'valor', 'Dias p/ Vencer']
            df_display = df_hist_final[cols].copy()
            if 'data_servico' in df_display.columns:
                df_display['data_servico'] = df_display['data_servico'].dt.strftime('%d/%m/%Y')
            
            if not df_display.empty:
                st.dataframe(df_display, use_container_width=True)
            else:
                st.warning("Nenhum registro encontrado.")
        else:
            st.info("Histórico vazio.")

    with tab_manual:
        opcao = st.radio("Gerenciar:", ["Veículo", "Serviço", "Prestador"], horizontal=True)
        st.divider()
        if opcao == "Veículo": generic_management_ui("Veículo", "veiculo", "nome")
        elif opcao == "Serviço": service_management_ui()
        elif opcao == "Prestador": generic_management_ui("Prestador", "prestador", "empresa")

if __name__ == '__main__':
    main()