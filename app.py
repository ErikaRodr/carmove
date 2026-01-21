import streamlit as st
import pandas as pd
from datetime import date, timedelta
import time
import gspread
import numpy as np
import altair as alt
import requests # 🟢 NOVA IMPORTAÇÃO PARA O CEP

# ==============================================================================
# 1. CONFIGURAÇÃO E CONEXÃO
# ==============================================================================

SHEET_ID = '1BNjgWhvEj8NbnGr4x7F42LW7QbQiG5kZ1FBhfr9Q-4g'
PLANILHA_TITULO = 'Dados Automóvel'

# Estrutura de Colunas
EXPECTED_COLS = {
    'veiculo': ['id_veiculo', 'nome', 'placa', 'ano', 'valor_pago', 'data_compra'],
    # Nota: O campo email existe no banco para compatibilidade, mas não será mostrado no form
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
        st.error(f"Erro Crítico de Autenticação: {e}")
        st.stop()

def get_sheet_data(sheet_name, force_refresh=False):
    if force_refresh:
        st.cache_data.clear()
    return _read_data_cached(sheet_name)

@st.cache_data(ttl=15)
def _read_data_cached(sheet_name):
    # Retry logic (3 tentativas)
    for i in range(3):
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
            time.sleep(0.5 + (i * 0.2))
            
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
        
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# ==============================================================================
# 2. CRUD E UTILITÁRIOS
# ==============================================================================

def execute_crud_operation(sheet_name, data=None, id_value=None, operation='insert'):
    df = get_sheet_data(sheet_name, force_refresh=True)
    id_col = f'id_{sheet_name}' if sheet_name in ('veiculo', 'prestador') else 'id_servico'

    if operation == 'insert':
        new_id = 1
        if not df.empty and id_col in df.columns:
            new_id = int(df[id_col].max() + 1)
        data[id_col] = new_id
        df_updated = pd.concat([df, pd.DataFrame([data])], ignore_index=True).fillna("")
        return write_sheet_data(sheet_name, df_updated)

    elif operation == 'update':
        if df.empty: return False
        idx = df[df[id_col] == int(id_value)].index
        if not idx.empty:
            for k, v in data.items(): df.loc[idx, k] = v
            return write_sheet_data(sheet_name, df)
        return False

    elif operation == 'delete':
        if df.empty: return False
        df_updated = df[df[id_col] != int(id_value)]
        return write_sheet_data(sheet_name, df_updated)

def consultar_cep(cep):
    """Consulta API ViaCEP"""
    cep = str(cep).replace("-", "").replace(".", "").strip()
    if len(cep) == 8:
        try:
            response = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=3)
            if response.status_code == 200:
                data = response.json()
                if "erro" not in data:
                    return data
        except:
            return None
    return None

# ==============================================================================
# 3. RELATÓRIOS
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
        df_merged['nome'] = 'Desconhecido'
        df_merged['placa'] = '-'

    if not df_p.empty:
        df_p['id_prestador'] = pd.to_numeric(df_p['id_prestador'], errors='coerce').fillna(0).astype(int)
        df_merged = pd.merge(df_merged, df_p[['id_prestador', 'empresa']], on='id_prestador', how='left')
    else:
        df_merged['empresa'] = 'Desconhecido'

    df_merged['nome'] = df_merged['nome'].fillna('Desconhecido').astype(str)
    df_merged['empresa'] = df_merged['empresa'].fillna('Desconhecido').astype(str)
    
    df_merged['data_vencimento'] = pd.to_datetime(df_merged['data_vencimento'], errors='coerce')
    df_merged['data_servico'] = pd.to_datetime(df_merged['data_servico'], errors='coerce')
    df_merged['valor'] = pd.to_numeric(df_merged['valor'], errors='coerce').fillna(0.0)
    df_merged['Dias p/ Vencer'] = (df_merged['data_vencimento'] - pd.to_datetime(date.today())).dt.days
    
    return df_merged.sort_values(by='data_servico', ascending=False)

# ==============================================================================
# 4. INTERFACES (UI)
# ==============================================================================

# --- UI PARA VEÍCULOS (GENÉRICA) ---
def generic_management_ui(category_name, sheet_name, display_col):
    st.subheader(f"Gestão de {category_name}")
    state_key = f'edit_{sheet_name}_id'
    id_col = f'id_{sheet_name}'
    
    if st.session_state[state_key] is None:
        c_top, _ = st.columns([0.3, 0.7])
        if c_top.button(f"➕ Novo {category_name}"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
        
        df = get_sheet_data(sheet_name)
        if df.empty:
            st.warning(f"Nenhum {category_name} encontrado.")
        else:
            for _, row in df.iterrows():
                c1, c2, c3 = st.columns([0.7, 0.15, 0.15])
                val_display = str(row.get(display_col, 'Sem Nome'))
                c1.write(f"**{val_display}**")
                sid = int(row.get(id_col, 0))
                
                if c2.button("✏️", key=f"btn_edit_{sheet_name}_{sid}"):
                    st.session_state[state_key] = sid
                    st.rerun()
                
                if c3.button("🗑️", key=f"btn_del_{sheet_name}_{sid}"):
                    with st.spinner("Excluindo..."):
                        execute_crud_operation(sheet_name, id_value=sid, operation='delete')
                    st.success("Excluído!")
                    time.sleep(1)
                    st.rerun()
    else:
        df = get_sheet_data(sheet_name)
        is_new = st.session_state[state_key] == 'NEW'
        curr = {}
        if not is_new:
            res = df[df[id_col] == st.session_state[state_key]]
            if not res.empty: curr = res.iloc[0].to_dict()
        
        with st.form(f"form_{sheet_name}"):
            payload = {}
            # Exibe todos os campos menos ID
            for col in EXPECTED_COLS.get(sheet_name):
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
            
            if st.form_submit_button("💾 Salvar"):
                for k,v in payload.items():
                    if isinstance(v, (date, pd.Timestamp)): payload[k] = v.strftime('%Y-%m-%d')
                with st.spinner("Salvando..."):
                    if is_new: execute_crud_operation(sheet_name, data=payload, operation='insert')
                    else: execute_crud_operation(sheet_name, data=payload, id_value=st.session_state[state_key], operation='update')
                st.session_state[state_key] = None
                st.success("Salvo!")
                time.sleep(0.5)
                st.rerun()
        if st.button("Cancelar"):
            st.session_state[state_key] = None
            st.rerun()

# --- UI ESPECÍFICA PARA PRESTADORES (COM CEP E VALIDAÇÃO) ---
def provider_management_ui():
    st.subheader("Gestão de Prestadores")
    state_key = 'edit_prestador_id'
    
    if st.session_state[state_key] is None:
        c_top, _ = st.columns([0.3, 0.7])
        if c_top.button("➕ Novo Prestador"):
            st.session_state[state_key] = 'NEW'
            st.rerun()
        
        df = get_sheet_data('prestador')
        if df.empty:
            st.warning("Nenhum prestador encontrado.")
        else:
            for _, row in df.iterrows():
                c1, c2, c3 = st.columns([0.7, 0.15, 0.15])
                c1.write(f"**{row.get('empresa', 'Sem Nome')}**")
                sid = int(row.get('id_prestador', 0))
                
                if c2.button("✏️", key=f"btn_edit_prest_{sid}"):
                    st.session_state[state_key] = sid
                    st.rerun()
                if c3.button("🗑️", key=f"btn_del_prest_{sid}"):
                    with st.spinner("Excluindo..."):
                        execute_crud_operation('prestador', id_value=sid, operation='delete')
                    st.success("Excluído!")
                    time.sleep(1)
                    st.rerun()
    else:
        # FORMULÁRIO DO PRESTADOR
        df = get_sheet_data('prestador')
        is_new = st.session_state[state_key] == 'NEW'
        curr = {}
        if not is_new:
            res = df[df['id_prestador'] == st.session_state[state_key]]
            if not res.empty: curr = res.iloc[0].to_dict()

        # Inicia variáveis para preenchimento automático
        if 'prov_cep' not in st.session_state: st.session_state.prov_cep = str(curr.get('cep', ''))
        if 'prov_end' not in st.session_state: st.session_state.prov_end = str(curr.get('endereco', ''))
        if 'prov_cid' not in st.session_state: st.session_state.prov_cid = str(curr.get('cidade', ''))
        if 'prov_bai' not in st.session_state: st.session_state.prov_bai = str(curr.get('bairro', ''))

        with st.form("form_prestador"):
            st.write("### Dados da Empresa")
            empresa = st.text_input("Nome da Empresa (Obrigatório)*", value=curr.get('empresa', ''))
            
            c1, c2 = st.columns(2)
            cnpj = c1.text_input("CNPJ", value=curr.get('cnpj', ''))
            nome_prest = c2.text_input("Nome do Contato", value=curr.get('nome_prestador', ''))
            
            tel = st.text_input("Telefone", value=str(curr.get('telefone', '')))
            
            st.write("### Endereço")
            cc1, cc2 = st.columns([0.4, 0.6])
            
            # Campo de CEP com funcionalidade de busca
            cep_input = cc1.text_input("CEP (Somente Números)", value=curr.get('cep', ''))
            
            # Botão de busca fora do fluxo principal para injetar dados no form
            # Nota: Em forms do Streamlit, botões internos recarregam a página.
            # Vamos usar um submit secundário ou apenas instruir. 
            # A melhor forma dentro de um form é processar tudo no final, 
            # mas para preencher visualmente, precisamos de um container separado.
            
            # WORKAROUND PARA CEP DENTRO DO FORM: 
            # O Streamlit não atualiza outros campos dinamicamente dentro de um st.form enquanto digita.
            # Então faremos a lógica de CEP fora do form se quisermos interatividade, ou processamos na hora de salvar.
            # Vou colocar o CEP e a busca ANTES do form principal para permitir o preenchimento.
            
        # --- BLOCO DE CEP FORA DO FORM PARA PERMITIR AUTO-FILL ---
        st.info("Preencha o CEP e clique na lupa para buscar o endereço.")
        c_cep, c_btn_cep = st.columns([0.4, 0.6])
        input_cep_val = c_cep.text_input("Digite o CEP:", value=curr.get('cep', ''), key="viacep_input")
        
        if c_btn_cep.button("🔍 Buscar Endereço"):
            data_cep = consultar_cep(input_cep_val)
            if data_cep:
                st.session_state.prov_end = data_cep.get('logradouro', '')
                st.session_state.prov_bai = data_cep.get('bairro', '')
                st.session_state.prov_cid = data_cep.get('localidade', '')
                st.success("Endereço encontrado!")
            else:
                st.error("CEP não encontrado.")
        
        # --- FORMULÁRIO FINAL ---
        with st.form("form_prestador_final"):
            # Usa os valores do session_state (atualizados pelo CEP) ou do banco
            end_val = st.session_state.prov_end if st.session_state.prov_end else curr.get('endereco', '')
            bai_val = st.session_state.prov_bai if st.session_state.prov_bai else curr.get('bairro', '')
            cid_val = st.session_state.prov_cid if st.session_state.prov_cid else curr.get('cidade', '')
            
            endereco = st.text_input("Endereço", value=end_val)
            c_num, c_bai = st.columns([0.3, 0.7])
            numero = c_num.text_input("Número", value=str(curr.get('numero', '')))
            bairro = c_bai.text_input("Bairro", value=bai_val)
            cidade = st.text_input("Cidade", value=cid_val)
            
            # E-MAIL REMOVIDO CONFORME SOLICITADO
            
            if st.form_submit_button("💾 Salvar Prestador"):
                # 🟢 VALIDAÇÃO DE EMPRESA
                if not empresa or empresa.strip() == "":
                    st.error("Erro: O campo 'Nome da Empresa' é obrigatório.")
                else:
                    payload = {
                        'empresa': empresa,
                        'telefone': tel,
                        'nome_prestador': nome_prest,
                        'cnpj': cnpj,
                        'email': "", # Salva vazio para manter compatibilidade com schema
                        'cep': input_cep_val,
                        'endereco': endereco,
                        'numero': numero,
                        'cidade': cidade,
                        'bairro': bairro
                    }
                    
                    with st.spinner("Salvando..."):
                        if is_new: execute_crud_operation('prestador', data=payload, operation='insert')
                        else: execute_crud_operation('prestador', data=payload, id_value=st.session_state[state_key], operation='update')
                    
                    st.session_state[state_key] = None
                    # Limpa estados temporários
                    for k in ['prov_cep', 'prov_end', 'prov_cid', 'prov_bai']:
                        if k in st.session_state: del st.session_state[k]
                        
                    st.success("Salvo com sucesso!")
                    time.sleep(0.5)
                    st.rerun()

        if st.button("Cancelar Edição"):
            st.session_state[state_key] = None
            st.rerun()

# --- UI PARA SERVIÇOS ---
def service_management_ui():
    st.subheader("Gestão de Serviços")
    state_key = 'edit_servico_id'
    
    df_v = get_sheet_data('veiculo')
    df_p = get_sheet_data('prestador')
    df_serv = get_sheet_data('servico')
    
    map_v = {f"{r['nome']} ({r.get('placa','S/P')})": int(r['id_veiculo']) for _, r in df_v.iterrows()} if not df_v.empty else {}
    map_p = {f"{r['empresa']}": int(r['id_prestador']) for _, r in df_p.iterrows()} if not df_p.empty else {}
    
    if st.session_state[state_key] is None:
        c_btn, _ = st.columns([0.3, 0.7])
        if c_btn.button("➕ Novo Serviço"):
            if not map_v or not map_p:
                st.error("Cadastre Veículos e Prestadores antes de criar um serviço.")
            else:
                st.session_state[state_key] = 'NEW'
                st.rerun()
        
        if not df_serv.empty:
            if 'data_servico' in df_serv.columns:
                df_serv['data_servico_dt'] = pd.to_datetime(df_serv['data_servico'], errors='coerce')
            
            for _, row in df_serv.iterrows():
                c1, c2, c3 = st.columns([0.7, 0.15, 0.15])
                d_str = row['data_servico_dt'].strftime('%d/%m/%Y') if pd.notna(row.get('data_servico_dt')) else ""
                val_display = str(row.get('nome_servico', 'Serviço'))
                c1.write(f"**{val_display}** - {d_str}")
                sid = int(row.get('id_servico', 0))
                
                if c2.button("✏️", key=f"btn_ed_s_{sid}"):
                    st.session_state[state_key] = sid
                    st.rerun()
                if c3.button("🗑️", key=f"btn_del_s_{sid}"):
                    with st.spinner("Apagando..."):
                        execute_crud_operation('servico', id_value=sid, operation='delete')
                    st.success("Apagado!")
                    time.sleep(1)
                    st.rerun()
        else:
            st.info("Nenhum serviço registrado.")
    else:
        is_new = st.session_state[state_key] == 'NEW'
        curr = {}
        curr_id_v = 0
        curr_id_p = 0
        if not is_new:
            res = df_serv[df_serv['id_servico'] == st.session_state[state_key]]
            if not res.empty:
                curr = res.iloc[0].to_dict()
                curr_id_v = int(curr.get('id_veiculo', 0))
                curr_id_p = int(curr.get('id_prestador', 0))

        with st.form("form_servico"):
            idx_v = 0
            if curr_id_v in map_v.values(): idx_v = list(map_v.values()).index(curr_id_v)
            idx_p = 0
            if curr_id_p in map_p.values(): idx_p = list(map_p.values()).index(curr_id_p)
            
            opts_v = list(map_v.keys()) if map_v else ["Sem Veículos"]
            opts_p = list(map_p.keys()) if map_p else ["Sem Prestadores"]
            
            sel_v = st.selectbox("Veículo", options=opts_v, index=min(idx_v, len(opts_v)-1))
            sel_p = st.selectbox("Prestador", options=opts_p, index=min(idx_p, len(opts_p)-1))
            
            nome_s = st.text_input("Descrição do Serviço", value=curr.get('nome_servico', ''))
            
            c1, c2 = st.columns(2)
            try: d_val = pd.to_datetime(curr.get('data_servico')) if curr.get('data_servico') else date.today()
            except: d_val = date.today()
            
            data_s = c1.date_input("Data", value=d_val, format="DD/MM/YYYY")
            garantia = c2.number_input("Garantia (dias)", value=int(curr.get('garantia_dias', 90)))
            
            c3, c4 = st.columns(2)
            valor = c3.number_input("Valor (R$)", value=float(curr.get('valor', 0.0)), format="%.2f")
            km_r = c4.number_input("KM Atual", value=int(float(curr.get('km_realizado', 0))), step=1, format="%d")
            
            reg = st.text_input("Nota/Registro", value=curr.get('registro', ''))
            
            if st.form_submit_button("💾 Salvar Serviço"):
                if not map_v or not map_p:
                    st.error("Não é possível salvar sem Veículo/Prestador.")
                else:
                    dt_venc = data_s + timedelta(days=int(garantia))
                    payload = {
                        'id_veiculo': map_v.get(sel_v, 0),
                        'id_prestador': map_p.get(sel_p, 0),
                        'nome_servico': nome_s,
                        'data_servico': data_s.strftime('%Y-%m-%d'),
                        'garantia_dias': int(garantia),
                        'valor': float(valor),
                        'km_realizado': int(km_r),
                        'registro': reg,
                        'data_vencimento': dt_venc.strftime('%Y-%m-%d')
                    }
                    
                    with st.spinner("Salvando..."):
                        if is_new: execute_crud_operation('servico', data=payload, operation='insert')
                        else: execute_crud_operation('servico', data=payload, id_value=st.session_state[state_key], operation='update')
                    
                    st.session_state[state_key] = None
                    st.success("Serviço Salvo!")
                    time.sleep(0.5)
                    st.rerun()

        if st.button("Cancelar"):
            st.session_state[state_key] = None
            st.rerun()

# ==============================================================================
# 5. MAIN
# ==============================================================================

def run_auto_test_data():
    st.info("Simulando...")
    execute_crud_operation('veiculo', data={'nome': 'Civic Teste', 'placa': 'TST-0001', 'ano': 2023, 'valor_pago': 150000, 'data_compra': '2023-01-01'}, operation='insert')
    execute_crud_operation('prestador', data={'empresa': 'Oficina Master', 'telefone': 1199999, 'cnpj': '00.000/0001-00'}, operation='insert')
    time.sleep(1)
    
    df_v = get_sheet_data('veiculo', force_refresh=True)
    df_p = get_sheet_data('prestador', force_refresh=True)
    
    id_v, id_p = 0, 0
    if not df_v.empty:
        res = df_v[df_v['placa'] == 'TST-0001']
        if not res.empty: id_v = int(res.iloc[0]['id_veiculo'])
    if not df_p.empty:
        res = df_p[df_p['cnpj'] == '00.000/0001-00']
        if not res.empty: id_p = int(res.iloc[0]['id_prestador'])
    
    if id_v and id_p:
        execute_crud_operation('servico', data={
            'id_veiculo': id_v, 'id_prestador': id_p,
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
        if st.button("🔄 Atualizar Dados"):
            st.cache_data.clear()
            st.rerun()
        if st.button("🧪 Rodar Simulação"): run_auto_test_data()

    # ABA RESUMO
    with tab_resumo:
        df_full = get_full_service_data()
        
        if not df_full.empty:
            df_full['Ano'] = df_full['data_servico'].dt.year
            
            st.subheader("Filtros")
            c1, c2 = st.columns(2)
            
            anos = sorted(df_full['Ano'].dropna().unique().astype(int).tolist(), reverse=True)
            sel_ano = c1.selectbox("Ano", ["Todos"] + anos)
            
            veiculos = sorted(df_full['nome'].astype(str).unique().tolist())
            sel_veiculo = c2.selectbox("Veículo", ["Todos"] + veiculos)
            
            df_filt = df_full.copy()
            if sel_ano != "Todos": df_filt = df_filt[df_filt['Ano'] == sel_ano]
            if sel_veiculo != "Todos": df_filt = df_filt[df_filt['nome'] == sel_veiculo]
            
            st.divider()
            
            if not df_filt.empty:
                k1, k2 = st.columns(2)
                k1.metric("Total Gasto", f"R$ {df_filt['valor'].sum():,.2f}")
                k2.metric("Serviços", len(df_filt))
                
                st.subheader("Gastos por Veículo")
                df_chart = df_filt.groupby('nome', as_index=False)['valor'].sum()
                
                base = alt.Chart(df_chart).encode(
                    x=alt.X('nome', sort='-y', title='Veículo'),
                    y=alt.Y('valor', title='Total (R$)')
                )
                barras = base.mark_bar(color='#FF4B4B')
                txt = base.mark_text(dy=-5).encode(text=alt.Text('valor', format=',.2f'))
                st.altair_chart((barras + txt).properties(height=400).interactive(), use_container_width=True)
            else:
                st.warning("Nenhum dado com estes filtros.")
        else:
            st.info("Nenhum serviço registrado para o resumo.")

    # ABA HISTÓRICO
    with tab_hist:
        df_full = get_full_service_data()
        if not df_full.empty:
            df_full['Ano'] = df_full['data_servico'].dt.year
            c1, c2 = st.columns(2)
            
            v_sel = c1.selectbox("Filtrar Veículo:", ["Todos"] + sorted(list(df_full['nome'].unique())), key="h_v")
            y_sel = c2.selectbox("Filtrar Ano:", ["Todos"] + sorted(list(df_full['Ano'].dropna().unique().astype(int))), key="h_y")
            
            df_filt = df_full.copy()
            if v_sel != "Todos": df_filt = df_filt[df_filt['nome'] == v_sel]
            if y_sel != "Todos": df_filt = df_filt[df_filt['Ano'] == y_sel]
            
            cols_view = ['nome', 'placa', 'nome_servico', 'empresa', 'data_servico', 'valor', 'Dias p/ Vencer']
            df_view = df_filt[cols_view].copy()
            if 'data_servico' in df_view.columns:
                df_view['data_servico'] = df_view['data_servico'].dt.strftime('%d/%m/%Y')
                
            st.dataframe(df_view, use_container_width=True)
        else:
            st.info("Histórico vazio.")

    # ABA MANUAL
    with tab_manual:
        opcao = st.radio("Gerenciar:", ["Veículo", "Serviço", "Prestador"], horizontal=True, key="nav_manual")
        st.divider()
        if opcao == "Veículo": generic_management_ui("Veículo", "veiculo", "nome")
        elif opcao == "Serviço": service_management_ui()
        elif opcao == "Prestador": provider_management_ui() # 🟢 UI NOVA

if __name__ == '__main__':
    main()