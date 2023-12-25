import streamlit as st
import os
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots 

# Import Dataset
df_orders_dataset = pd.read_csv('olist_orders_dataset.csv')
df_customer = pd.read_csv('olist_customers_dataset.csv')
df_order_payments = pd.read_csv('olist_order_payments_dataset.csv')
df_products_dataset = pd.read_csv('olist_products_dataset.csv')
df_product_name = pd.read_csv('product_category_name_translation.csv')
df_order_items = pd.read_csv('olist_order_items_dataset.csv')

# Hanya pesanan yang telah dikirim yang akan digunakan dalam analisis berikut.
df_orders123 = df_orders_dataset[df_orders_dataset['order_status'] == 'delivered'] # Pilih pesanan dengan status terkirim
df_orders123 = df_orders123.reset_index(drop = True) # Mengatur ulang indeks kolom

# Dalam analisis berikut ini ada 4 kolom yang akan digunakan dari tabel orders: 
# order_id, customer_id, order_status, order_purchase_timestamp

kolom_yang_dihapus = ['order_approved_at','order_delivered_carrier_date', 'order_delivered_customer_date', 
'order_estimated_delivery_date']
df_orders123 = df_orders123.drop(kolom_yang_dihapus, axis=1)

df_orders123['order_purchase_timestamp'] = pd.to_datetime(df_orders123['order_purchase_timestamp'])
# Gabungkan dataframe df_orders123 dan df, lalu simpan di dataframe revenue_states
revenue_states = pd.merge(df_orders123, df_customer, how='left', on='customer_id')
# Gabungkan dataframe revenue_states dan payments table, then stored in revenue_states
revenue_states = pd.merge(revenue_states, df_order_payments, how='left', on='order_id')
# Kamus untuk mengganti singkatan dengan nama lengkap
state_mapping = {
    'AC': 'Acre',
    'AL': 'Alagoas',
    'AM': 'Amazonas',
    'AP': 'Amapa',
    'BA': 'Bahia',
    'CE': 'Ceara',
    'DF': 'Distrito Federal',
    'ES': 'Espirito Santo',
    'GO': 'Goias',
    'MA': 'Maranhao',
    'MG': 'Minas Gerais',
    'MS': 'Mato Grosso do Sul',
    'MT': 'Mato Grosso',
    'PA': 'Para',
    'PB': 'Paraiba',
    'PE': 'Pernambuco',
    'PI': 'Piaui',
    'PR': 'Parana',
    'RJ': 'Rio de Janeiro',
    'RN': 'Rio Grande do Norte',
    'RO': 'Rondonia',
    'RR': 'Roraima',
    'RS': 'Rio Grande do Sul',
    'SC': 'Santa Catarina',
    'SE': 'Sergipe',
    'SP': 'Sao Paulo',
    'TO': 'Tocantins'
}

# Mengganti singkatan dengan nama lengkap menggunakan map
revenue_states['customer_state'] = revenue_states['customer_state'].map(state_mapping)
revenue_states = revenue_states.dropna()
# kelompokkan berdasarkan 'customer_state' dan jumlahkan 'payment_value' untuk setiap status
revenue_by_state = revenue_states.groupby('customer_state')['payment_value'].sum().reset_index()

# Urutkan ringkasan berdasarkan pendapatan tertinggi dalam urutan descending
revenue_by_state = revenue_by_state.sort_values(by='payment_value', ascending=False)
revenue_by_state.columns = ['State', 'Revenue']

def calculate_monthly_revenue(revenue_states, state_column, states=None):
    if states:
        revenue_states = revenue_states[revenue_states[state_column].isin(states)]

    revenue_states['year'] = revenue_states['order_purchase_timestamp'].dt.year
    revenue_states['month'] = revenue_states['order_purchase_timestamp'].dt.month
    group_columns = ['year', 'month']
    if state_column in revenue_states.columns:
        group_columns.append(state_column)
    monthly_revenue = revenue_states.groupby(group_columns)['payment_value'].sum().reset_index()
    monthly_revenue = monthly_revenue.sort_values(by=[state_column, 'year', 'month']).reset_index(drop=True)
    monthly_revenue['year_month'] = monthly_revenue['year'].astype(str) + '-' + monthly_revenue['month'].astype(str)
    monthly_revenue.columns = ['year', 'month', 'state', 'revenue', 'year_month']
    return monthly_revenue

# Hitung pendapatan bulanan untuk negara bagian tertentu
states_to_calculate = ['Sao Paulo', 'Rio de Janeiro', 'Minas Gerais', 'Rio Grande do Sul', 'Parana']
monthly_revenue_states = {}
for state in states_to_calculate:
    monthly_revenue_states[state] = calculate_monthly_revenue(revenue_states, 'customer_state', states=[state])

# Access the results
monthly_revenue_SP = monthly_revenue_states['Sao Paulo']
monthly_revenue_RJ = monthly_revenue_states['Rio de Janeiro']
monthly_revenue_MG = monthly_revenue_states['Minas Gerais']
monthly_revenue_RS = monthly_revenue_states['Rio Grande do Sul']
monthly_revenue_PR = monthly_revenue_states['Parana']

# Merge df_products_dataset table and df_product_name table, stored in products_merged
products_merged = pd.merge(df_products_dataset, df_product_name, how='left', on='product_category_name')
# Merge revenue_states table and df_order_items table, stored in revenue_order_items
revenue_order_items = pd.merge(revenue_states, df_order_items, how='left', on='order_id')
# Merge revenue_order_items table and products_merged table, stored in revenue_order_items
revenue_products = pd.merge(revenue_order_items, products_merged, how='left', on='product_id')
revenue_products = revenue_products.rename(columns={'product_category_name_english': 'product_category'})
revenue_products = revenue_products[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'customer_state', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value', 'product_id', 'product_category']]
revenue_products = revenue_products.dropna()
# Menghapus nilai duplikat
revenue_products = revenue_products.drop_duplicates(keep='first') # Remove duplicate data and keep the first data that appears
revenue_products = revenue_products.reset_index(drop = True) # Reset the column index

def calculate_revenue_per_product_cat(revenue_data, state_list):
    revenue_by_state = {}    
    for state in state_list:
        state_data = revenue_data[revenue_data['customer_state'] == state]
        revenue_by_state[state] = state_data.groupby('product_category')['payment_value'].sum().reset_index()
        
    return revenue_by_state

def calculate_revenue_proportion(revenue_by_state_dict, state):
    state_summary = revenue_by_state_dict.get(state)
    if state_summary is not None:
        state_summary['revenue_proportion'] = (state_summary['payment_value'] / state_summary['payment_value'].sum()) * 100
    return state_summary.rename(columns={'payment_value': 'revenue', 'revenue_proportion': 'revenue_proportion'})

def sort_by_revenue(revenue_summary, ascending=True):
    return revenue_summary.sort_values(by='revenue', ascending=ascending).head(10).reset_index(drop=True)

# 5 negara bagian teratas dengan pendapatan tertinggi
states = ['Sao Paulo', 'Rio de Janeiro', 'Minas Gerais', 'Rio Grande do Sul', 'Parana']

# Hitung pendapatan per kategori produk untuk 5 negara bagian teratas
revenue_by_state_dict = calculate_revenue_per_product_cat(revenue_products, states)

state = 'Sao Paulo'
state_summary = calculate_revenue_proportion(revenue_by_state_dict, state)
highest_SP = sort_by_revenue(state_summary, ascending=False)

state = 'Rio de Janeiro'
state_summary = calculate_revenue_proportion(revenue_by_state_dict, state)
highest_RJ = sort_by_revenue(state_summary, ascending=False)

state = 'Minas Gerais'
state_summary = calculate_revenue_proportion(revenue_by_state_dict, state)
highest_MG = sort_by_revenue(state_summary, ascending=False)

state = 'Rio Grande do Sul'
state_summary = calculate_revenue_proportion(revenue_by_state_dict, state)
highest_RS = sort_by_revenue(state_summary, ascending=False)

state = 'Parana'
state_summary = calculate_revenue_proportion(revenue_by_state_dict, state)
highest_PR = sort_by_revenue(state_summary, ascending=False)

# Set Streamlit page configuration
st.set_page_config(
    page_title="Brazilian E-Commerce Dashboard",
    page_icon=":bar_chart:",
)

# Streamlit app
def main():
    # Sidebar
    st.sidebar.image("https://storage.googleapis.com/kaggle-organizations/1942/thumbnail.png?r=51", use_column_width=True)
    st.sidebar.markdown("[Link Dataset Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)")
    st.sidebar.markdown("Copyright © 2023 Yunico Ardian")
    st.title("Brazilian E-Commerce Dashboard")

    # Create a visualization of revenue by state
    fig_revenue_by_state = px.bar(data_frame=revenue_by_state, x='State', y='Revenue', title='Negara dengan Revenue Tertinggi')
    fig_revenue_by_state.update_layout(title='Negara dengan Revenue/Pendapatan Tertinggi', title_x=0.35)
    fig_revenue_by_state.update_traces(marker_color='#099181')
    st.plotly_chart(fig_revenue_by_state, use_container_width=True)

    # Create a dropdown widget for selecting the state
    selected_state = st.selectbox("Pilih Negara", ['Sao Paulo', 'Rio de Janeiro', 'Minas Gerais', 'Rio Grande do Sul', 'Parana'])
    # Use the selected state to filter the data
    selected_data = monthly_revenue_states.get(selected_state)
    if selected_data is not None:
        # Create line plot for the selected state using Plotly Express
        fig_line = px.line(selected_data, x='year_month', y='revenue', title=f'Trend Pendapatan di {selected_state}')
        # Ubah warna garis sesuai dengan preferensi Anda
        fig_line.update_traces(line_color='#099181')  # Ganti warna garis menjadi merah, sesuaikan dengan warna yang diinginkan

        fig_line.update_layout(
            title_text=f'Trend Revenue in {selected_state}', autosize=False, width=800, height=500
        )
        # Tampilkan visualisasi di Streamlit
        st.plotly_chart(fig_line, use_container_width=True)

        # Daftar summary dan judul subplot
        summaries = [highest_SP, highest_RJ, highest_MG, highest_RS, highest_PR]
        titles = ['Sumber Pendapatan tertinggi Sao Paulo', 'Sumber Pendapatan tertinggi Rio de Janeiro', 'Sumber Pendapatan tertinggi Minas Gerais',
          'Sumber Pendapatan tertinggi Rio Grande do Sul', 'Sumber Pendapatan tertinggi Parana']

        # Pilihan dropdown untuk memilih negara
        selected_country = st.selectbox("Pilih Negara untuk Subplot", ['Sao Paulo', 'Rio de Janeiro', 'Minas Gerais', 'Rio Grande do Sul', 'Parana'])

        # Membuat subplot
        fig_subplots = make_subplots(rows=1, cols=1, subplot_titles=[f'Sumber Pendapatan terbesar di {selected_country}'])

        # Menambahkan Plotly Express plots ke subplot
        for i, (summary, title) in enumerate(zip(summaries, titles)):
            if selected_country in title:
                trace = px.bar(summary, x='product_category', y='revenue')
                # Mengatur warna jejak (trace) dengan nilai RGB
                trace.update_traces(marker_color='#099181')
                fig_subplots.add_trace(trace.data[0])

        # Memperbarui layout subplot
        fig_subplots.update_layout(
            title_text=f'Sumber Pendapatan terbesar di 5 Negara dengan Revenue Tertinggi',
            autosize=False,
            width=800,
            height=500
        )

        # Menampilkan subplot
        st.plotly_chart(fig_subplots, use_container_width=True)


if __name__ == "__main__":
    main()


