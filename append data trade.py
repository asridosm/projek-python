import os
import pandas as pd
from impala.dbapi import connect
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import quote_plus
import requests

# Memuat variabel lingkungan dari file .env
load_dotenv()

# Fungsi untuk mengirim pesan ke Telegram
def send_telegram_message(message):
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")  # Token bot Telegram dari .env
    chat_id = os.getenv("TELEGRAM_CHAT_ID")  # Chat ID penerima dari .env
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("✅ Pesan berhasil dikirim ke Telegram.")
        else:
            print(f"❌ Gagal mengirim pesan ke Telegram: {response.text}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat mengirim pesan ke Telegram: {e}")

# Fungsi untuk mengirim file ke Telegram
def send_telegram_file(file_path, caption):
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")  # Token bot Telegram dari .env
    chat_id = os.getenv("TELEGRAM_CHAT_ID")  # Chat ID penerima dari .env
    url = f"https://api.telegram.org/bot{telegram_token}/sendDocument"
    try:
        with open(file_path, 'rb') as file:
            response = requests.post(url, data={"chat_id": chat_id, "caption": caption}, files={"document": file})
        if response.status_code == 200:
            print("✅ File berhasil dikirim ke Telegram.")
        else:
            print(f"❌ Gagal mengirim file ke Telegram: {response.text}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat mengirim file ke Telegram: {e}")

# Fungsi untuk membuat backup tabel PostgreSQL
def backup_postgres_table(engine, schema, table_name):
    backup_table_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_query = f"""
    CREATE TABLE "{schema}"."{backup_table_name}" AS
    SELECT * FROM "{schema}"."{table_name}";
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(backup_query))
            conn.commit()
        print(f"✅ Backup tabel berhasil dibuat: {schema}.{backup_table_name}")
        send_telegram_message(f"✅ Backup tabel berhasil dibuat: {schema}.{backup_table_name}")
    except Exception as e:
        print(f"❌ Gagal membuat backup tabel: {e}")
        send_telegram_message(f"❌ Gagal membuat backup tabel: {e}")

# sambungan ke Impala
impala_conn = connect(
    host=os.getenv("I_HOST"),
    port=21050,
    user=os.getenv("I_USER"),
    password=os.getenv("I_PASSWORD"),
    database="repo_external_sector"
)
impala_cursor = impala_conn.cursor()

# URL encoding untuk password PostgreSQL
encoded_password = quote_plus(os.getenv("POSTGRES_PASSWORD"))

# Sambungan ke PostgreSQL
postgres_connection_string = f'postgresql+psycopg2://{os.getenv("POSTGRES_USER")}:{encoded_password}@{os.getenv("POSTGRES_HOST")}:5432/{os.getenv("POSTGRES_DATABASE")}'
postgres_engine = create_engine(postgres_connection_string)

# Nama jadual PostgreSQL
postgres_table = 'md_external_sector_active_copy1_backup_20250515_142331'
postgres_schema = 'ADEXTR'

# Query untuk mengambil data dari Impala
impala_query = "SELECT * FROM repo_external_sector.md_external_active_janmac2025"

try:
    # Ambil data dari Impala
    print("📥 Mengambil data dari Impala...")
    impala_cursor.execute(impala_query)
    impala_data = impala_cursor.fetchall()
    impala_columns = [col[0] for col in impala_cursor.description]
    df_impala = pd.DataFrame(impala_data, columns=impala_columns)
    print(f"✅ Data dari Impala berhasil diambil. Jumlah baris: {len(df_impala)}")
    send_telegram_message(f"✅ Data dari Impala berhasil diambil. Jumlah baris: {len(df_impala)}")

    # Tentukan bulan terakhir dengan status_data_code = '03'
    print("🔍 Mencari bulan terakhir dengan status_data_code = '03'...")
    with postgres_engine.connect() as conn:
        last_month_query = """
        SELECT year, month_code
        FROM "ADEXTR"."md_external_sector_active_copy1"
        WHERE status_data_code = '03'
        ORDER BY year DESC, month_code DESC
        LIMIT 1
        """
        result = conn.execute(text(last_month_query)).fetchone()
        if result:
            last_year, last_month = result
            print(f"🔍 Data yang ditemukan: year = {last_year}, month_code = {last_month}")
            send_telegram_message(f"🔍 Data yang ditemukan: year = {last_year}, month_code = {last_month}")

            # Backup jadual sebelum menghapus data
            print("📦 Membuat backup tabel sebelum menghapus data...")
            backup_postgres_table(postgres_engine, postgres_schema, postgres_table)

            print(f"🗑️ Menghapus data untuk year = '{last_year}', month_code = '{last_month}', status_data_code = '03'...")
            delete_query = f"""
            DELETE FROM "ADEXTR"."md_external_sector_active_copy1"
            WHERE year = '{last_year}' AND month_code = '{last_month}' AND status_data_code = '03'
            """
            conn.execute(text(delete_query))
            conn.commit()
            print(f"✅ Data untuk year = '{last_year}', month_code = '{last_month}', status_data_code = '03' berhasil dihapus.")
            send_telegram_message(f"✅ Data untuk year = '{last_year}', month_code = '{last_month}', status_data_code = '03' berhasil dihapus.")
        else:
            print("ℹ️ Tidak ada data dengan status_data_code = '03' yang ditemukan untuk dihapus.")
            send_telegram_message("ℹ️ Tidak ada data dengan status_data_code = '03' yang ditemukan untuk dihapus.")

    # Backup jadual PostgreSQL sebelum append data baru
    print("📦 Membuat backup tabel PostgreSQL sebelum append data...")
    backup_postgres_table(postgres_engine, postgres_schema, postgres_table)

    # Append data baru ke PostgreSQL
    print("📤 Mengappend data baru ke PostgreSQL...")
    df_impala.to_sql(postgres_table, postgres_engine, schema=postgres_schema, if_exists='append', index=False)
    print(f"✅ Data baru berhasil diappend ke tabel: {postgres_schema}.{postgres_table}")
    send_telegram_message(f"✅ Data baru berhasil diappend ke tabel: {postgres_schema}.{postgres_table}")

    # Ambil data dari PostgreSQL untuk membuat summary
    print("📊 Membuat summary data dari PostgreSQL...")
    with postgres_engine.connect() as conn:
        summary_query = """
        SELECT 
            year, 
            month_code, 
            trade_classification_code, 
            SUM(rm_value) AS rm_value
        FROM "ADEXTR"."md_external_sector_active_copy1"
        WHERE port_of_lading_code = '0'
        GROUP BY year, month_code, trade_classification_code
        ORDER BY year, month_code
        """
        summary_data = pd.read_sql_query(text(summary_query), conn)

    # Debugging: Periksa data yang diambil
    print(f"📊 Data yang diambil dari PostgreSQL:\n{summary_data.head().to_string(index=False)}")
    print(f"Jumlah baris: {len(summary_data)}")

    # Pastikan kolom rm_value float
    summary_data['rm_value'] = summary_data['rm_value'].astype(float)

    # Hitung summary seperti query SQL yang diminta
    print("📊 Menghitung summary data...")
    summary = summary_data.groupby(['year', 'month_code']).agg(
        total_imports=pd.NamedAgg(column='rm_value', aggfunc=lambda x: x[summary_data['trade_classification_code'].isin(['1', '4'])].sum()),
        total_exports=pd.NamedAgg(column='rm_value', aggfunc=lambda x: x[summary_data['trade_classification_code'].isin(['2', '3', '5', '6'])].sum()),
        domestic_exports=pd.NamedAgg(column='rm_value', aggfunc=lambda x: x[summary_data['trade_classification_code'].isin(['2', '5'])].sum()),
        re_exports=pd.NamedAgg(column='rm_value', aggfunc=lambda x: x[summary_data['trade_classification_code'].isin(['3', '6'])].sum()),
        total_trade=pd.NamedAgg(column='rm_value', aggfunc='sum'),
        trade_balance=pd.NamedAgg(column='rm_value', aggfunc=lambda x: x[summary_data['trade_classification_code'].isin(['2', '3', '5', '6'])].sum() - x[summary_data['trade_classification_code'].isin(['1', '4'])].sum())
    ).reset_index()

    # Tambahkan baris total per tahun
    print("📊 Menambahkan baris total per tahun...")
    total_per_year = summary.groupby('year').agg(
        total_imports=('total_imports', 'sum'),
        total_exports=('total_exports', 'sum'),
        domestic_exports=('domestic_exports', 'sum'),
        re_exports=('re_exports', 'sum'),
        total_trade=('total_trade', 'sum'),
        trade_balance=('trade_balance', 'sum')
    ).reset_index()
    total_per_year['month_code'] = 'Total'

    # Gabungkan data bulanan dengan total per tahun
    final_summary = pd.concat([summary, total_per_year], ignore_index=True)

    # Urutkan data berdasarkan tahun dan bulan
    final_summary['month_code_order'] = final_summary['month_code'].apply(
        lambda x: 99 if x == 'Total' else int(x))
    final_summary = final_summary.sort_values(by=['year', 'month_code_order']).drop(columns=['month_code_order'])

    # Simpan summary ke file Excel
    documents_folder = os.path.expanduser("~/Documents")
    summary_file = os.path.join(documents_folder, f"summary_{postgres_table}.xlsx")
    try:
        final_summary.to_excel(summary_file, index=False, float_format="%.2f")
        print(f"✅ Summary data disimpan ke file: {summary_file}")
        send_telegram_message(f"✅ Summary data disimpan ke file: {summary_file}")
        
        # Kirim file summary ke Telegram
        send_telegram_file(summary_file, f"📊 Summary data dari jadual {postgres_table}")
    except Exception as e:
        print(f"❌ Gagal menyimpan summary ke file: {e}")
        send_telegram_message(f"❌ Gagal menyimpan summary ke file: {e}")

except Exception as e:
    send_telegram_message(f"❌ Ada kesalahan: {e}")

finally:
    # Tutup sambungan
    impala_cursor.close()
    impala_conn.close()
    print("🔒 Sambungan ke Impala ditutup.")
    send_telegram_message("🔒 Sambungan ke Impala ditutup.")
