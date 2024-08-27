import pymysql
import pandas as pd
from zk import ZK
from datetime import datetime, timedelta

ip = '192.168.1.250'
port = 4370

def conDB():
    return pymysql.connect(
        host='150.95.30.70',
        port=3306,
        user='hrm_aes',
        password='hrm_aes@2024',
        database='hrm_aes',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def map_id():
    db_conn = conDB()
    try:
        with db_conn.cursor() as cursor:

            query = '''
            SELECT manpower_no, id FROM employees;
            '''
            cursor.execute(query)

            mapping_dict = {}
            for row in cursor.fetchall():
                manpower_no = row['manpower_no']
                id = row['id']
                mapping_dict[manpower_no] = str(id)
    finally:
        db_conn.close()
    return mapping_dict

def lastday():
    db_conn = conDB()
    try:
        with db_conn.cursor() as cursor:
            sql = "SELECT date FROM emp_work_times ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                return result['date']
            else:
                return datetime(2035, 1, 1)
    finally:
        db_conn.close()

def fetch_and_save_records():
    zk = ZK(ip, port=port, timeout=5)
    conn = zk.connect()
    conn.disable_device()

    attendance = conn.get_attendance()
    user_dict = map_id()

    records_list = [
        {
            'emp_id': user_dict.get(record.user_id, 'Unknown'),
            'timestamp': record.timestamp
        }
        for record in attendance
    ]

    df = pd.DataFrame(records_list)
    
    # Convert timestamps to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'])
    
    # Apply cutoff time of 05:00
    df['cutoff_date'] = df['datetime'].apply(lambda dt: dt.date() if dt.time() >= datetime.strptime('05:00:00', '%H:%M:%S').time() else (dt - timedelta(days=1)).date())
    df['cutoff_time'] = df['datetime'].apply(lambda dt: dt.time() if dt.time() >= datetime.strptime('05:00:00', '%H:%M:%S').time() else dt.time())

    # Group by employee ID and cutoff date
    grouped = df.groupby(['emp_id', 'cutoff_date'])
    
    result = []
    for (emp_id, date), group in grouped:
        if len(group) == 1:
            start_time = group['cutoff_time'].min().strftime('%H:%M:%S')
            end_time = None
        else:
            if group['cutoff_time'].min() > datetime.strptime('05:00:00', '%H:%M:%S').time():
                start_time = group['cutoff_time'].min().strftime('%H:%M:%S')
                end_time = group['cutoff_time'].max().strftime('%H:%M:%S')
            else:
                start_time = group['cutoff_time'].max().strftime('%H:%M:%S')
                end_time = group['cutoff_time'].min().strftime('%H:%M:%S')

        result.append({
            'emp_id': emp_id,
            'date': date,
            'start_time': start_time,
            'end_time': end_time
        })

    result_df = pd.DataFrame(result)
    result_df = result_df[~result_df['date'].astype(str).str.startswith('2103')]
    result_df = result_df[~result_df['date'].astype(str).str.startswith('2035')]
    result_df = result_df.sort_values(by=['date', 'start_time'])
    result_df = result_df.reset_index(drop=True)

    conn.enable_device()
    conn.disconnect()
    return result_df


def update_records(new_data):
    db_conn = conDB()
    try:
        with db_conn.cursor() as cursor:
            new_data = new_data[new_data['emp_id'] != 'Unknown']
            for index, row in new_data.iterrows():
                emp_id = row['emp_id']
                date = row['date']
                start_time = row['start_time']
                end_time = row['end_time']

                date_str = date.strftime('%Y-%m-%d')

                time_late = None
                leave_early = None

                standard_start_time = datetime.strptime('09:01:00', '%H:%M:%S').time()
                standard_start_time2 = datetime.strptime('09:00:00', '%H:%M:%S').time()
                standard_end_time = datetime.strptime('20:00:00', '%H:%M:%S').time()

                start_time_dt = datetime.strptime(start_time, '%H:%M:%S').time()
                if start_time_dt > standard_start_time:
                    time_late_td = datetime.combine(datetime.min, start_time_dt) - datetime.combine(datetime.min, standard_start_time2)
                    time_late = str(timedelta(seconds=time_late_td.total_seconds()))

                if end_time:
                    end_time_dt = datetime.strptime(end_time, '%H:%M:%S').time()
                    if end_time_dt > standard_end_time:
                        leave_early_td = datetime.combine(datetime.min, end_time_dt) -datetime.combine(datetime.min, standard_end_time)
                        leave_early = str(timedelta(seconds=leave_early_td.total_seconds()))

                sql_update = """
                    UPDATE emp_work_times
                    SET time_in = %s, time_out = %s, time_late = %s, leave_early = %s
                    WHERE emp_id = %s AND date = %s
                """
                cursor.execute(sql_update, (start_time, end_time, time_late, leave_early, emp_id, date_str))

            db_conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_conn.close()


def insert_records_to_db(df):
    db_conn = conDB()
    try:
        with db_conn.cursor() as cursor:
            now = datetime.now()
            formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
            df = df[df['emp_id'] != 'Unknown']

            for index, row in df.iterrows():
                emp_id = row['emp_id']
                try:
                    emp_id = int(emp_id)
                except ValueError:
                    emp_id = None

                date = datetime.combine(row['date'], datetime.min.time())
                start_time = row['start_time']
                end_time = row['end_time']
                
                time_late = None
                leave_early = None
                
                standard_start_time = datetime.strptime('09:01:00', '%H:%M:%S').time()
                standard_start_time2 = datetime.strptime('09:00:00', '%H:%M:%S').time()
                standard_end_time = datetime.strptime('20:00:00', '%H:%M:%S').time()
                
                start_time_dt = datetime.strptime(start_time, '%H:%M:%S').time()
                
                if start_time_dt > standard_start_time:
                    time_late_td = datetime.combine(datetime.min, start_time_dt) - datetime.combine(datetime.min, standard_start_time2)
                    time_late = str(timedelta(seconds=time_late_td.total_seconds()))

                if end_time:
                    end_time_dt = datetime.strptime(end_time, '%H:%M:%S').time()
                    if end_time_dt > standard_end_time:
                        leave_early_td = datetime.combine(datetime.min, end_time_dt) - datetime.combine(datetime.min, standard_end_time)
                        leave_early = str(timedelta(seconds=leave_early_td.total_seconds()))

                status = 1
                created_at = formatted_now
                created_by = 2
                updated_at = formatted_now
                updated_by = 2

                sql = """
                    INSERT INTO emp_work_times (emp_id, date, time_in, time_out, time_late, leave_early, status, created_at, created_by, updated_at, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (emp_id, date, start_time, end_time, time_late, leave_early, status, created_at, created_by, updated_at, updated_by))

            db_conn.commit()
            print(f"Records {formatted_now} successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    finally:
        db_conn.close()

def delete_records(date_threshold):
    db_conn = conDB()
    try:
        with db_conn.cursor() as cursor:
            sql_delete = """
                DELETE FROM emp_work_times
                WHERE date >= %s
            """
            cursor.execute(sql_delete, (date_threshold,))
            db_conn.commit()
            print(f"Records deleted successfully where date >= {date_threshold}.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_conn.close()
def reset_auto_increment(table_name, start_value):
    db_conn = conDB()
    try:
        with db_conn.cursor() as cursor:
            sql_reset_auto_increment = f"""
                ALTER TABLE {table_name} AUTO_INCREMENT = {start_value};
            """
            cursor.execute(sql_reset_auto_increment)
            db_conn.commit()
            print(f"Auto-increment value for table '{table_name}' has been reset to {start_value}.")
    except Exception as e:
        print(f"An error occurred while resetting AUTO_INCREMENT: {e}")
    finally:
        db_conn.close()

if __name__ == '__main__':
    new_data = fetch_and_save_records()
    new_data['date'] = pd.to_datetime(new_data['date']).dt.date
    last_date = lastday()
    last_data =new_data[new_data['date']>=last_date.date()]
    update_records(last_data)
    new_data =new_data[new_data['date']>last_date.date()]
    insert_records_to_db(new_data)
