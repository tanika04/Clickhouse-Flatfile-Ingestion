from flask import Flask, render_template, request, jsonify, send_file
from clickhouse_connect import get_client
from werkzeug.utils import secure_filename
import pandas as pd
import os
import numpy as np
import time

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    try:
        file = request.files['file']
        if not file or file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
            
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Only CSV files allowed'}), 400

        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        delimiter = request.args.get('delimiter', ',')
        try:
            df = pd.read_csv(file_path, delimiter=delimiter)
            return jsonify({
                'columns': df.columns.tolist(),
                'preview': df.head(100).to_dict(orient='records'),
                'filename': filename,
                'total_rows': len(df)
            })
        except pd.errors.EmptyDataError:
            return jsonify({'error': 'The CSV file is empty'}), 400
        except pd.errors.ParserError:
            return jsonify({'error': 'Could not parse the CSV file. Please check the delimiter.'}), 400
        except Exception as e:
            return jsonify({'error': f'Error reading CSV: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


@app.route('/ingest_csv', methods=['POST'])
def ingest_csv():
    data = request.json
    filename = data['filename']
    table_name = data['target_table']
    selected_columns = data['columns']
    delimiter = data.get('delimiter', ',')

    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        df = pd.read_csv(file_path, delimiter=delimiter)[selected_columns]

        # Convert numpy.int64 values to regular Python integers
        record_count = int(len(df))
        total_rows = record_count

        client = get_client(
            host=data['host'],
            port=int(data['port']),
            username=data['user'],
            password=data['jwt_token'],
            secure=True,
            database=data['database']
        )

        # Create table with appropriate column types
        column_types = []
        for col in selected_columns:
            # Determine ClickHouse data type based on column content
            if pd.api.types.is_numeric_dtype(df[col]):
                if pd.api.types.is_integer_dtype(df[col]):
                    column_types.append(f'{col} Int64')
                else:
                    column_types.append(f'{col} Float64')
            else:
                column_types.append(f'{col} String')

        column_def = ', '.join(column_types)
        client.command(f'''
            CREATE TABLE IF NOT EXISTS {table_name} ({column_def})
            ENGINE = MergeTree()
            ORDER BY tuple()
        ''')

        # Calculate batch size (5000 records or 20% of total, whichever is smaller)
        batch_size = min(5000, max(100, int(record_count * 0.2)))
        num_batches = (record_count + batch_size - 1) // batch_size  # ceiling division
        
        progress_updates = []
        
        # Insert dataframe to ClickHouse in batches
        for i in range(0, record_count, batch_size):
            batch_end = min(i + batch_size, record_count)
            client.insert_df(table_name, df.iloc[i:batch_end])
            
            # Calculate progress percentage
            progress = int((batch_end / record_count) * 100)
            progress_updates.append({
                'processed': batch_end,
                'total': record_count,
                'percentage': progress
            })
            # Simulate some processing time for demo purposes
            time.sleep(0.5)

        return jsonify({
            'message': 'Data ingested successfully',
            'records': record_count,
            'progress': progress_updates
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/fetch_tables', methods=['POST'])
def fetch_tables():
    data = request.json
    try:
        client = get_client(
            host=data['host'],
            port=int(data['port']),
            username=data['user'],
            password=data['jwt_token'],
            secure=True,
            database=data['database']
        )
        tables = client.query("SHOW TABLES").result_rows
        return jsonify({'tables': [t[0] for t in tables]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/fetch_columns', methods=['POST'])
def fetch_columns():
    data = request.json
    try:
        client = get_client(
            host=data['host'],
            port=int(data['port']),
            username=data['user'],
            password=data['jwt_token'],
            secure=True,
            database=data['database']
        )
        table = data['table']
        desc = client.query(f"DESCRIBE TABLE {table}").result_rows
        return jsonify({'columns': [col[0] for col in desc]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/execute_join', methods=['POST'])
def execute_join():
    data = request.json
    try:
        client = get_client(
            host=data['host'],
            port=int(data['port']),
            username=data['user'],
            password=data['jwt_token'],
            secure=True,
            database=data['database']
        )
        
        # Extract join configuration
        tables = data['tables']
        selected_columns = data['columns']
        join_conditions = data['joinConditions']
        export_filename = data.get('exportFilename', 'joined_export.csv')
        
        # Validate tables
        if len(tables) < 2:
            return jsonify({'error': 'At least two tables are required for a join'}), 400
            
        # Begin constructing the query
        # Format: SELECT <cols> FROM table1 JOIN table2 ON <join_condition>
        
        # Format selected columns with table prefixes
        formatted_columns = []
        for col in selected_columns:
            if '.' in col:  # If column already has table prefix
                formatted_columns.append(col)
            else:
                # Use the first table as default for columns without prefix
                formatted_columns.append(f"{tables[0]}.{col}")
        
        select_clause = ', '.join(formatted_columns)
        
        # Construct join part of query
        from_clause = tables[0]
        for i in range(1, len(tables)):
            # Get the join condition for this table pair
            join_condition = join_conditions[i-1]  # -1 because joinConditions has 1 less item than tables
            from_clause += f" JOIN {tables[i]} ON {join_condition}"
        
        # Complete query
        query = f"SELECT {select_clause} FROM {from_clause}"
        
        # Execute query
        result = client.query_df(query)
        
        # Get row count
        record_count = len(result)
        
        # Save to CSV
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], export_filename)
        result.to_csv(output_path, index=False)
        
        return jsonify({
            'message': 'Join executed successfully',
            'records': record_count,
            'filename': export_filename
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/export_to_csv', methods=['POST'])
def export_to_csv():
    data = request.json
    try:
        client = get_client(
            host=data['host'],
            port=int(data['port']),
            username=data['user'],
            password=data['jwt_token'],
            secure=True,
            database=data['database']
        )
        cols = ', '.join(data['columns'])
        query = f"SELECT {cols} FROM {data['table']}"
        df = client.query_df(query)
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{data['table']}_export.csv")
        df.to_csv(output_path, index=False)

        return send_file(output_path, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/download_file/<filename>', methods=['GET'])
def download_file(filename):
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500




if __name__ == '__main__':
    app.run(debug=True)