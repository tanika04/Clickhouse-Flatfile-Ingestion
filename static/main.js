let uploadedFilename = '';
let previewData = null;
let selectedJoinTables = [];
let tableColumnsCache = {};

// function toggleSourceForms() {
//     const src = document.getElementById('sourceSelect').value;
//     document.getElementById('flatfileToClickhouse').style.display = src === 'flatfile' ? 'block' : 'none';
//     document.getElementById('clickhouseToFlatfile').style.display = src === 'clickhouse' ? 'block' : 'none';
//     document.getElementById('multiTableJoin').style.display = src === 'join' ? 'block' : 'none';
// }

function toggleSourceForms() {
	const src = document.getElementById('sourceSelect').value;
	document.getElementById('flatfileToClickhouse').style.display = src === 'flatfile' ? 'block' : 'none';
	document.getElementById('clickhouseToFlatfile').style.display = src === 'clickhouse' ? 'block' : 'none';
	document.getElementById('multiTableJoin').style.display = src === 'join' ? 'block' : 'none';

	// Reset state when changing source type
	if (src === 'flatfile') {
		if (document.getElementById('csvFile').value) {
			document.getElementById('csvFile').value = '';
			document.getElementById('dataPreview').style.display = 'none';
			document.getElementById('columnSelect').style.display = 'none';
			document.getElementById('progressSection').style.display = 'none';
			uploadedFilename = '';
			previewData = null;
		}
	} else if (src === 'clickhouse') {
		document.getElementById('tablesDropdown').style.display = 'none';
		document.getElementById('tableColumnsSection').style.display = 'none';
		document.getElementById('exportProgressSection').style.display = 'none';
	} else if (src === 'join') {
		document.getElementById('joinTablesSection').style.display = 'none';
		document.getElementById('joinConditionsSection').style.display = 'none';
		document.getElementById('joinColumnsSection').style.display = 'none';
		document.getElementById('joinProgressSection').style.display = 'none';
		selectedJoinTables = [];
		updateSelectedTablesUI();
	}

	document.getElementById('status').innerText = '';
	document.getElementById('status').className = '';
}

function selectAllColumns(checked) {
	const checkboxes = document.querySelectorAll('#columns input[type="checkbox"]');
	checkboxes.forEach(cb => cb.checked = checked);
}

function selectAllSrcColumns(checked) {
	const checkboxes = document.querySelectorAll('#src_columns input[type="checkbox"]');
	checkboxes.forEach(cb => cb.checked = checked);
}

function selectAllJoinColumns(checked) {
	const checkboxes = document.querySelectorAll('#joinColumns input[type="checkbox"]');
	checkboxes.forEach(cb => cb.checked = checked);
}

function showStatus(message, type) {
	const statusEl = document.getElementById('status');
	statusEl.innerText = message;
	statusEl.className = type;

	// Auto-scroll to the status message
	statusEl.scrollIntoView({ behavior: 'smooth' });
}

function renderPreviewTable(columns, data) {
	const container = document.getElementById('previewContainer');
	let html = '<table class="preview-table"><thead><tr>';

	// Table headers
	columns.forEach(col => {
		html += `<th>${col}</th>`;
	});
	html += '</tr></thead><tbody>';

	// Table data
	data.forEach(row => {
		html += '<tr>';
		columns.forEach(col => {
			html += `<td>${row[col] !== null && row[col] !== undefined ? row[col] : ''}</td>`;
		});
		html += '</tr>';
	});

	html += '</tbody></table>';
	container.innerHTML = html;
}

function updateProgressBar(element, percent, detailsElement, details) {
	element.style.width = percent + '%';
	element.textContent = percent + '%';

	if (detailsElement && details) {
		detailsElement.textContent = details;
	}
}

function getClickhouseConnection(prefix) {
	return {
		host: document.getElementById(prefix + 'host').value,
		port: document.getElementById(prefix + 'port').value,
		database: document.getElementById(prefix + 'db').value,
		user: document.getElementById(prefix + 'user').value,
		jwt_token: document.getElementById(prefix + 'jwt').value
	};
}

function validateConnection(conn) {
	if (!conn.host || !conn.port || !conn.database || !conn.user || !conn.jwt_token) {
		showStatus("Please fill in all ClickHouse connection details", "error");
		return false;
	}
	return true;
}

window.onload = toggleSourceForms;

document.getElementById("uploadForm").onsubmit = async (e) => {
	e.preventDefault();

	showStatus("Uploading file...", "loading");

	const fileInput = document.getElementById("csvFile");
	if (!fileInput.files || fileInput.files.length === 0) {
		showStatus("Please select a file", "error");
		return;
	}

	const formData = new FormData();
	const delimiter = document.getElementById("delimiter").value;
	formData.append("file", fileInput.files[0]);

	try {
		const res = await fetch(`/upload_csv?delimiter=${encodeURIComponent(delimiter)}`, {
			method: "POST",
			body: formData
		});

		if (!res.ok) {
			const errorData = await res.json();
			throw new Error(errorData.error || "Upload failed");
		}

		const data = await res.json();
		previewData = data;
		uploadedFilename = data.filename;

		// data preview
		document.getElementById("dataPreview").style.display = 'block';
		renderPreviewTable(data.columns, data.preview);

		// column selection
		document.getElementById("columnSelect").style.display = 'block';
		const colDiv = document.getElementById("columns");
		colDiv.innerHTML = '';

		data.columns.forEach(col => {
			const container = document.createElement('div');
			container.className = 'checkbox-container';

			const checkbox = document.createElement('input');
			checkbox.type = 'checkbox';
			checkbox.id = `col-${col}`;
			checkbox.value = col;
			checkbox.checked = true;

			const label = document.createElement('label');
			label.htmlFor = `col-${col}`;
			label.textContent = col;

			container.appendChild(checkbox);
			container.appendChild(label);
			colDiv.appendChild(container);
		});

		showStatus(`File uploaded successfully with ${data.total_rows} rows. Please select columns and configure ClickHouse connection.`, "success");
	} catch (error) {
		showStatus(`Error: ${error.message}`, "error");
	}
};

async function startIngestion() {
	const selectedColumns = [...document.querySelectorAll('#columns input:checked')].map(cb => cb.value);

	if (selectedColumns.length === 0) {
		showStatus("Please select at least one column to ingest", "error");
		return;
	}

	const host = document.getElementById('host').value;
	const port = document.getElementById('port').value;
	const database = document.getElementById('database').value;
	const user = document.getElementById('user').value;
	const jwt_token = document.getElementById('jwt_token').value;
	const target_table = document.getElementById('target_table').value;

	if (!host || !port || !database || !user || !jwt_token || !target_table) {
		showStatus("Please fill in all ClickHouse connection details", "error");
		return;
	}

	showStatus("Ingesting data to ClickHouse...", "loading");

	// progress bar section
	document.getElementById('progressSection').style.display = 'block';
	updateProgressBar(
		document.getElementById('progressBar'),
		0,
		document.getElementById('progressDetails'),
		`Starting ingestion...`
	);

	const body = {
		filename: uploadedFilename,
		columns: selectedColumns,
		host: host,
		port: port,
		database: database,
		user: user,
		jwt_token: jwt_token,
		target_table: target_table,
		delimiter: document.getElementById('delimiter').value
	};

	try {
		const res = await fetch('/ingest_csv', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(body)
		});

		if (!res.ok) {
			const errorData = await res.json();
			throw new Error(errorData.error || "Ingestion failed");
		}

		const result = await res.json();

		// Process any progress updates that were sent
		if (result.progress && result.progress.length > 0) {
			const lastProgress = result.progress[result.progress.length - 1];
			updateProgressBar(
				document.getElementById('progressBar'),
				lastProgress.percentage,
				document.getElementById('progressDetails'),
				`Processed ${lastProgress.processed} of ${lastProgress.total} records`
			);
		}

		// Final update to 100%
		updateProgressBar(
			document.getElementById('progressBar'),
			100,
			document.getElementById('progressDetails'),
			`Completed: ${result.records} records processed`
		);
		showStatus(`Successfully ingested ${result.records} records to ClickHouse`, "success");
	} catch (error) {
		updateProgressBar(
			document.getElementById('progressBar'),
			0,
			document.getElementById('progressDetails'),
			`Error: ${error.message}`
		);
		showStatus(`Error: ${error.message}`, "error");
	}
}

async function fetchTables() {
	const connection = getClickhouseConnection('src_');

	if (!validateConnection(connection)) {
		return;
	}

	showStatus("Connecting to ClickHouse and fetching tables...", "loading");

	try {
		const res = await fetch('/fetch_tables', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(connection)
		});

		if (!res.ok) {
			const errorData = await res.json();
			throw new Error(errorData.error || "Failed to fetch tables");
		}

		const data = await res.json();
		const tableSelect = document.getElementById('src_table');
		tableSelect.innerHTML = '';

		data.tables.forEach(table => {
			const option = document.createElement('option');
			option.value = table;
			option.text = table;
			tableSelect.appendChild(option);
		});

		document.getElementById('tablesDropdown').style.display = 'block';
		showStatus(`Successfully fetched ${data.tables.length} tables`, "success");
	} catch (error) {
		showStatus(`Error: ${error.message}`, "error");
	}
}

async function loadTableColumns() {
	const selectedTable = document.getElementById('src_table').value;
	if (!selectedTable) {
		showStatus("Please select a table", "error");
		return;
	}

	const connection = getClickhouseConnection('src_');
	if (!validateConnection(connection)) {
		return;
	}

	showStatus(`Fetching columns for table ${selectedTable}...`, "loading");

	try {
		const res = await fetch('/fetch_columns', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				...connection,
				table: selectedTable
			})
		});

		if (!res.ok) {
			const errorData = await res.json();
			throw new Error(errorData.error || "Failed to fetch columns");
		}

		const data = await res.json();
		const colDiv = document.getElementById('src_columns');
		colDiv.innerHTML = '';

		data.columns.forEach(col => {
			const container = document.createElement('div');
			container.className = 'checkbox-container';

			const checkbox = document.createElement('input');
			checkbox.type = 'checkbox';
			checkbox.id = `src-col-${col}`;
			checkbox.value = col;
			checkbox.checked = true;

			const label = document.createElement('label');
			label.htmlFor = `src-col-${col}`;
			label.textContent = col;

			container.appendChild(checkbox);
			container.appendChild(label);
			colDiv.appendChild(container);
		});

		document.getElementById('tableColumnsSection').style.display = 'block';
		showStatus(`Found ${data.columns.length} columns in table ${selectedTable}`, "success");

		// Cache the columns for this table
		tableColumnsCache[selectedTable] = data.columns;
	} catch (error) {
		showStatus(`Error: ${error.message}`, "error");
	}
}

async function exportToCSV() {
	const selectedTable = document.getElementById('src_table').value;
	if (!selectedTable) {
		showStatus("Please select a table", "error");
		return;
	}

	const selectedColumns = [...document.querySelectorAll('#src_columns input:checked')].map(cb => cb.value);
	if (selectedColumns.length === 0) {
		showStatus("Please select at least one column to export", "error");
		return;
	}

	const connection = getClickhouseConnection('src_');
	if (!validateConnection(connection)) {
		return;
	}

	showStatus(`Exporting data from ${selectedTable}...`, "loading");

	document.getElementById('exportProgressSection').style.display = 'block';
	updateProgressBar(
		document.getElementById('exportProgressBar'),
		50,
		document.getElementById('exportProgressDetails'),
		`Exporting data from ${selectedTable}...`
	);

	try {
		const res = await fetch('/export_to_csv', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				...connection,
				table: selectedTable,
				columns: selectedColumns
			})
		});

		if (!res.ok) {
			const errorData = await res.json();
			throw new Error(errorData.error || "Export failed");
		}

		// Update progress to 100%
		updateProgressBar(
			document.getElementById('exportProgressBar'),
			100,
			document.getElementById('exportProgressDetails'),
			`Export completed successfully`
		);

		// Handle the file download
		const blob = await res.blob();
		const url = window.URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `${selectedTable}_export.csv`;
		document.body.appendChild(a);
		a.click();
		a.remove();

		showStatus(`Successfully exported data from ${selectedTable}`, "success");
	} catch (error) {
		updateProgressBar(
			document.getElementById('exportProgressBar'),
			0,
			document.getElementById('exportProgressDetails'),
			`Error: ${error.message}`
		);
		showStatus(`Error: ${error.message}`, "error");
	}
}


async function fetchJoinTables() {
	const connection = getClickhouseConnection('join_');

	if (!validateConnection(connection)) {
		return;
	}

	showStatus("Connecting to ClickHouse and fetching tables for join...", "loading");

	try {
		const res = await fetch('/fetch_tables', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(connection)
		});

		if (!res.ok) {
			const errorData = await res.json();
			throw new Error(errorData.error || "Failed to fetch tables");
		}

		const data = await res.json();
		const tableSelect = document.getElementById('join_available_tables');
		tableSelect.innerHTML = '';

		data.tables.forEach(table => {
			const option = document.createElement('option');
			option.value = table;
			option.text = table;
			tableSelect.appendChild(option);
		});

		document.getElementById('joinTablesSection').style.display = 'block';
		showStatus(`Successfully fetched ${data.tables.length} tables`, "success");
	} catch (error) {
		showStatus(`Error: ${error.message}`, "error");
	}
}

async function addTableToJoin() {
	const tableSelect = document.getElementById('join_available_tables');
	const selectedTable = tableSelect.value;

	if (!selectedTable) {
		showStatus("Please select a table to add", "error");
		return;
	}

	if (selectedJoinTables.includes(selectedTable)) {
		showStatus(`Table ${selectedTable} is already added to the join`, "error");
		return;
	}

	// Fetch columns for the selected table if not already cached
	if (!tableColumnsCache[selectedTable]) {
		const connection = getClickhouseConnection('join_');

		try {
			const res = await fetch('/fetch_columns', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					...connection,
					table: selectedTable
				})
			});

			if (!res.ok) {
				const errorData = await res.json();
				throw new Error(errorData.error || "Failed to fetch columns");
			}

			const data = await res.json();
			tableColumnsCache[selectedTable] = data.columns;
		} catch (error) {
			showStatus(`Error fetching columns: ${error.message}`, "error");
			return;
		}
	}

	selectedJoinTables.push(selectedTable);

	updateSelectedTablesUI();

	if (selectedJoinTables.length >= 2) {
		document.getElementById('joinConditionsSection').style.display = 'block';
		updateJoinConditionsUI();
		document.getElementById('configureJoinBtn').style.display = 'block';
	}

	showStatus(`Added table ${selectedTable} to join`, "success");
}

function updateSelectedTablesUI() {
	const container = document.getElementById('selectedTables');
	container.innerHTML = '';

	if (selectedJoinTables.length === 0) {
		container.innerHTML = '<p>No tables selected yet. Please select tables from the list below.</p>';
		return;
	}

	selectedJoinTables.forEach((table, index) => {
		const tableElement = document.createElement('div');
		tableElement.className = 'table-join-container';

		const tableHeader = document.createElement('h5');
		tableHeader.textContent = table;

		const removeButton = document.createElement('button');
		removeButton.textContent = 'Remove';
		removeButton.onclick = () => removeTableFromJoin(index);

		tableElement.appendChild(tableHeader);
		tableElement.appendChild(removeButton);
		container.appendChild(tableElement);
	});
}

function removeTableFromJoin(index) {
	const removedTable = selectedJoinTables[index];
	selectedJoinTables.splice(index, 1);

	updateSelectedTablesUI();

	if (selectedJoinTables.length < 2) {
		document.getElementById('joinConditionsSection').style.display = 'none';
		document.getElementById('configureJoinBtn').style.display = 'none';
		document.getElementById('joinColumnsSection').style.display = 'none';
	} else {
		updateJoinConditionsUI();
	}

	showStatus(`Removed table ${removedTable} from join`, "success");
}

function updateJoinConditionsUI() {
	const container = document.getElementById('joinConditionsContainer');
	container.innerHTML = '';

	for (let i = 1; i < selectedJoinTables.length; i++) {
		const conditionDiv = document.createElement('div');
		conditionDiv.className = 'join-condition';

		const leftTable = selectedJoinTables[0]; // First table is always the left table in our implementation
		const rightTable = selectedJoinTables[i];

		const conditionLabel = document.createElement('h5');
		conditionLabel.textContent = `Join ${leftTable} with ${rightTable}`;

		const conditionInput = document.createElement('input');
		conditionInput.type = 'text';
		conditionInput.id = `join-condition-${i}`;
		conditionInput.placeholder = `${leftTable}.id = ${rightTable}.id`;
		conditionInput.style.width = '100%';

		conditionDiv.appendChild(conditionLabel);
		conditionDiv.appendChild(conditionInput);
		container.appendChild(conditionDiv);
	}
}

function configureJoinColumns() {

	const joinConditions = [];
	for (let i = 1; i < selectedJoinTables.length; i++) {
		const conditionInput = document.getElementById(`join-condition-${i}`);
		if (!conditionInput.value.trim()) {
			showStatus(`Please specify join condition between ${selectedJoinTables[0]} and ${selectedJoinTables[i]}`, "error");
			return;
		}
		joinConditions.push(conditionInput.value.trim());
	}

	const joinColumnsDiv = document.getElementById('joinColumns');
	joinColumnsDiv.innerHTML = '';

	selectedJoinTables.forEach(table => {
		const columns = tableColumnsCache[table];
		if (columns) {
			const tableHeader = document.createElement('h5');
			tableHeader.textContent = table;
			joinColumnsDiv.appendChild(tableHeader);

			columns.forEach(col => {
				const container = document.createElement('div');
				container.className = 'checkbox-container';

				const checkbox = document.createElement('input');
				checkbox.type = 'checkbox';
				checkbox.id = `join-col-${table}-${col}`;
				checkbox.value = `${table}.${col}`;
				checkbox.checked = true;

				const label = document.createElement('label');
				label.htmlFor = `join-col-${table}-${col}`;
				label.textContent = `${table}.${col}`;

				container.appendChild(checkbox);
				container.appendChild(label);
				joinColumnsDiv.appendChild(container);
			});
		}
	});

	document.getElementById('joinColumnsSection').style.display = 'block';
}

async function executeJoin() {
	// Validate tables and columns selection
	if (selectedJoinTables.length < 2) {
		showStatus("Please select at least two tables for join", "error");
		return;
	}

	// Get all join conditions
	const joinConditions = [];
	for (let i = 1; i < selectedJoinTables.length; i++) {
		const conditionInput = document.getElementById(`join-condition-${i}`);
		joinConditions.push(conditionInput.value.trim());
	}

	// Get selected columns
	const selectedColumns = [...document.querySelectorAll('#joinColumns input:checked')].map(cb => cb.value);
	if (selectedColumns.length === 0) {
		showStatus("Please select at least one column to export", "error");
		return;
	}

	const connection = getClickhouseConnection('join_');
	if (!validateConnection(connection)) {
		return;
	}

	const exportFilename = document.getElementById('join_export_filename').value;

	showStatus("Executing join query...", "loading");

	// progress section
	document.getElementById('joinProgressSection').style.display = 'block';
	updateProgressBar(
		document.getElementById('joinProgressBar'),
		25,
		document.getElementById('joinProgressDetails'),
		`Executing join query...`
	);

	try {
		const res = await fetch('/execute_join', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				...connection,
				tables: selectedJoinTables,
				columns: selectedColumns,
				joinConditions: joinConditions,
				exportFilename: exportFilename
			})
		});

		if (!res.ok) {
			const errorData = await res.json();
			throw new Error(errorData.error || "Join execution failed");
		}

		const result = await res.json();

		// Update progress to 100%
		updateProgressBar(
			document.getElementById('joinProgressBar'),
			100,
			document.getElementById('joinProgressDetails'),
			`Join completed with ${result.records} records`
		);

		// Trigger download
		window.location.href = `/download_file/${result.filename}`;

		showStatus(`Join executed successfully with ${result.records} records`, "success");
	} catch (error) {
		updateProgressBar(
			document.getElementById('joinProgressBar'),
			0,
			document.getElementById('joinProgressDetails'),
			`Error: ${error.message}`
		);
		showStatus(`Error: ${error.message}`, "error");
	}
}