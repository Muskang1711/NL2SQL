/**
 * NL2SQL — Frontend Application
 * Handles query submission, result rendering, and pipeline visualization.
 */

(function () {
    "use strict";

    // ─── DOM Elements (populated in init after DOM is ready) ───
    let el = {};

    // ─── State ───
    let isLoading = false;
    let selectedFile = null;

    function initElements() {
        el = {
            queryInput: document.getElementById("queryInput"),
            btnSubmit: document.getElementById("btnSubmit"),
            statusDot: document.getElementById("statusDot"),
            statusText: document.getElementById("statusText"),
            dbName: document.getElementById("dbName"),
            llmInfo: document.getElementById("llmInfo"),
            schemaSelect: document.getElementById("schemaSelect"),
            btnRefreshMeta: document.getElementById("btnRefreshMeta"),
            resultsSection: document.getElementById("resultsSection"),
            pipelineSteps: document.getElementById("pipelineSteps"),
            btnTogglePipeline: document.getElementById("btnTogglePipeline"),
            sqlCode: document.getElementById("sqlCode"),
            btnCopySQL: document.getElementById("btnCopySQL"),
            attemptBadge: document.getElementById("attemptBadge"),
            dataCard: document.getElementById("dataCard"),
            tableHead: document.getElementById("tableHead"),
            tableBody: document.getElementById("tableBody"),
            rowCount: document.getElementById("rowCount"),
            queryTime: document.getElementById("queryTime"),
            errorCard: document.getElementById("errorCard"),
            errorMessage: document.getElementById("errorMessage"),
            correctionCard: document.getElementById("correctionCard"),
            correctionTimeline: document.getElementById("correctionTimeline"),
            // Data Sources
            datasourceToggle: document.getElementById("datasourceToggle"),
            datasourceBody: document.getElementById("datasourceBody"),
            btnToggleDS: document.getElementById("btnToggleDS"),
            tableCount: document.getElementById("tableCount"),
            uploadDrop: document.getElementById("uploadDrop"),
            csvFileInput: document.getElementById("dataFile"),
            uploadForm: document.getElementById("uploadForm"),
            uploadFilename: document.getElementById("uploadFilename"),
            uploadClear: document.getElementById("uploadClear"),
            tableNameInput: document.getElementById("tableNameInput"),
            btnUpload: document.getElementById("btnUpload"),
            uploadProgress: document.getElementById("uploadProgress"),
            progressText: document.getElementById("progressText"),
            tablesList: document.getElementById("tablesList"),
        };
    }

    // ─── Initialize ───
    async function init() {
        initElements();
        await checkHealth();
        await loadSchemas();
        await loadTables();
        setupEventListeners();
    }

    // ─── Health Check ───
    async function checkHealth() {
        try {
            const res = await fetch("/api/health");
            const data = await res.json();

            if (data.status === "healthy") {
                el.statusDot.className = "status-dot connected";
                el.statusText.textContent = "Connected";
            } else {
                el.statusDot.className = "status-dot error";
                el.statusText.textContent = "Degraded";
            }

            if (data.database && data.database.database) {
                el.dbName.textContent = data.database.database;
            }

            if (data.llm) {
                el.llmInfo.textContent = `${data.llm.model}${data.llm.configured ? "" : " (not configured)"}`;
            }
        } catch (err) {
            el.statusDot.className = "status-dot error";
            el.statusText.textContent = "Disconnected";
            el.dbName.textContent = "Error";
            console.error("Health check failed:", err);
        }
    }

    // ─── Load Schemas ───
    async function loadSchemas() {
        try {
            const res = await fetch("/api/schemas");
            const data = await res.json();

            el.schemaSelect.innerHTML = "";
            (data.schemas || []).forEach((schema) => {
                const opt = document.createElement("option");
                opt.value = schema;
                opt.textContent = schema;
                if (schema === "shopify") opt.selected = true;
                el.schemaSelect.appendChild(opt);
            });
        } catch (err) {
            console.error("Failed to load schemas:", err);
        }
    }

    // ─── Event Listeners ───
    function setupEventListeners() {
        // Submit button
        el.btnSubmit.addEventListener("click", submitQuery);

        // Enter to submit, Shift+Enter for new line
        el.queryInput.addEventListener("keydown", (e) => {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                submitQuery();
            }
        });

        // Example chips
        document.querySelectorAll(".example-chip").forEach((chip) => {
            chip.addEventListener("click", () => {
                el.queryInput.value = chip.dataset.query;
                el.queryInput.focus();
            });
        });

        // Copy SQL
        el.btnCopySQL.addEventListener("click", copySQL);

        // Toggle pipeline
        el.btnTogglePipeline.addEventListener("click", togglePipeline);

        // Refresh metadata
        el.btnRefreshMeta.addEventListener("click", refreshMetadata);

        // Reload tables when schema changes
        el.schemaSelect.addEventListener("change", () => {
            loadTables();
        });

        // Data Sources toggle
        el.datasourceToggle.addEventListener("click", toggleDataSources);

        // CSV Upload - Click to browse
        el.csvFileInput.addEventListener("change", handleFileSelect);

        // CSV Upload - Drag and drop
        el.uploadDrop.addEventListener("dragover", (e) => {
            e.preventDefault();
            el.uploadDrop.classList.add("dragover");
        });
        el.uploadDrop.addEventListener("dragleave", () => {
            el.uploadDrop.classList.remove("dragover");
        });
        el.uploadDrop.addEventListener("drop", (e) => {
            e.preventDefault();
            el.uploadDrop.classList.remove("dragover");
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                const name = files[0].name.toLowerCase();
                if (name.endsWith('.csv') || name.endsWith('.xlsx') || name.endsWith('.xls')) {
                    showUploadForm(files[0]);
                } else {
                    alert("Please upload an Excel or CSV file.");
                }
            }
        });

        // Upload clear
        el.uploadClear.addEventListener("click", clearUpload);

        // Upload button
        el.btnUpload.addEventListener("click", uploadCSV);
    }

    // ─── Submit Query ───
    async function submitQuery() {
        const query = el.queryInput.value.trim();
        if (!query || isLoading) return;

        setLoading(true);
        resetResults();
        el.resultsSection.style.display = "block";

        // Show loading in pipeline
        addPipelineStep("Sending query", "Processing your question...", "running");

        try {
            const res = await fetch("/api/query", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    query: query,
                    schema: el.schemaSelect.value,
                }),
            });

            const data = await res.json();

            // Clear loading steps
            el.pipelineSteps.innerHTML = "";

            if (data.pipeline_log) {
                renderPipeline(data.pipeline_log);
            }

            if (data.status === "success") {
                renderSuccess(data);
            } else {
                renderError(data);
            }

            // Show correction log if multiple attempts
            if (data.attempts && data.attempts.length > 1) {
                renderCorrectionLog(data.attempts);
            }
        } catch (err) {
            el.pipelineSteps.innerHTML = "";
            addPipelineStep("Network Error", err.message, "error");
            showError("Failed to connect to the server. Please check if the application is running.");
            console.error("Query failed:", err);
        } finally {
            setLoading(false);
        }
    }

    // ─── Render Pipeline ───
    function renderPipeline(steps) {
        el.pipelineSteps.innerHTML = "";
        steps.forEach((step) => {
            const statusType = step.step.includes("error") || step.step.includes("failed")
                ? "error"
                : step.step.includes("correction") || step.step.includes("warning")
                    ? "warning"
                    : "success";
            addPipelineStep(
                formatStepName(step.step),
                step.detail,
                statusType,
                step.elapsed_seconds
            );
        });
    }

    function formatStepName(step) {
        return step
            .replace(/_/g, " ")
            .replace(/\b\w/g, (c) => c.toUpperCase());
    }

    function addPipelineStep(name, detail, status, time) {
        const stepEl = document.createElement("div");
        stepEl.className = "pipeline-step";

        const iconSvgs = {
            running: '<svg viewBox="0 0 12 12"><circle cx="6" cy="6" r="3" fill="currentColor" opacity="0.6"><animate attributeName="opacity" values="0.4;1;0.4" dur="1.2s" repeatCount="indefinite"/></circle></svg>',
            success: '<svg viewBox="0 0 12 12"><path d="M3 6l2 2 4-4" stroke="currentColor" stroke-width="1.5" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>',
            error: '<svg viewBox="0 0 12 12"><path d="M3 3l6 6M9 3l-6 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
            warning: '<svg viewBox="0 0 12 12"><path d="M6 3v3M6 8h.01" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
        };

        stepEl.innerHTML = `
            <div class="step-icon ${status}">${iconSvgs[status]}</div>
            <div class="step-content">
                <div class="step-name">${escapeHtml(name)}</div>
                <div class="step-detail">${escapeHtml(detail || "")}</div>
            </div>
            ${time !== undefined ? `<span class="step-time">${time}s</span>` : ""}
        `;

        el.pipelineSteps.appendChild(stepEl);
    }

    // ─── Render Success ───
    function renderSuccess(data) {
        // SQL
        el.sqlCode.innerHTML = highlightSQL(data.generated_sql || "");

        // Attempt badge
        if (data.total_attempts > 1) {
            el.attemptBadge.textContent = `Corrected (${data.total_attempts} attempts)`;
            el.attemptBadge.className = "attempt-badge corrected";
        } else {
            el.attemptBadge.textContent = "1st attempt";
            el.attemptBadge.className = "attempt-badge single";
        }

        // Data table
        if (data.columns && data.columns.length > 0 && data.data && data.data.length > 0) {
            renderTable(data.columns, data.data);
            el.rowCount.textContent = `${data.row_count} row${data.row_count !== 1 ? "s" : ""}${data.truncated ? " (truncated)" : ""}`;
            el.queryTime.textContent = `${data.total_time_seconds}s`;
            el.dataCard.style.display = "block";
        } else if (data.data && data.data.length === 0) {
            el.rowCount.textContent = "0 rows returned";
            el.queryTime.textContent = `${data.total_time_seconds}s`;
            el.dataCard.style.display = "block";
            el.tableHead.innerHTML = "";
            el.tableBody.innerHTML = '<tr><td style="text-align:center;padding:24px;color:var(--color-text-tertiary)">No results found</td></tr>';
        }
    }

    // ─── Render Table ───
    function renderTable(columns, data) {
        // Header
        el.tableHead.innerHTML = `<tr>${columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr>`;

        // Body
        el.tableBody.innerHTML = data
            .map(
                (row) =>
                    `<tr>${columns
                        .map((col) => {
                            const val = row[col];
                            if (val === null || val === undefined) {
                                return `<td class="null-value">NULL</td>`;
                            }
                            return `<td title="${escapeHtml(String(val))}">${escapeHtml(String(val))}</td>`;
                        })
                        .join("")}</tr>`
            )
            .join("");
    }

    // ─── Render Error ───
    function renderError(data) {
        // Still show SQL if generated
        if (data.generated_sql) {
            el.sqlCode.innerHTML = highlightSQL(data.generated_sql);
            el.attemptBadge.textContent = "Failed";
            el.attemptBadge.className = "attempt-badge failed";
        }

        showError(data.error || "An unknown error occurred.");
    }

    function showError(message) {
        el.errorMessage.textContent = message;
        el.errorCard.style.display = "block";
    }

    // ─── Render Correction Log ───
    function renderCorrectionLog(attempts) {
        el.correctionTimeline.innerHTML = "";

        attempts.forEach((attempt) => {
            const item = document.createElement("div");
            item.className = `correction-item ${attempt.status === "success" ? "success" : "error"}`;

            let html = `<div class="correction-attempt">Attempt ${attempt.attempt}</div>`;

            if (attempt.sql) {
                html += `<div class="correction-sql">${escapeHtml(attempt.sql)}</div>`;
            }

            if (attempt.error) {
                html += `<div class="correction-error-msg">Error: ${escapeHtml(attempt.error)}</div>`;
            }

            if (attempt.status === "success") {
                html += `<div style="color: var(--color-success); font-size: 0.786rem; margin-top: 4px; font-weight: 500;">✓ Query executed successfully</div>`;
            }

            item.innerHTML = html;
            el.correctionTimeline.appendChild(item);
        });

        el.correctionCard.style.display = "block";
    }

    // ─── SQL Syntax Highlighting ───
    function highlightSQL(sql) {
        if (!sql) return "";

        const keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
            "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "FULL", "CROSS",
            "ON", "AS", "WITH", "UNION", "ALL", "DISTINCT", "HAVING",
            "GROUP", "BY", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
            "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
            "CREATE", "TABLE", "ALTER", "DROP", "INDEX", "VIEW",
            "CASE", "WHEN", "THEN", "ELSE", "END", "BETWEEN", "LIKE",
            "EXISTS", "TRUE", "FALSE", "CAST", "COALESCE", "NULLIF",
            "OVER", "PARTITION", "ROW_NUMBER", "RANK", "DENSE_RANK",
            "CTE", "RECURSIVE", "LATERAL", "ILIKE", "SIMILAR", "TO",
        ];

        const functions = [
            "COUNT", "SUM", "AVG", "MIN", "MAX", "ROUND", "FLOOR", "CEIL",
            "UPPER", "LOWER", "LENGTH", "TRIM", "SUBSTRING", "CONCAT",
            "NOW", "CURRENT_DATE", "CURRENT_TIMESTAMP", "DATE_TRUNC",
            "EXTRACT", "TO_CHAR", "TO_DATE", "TO_NUMBER", "TO_TIMESTAMP",
            "COALESCE", "NULLIF", "GREATEST", "LEAST", "STRING_AGG",
            "ARRAY_AGG", "JSON_AGG", "JSONB_AGG",
        ];

        let escaped = escapeHtml(sql);

        // Highlight strings (single-quoted)
        escaped = escaped.replace(
            /'([^']*)'/g,
            '<span class="sql-string">\'$1\'</span>'
        );

        // Highlight numbers
        escaped = escaped.replace(
            /\b(\d+\.?\d*)\b/g,
            '<span class="sql-number">$1</span>'
        );

        // Highlight comments
        escaped = escaped.replace(
            /(--.*$)/gm,
            '<span class="sql-comment">$1</span>'
        );

        // Highlight keywords (case-insensitive, word boundary)
        keywords.forEach((kw) => {
            const regex = new RegExp(`\\b(${kw})\\b`, "gi");
            escaped = escaped.replace(regex, '<span class="sql-keyword">$1</span>');
        });

        // Highlight functions (followed by parenthesis)
        functions.forEach((fn) => {
            const regex = new RegExp(`\\b(${fn})\\s*\\(`, "gi");
            escaped = escaped.replace(regex, '<span class="sql-function">$1</span>(');
        });

        // Highlight operators
        escaped = escaped.replace(
            /(\*|=|&lt;|&gt;|&lt;=|&gt;=|&lt;&gt;|!=|\|\||::)/g,
            '<span class="sql-operator">$1</span>'
        );

        return escaped;
    }

    // ─── Utility Functions ───
    function escapeHtml(text) {
        const div = document.createElement("div");
        div.textContent = text;
        return div.innerHTML;
    }

    function setLoading(loading) {
        isLoading = loading;
        el.btnSubmit.disabled = loading;
        const btnText = el.btnSubmit.querySelector(".btn-text");
        const btnIcon = el.btnSubmit.querySelector(".btn-icon");
        const btnSpinner = el.btnSubmit.querySelector(".btn-spinner");

        if (loading) {
            btnText.textContent = "Generating...";
            btnIcon.style.display = "none";
            btnSpinner.style.display = "flex";
        } else {
            btnText.textContent = "Generate & Execute";
            btnIcon.style.display = "block";
            btnSpinner.style.display = "none";
        }
    }

    function resetResults() {
        el.pipelineSteps.innerHTML = "";
        el.sqlCode.textContent = "";
        el.attemptBadge.textContent = "";
        el.attemptBadge.className = "attempt-badge";
        el.tableHead.innerHTML = "";
        el.tableBody.innerHTML = "";
        el.rowCount.textContent = "";
        el.queryTime.textContent = "";
        el.dataCard.style.display = "none";
        el.errorCard.style.display = "none";
        el.errorMessage.textContent = "";
        el.correctionCard.style.display = "none";
        el.correctionTimeline.innerHTML = "";
    }

    function copySQL() {
        const sql = el.sqlCode.textContent;
        navigator.clipboard.writeText(sql).then(() => {
            const span = el.btnCopySQL.querySelector("span");
            span.textContent = "Copied!";
            setTimeout(() => {
                span.textContent = "Copy";
            }, 2000);
        });
    }

    function togglePipeline() {
        const steps = el.pipelineSteps;
        const btn = el.btnTogglePipeline;
        steps.classList.toggle("collapsed");
        btn.classList.toggle("collapsed");
    }

    async function refreshMetadata() {
        const btn = el.btnRefreshMeta;
        const originalHTML = btn.innerHTML;
        btn.innerHTML = '<div class="spinner" style="width:12px;height:12px;border-width:1.5px"></div><span>Refreshing...</span>';
        btn.disabled = true;

        try {
            await fetch("/api/refresh-metadata", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ schema: el.schemaSelect.value }),
            });
        } catch (err) {
            console.error("Metadata refresh failed:", err);
        } finally {
            btn.innerHTML = originalHTML;
            btn.disabled = false;
        }
    }

    // ─── Data Sources ───

    function toggleDataSources() {
        el.datasourceBody.classList.toggle("collapsed");
        el.btnToggleDS.classList.toggle("collapsed");
    }

    function handleFileSelect(e) {
        const file = e.target.files[0];
        if (file) {
            showUploadForm(file);
        }
    }

    function showUploadForm(file) {
        selectedFile = file;
        el.uploadFilename.textContent = `${file.name} (${formatFileSize(file.size)})`;
        el.uploadForm.style.display = "block";
        el.tableNameInput.value = "";
        el.uploadProgress.style.display = "none";
    }

    function clearUpload() {
        selectedFile = null;
        el.csvFileInput.value = "";
        el.uploadForm.style.display = "none";
        el.tableNameInput.value = "";
    }

    async function uploadCSV() {
        if (!selectedFile) return;

        el.btnUpload.disabled = true;
        el.uploadProgress.style.display = "flex";
        el.progressText.textContent = "Uploading...";

        const formData = new FormData();
        formData.append("file", selectedFile);

        const fileName = selectedFile.name.toLowerCase();
        const isExcel = fileName.endsWith('.xlsx') || fileName.endsWith('.xls');

        try {
            if (isExcel) {
                // Excel files: use /upload → /ingest/{session_id} pipeline
                el.progressText.textContent = "Uploading Excel file...";
                const uploadRes = await fetch("/upload", {
                    method: "POST",
                    body: formData,
                });

                if (!uploadRes.ok) {
                    const err = await uploadRes.json();
                    throw new Error(err.detail || err.error || "Upload failed");
                }

                const uploadData = await uploadRes.json();
                const sessionId = uploadData.session_id;
                const sheets = uploadData.sheets_found || [];

                el.progressText.textContent = `Importing ${sheets.length} sheets to database...`;

                // Step 2: Ingest into database
                const ingestRes = await fetch(`/ingest/${sessionId}`, {
                    method: "POST",
                });

                if (!ingestRes.ok) {
                    const err = await ingestRes.json();
                    throw new Error(err.detail || err.error || "Ingestion failed");
                }

                const ingestData = await ingestRes.json();
                const created = ingestData.tables_created || sheets;

                el.progressText.textContent = `✓ Imported ${created.length} tables: ${created.join(', ')}`;
                el.progressText.style.color = "var(--color-success)";

            } else {
                // CSV files: use /api/upload-csv
                el.progressText.textContent = "Importing CSV...";
                const tableName = el.tableNameInput.value.trim();
                if (tableName) {
                    formData.append("table_name", tableName);
                }
                formData.append("schema", el.schemaSelect.value);

                const res = await fetch("/api/upload-csv", {
                    method: "POST",
                    body: formData,
                });

                const data = await res.json();

                if (data.status === "success") {
                    el.progressText.textContent = `✓ ${data.message}`;
                    el.progressText.style.color = "var(--color-success)";
                } else {
                    throw new Error(data.error || data.message || "Import failed");
                }
            }

            clearUpload();
            await loadTables();
            await refreshMetadata();

        } catch (err) {
            el.progressText.textContent = `✗ ${err.message}`;
            el.progressText.style.color = "var(--color-error)";
            console.error("Upload failed:", err);
        } finally {
            el.btnUpload.disabled = false;
            setTimeout(() => {
                el.progressText.style.color = "";
            }, 3000);
        }
    }

    async function loadTables() {
        try {
            const schema = el.schemaSelect ? el.schemaSelect.value : "shopify";
            const res = await fetch(`/api/data-tables/${schema}`);
            const data = await res.json();
            const tables = data.tables || [];

            el.tableCount.textContent = `${tables.length} table${tables.length !== 1 ? 's' : ''}`;

            if (tables.length === 0) {
                el.tablesList.innerHTML = '';
                return;
            }

            el.tablesList.innerHTML = tables.map(t => `
                <div class="table-item">
                    <div class="table-item-info">
                        <svg class="table-item-icon" width="14" height="14" viewBox="0 0 14 14" fill="none"><rect x="1" y="1" width="12" height="12" rx="1.5" stroke="currentColor" stroke-width="1.2"/><path d="M1 4.5h12M1 8h12M5 4.5v7.5" stroke="currentColor" stroke-width="1.2"/></svg>
                        <span class="table-item-name">${escapeHtml(t.table_name)}</span>
                        <span class="table-item-meta">${t.column_count} cols · ${t.row_count >= 0 ? t.row_count + ' rows' : '—'}</span>
                    </div>
                    <div class="table-item-actions">
                        <button class="btn-delete-table" data-table="${escapeHtml(t.table_name)}" title="Delete table">Delete</button>
                    </div>
                </div>
            `).join('');

            // Attach delete handlers
            el.tablesList.querySelectorAll('.btn-delete-table').forEach(btn => {
                btn.addEventListener('click', async () => {
                    const name = btn.dataset.table;
                    if (!confirm(`Delete table "${name}"? This cannot be undone.`)) return;
                    btn.textContent = '...';
                    btn.disabled = true;
                    try {
                        await fetch(`/api/data-tables/${el.schemaSelect.value}/${name}`, { method: 'DELETE' });
                        await loadTables();
                    } catch (err) {
                        console.error('Delete failed:', err);
                    }
                });
            });
        } catch (err) {
            console.error('Failed to load tables:', err);
        }
    }

    function formatFileSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / 1048576).toFixed(1) + ' MB';
    }

    // ─── Start ───
    document.addEventListener("DOMContentLoaded", init);
})();
