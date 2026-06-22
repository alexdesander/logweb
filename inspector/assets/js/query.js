(() => {
    const { fetchJson, formatQueryValue, pluralize, textCell } =
        window.Inspector;
    const queryInput = document.querySelector("#sql-query");
    const runButton = document.querySelector("#run-query");
    const clearButton = document.querySelector("#clear-query");
    const replaceNamesButton = document.querySelector("#replace-names");
    const resultSummary = document.querySelector("#result-summary");
    const resultMessage = document.querySelector("#result-message");
    const tableWrap = document.querySelector("#result-table-wrap");
    const tableHead = document.querySelector("#result-head");
    const tableBody = document.querySelector("#result-body");
    let replaceNames = true;

    runButton.addEventListener("click", runQuery);
    clearButton.addEventListener("click", clearQuery);
    replaceNamesButton.addEventListener("click", toggleNameReplacement);
    setReplacementToggle(replaceNames);

    async function runQuery() {
        setLoading(true);
        clearResults();

        try {
            const payload = await fetchJson(
                "/api/query",
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({
                        sql: queryInput.value,
                        replace_names: replaceNames,
                    }),
                },
                "Query failed",
            );
            renderResults(payload);
        } catch (error) {
            renderError(error.message);
        } finally {
            setLoading(false);
        }
    }

    function clearQuery() {
        queryInput.value = "";
        clearResults();
        resultSummary.textContent = "No query run";
        resultMessage.textContent = "Run a query to see results.";
    }

    function toggleNameReplacement() {
        replaceNames = !replaceNames;
        setReplacementToggle(replaceNames);
    }

    function setReplacementToggle(enabled) {
        replaceNamesButton.setAttribute("aria-pressed", String(enabled));
        replaceNamesButton.textContent = enabled ? "Names: On" : "Names: Off";
    }

    function setLoading(loading) {
        runButton.disabled = loading;
        runButton.textContent = loading ? "Running" : "Run";
        resultSummary.textContent = loading
            ? "Running query"
            : resultSummary.textContent;
    }

    function renderResults(payload) {
        const columns = payload.columns || [];
        const rows = payload.rows || [];
        const rowCount = payload.row_count || rows.length;
        resultSummary.textContent = `${rowCount} ${pluralize(
            rowCount,
            "row",
        )} returned`;

        if (columns.length === 0) {
            resultMessage.textContent = "Query completed with no columns.";
            return;
        }

        tableHead.append(headerRow(columns));
        tableBody.append(bodyRows(rows));
        resultMessage.textContent = rows.length === 0 ? "No rows matched." : "";
        tableWrap.hidden = false;
    }

    function headerRow(columns) {
        const row = document.createElement("tr");

        for (const column of columns) {
            const cell = document.createElement("th");
            cell.textContent = column;
            row.append(cell);
        }

        return row;
    }

    function bodyRows(rows) {
        const fragment = document.createDocumentFragment();

        for (const row of rows) {
            const tableRow = document.createElement("tr");

            for (const value of row) {
                tableRow.append(textCell(formatQueryValue(value)));
            }

            fragment.append(tableRow);
        }

        return fragment;
    }

    function renderError(message) {
        resultSummary.textContent = "Query failed";
        resultMessage.textContent = message;
        resultMessage.classList.add("error");
    }

    function clearResults() {
        tableHead.replaceChildren();
        tableBody.replaceChildren();
        tableWrap.hidden = true;
        resultMessage.textContent = "";
        resultMessage.classList.remove("error");
    }
})();
