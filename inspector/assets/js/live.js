(() => {
    const { fetchJson, pluralize, renderLogError, renderLogTable } =
        window.Inspector;
    const refreshInput = document.querySelector("#refresh-interval");
    const refreshButton = document.querySelector("#refresh-now");
    const feedStatus = document.querySelector("#feed-status");
    const logElements = {
        summary: document.querySelector("#log-summary"),
        message: document.querySelector("#log-message"),
        tableWrap: document.querySelector("#log-table-wrap"),
        body: document.querySelector("#log-body"),
    };
    let refreshTimer = 0;
    let isLoading = false;

    refreshInput.addEventListener("change", updateRefreshInterval);
    refreshButton.addEventListener("click", loadLogs);

    loadLogs();
    startRefreshTimer();

    function updateRefreshInterval() {
        refreshInput.value = String(refreshSeconds());
        startRefreshTimer();
        loadLogs();
    }

    function startRefreshTimer() {
        clearInterval(refreshTimer);
        refreshTimer = setInterval(loadLogs, refreshSeconds() * 1000);
        renderStatus("Ready");
    }

    async function loadLogs() {
        if (isLoading) {
            return;
        }

        isLoading = true;
        setLoading(true);

        try {
            const payload = await fetchJson(
                "/api/logs?limit=50",
                undefined,
                "Could not load logs",
            );
            renderLogTable({
                ...logElements,
                payload,
                summaryText: (count) =>
                    `${count} latest ${pluralize(count, "log")}`,
                emptyMessage: "No logs available.",
            });
            renderStatus(`Updated ${new Date().toLocaleTimeString()}`);
        } catch (error) {
            renderLogError(logElements, error);
            renderStatus("Error");
        } finally {
            isLoading = false;
            setLoading(false);
        }
    }

    function setLoading(loading) {
        refreshButton.disabled = loading;
        feedStatus.textContent = loading ? "Refreshing" : feedStatus.textContent;
    }

    function renderStatus(prefix) {
        const seconds = refreshSeconds();
        feedStatus.textContent = `${prefix} - every ${seconds} ${pluralize(
            seconds,
            "second",
        )}`;
    }

    function refreshSeconds() {
        const value = Number.parseInt(refreshInput.value, 10);
        return Math.min(60, Math.max(1, Number.isNaN(value) ? 1 : value));
    }
})();
