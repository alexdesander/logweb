(() => {
    const { fetchJson, pluralize, renderLogError, renderLogTable } =
        window.Inspector;
    const controlsForm = document.querySelector("#log-controls");
    const levelFilter = document.querySelector("#level-filter");
    const producerFilter = document.querySelector("#producer-filter");
    const contentFilter = document.querySelector("#content-filter");
    const utcLowestFilter = document.querySelector("#utc-lowest-filter");
    const utcHighestFilter = document.querySelector("#utc-highest-filter");
    const limitFilter = document.querySelector("#limit-filter");
    const resetButton = document.querySelector("#reset-filters");
    const logElements = {
        summary: document.querySelector("#log-summary"),
        message: document.querySelector("#log-message"),
        tableWrap: document.querySelector("#log-table-wrap"),
        body: document.querySelector("#log-body"),
    };

    controlsForm.addEventListener("submit", (event) => {
        event.preventDefault();
        loadLogs();
    });
    levelFilter.addEventListener("change", loadLogs);
    producerFilter.addEventListener("change", loadLogs);
    utcLowestFilter.addEventListener("change", loadLogs);
    utcHighestFilter.addEventListener("change", loadLogs);
    resetButton.addEventListener("click", resetFilters);

    init();

    async function init() {
        await loadFilters();
        await loadLogs();
    }

    async function loadFilters() {
        try {
            const payload = await fetchJson(
                "/api/log-filters",
                undefined,
                "Could not load filters",
            );
            fillOptions(levelFilter, payload.levels || [], "text");
            fillOptions(producerFilter, payload.producers || [], "name");
        } catch (error) {
            renderLogError(logElements, error);
        }
    }

    async function loadLogs() {
        setLoading(true);

        try {
            const payload = await fetchJson(
                `/api/logs?${buildLogParams()}`,
                undefined,
                "Could not load logs",
            );
            renderLogTable({
                ...logElements,
                payload,
                summaryText: (count) => `${count} ${pluralize(count, "log")} shown`,
                emptyMessage: "No logs matched.",
            });
        } catch (error) {
            renderLogError(logElements, error);
        } finally {
            setLoading(false);
        }
    }

    function buildLogParams() {
        const params = new URLSearchParams();
        params.set("limit", limitFilter.value || "50");
        setParam(params, "level", levelFilter.value);
        setParam(params, "producer", producerFilter.value);
        setParam(params, "content", contentFilter.value.trim());
        setParam(
            params,
            "utc_lowest",
            utcDateTimeToUnixSeconds(utcLowestFilter.value),
        );
        setParam(
            params,
            "utc_highest",
            utcDateTimeToUnixSeconds(utcHighestFilter.value),
        );
        return params.toString();
    }

    function setParam(params, name, value) {
        if (value !== null && value !== "") {
            params.set(name, String(value));
        }
    }

    function utcDateTimeToUnixSeconds(value) {
        if (!value) {
            return null;
        }

        const timestamp = Date.parse(`${value}Z`);
        return Number.isNaN(timestamp) ? null : Math.floor(timestamp / 1000);
    }

    function fillOptions(select, values, labelKey) {
        const firstOption = select.querySelector("option");
        const fragment = document.createDocumentFragment();
        fragment.append(firstOption);

        for (const value of values) {
            const option = document.createElement("option");
            option.value = String(value.id);
            option.textContent = value[labelKey];
            fragment.append(option);
        }

        select.replaceChildren(fragment);
    }

    function resetFilters() {
        levelFilter.value = "";
        producerFilter.value = "";
        contentFilter.value = "";
        utcLowestFilter.value = "";
        utcHighestFilter.value = "";
        limitFilter.value = "50";
        loadLogs();
    }

    function setLoading(loading) {
        controlsForm.classList.toggle("is-loading", loading);
        logElements.summary.textContent = loading
            ? "Loading logs"
            : logElements.summary.textContent;
    }
})();
