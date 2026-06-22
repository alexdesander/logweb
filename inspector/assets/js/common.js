(() => {
    async function fetchJson(url, options, fallbackMessage) {
        const response = await fetch(url, options);
        const payload = await response.json();

        if (!response.ok) {
            throw new Error(payload.error || fallbackMessage);
        }

        return payload;
    }

    function renderLogTable({
        payload,
        body,
        tableWrap,
        message,
        summary,
        summaryText,
        emptyMessage,
    }) {
        const rows = payload.rows || [];
        const rowCount = payload.row_count || rows.length;
        const fragment = document.createDocumentFragment();

        body.replaceChildren();
        summary.textContent = summaryText(rowCount);

        for (const log of rows) {
            const row = document.createElement("tr");
            row.append(textCell(log.id));
            row.append(textCell(formatUtcOccurrence(log.occurrence)));
            row.append(levelCell(log.level));
            row.append(textCell(log.producer));
            row.append(textCell(log.content, "log-content"));
            fragment.append(row);
        }

        body.append(fragment);
        tableWrap.hidden = rows.length === 0;
        message.textContent = rows.length === 0 ? emptyMessage : "";
        message.classList.remove("error");
        return { rows, rowCount };
    }

    function renderLogError({ body, tableWrap, message, summary }, error) {
        tableWrap.hidden = true;
        body.replaceChildren();
        summary.textContent = "Could not load logs";
        message.textContent = error.message;
        message.classList.add("error");
    }

    function levelCell(level) {
        const cell = document.createElement("td");
        const badge = document.createElement("span");
        badge.className = `level ${String(level).toLowerCase()}`;
        badge.textContent = level;
        cell.append(badge);
        return cell;
    }

    function textCell(value, className) {
        const cell = document.createElement("td");
        cell.textContent =
            value === null || value === undefined ? "" : String(value);

        if (className) {
            cell.classList.add(className);
        }

        return cell;
    }

    function formatUtcOccurrence(value) {
        if (value === null || value === undefined || value === "") {
            return "";
        }

        const seconds = Number(value);
        if (!Number.isFinite(seconds)) {
            return "";
        }

        const date = new Date(seconds * 1000);
        if (Number.isNaN(date.getTime())) {
            return "";
        }

        return date.toISOString().replace(".000Z", "Z");
    }

    function formatQueryValue(value) {
        if (value === null) {
            return "NULL";
        }

        if (typeof value === "object") {
            return JSON.stringify(value);
        }

        return String(value);
    }

    function pluralize(count, singular) {
        return count === 1 ? singular : `${singular}s`;
    }

    window.Inspector = {
        fetchJson,
        formatQueryValue,
        pluralize,
        renderLogError,
        renderLogTable,
        textCell,
    };
})();
