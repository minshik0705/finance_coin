let compareChart = null;
let detailChart = null;
let chartFeature = null;
let chartVol1h = null;

let compareSeriesMap = {};
let candleSeries = null;
let volumeSeries = null;
let volZSeries = null;
let vol1hSeries = null;

let ws = null;
let currentSymbol = null;
let selectedExchange = window.DEFAULT_EXCHANGE || "binance";
let visibleExchanges = new Set(["binance", "bybit", "okx"]);
let timezone = "UTC";

const EXCHANGES = ["binance", "bybit", "okx"];
const EXCHANGE_META = {
    binance: { label: "Binance", lineColor: "#f0b90b" },
    bybit:   { label: "Bybit",   lineColor: "#58a6ff" },
    okx:     { label: "OKX",     lineColor: "#3fb950" },
};

function formatTime(unixTimestamp) {
    if (!unixTimestamp) return "-";
    const date = new Date(unixTimestamp * 1000);
    if (timezone === "KST") {
        return new Date(date.getTime() + 9 * 60 * 60 * 1000).toISOString().slice(11, 16);
    }
    return date.toISOString().slice(11, 16);
}

function toggleTimezone() {
    timezone = timezone === "UTC" ? "KST" : "UTC";
    document.getElementById("tz-btn").textContent = timezone;
    if (currentSymbol) loadAll(currentSymbol);
    loadCoreOverview();
}

function initCharts() {
    const compareContainer = document.getElementById("chart-compare");
    const detailContainer = document.getElementById("chart");
    const volZContainer = document.getElementById("chart-volz");
    const vol1hContainer = document.getElementById("chart-vol1h");

    const width = Math.max(compareContainer.clientWidth || 0, 1000);

    compareChart = LightweightCharts.createChart(compareContainer, {
        width,
        height: 220,
        layout: { background: { color: "#161b22" }, textColor: "#8b949e" },
        grid: { vertLines: { color: "#21262d" }, horzLines: { color: "#21262d" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        timeScale: { borderColor: "#21262d", timeVisible: true, secondsVisible: false },
        rightPriceScale: { borderColor: "#21262d" },
    });

    detailChart = LightweightCharts.createChart(detailContainer, {
        width,
        height: 350,
        layout: { background: { color: "#161b22" }, textColor: "#8b949e" },
        grid: { vertLines: { color: "#21262d" }, horzLines: { color: "#21262d" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        timeScale: { borderColor: "#21262d", timeVisible: true, secondsVisible: false },
        rightPriceScale: { borderColor: "#21262d" },
    });

    chartFeature = LightweightCharts.createChart(volZContainer, {
        width,
        height: 120,
        layout: { background: { color: "#161b22" }, textColor: "#8b949e" },
        grid: { vertLines: { color: "#21262d" }, horzLines: { color: "#21262d" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        timeScale: { borderColor: "#21262d", timeVisible: true, secondsVisible: false },
        rightPriceScale: { borderColor: "#21262d" },
    });

    chartVol1h = LightweightCharts.createChart(vol1hContainer, {
        width,
        height: 120,
        layout: { background: { color: "#161b22" }, textColor: "#8b949e" },
        grid: { vertLines: { color: "#21262d" }, horzLines: { color: "#21262d" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        timeScale: { borderColor: "#21262d", timeVisible: true, secondsVisible: false },
        rightPriceScale: { borderColor: "#21262d" },
    });

    EXCHANGES.forEach(exchange => {
        compareSeriesMap[exchange] = compareChart.addLineSeries({
            color: EXCHANGE_META[exchange].lineColor,
            lineWidth: 2,
            visible: true,
            lastValueVisible: true,
            priceLineVisible: false,
        });
    });

    candleSeries = detailChart.addCandlestickSeries({
        upColor: "#3fb950",
        downColor: "#f85149",
        borderUpColor: "#3fb950",
        borderDownColor: "#f85149",
        wickUpColor: "#3fb950",
        wickDownColor: "#f85149",
    });

    volumeSeries = detailChart.addHistogramSeries({
        priceFormat: { type: "volume" },
        priceScaleId: "volume",
        scaleMargins: { top: 0.85, bottom: 0 },
    });

    volZSeries = chartFeature.addHistogramSeries({
        color: "#58a6ff",
        priceScaleId: "right",
    });

    vol1hSeries = chartVol1h.addLineSeries({
        color: "#f0b90b",
        lineWidth: 2,
    });

    detailChart.timeScale().subscribeVisibleTimeRangeChange(range => {
        if (range) {
            chartFeature.timeScale().setVisibleRange(range);
            chartVol1h.timeScale().setVisibleRange(range);
        }
    });

    window.addEventListener("resize", () => {
        const newWidth = Math.max(compareContainer.clientWidth || 0, 1000);
        compareChart.applyOptions({ width: newWidth });
        detailChart.applyOptions({ width: newWidth });
        chartFeature.applyOptions({ width: newWidth });
        chartVol1h.applyOptions({ width: newWidth });
    });
}

function exchangeBadge(exchange) {
    const colors = {
        binance: { bg: "#f0b90b22", text: "#f0b90b", label: "Binance" },
        okx:     { bg: "#00b4d822", text: "#00b4d8", label: "OKX" },
        bybit:   { bg: "#58a6ff22", text: "#58a6ff", label: "Bybit" },
    };
    const c = colors[exchange] || { bg: "#8b949e22", text: "#8b949e", label: exchange };
    return `<span class="exchange-badge" style="background:${c.bg};color:${c.text}">${c.label}</span>`;
}

function scopeBadge(exchanges) {
    const count = exchanges ? exchanges.length : 1;
    if (count >= 3) return `<span class="scope-badge scope-global">🌐 GLOBAL</span>`;
    if (count === 2) return `<span class="scope-badge scope-cross">⚡ CROSS</span>`;
    return `<span class="scope-badge scope-local">📍 LOCAL</span>`;
}

function overviewScopeBadge(scope) {
    if (scope === "global") return `<span class="scope-badge scope-global">GLOBAL</span>`;
    if (scope === "cross") return `<span class="scope-badge scope-cross">CROSS</span>`;
    if (scope === "local") return `<span class="scope-badge scope-local">LOCAL</span>`;
    return `<span class="scope-badge scope-local">NONE</span>`;
}

function updateSelectedExchangeBadge() {
    const el = document.getElementById("selected-exchange-badge");
    if (el) el.textContent = EXCHANGE_META[selectedExchange].label;
}

async function loadCompareChart(symbol) {
    const resp = await fetch(`/api/ohlcv_compare/${symbol}?hours=3&exchanges=binance,bybit,okx`);
    const data = await resp.json();

    EXCHANGES.forEach(exchange => {
        const rows = data[exchange] || [];
        const lineData = rows.map(d => ({ time: d.time, value: d.close }));
        compareSeriesMap[exchange].setData(lineData);
        compareSeriesMap[exchange].applyOptions({ visible: visibleExchanges.has(exchange) });
    });

    compareChart.timeScale().fitContent();
}

async function loadDetailChart(symbol) {
    const resp = await fetch(`/api/ohlcv/${symbol}?hours=3&exchange=${selectedExchange}`);
    const data = await resp.json();

    const candles = data.map(d => ({
        time: d.time,
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
    }));

    const volumes = data.map(d => ({
        time: d.time,
        value: d.volume,
        color: d.close >= d.open ? "#3fb95066" : "#f8514966",
    }));

    candleSeries.setData(candles);
    volumeSeries.setData(volumes);
    detailChart.timeScale().fitContent();

    if (data.length > 1) {
        const last = data[data.length - 1];
        const prev = data[data.length - 2];
        updatePrice(last.close, prev.close);
    }
}

async function loadFeatures(symbol) {
    const resp = await fetch(`/api/features/${symbol}?hours=3&exchange=${selectedExchange}`);
    const data = await resp.json();

    const volZData = data
        .filter(d => d.vol_z_1d !== null)
        .map(d => ({
            time: d.time,
            value: d.vol_z_1d,
            color: d.vol_z_1d > 2 ? "#f85149" :
                   d.vol_z_1d > 1 ? "#f0b90b" :
                   d.vol_z_1d < -1 ? "#8b949e" : "#58a6ff",
        }));

    const vol1hData = data
        .filter(d => d.vol_1h !== null)
        .map(d => ({ time: d.time, value: d.vol_1h }));

    volZSeries.setData(volZData);
    vol1hSeries.setData(vol1hData);

    chartFeature.timeScale().fitContent();
    chartVol1h.timeScale().fitContent();
}

async function loadAnomalies(symbol) {
    const resp = await fetch(`/api/anomalies/${symbol}?hours=3&exchange=${selectedExchange}`);
    const data = await resp.json();
    renderAnomalies(data);
}

async function loadGlobalAnomalies() {
    const resp = await fetch(`/api/anomalies/global?hours=3`);
    const data = await resp.json();
    renderGlobalAnomalies(data);
}

async function loadCoreOverview() {
    const grid = document.getElementById("core-overview-grid");
    if (!grid) return;

    const resp = await fetch(`/api/core_overview?hours=3`);
    const items = await resp.json();

    if (!items || items.length === 0) {
        grid.innerHTML = `<p class="no-data">표시할 core overview 데이터가 없습니다.</p>`;
        return;
    }

    grid.innerHTML = items.map(item => {
        const prices = (item.prices || []).map(p => `
            <div class="overview-price-row">
                <span class="overview-exchange">${EXCHANGE_META[p.exchange]?.label || p.exchange}</span>
                <span class="overview-price">$${Number(p.close).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 4 })}</span>
            </div>
        `).join("");

        const spread = item.spread_pct === null || item.spread_pct === undefined
            ? "-"
            : `${item.spread_pct.toFixed(3)}%`;

        const anomalyMeta = item.latest_anomaly_time
            ? `<div class="overview-anomaly-meta">${formatTime(item.latest_anomaly_time)} · ${(item.anomaly_exchanges || []).join(", ")}</div>`
            : `<div class="overview-anomaly-meta muted">최근 anomaly 없음</div>`;

        return `
            <button class="core-card" data-symbol="${item.symbol}">
                <div class="core-card-top">
                    <div class="core-symbol">${item.display_symbol}</div>
                    ${overviewScopeBadge(item.anomaly_scope)}
                </div>

                <div class="core-card-body">
                    ${prices}
                </div>

                <div class="core-card-footer">
                    <div class="overview-spread">Spread <span>${spread}</span></div>
                    ${anomalyMeta}
                </div>
            </button>
        `;
    }).join("");

    document.querySelectorAll(".core-card").forEach(card => {
        card.addEventListener("click", () => {
            const symbol = card.dataset.symbol;
            switchSymbol(symbol);
            window.scrollTo({ top: 0, behavior: "smooth" });
        });
    });
}

async function loadAll(symbol) {
    updateSelectedExchangeBadge();
    await Promise.all([
        loadCompareChart(symbol),
        loadDetailChart(symbol),
        loadFeatures(symbol),
        loadAnomalies(symbol),
        loadGlobalAnomalies(),
    ]);
}

function connectWS(symbol) {
    if (ws) {
        ws.close();
        ws = null;
    }

    ws = new WebSocket(`ws://${location.host}/ws/${symbol}`);

    ws.onopen = () => console.log(`[WS] connected: ${symbol}`);
    ws.onclose = () => {
        setTimeout(() => {
            if (currentSymbol === symbol) connectWS(symbol);
        }, 5000);
    };

    ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.type !== "snapshot") return;

        const priceRows = msg.prices || [];

        priceRows.forEach(row => {
            if (compareSeriesMap[row.exchange]) {
                compareSeriesMap[row.exchange].update({
                    time: row.time,
                    value: row.close,
                });
            }

            if (row.exchange === selectedExchange) {
                candleSeries.update({
                    time: row.time,
                    open: row.open,
                    high: row.high,
                    low: row.low,
                    close: row.close,
                });

                volumeSeries.update({
                    time: row.time,
                    value: row.volume,
                    color: row.close >= row.open ? "#3fb95066" : "#f8514966",
                });

                updatePrice(row.close, null);
            }
        });

        const matchingAnomaly = (msg.anomalies || []).find(a => a.exchange === selectedExchange);
        if (matchingAnomaly) prependAnomaly(matchingAnomaly);
    };
}

function updatePrice(price, prevPrice) {
    const priceEl = document.getElementById("current-price");
    const changeEl = document.getElementById("price-change");

    priceEl.textContent = `$${price.toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 4,
    })}`;

    if (prevPrice !== null && prevPrice > 0) {
        const pct = ((price - prevPrice) / prevPrice * 100).toFixed(2);
        changeEl.textContent = `${pct > 0 ? "+" : ""}${pct}%`;
        changeEl.className = price >= prevPrice ? "up" : "down";
    }
}

function renderAnomalies(anomalies) {
    const list = document.getElementById("anomaly-list");
    if (!anomalies || anomalies.length === 0) {
        list.innerHTML = `<p class="no-data">탐지된 이상 없음</p>`;
        return;
    }

    list.innerHTML = anomalies.map(a => `
        <div class="anomaly-card ${a.severity}">
            <span class="anom-time">${formatTime(a.ohlcv_time)}</span>
            ${exchangeBadge(a.exchange)}
            <span class="anom-reason">${a.reason}</span>
            <span class="anom-score">${a.anomaly_score}</span>
        </div>
    `).join("");
}

function prependAnomaly(a) {
    const list = document.getElementById("anomaly-list");
    const noData = list.querySelector(".no-data");
    if (noData) noData.remove();

    const card = document.createElement("div");
    card.className = `anomaly-card ${a.severity}`;
    card.innerHTML = `
        <span class="anom-time">${formatTime(a.ohlcv_time)}</span>
        ${exchangeBadge(a.exchange)}
        <span class="anom-reason">${a.reason}</span>
        <span class="anom-score">${a.anomaly_score}</span>
    `;
    list.prepend(card);

    const cards = list.querySelectorAll(".anomaly-card");
    if (cards.length > 20) cards[cards.length - 1].remove();
}

function renderGlobalAnomalies(anomalies) {
    const list = document.getElementById("global-anomaly-list");
    if (!anomalies || anomalies.length === 0) {
        list.innerHTML = `<p class="no-data">탐지된 글로벌 이상 없음</p>`;
        return;
    }

    list.innerHTML = anomalies.map(a => `
        <div class="anomaly-card ${a.severity}">
            <span class="anom-time">${formatTime(a.ohlcv_time)}</span>
            ${scopeBadge(a.exchanges)}
            <span class="anom-symbol">${a.symbol.replace("USDT", "")}</span>
            <span class="anom-reason">${a.exchanges.join(", ")}</span>
            <span class="anom-score">${a.avg_score}</span>
        </div>
    `).join("");
}

function switchSymbol(symbol) {
    currentSymbol = symbol;

    document.querySelectorAll(".symbol-btn").forEach(btn => {
        btn.classList.toggle("active", btn.dataset.symbol === symbol);
    });

    loadAll(symbol);
    connectWS(symbol);
}

function bindExchangeToggles() {
    document.querySelectorAll(".exchange-toggle").forEach(btn => {
        btn.addEventListener("click", () => {
            const exchange = btn.dataset.exchange;

            if (visibleExchanges.has(exchange)) {
                if (visibleExchanges.size === 1) return;
                visibleExchanges.delete(exchange);
                btn.classList.remove("active");
            } else {
                visibleExchanges.add(exchange);
                btn.classList.add("active");
            }

            compareSeriesMap[exchange].applyOptions({
                visible: visibleExchanges.has(exchange),
            });
        });
    });

    document.querySelectorAll(".exchange-focus").forEach(btn => {
        btn.addEventListener("click", () => {
            selectedExchange = btn.dataset.exchange;

            document.querySelectorAll(".exchange-focus").forEach(b => {
                b.classList.toggle("active", b.dataset.exchange === selectedExchange);
            });

            updateSelectedExchangeBadge();

            if (currentSymbol) {
                loadDetailChart(currentSymbol);
                loadFeatures(currentSymbol);
                loadAnomalies(currentSymbol);
            }
        });
    });
}

document.addEventListener("DOMContentLoaded", () => {
    initCharts();
    bindExchangeToggles();

    document.getElementById("tz-btn").addEventListener("click", toggleTimezone);

    document.querySelectorAll(".symbol-btn").forEach(btn => {
        btn.addEventListener("click", () => switchSymbol(btn.dataset.symbol));
    });

    document.getElementById("refresh-overview-btn")?.addEventListener("click", loadCoreOverview);

    updateSelectedExchangeBadge();
    loadCoreOverview();

    const firstBtn = document.querySelector(".symbol-btn");
    if (firstBtn) {
        switchSymbol(firstBtn.dataset.symbol);
    }

    setInterval(() => {
        if (currentSymbol) loadGlobalAnomalies();
        loadCoreOverview();
    }, 60000);
});
