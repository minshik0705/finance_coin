// static/chart.js

// ────────────────────────────────────────
// 상태
// ────────────────────────────────────────
let chart        = null;
let candleSeries = null;
let volumeSeries = null;
let volZSeries   = null;
let vol1hSeries  = null;
let chartFeature = null;
let chartVol1h   = null;
let ws           = null;
let currentSymbol = null;
let timezone     = 'UTC'; // 'UTC' or 'KST'


// ────────────────────────────────────────
// 시간 포맷
// ────────────────────────────────────────
function formatTime(unixTimestamp) {
    const date = new Date(unixTimestamp * 1000);
    if (timezone === 'KST') {
        return new Date(date.getTime() + 9 * 60 * 60 * 1000)
            .toISOString().slice(11, 16);
    }
    return date.toISOString().slice(11, 16);
}

function toggleTimezone() {
    timezone = timezone === 'UTC' ? 'KST' : 'UTC';
    document.getElementById('tz-btn').textContent = timezone;

    // 현재 심볼 다시 로드해서 시간 갱신
    if (currentSymbol) {
        loadAnomalies(currentSymbol);
        loadGlobalAnomalies();
    }
}


// ────────────────────────────────────────
// 차트 초기화
// ────────────────────────────────────────
function initChart() {
    const container = document.getElementById("chart");
    const width = container.clientWidth || container.parentElement.clientWidth || 800;

    chart = LightweightCharts.createChart(container, {
        width:  width,
        height: 350,
        layout: { background: { color: "#161b22" }, textColor: "#8b949e" },
        grid:   { vertLines: { color: "#21262d" }, horzLines: { color: "#21262d" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        timeScale: { borderColor: "#21262d", timeVisible: true, secondsVisible: false },
        rightPriceScale: { borderColor: "#21262d" },
    });

    candleSeries = chart.addCandlestickSeries({
        upColor: "#3fb950", downColor: "#f85149",
        borderUpColor: "#3fb950", borderDownColor: "#f85149",
        wickUpColor: "#3fb950", wickDownColor: "#f85149",
    });

    volumeSeries = chart.addHistogramSeries({
        priceFormat:  { type: "volume" },
        priceScaleId: "volume",
        scaleMargins: { top: 0.85, bottom: 0 },
    });

    const volZContainer = document.getElementById("chart-volz");
    chartFeature = LightweightCharts.createChart(volZContainer, {
        width:  width,
        height: 120,
        layout: { background: { color: "#161b22" }, textColor: "#8b949e" },
        grid:   { vertLines: { color: "#21262d" }, horzLines: { color: "#21262d" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        timeScale: { borderColor: "#21262d", timeVisible: true, secondsVisible: false },
        rightPriceScale: { borderColor: "#21262d" },
    });

    volZSeries = chartFeature.addHistogramSeries({
        color: "#58a6ff",
        priceScaleId: "right",
    });

    const vol1hContainer = document.getElementById("chart-vol1h");
    chartVol1h = LightweightCharts.createChart(vol1hContainer, {
        width:  width,
        height: 120,
        layout: { background: { color: "#161b22" }, textColor: "#8b949e" },
        grid:   { vertLines: { color: "#21262d" }, horzLines: { color: "#21262d" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        timeScale: { borderColor: "#21262d", timeVisible: true, secondsVisible: false },
        rightPriceScale: { borderColor: "#21262d" },
    });

    vol1hSeries = chartVol1h.addLineSeries({
        color: "#f0b90b",
        lineWidth: 2,
    });

    chart.timeScale().subscribeVisibleTimeRangeChange(range => {
        if (range) {
            chartFeature.timeScale().setVisibleRange(range);
            chartVol1h.timeScale().setVisibleRange(range);
        }
    });

    window.addEventListener("resize", () => {
        const w = container.clientWidth;
        chart.applyOptions({ width: w });
        chartFeature.applyOptions({ width: w });
        chartVol1h.applyOptions({ width: w });
    });
}


// ────────────────────────────────────────
// 거래소 배지
// ────────────────────────────────────────
function exchangeBadge(exchange) {
    const colors = {
        binance: { bg: "#f0b90b22", text: "#f0b90b", label: "Binance" },
        okx:     { bg: "#00b4d822", text: "#00b4d8", label: "OKX"     },
        bybit:   { bg: "#f7931a22", text: "#f7931a", label: "Bybit"   },
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


// ────────────────────────────────────────
// 데이터 로드
// ────────────────────────────────────────
async function loadOHLCV(symbol) {
    const resp = await fetch(`/api/ohlcv/${symbol}?hours=3`);
    const data = await resp.json();

    const candles = data.map(d => ({
        time: d.time, open: d.open, high: d.high, low: d.low, close: d.close,
    }));
    const volumes = data.map(d => ({
        time: d.time, value: d.volume,
        color: d.close >= d.open ? "#3fb95066" : "#f8514966",
    }));

    candleSeries.setData(candles);
    volumeSeries.setData(volumes);
    chart.timeScale().fitContent();

    if (data.length > 0) {
        const last = data[data.length - 1];
        const prev = data.length > 1 ? data[data.length - 2] : null;
        updatePrice(last.close, prev ? prev.close : null);
    }
}


async function loadFeatures(symbol) {
    const resp = await fetch(`/api/features/${symbol}?hours=3`);
    const data = await resp.json();

    const volZData = data
        .filter(d => d.vol_z_1d !== null)
        .map(d => ({
            time:  d.time,
            value: d.vol_z_1d,
            color: d.vol_z_1d > 2  ? "#f85149" :
                   d.vol_z_1d > 1  ? "#f0b90b" :
                   d.vol_z_1d < -1 ? "#8b949e" : "#58a6ff",
        }));
    volZSeries.setData(volZData);

    const vol1hData = data
        .filter(d => d.vol_1h !== null)
        .map(d => ({ time: d.time, value: d.vol_1h }));
    vol1hSeries.setData(vol1hData);

    chartFeature.timeScale().fitContent();
    chartVol1h.timeScale().fitContent();
}


async function loadAnomalies(symbol) {
    const resp = await fetch(`/api/anomalies/${symbol}?hours=3`);
    const data = await resp.json();
    renderAnomalies(data);
}


async function loadGlobalAnomalies() {
    const resp = await fetch(`/api/anomalies/global?hours=3`);
    const data = await resp.json();
    renderGlobalAnomalies(data);
}


// ────────────────────────────────────────
// WebSocket
// ────────────────────────────────────────
function connectWS(symbol) {
    if (ws) { ws.close(); ws = null; }

    const wsUrl = `ws://${location.host}/ws/${symbol}`;
    ws = new WebSocket(wsUrl);

    ws.onopen  = () => console.log(`[WS] 연결: ${symbol}`);
    ws.onclose = () => {
        console.log(`[WS] 연결 종료: ${symbol}`);
        setTimeout(() => { if (currentSymbol === symbol) connectWS(symbol); }, 5000);
    };
    ws.onerror = (e) => console.error("[WS] 오류:", e);

    ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);

        if (msg.type === "ohlcv") {
            candleSeries.update({
                time: msg.time, open: msg.open, high: msg.high,
                low: msg.low, close: msg.close,
            });
            volumeSeries.update({
                time: msg.time, value: msg.volume,
                color: msg.close >= msg.open ? "#3fb95066" : "#f8514966",
            });
            updatePrice(msg.close, null);
        }

        if (msg.type === "anomaly") {
            prependAnomaly(msg);
        }
    };
}


// ────────────────────────────────────────
// UI 업데이트
// ────────────────────────────────────────
function updatePrice(price, prevPrice) {
    const priceEl  = document.getElementById("current-price");
    const changeEl = document.getElementById("price-change");

    priceEl.textContent = `$${price.toLocaleString("en-US", {
        minimumFractionDigits: 2, maximumFractionDigits: 4,
    })}`;

    if (prevPrice !== null) {
        const pct = ((price - prevPrice) / prevPrice * 100).toFixed(2);
        changeEl.textContent = `${pct > 0 ? "+" : ""}${pct}%`;
        changeEl.className   = price >= prevPrice ? "up" : "down";
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
    if (!list) return;
    if (!anomalies || anomalies.length === 0) {
        list.innerHTML = `<p class="no-data">탐지된 글로벌 이상 없음</p>`;
        return;
    }
    list.innerHTML = anomalies.map(a => `
        <div class="anomaly-card ${a.severity}">
            <span class="anom-time">${formatTime(a.ohlcv_time)}</span>
            ${scopeBadge(a.exchanges)}
            <span class="anom-symbol">${a.symbol.replace("USDT","")}</span>
            <span class="anom-reason">${a.exchanges.join(", ")}</span>
            <span class="anom-score">${a.avg_score}</span>
        </div>
    `).join("");
}


// ────────────────────────────────────────
// 심볼 전환
// ────────────────────────────────────────
function switchSymbol(symbol) {
    currentSymbol = symbol;

    document.querySelectorAll(".symbol-btn").forEach(btn => {
        btn.classList.toggle("active", btn.dataset.symbol === symbol);
    });

    loadOHLCV(symbol);
    loadFeatures(symbol);
    loadAnomalies(symbol);
    connectWS(symbol);
}


// ────────────────────────────────────────
// 초기화
// ────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
    initChart();

    document.getElementById("tz-btn").addEventListener("click", toggleTimezone);

    document.querySelectorAll(".symbol-btn").forEach(btn => {
        btn.addEventListener("click", () => switchSymbol(btn.dataset.symbol));
    });

    const firstBtn = document.querySelector(".symbol-btn");
    if (firstBtn) switchSymbol(firstBtn.dataset.symbol);

    loadGlobalAnomalies();
    setInterval(loadGlobalAnomalies, 60000);
});
