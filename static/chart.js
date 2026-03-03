// static/chart.js

// ────────────────────────────────────────
// 상태
// ────────────────────────────────────────
let chart        = null;
let candleSeries = null;
let volumeSeries = null;
let ws           = null;
let currentSymbol = null;


// ────────────────────────────────────────
// 차트 초기화
// ────────────────────────────────────────
function initChart() {
    const container = document.getElementById("chart");

    // 컨테이너 크기가 0이면 부모 기준으로 계산
    const width = container.clientWidth || container.parentElement.clientWidth || 800;
    chart = LightweightCharts.createChart(container, {
        width:  width,
        height: 450,
        layout: {
            background: { color: "#161b22" },
            textColor:  "#8b949e",
        },
        grid: {
            vertLines:  { color: "#21262d" },
            horzLines:  { color: "#21262d" },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
        },
        timeScale: {
            borderColor:      "#21262d",
            timeVisible:      true,
            secondsVisible:   false,
        },
        rightPriceScale: {
            borderColor: "#21262d",
        },
    });

    // 캔들 시리즈
    candleSeries = chart.addCandlestickSeries({
        upColor:        "#3fb950",
        downColor:      "#f85149",
        borderUpColor:  "#3fb950",
        borderDownColor:"#f85149",
        wickUpColor:    "#3fb950",
        wickDownColor:  "#f85149",
    });

    // 볼륨 시리즈
    volumeSeries = chart.addHistogramSeries({
        priceFormat:     { type: "volume" },
        priceScaleId:    "volume",
        scaleMargins:    { top: 0.85, bottom: 0 },
    });

    // 창 크기 변경 시 차트 리사이즈
    window.addEventListener("resize", () => {
        chart.applyOptions({ width: container.clientWidth });
    });
}


// ────────────────────────────────────────
// 데이터 로드 (REST API → 초기 차트)
// ────────────────────────────────────────
async function loadOHLCV(symbol) {
    const resp = await fetch(`/api/ohlcv/${symbol}?hours=3`);
    const data = await resp.json();

    const candles = data.map(d => ({
        time:  d.time,
        open:  d.open,
        high:  d.high,
        low:   d.low,
        close: d.close,
    }));

    const volumes = data.map(d => ({
        time:  d.time,
        value: d.volume,
        color: d.close >= d.open ? "#3fb95066" : "#f8514966",
    }));

    candleSeries.setData(candles);
    volumeSeries.setData(volumes);
    chart.timeScale().fitContent();

    // 현재가 업데이트
    if (data.length > 0) {
        const last = data[data.length - 1];
        const prev = data.length > 1 ? data[data.length - 2] : null;
        updatePrice(last.close, prev ? prev.close : null);
    }
}


async function loadAnomalies(symbol) {
    const resp = await fetch(`/api/anomalies/${symbol}?hours=3`);
    const data = await resp.json();
    renderAnomalies(data);
}


// ────────────────────────────────────────
// WebSocket (실시간 업데이트)
// ────────────────────────────────────────
function connectWS(symbol) {
    // 기존 연결 닫기
    if (ws) {
        ws.close();
        ws = null;
    }

    const wsUrl = `ws://${location.host}/ws/${symbol}`;
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log(`[WS] 연결: ${symbol}`);
    };

    ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);

        if (msg.type === "ohlcv") {
            // 차트에 최신 캔들 업데이트
            candleSeries.update({
                time:  msg.time,
                open:  msg.open,
                high:  msg.high,
                low:   msg.low,
                close: msg.close,
            });
            volumeSeries.update({
                time:  msg.time,
                value: msg.volume,
                color: msg.close >= msg.open ? "#3fb95066" : "#f8514966",
            });
            updatePrice(msg.close, null);
        }

        if (msg.type === "anomaly") {
            // 이상탐지 결과 맨 위에 추가
            prependAnomaly(msg);
        }
    };

    ws.onclose = () => {
        console.log(`[WS] 연결 종료: ${symbol}`);
        // 5초 후 재연결
        setTimeout(() => {
            if (currentSymbol === symbol) connectWS(symbol);
        }, 5000);
    };

    ws.onerror = (e) => {
        console.error("[WS] 오류:", e);
    };
}


// ────────────────────────────────────────
// UI 업데이트
// ────────────────────────────────────────
function updatePrice(price, prevPrice) {
    const priceEl  = document.getElementById("current-price");
    const changeEl = document.getElementById("price-change");

    priceEl.textContent = `$${price.toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 4,
    })}`;

    if (prevPrice !== null) {
        const pct = ((price - prevPrice) / prevPrice * 100).toFixed(2);
        const dir = price >= prevPrice ? "up" : "down";
        changeEl.textContent  = `${pct > 0 ? "+" : ""}${pct}%`;
        changeEl.className    = dir;
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
            <span class="anom-time">${a.time}</span>
            <span class="anom-reason">${a.reason}</span>
            <span class="anom-score">${a.anomaly_score}</span>
        </div>
    `).join("");
}


function prependAnomaly(a) {
    const list = document.getElementById("anomaly-list");

    // "탐지된 이상 없음" 제거
    const noData = list.querySelector(".no-data");
    if (noData) noData.remove();

    const card = document.createElement("div");
    card.className = `anomaly-card ${a.severity}`;
    card.innerHTML = `
        <span class="anom-time">${a.time}</span>
        <span class="anom-reason">${a.reason}</span>
        <span class="anom-score">${a.anomaly_score}</span>
    `;

    list.prepend(card);

    // 최대 20개만 유지
    const cards = list.querySelectorAll(".anomaly-card");
    if (cards.length > 20) cards[cards.length - 1].remove();
}


// ────────────────────────────────────────
// 심볼 전환
// ────────────────────────────────────────
function switchSymbol(symbol) {
    currentSymbol = symbol;

    // 탭 활성화
    document.querySelectorAll(".symbol-btn").forEach(btn => {
        btn.classList.toggle("active", btn.dataset.symbol === symbol);
    });

    // 데이터 로드
    loadOHLCV(symbol);
    loadAnomalies(symbol);
    connectWS(symbol);
}


// ────────────────────────────────────────
// 초기화
// ────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
    initChart();

    // 심볼 탭 클릭 이벤트
    document.querySelectorAll(".symbol-btn").forEach(btn => {
        btn.addEventListener("click", () => {
            switchSymbol(btn.dataset.symbol);
        });
    });

    // 첫 번째 심볼 자동 선택
    const firstBtn = document.querySelector(".symbol-btn");
    if (firstBtn) {
        switchSymbol(firstBtn.dataset.symbol);
    }
});