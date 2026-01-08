// chart.js — robust candlestick setup

(function () {
    // 0) Sanity checks
    if (!window.LightweightCharts) {
      console.error('LightweightCharts not loaded. Make sure the <script> from unpkg is before chart.js');
      return;
    }
  
    const el = document.getElementById('chart');
    if (!el) {
      console.error('No element with id="chart" found in the page.');
      return;
    }
  
    // 1) Give the container a definite size so the chart can render
    //    (works even if your page has no CSS)
    el.style.width = '600px';
    el.style.height = '360px';
  
    // 2) Create chart (let it read size from the element)
    const chart = LightweightCharts.createChart(el, {
      layout: { textColor: 'black', background: { type: 'solid', color: 'white' } },
      // You can also set grid/priceScale/timeScale options here later
    });
  
    // 3) Add a candlestick series (generic API works on every version)
    const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
    });
  
    // 4) OHLC data — time must be **seconds** since epoch (not ms)
    const data = [
      { open: 10,   high: 10.63, low: 9.49,  close: 9.55,  time: 1642427876 },
      { open: 9.55, high: 10.30, low: 9.42,  close: 9.94,  time: 1642514276 },
      { open: 9.94, high: 10.17, low: 9.92,  close: 9.78,  time: 1642600676 },
      { open: 9.78, high: 10.59, low: 9.18,  close: 9.51,  time: 1642687076 },
      { open: 9.51, high: 10.46, low: 9.10,  close: 10.17, time: 1642773476 },
      { open: 10.17, high: 10.96, low: 10.16, close: 10.47, time: 1642859876 },
      { open: 10.47, high: 11.39, low: 10.40, close: 10.81, time: 1642946276 },
      { open: 10.81, high: 11.60, low: 10.30, close: 10.75, time: 1643032676 },
      { open: 10.75, high: 11.60, low: 10.49, close: 10.93, time: 1643119076 },
      { open: 10.93, high: 11.53, low: 10.76, close: 10.96, time: 1643205476 },
    ];
    candleSeries.setData(data);
  
    chart.timeScale().fitContent();
  
    // 5) Make it responsive
    const resize = () => {
      chart.applyOptions({ width: el.clientWidth, height: el.clientHeight });
    };
    new ResizeObserver(resize).observe(el);
    window.addEventListener('load', resize);
  
    // Helpful debug
    console.log('LightweightCharts version:', LightweightCharts.version);
  })();
  