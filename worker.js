/**
 * GEX Monitor - Cloudflare Worker
 * 处理 Schwab API 认证、期权链获取、GEX计算
 */

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Content-Type': 'application/json',
};

// ─── 工具函数 ─────────────────────────────────────────────────────────────────

function jsonRes(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: CORS_HEADERS });
}

function errRes(msg, status = 500) {
  return new Response(JSON.stringify({ error: msg }), { status, headers: CORS_HEADERS });
}

/** 美东时间工具 */
function isMarketHours() {
  const now = new Date();
  const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const day = et.getDay(); // 0=Sun, 6=Sat
  if (day === 0 || day === 6) return false;
  const h = et.getHours(), m = et.getMinutes();
  const minutes = h * 60 + m;
  return minutes >= 8 * 60 + 30 && minutes <= 16 * 60; // 8:30 AM - 4:00 PM ET
}

function getETTimeString() {
  return new Date().toLocaleString('zh-CN', {
    timeZone: 'America/New_York',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }) + ' ET';
}

// ─── Schwab OAuth ─────────────────────────────────────────────────────────────

async function getAccessToken(env) {
  // 先查 KV 缓存
  const cached = await env.GEX_KV.get('access_token');
  if (cached) {
    const { token, expiry } = JSON.parse(cached);
    if (Date.now() < expiry - 60000) return token; // 提前1分钟刷新
  }

  const credentials = btoa(`${env.SCHWAB_APP_KEY}:${env.SCHWAB_SECRET}`);
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: env.SCHWAB_REFRESH_TOKEN,
  });

  const res = await fetch('https://api.schwabapi.com/v1/oauth/token', {
    method: 'POST',
    headers: {
      'Authorization': `Basic ${credentials}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Token refresh failed: ${res.status} ${text}`);
  }

  const data = await res.json();
  const tokenData = {
    token: data.access_token,
    expiry: Date.now() + data.expires_in * 1000,
  };
  await env.GEX_KV.put('access_token', JSON.stringify(tokenData), { expirationTtl: data.expires_in - 60 });
  return data.access_token;
}

// ─── Schwab API 调用 ──────────────────────────────────────────────────────────

async function fetchQuote(symbol, token) {
  const url = `https://api.schwabapi.com/marketdata/v1/quotes?symbols=${encodeURIComponent(symbol)}&fields=quote`;
  const res = await fetch(url, { headers: { 'Authorization': `Bearer ${token}` } });
  if (!res.ok) throw new Error(`Quote fetch failed: ${res.status}`);
  const data = await res.json();
  const q = data[symbol]?.quote;
  if (!q) throw new Error(`No quote data for ${symbol}`);
  return {
    symbol,
    last: q.lastPrice || q.mark || 0,
    bid: q.bidPrice || 0,
    ask: q.askPrice || 0,
  };
}

async function fetchOptionChain(symbol, token, fromDate, toDate) {
  const params = new URLSearchParams({
    symbol,
    contractType: 'ALL',
    includeUnderlyingQuote: 'true',
    strategy: 'SINGLE',
    optionType: 'ALL',
  });
  if (fromDate) params.set('fromDate', fromDate);
  if (toDate) params.set('toDate', toDate);

  const url = `https://api.schwabapi.com/marketdata/v1/chains?${params}`;
  const res = await fetch(url, { headers: { 'Authorization': `Bearer ${token}` } });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Option chain fetch failed: ${res.status} ${text}`);
  }
  return res.json();
}

// ─── GEX 计算核心 ─────────────────────────────────────────────────────────────

/**
 * GEX = Gamma × OI × 100 × SpotPrice² × 0.01
 * Call GEX 为正，Put GEX 为负
 */
function calculateGEX(chainData, spotPrice) {
  const gexByStrike = {}; // strike -> { callGex, putGex, callOI, putOI, callVol, putVol, callIV, putIV }
  const expirationDates = [];

  const processMap = (optionMap, isCall) => {
    for (const [expDateStr, strikesObj] of Object.entries(optionMap)) {
      // expDateStr 格式: "2026-02-21:7" 或 "2026-02-21"
      const expDate = expDateStr.split(':')[0];
      if (!expirationDates.includes(expDate)) expirationDates.push(expDate);

      for (const [strikeStr, contracts] of Object.entries(strikesObj)) {
        const strike = parseFloat(strikeStr);
        const contract = Array.isArray(contracts) ? contracts[0] : contracts;

        const gamma = contract.gamma || 0;
        const oi = contract.openInterest || 0;
        const vol = contract.totalVolume || 0;
        const iv = contract.volatility || 0; // 百分比形式

        // GEX 公式
        const gex = gamma * oi * 100 * spotPrice * spotPrice * 0.01;

        if (!gexByStrike[strike]) {
          gexByStrike[strike] = { callGex: 0, putGex: 0, callOI: 0, putOI: 0, callVol: 0, putVol: 0, callIV: 0, putIV: 0 };
        }

        if (isCall) {
          gexByStrike[strike].callGex += gex;
          gexByStrike[strike].callOI += oi;
          gexByStrike[strike].callVol += vol;
          gexByStrike[strike].callIV = iv; // 用最后一个（通常只有一个）
        } else {
          gexByStrike[strike].putGex -= gex; // Put GEX 为负
          gexByStrike[strike].putOI += oi;
          gexByStrike[strike].putVol += vol;
          gexByStrike[strike].putIV = iv;
        }
      }
    }
  };

  if (chainData.callExpDateMap) processMap(chainData.callExpDateMap, true);
  if (chainData.putExpDateMap) processMap(chainData.putExpDateMap, false);

  // 构建有序的行权价数组
  const strikes = Object.keys(gexByStrike).map(Number).sort((a, b) => a - b);

  // 计算每个行权价的净GEX和聚合GEX
  let cumulativeGex = 0;
  const result = strikes.map(strike => {
    const d = gexByStrike[strike];
    const netGex = d.callGex + d.putGex;
    cumulativeGex += netGex;
    return {
      strike,
      callGex: d.callGex,
      putGex: d.putGex,
      netGex,
      totalOI: d.callOI + d.putOI,
      callOI: d.callOI,
      putOI: d.putOI,
      callVol: d.callVol,
      putVol: d.putVol,
      volOIRatio: (d.callOI + d.putOI) > 0 ? (d.callVol + d.putVol) / (d.callOI + d.putOI) : 0,
      callIV: d.callIV,
      putIV: d.putIV,
    };
  });

  // 计算聚合GEX曲线（累加）
  let runningSum = 0;
  const aggregateGex = strikes.map(strike => {
    runningSum += gexByStrike[strike].callGex + gexByStrike[strike].putGex;
    return runningSum;
  });

  // 找Gamma Flip点（聚合GEX由负转正或由正转负的行权价）
  let gammaFlip = null;
  for (let i = 1; i < strikes.length; i++) {
    if ((aggregateGex[i - 1] < 0 && aggregateGex[i] >= 0) ||
        (aggregateGex[i - 1] >= 0 && aggregateGex[i] < 0)) {
      // 线性插值
      const ratio = Math.abs(aggregateGex[i - 1]) / (Math.abs(aggregateGex[i - 1]) + Math.abs(aggregateGex[i]));
      gammaFlip = strikes[i - 1] + ratio * (strikes[i] - strikes[i - 1]);
      break;
    }
  }
  // 如果没有交叉，用聚合GEX最接近0的点
  if (!gammaFlip) {
    let minAbs = Infinity;
    for (let i = 0; i < strikes.length; i++) {
      if (Math.abs(aggregateGex[i]) < minAbs) {
        minAbs = Math.abs(aggregateGex[i]);
        gammaFlip = strikes[i];
      }
    }
  }

  // Call Wall: 正GEX最高的行权价
  let callWall = null, maxCallGex = -Infinity;
  for (const s of result) {
    if (s.callGex > maxCallGex) { maxCallGex = s.callGex; callWall = s.strike; }
  }

  // Put Wall: 负GEX绝对值最大的行权价
  let putWall = null, maxPutGexAbs = -Infinity;
  for (const s of result) {
    if (Math.abs(s.putGex) > maxPutGexAbs) { maxPutGexAbs = Math.abs(s.putGex); putWall = s.strike; }
  }

  // ATM IV（最近行权价的平均IV）
  let atmIV = 0;
  if (spotPrice && result.length > 0) {
    const atm = result.reduce((prev, curr) =>
      Math.abs(curr.strike - spotPrice) < Math.abs(prev.strike - spotPrice) ? curr : prev
    );
    atmIV = (atm.callIV + atm.putIV) / 2;
  }

  return {
    strikes: result,
    aggregateGex,
    gammaFlip: gammaFlip ? parseFloat(gammaFlip.toFixed(2)) : null,
    callWall,
    putWall,
    atmIV,
    expirationDates: expirationDates.sort(),
  };
}

// ─── IV 历史存储 ──────────────────────────────────────────────────────────────

async function storeIVHistory(env, symbol, atmIV, callWallIV, putWallIV) {
  const today = new Date().toISOString().split('T')[0];
  const key = `iv_history:${symbol}:${today}`;
  const existing = await env.GEX_KV.get(key);
  const history = existing ? JSON.parse(existing) : [];

  history.push({
    time: getETTimeString(),
    timestamp: Date.now(),
    atmIV,
    callWallIV,
    putWallIV,
  });

  // 只保留当天数据，最多96条（每15分钟一条，7.5小时）
  const trimmed = history.slice(-96);
  await env.GEX_KV.put(key, JSON.stringify(trimmed), { expirationTtl: 86400 * 2 });
  return trimmed;
}

async function getIVHistory(env, symbol) {
  const today = new Date().toISOString().split('T')[0];
  const key = `iv_history:${symbol}:${today}`;
  const data = await env.GEX_KV.get(key);
  return data ? JSON.parse(data) : [];
}

// ─── 获取到期日列表 ───────────────────────────────────────────────────────────

async function fetchExpirationDates(symbol, token) {
  // 获取当月+次月范围
  const now = new Date();
  const fromDate = now.toISOString().split('T')[0];
  // 次月底
  const nextMonth = new Date(now.getFullYear(), now.getMonth() + 2, 0);
  const toDate = nextMonth.toISOString().split('T')[0];

  const chainData = await fetchOptionChain(symbol, token, fromDate, toDate);

  const dates = new Set();
  const addDates = (map) => {
    if (!map) return;
    for (const key of Object.keys(map)) {
      dates.add(key.split(':')[0]);
    }
  };
  addDates(chainData.callExpDateMap);
  addDates(chainData.putExpDateMap);

  return [...dates].sort();
}

// ─── 路由处理 ─────────────────────────────────────────────────────────────────

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // ── GET /api/expirations?symbol=NVDA ──────────────────────────────────
      if (path === '/api/expirations') {
        const symbol = url.searchParams.get('symbol')?.toUpperCase();
        if (!symbol) return errRes('symbol required', 400);

        const token = await getAccessToken(env);
        const dates = await fetchExpirationDates(symbol, token);
        return jsonRes({ symbol, dates, updatedAt: getETTimeString() });
      }

      // ── GET /api/gex?symbol=NVDA&dates=2026-02-21,2026-02-28 ─────────────
      if (path === '/api/gex') {
        const symbol = url.searchParams.get('symbol')?.toUpperCase();
        const datesParam = url.searchParams.get('dates'); // 逗号分隔的到期日
        if (!symbol) return errRes('symbol required', 400);

        const token = await getAccessToken(env);

        // 获取现货价格
        const quoteData = await fetchQuote(symbol, token);
        const spotPrice = quoteData.last;

        let chainData;
        if (datesParam) {
          // 指定了到期日，按日期范围获取
          const dates = datesParam.split(',').sort();
          const fromDate = dates[0];
          const toDate = dates[dates.length - 1];
          chainData = await fetchOptionChain(symbol, token, fromDate, toDate);
        } else {
          // 默认：当月+次月
          const now = new Date();
          const fromDate = now.toISOString().split('T')[0];
          const nextMonth = new Date(now.getFullYear(), now.getMonth() + 2, 0);
          const toDate = nextMonth.toISOString().split('T')[0];
          chainData = await fetchOptionChain(symbol, token, fromDate, toDate);
        }

        const gex = calculateGEX(chainData, spotPrice);

        // 存储IV历史
        let callWallIV = 0, putWallIV = 0;
        if (gex.callWall) {
          const cw = gex.strikes.find(s => s.strike === gex.callWall);
          if (cw) callWallIV = cw.callIV;
        }
        if (gex.putWall) {
          const pw = gex.strikes.find(s => s.strike === gex.putWall);
          if (pw) putWallIV = pw.putIV;
        }
        await storeIVHistory(env, symbol, gex.atmIV, callWallIV, putWallIV);

        return jsonRes({
          symbol,
          spotPrice,
          gammaFlip: gex.gammaFlip,
          callWall: gex.callWall,
          putWall: gex.putWall,
          atmIV: gex.atmIV,
          strikes: gex.strikes,
          aggregateGex: gex.aggregateGex,
          expirationDates: gex.expirationDates,
          updatedAt: getETTimeString(),
          isMarketHours: isMarketHours(),
        });
      }

      // ── GET /api/doomsday?symbol=NVDA  (末日观察) ─────────────────────────
      if (path === '/api/doomsday') {
        const symbol = url.searchParams.get('symbol')?.toUpperCase();
        if (!symbol) return errRes('symbol required', 400);

        const token = await getAccessToken(env);
        const quoteData = await fetchQuote(symbol, token);
        const spotPrice = quoteData.last;

        // 只取最近一个到期日
        const allDates = await fetchExpirationDates(symbol, token);
        if (!allDates.length) return errRes('No expiration dates found', 404);

        const nearestDate = allDates[0];
        const chainData = await fetchOptionChain(symbol, token, nearestDate, nearestDate);
        const gex = calculateGEX(chainData, spotPrice);

        // 0DTE max/min GEX
        let maxGex = -Infinity, minGex = Infinity;
        let maxGexStrike = null, minGexStrike = null;
        for (const s of gex.strikes) {
          if (s.netGex > maxGex) { maxGex = s.netGex; maxGexStrike = s.strike; }
          if (s.netGex < minGex) { minGex = s.netGex; minGexStrike = s.strike; }
        }

        return jsonRes({
          symbol,
          spotPrice,
          expirationDate: nearestDate,
          gammaFlip: gex.gammaFlip,
          callWall: gex.callWall,
          putWall: gex.putWall,
          atmIV: gex.atmIV,
          strikes: gex.strikes,
          aggregateGex: gex.aggregateGex,
          maxGex: { strike: maxGexStrike, value: maxGex },
          minGex: { strike: minGexStrike, value: minGex },
          updatedAt: getETTimeString(),
          isMarketHours: isMarketHours(),
        });
      }

      // ── GET /api/iv-history?symbol=NVDA ───────────────────────────────────
      if (path === '/api/iv-history') {
        const symbol = url.searchParams.get('symbol')?.toUpperCase();
        if (!symbol) return errRes('symbol required', 400);
        const history = await getIVHistory(env, symbol);
        return jsonRes({ symbol, history });
      }

      // ── GET /api/health ───────────────────────────────────────────────────
      if (path === '/api/health') {
        return jsonRes({ status: 'ok', time: getETTimeString(), isMarketHours: isMarketHours() });
      }

      return errRes('Not found', 404);

    } catch (e) {
      console.error(e);
      return errRes(e.message || 'Internal server error');
    }
  },

  // 定时触发器：每15分钟预热缓存（可选，需在 wrangler.toml 配置 crons）
  async scheduled(event, env, ctx) {
    if (!isMarketHours()) return;
    // 此处可以预先刷新常用标的，但我们让前端主动请求即可
    console.log('Scheduled tick at', getETTimeString());
  },
};
