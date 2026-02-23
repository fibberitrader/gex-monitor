/**
 * GEX Monitor - Cloudflare Worker v2 (修复版)
 */

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Content-Type': 'application/json',
};

function jsonRes(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: CORS_HEADERS });
}
function errRes(msg, status = 500) {
  return new Response(JSON.stringify({ error: msg }), { status, headers: CORS_HEADERS });
}

function getETTimeString() {
  return new Date().toLocaleString('zh-CN', {
    timeZone: 'America/New_York',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }) + ' ET';
}

function isMarketHours() {
  const now = new Date();
  const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const day = et.getDay();
  if (day === 0 || day === 6) return false;
  const mins = et.getHours() * 60 + et.getMinutes();
  return mins >= 8 * 60 + 30 && mins <= 16 * 60;
}

// ─── Schwab OAuth ─────────────────────────────────────────────────────────────
async function getAccessToken(env) {
  try {
    const cached = await env.GEX_KV.get('access_token');
    if (cached) {
      const { token, expiry } = JSON.parse(cached);
      if (Date.now() < expiry - 60000) return token;
    }
  } catch(e) {}

  const credentials = btoa(`${env.SCHWAB_APP_KEY}:${env.SCHWAB_SECRET}`);
  const res = await fetch('https://api.schwabapi.com/v1/oauth/token', {
    method: 'POST',
    headers: {
      'Authorization': `Basic ${credentials}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: env.SCHWAB_REFRESH_TOKEN,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Token refresh failed: ${res.status} - ${text}`);
  }

  const data = await res.json();
  const tokenData = { token: data.access_token, expiry: Date.now() + data.expires_in * 1000 };
  await env.GEX_KV.put('access_token', JSON.stringify(tokenData), { expirationTtl: data.expires_in - 60 });
  return data.access_token;
}

// ─── Schwab Quote ─────────────────────────────────────────────────────────────
async function fetchQuote(symbol, token) {
  const url = `https://api.schwabapi.com/marketdata/v1/quotes?symbols=${encodeURIComponent(symbol)}&fields=quote`;
  const res = await fetch(url, { headers: { 'Authorization': `Bearer ${token}` } });
  if (!res.ok) throw new Error(`Quote fetch failed: ${res.status}`);
  const data = await res.json();
  const q = data[symbol]?.quote;
  if (!q) throw new Error(`No quote for ${symbol}`);
  return { symbol, last: q.lastPrice || q.mark || q.closePrice || 0 };
}

// ─── Schwab Option Chain ──────────────────────────────────────────────────────
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
    throw new Error(`Chain fetch failed: ${res.status} - ${text}`);
  }
  const data = await res.json();
  if (data.status === 'FAILED') throw new Error(`No option chain for ${symbol}. 期货期权请用对应ETF（如/ES→SPY，/GC→GLD）`);
  return data;
}

// ─── IV 清洗 ──────────────────────────────────────────────────────────────────
// Schwab 返回的 volatility 是百分比形式（如 45.74），异常值过滤
function cleanIV(v) {
  if (v == null || isNaN(v) || v < 0 || v > 500) return null;
  return v;
}

// ─── GEX 核心计算（修复版）────────────────────────────────────────────────────
/**
 * 【重要修正】
 * Schwab API 的 gamma 字段：
 *   - Call 的 gamma 为正数
 *   - Put 的 gamma 为负数（已经是负的）
 * 
 * GEX = gamma × OI × 100 × spot² × 0.01
 * 
 * Call GEX > 0 → 做市商净多gamma（稳定器）
 * Put GEX < 0  → 做市商净空gamma（放大器）
 * 
 * 不需要手动给Put加负号，gamma本身符号已经正确！
 */
function calculateGEX(chainData, spotPrice) {
  // strike → { callGex, putGex, callOI, putOI, callVol, putVol, callIV, putIV }
  const byStrike = {};
  const expirationDates = [];

  const processMap = (optionMap, isCall) => {
    if (!optionMap) return;
    for (const [expDateStr, strikesObj] of Object.entries(optionMap)) {
      const expDate = expDateStr.split(':')[0];
      if (!expirationDates.includes(expDate)) expirationDates.push(expDate);

      for (const [strikeStr, contracts] of Object.entries(strikesObj)) {
        const strike = parseFloat(strikeStr);
        const contract = Array.isArray(contracts) ? contracts[0] : contracts;

        const gamma = contract.gamma ?? 0;       // Call: 正, Put: 负（Schwab已处理）
        const oi    = contract.openInterest ?? 0;
        const vol   = contract.totalVolume ?? 0;
        const iv    = cleanIV(contract.volatility); // 百分比，如45.74

        // GEX = gamma × OI × 100 × spot² × 0.01
        // 对于LEAPS等deep OTM gamma接近0，不影响
        const gex = gamma * oi * 100 * spotPrice * spotPrice * 0.01;

        if (!byStrike[strike]) {
          byStrike[strike] = { callGex: 0, putGex: 0, callOI: 0, putOI: 0, callVol: 0, putVol: 0, callIV: null, putIV: null };
        }

        if (isCall) {
          byStrike[strike].callGex += gex;   // Call gamma > 0 → GEX > 0
          byStrike[strike].callOI  += oi;
          byStrike[strike].callVol += vol;
          if (iv !== null) byStrike[strike].callIV = iv;
        } else {
          byStrike[strike].putGex  += gex;   // Put gamma < 0 → GEX < 0（自然）
          byStrike[strike].putOI   += oi;
          byStrike[strike].putVol  += vol;
          if (iv !== null) byStrike[strike].putIV = iv;
        }
      }
    }
  };

  processMap(chainData.callExpDateMap, true);
  processMap(chainData.putExpDateMap, false);

  const strikes = Object.keys(byStrike).map(Number).sort((a, b) => a - b);
  if (!strikes.length) return null;

  // 计算每个行权价的净GEX
  const strikeData = strikes.map(strike => {
    const d = byStrike[strike];
    const netGex = d.callGex + d.putGex;
    const totalOI = d.callOI + d.putOI;
    const totalVol = d.callVol + d.putVol;
    return {
      strike,
      callGex: d.callGex,
      putGex: d.putGex,
      netGex,
      callOI: d.callOI,
      putOI: d.putOI,
      totalOI,
      callVol: d.callVol,
      putVol: d.putVol,
      totalVol,
      volOIRatio: totalOI > 0 ? Math.min(totalVol / totalOI, 10) : 0,
      callIV: d.callIV,
      putIV: d.putIV,
    };
  });

  // 聚合GEX曲线（累加，用于找Flip点）
  let running = 0;
  const aggregateGex = strikeData.map(s => {
    running += s.netGex;
    return running;
  });

  // ── Gamma Flip 点（聚合GEX第一次由负转正的点，最接近Spot）────────────────
  // 找所有过零点，取最接近Spot的那个
  const crossings = [];
  for (let i = 1; i < strikes.length; i++) {
    const a = aggregateGex[i - 1];
    const b = aggregateGex[i];
    if ((a < 0 && b >= 0) || (a >= 0 && b < 0)) {
      // 线性插值
      const ratio = Math.abs(a) / (Math.abs(a) + Math.abs(b));
      const flipPrice = strikes[i - 1] + ratio * (strikes[i] - strikes[i - 1]);
      crossings.push(flipPrice);
    }
  }

  let gammaFlip = null;
  if (crossings.length > 0 && spotPrice) {
    // 取最接近Spot的过零点
    gammaFlip = crossings.reduce((prev, curr) =>
      Math.abs(curr - spotPrice) < Math.abs(prev - spotPrice) ? curr : prev
    );
  } else if (crossings.length > 0) {
    gammaFlip = crossings[0];
  }
  if (gammaFlip) gammaFlip = parseFloat(gammaFlip.toFixed(2));

  // ── Call Wall / Put Wall（限制在Spot ±40%范围内）────────────────────────
  const rangeMin = spotPrice * 0.60;
  const rangeMax = spotPrice * 1.40;
  const inRange = strikeData.filter(s => s.strike >= rangeMin && s.strike <= rangeMax);

  let callWall = null, maxCallGex = -Infinity;
  let putWall = null, maxPutGexAbs = -Infinity;

  for (const s of inRange) {
    if (s.callGex > maxCallGex) { maxCallGex = s.callGex; callWall = s.strike; }
    if (Math.abs(s.putGex) > maxPutGexAbs) { maxPutGexAbs = Math.abs(s.putGex); putWall = s.strike; }
  }

  // ── ATM IV（最近行权价的有效IV）────────────────────────────────────────────
  let atmIV = null;
  if (spotPrice && strikeData.length) {
    // 找最近5个行权价，取有效IV的平均
    const sorted = [...strikeData].sort((a, b) => Math.abs(a.strike - spotPrice) - Math.abs(b.strike - spotPrice));
    const candidates = sorted.slice(0, 5);
    const ivValues = [];
    for (const s of candidates) {
      if (s.callIV !== null) ivValues.push(s.callIV);
      if (s.putIV !== null) ivValues.push(s.putIV);
    }
    if (ivValues.length > 0) {
      atmIV = ivValues.reduce((a, b) => a + b, 0) / ivValues.length;
    }
  }

  // ── 获取Call Wall / Put Wall 的 IV ──────────────────────────────────────
  let callWallIV = null, putWallIV = null;
  if (callWall) {
    const cws = strikeData.find(s => s.strike === callWall);
    if (cws) callWallIV = cws.callIV ?? cws.putIV;
  }
  if (putWall) {
    const pws = strikeData.find(s => s.strike === putWall);
    if (pws) putWallIV = pws.putIV ?? pws.callIV;
  }

  return {
    strikes: strikeData,
    aggregateGex,
    gammaFlip,
    callWall,
    putWall,
    atmIV,
    callWallIV,
    putWallIV,
    expirationDates: expirationDates.sort(),
  };
}

// ─── IV 历史 ──────────────────────────────────────────────────────────────────
async function storeIVHistory(env, symbol, atmIV, callWallIV, putWallIV) {
  const today = new Date().toISOString().split('T')[0];
  const key = `iv2:${symbol}:${today}`;
  let history = [];
  try {
    const existing = await env.GEX_KV.get(key);
    if (existing) history = JSON.parse(existing);
  } catch(e) {}

  history.push({
    time: getETTimeString(),
    timestamp: Date.now(),
    atmIV: atmIV ?? null,
    callWallIV: callWallIV ?? null,
    putWallIV: putWallIV ?? null,
  });

  const trimmed = history.slice(-96);
  await env.GEX_KV.put(key, JSON.stringify(trimmed), { expirationTtl: 86400 * 2 });
  return trimmed;
}

async function getIVHistory(env, symbol) {
  const today = new Date().toISOString().split('T')[0];
  const key = `iv2:${symbol}:${today}`;
  try {
    const data = await env.GEX_KV.get(key);
    return data ? JSON.parse(data) : [];
  } catch(e) { return []; }
}

// ─── 到期日列表 ───────────────────────────────────────────────────────────────
async function fetchExpirationDates(symbol, token) {
  const now = new Date();
  const fromDate = now.toISOString().split('T')[0];
  const nextMonth = new Date(now.getFullYear(), now.getMonth() + 2, 0);
  const toDate = nextMonth.toISOString().split('T')[0];

  const chainData = await fetchOptionChain(symbol, token, fromDate, toDate);
  const dates = new Set();
  const addDates = (map) => {
    if (!map) return;
    for (const key of Object.keys(map)) dates.add(key.split(':')[0]);
  };
  addDates(chainData.callExpDateMap);
  addDates(chainData.putExpDateMap);
  return [...dates].sort();
}

// ─── 路由 ─────────────────────────────────────────────────────────────────────
export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS_HEADERS });

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // ── /api/health ──────────────────────────────────────────────────────
      if (path === '/api/health') {
        return jsonRes({ status: 'ok', time: getETTimeString(), isMarketHours: isMarketHours() });
      }

      // ── /api/expirations?symbol=NVDA ─────────────────────────────────────
      if (path === '/api/expirations') {
        const symbol = url.searchParams.get('symbol')?.toUpperCase();
        if (!symbol) return errRes('symbol required', 400);
        if (symbol.startsWith('/')) return errRes('暂不支持期货期权。建议使用对应ETF：/ES→SPY，/GC→GLD，/NQ→QQQ', 400);

        const token = await getAccessToken(env);
        const dates = await fetchExpirationDates(symbol, token);
        return jsonRes({ symbol, dates, updatedAt: getETTimeString() });
      }

      // ── /api/gex?symbol=NVDA&dates=2026-02-21,2026-02-28 ─────────────────
      if (path === '/api/gex') {
        const symbol = url.searchParams.get('symbol')?.toUpperCase();
        if (!symbol) return errRes('symbol required', 400);
        if (symbol.startsWith('/')) return errRes('暂不支持期货期权。建议：/ES→SPY，/GC→GLD，/NQ→QQQ', 400);

        const token = await getAccessToken(env);
        const quoteData = await fetchQuote(symbol, token);
        const spotPrice = quoteData.last;
        if (!spotPrice) throw new Error('无法获取现货价格');

        let chainData;
        const datesParam = url.searchParams.get('dates');
        if (datesParam) {
          const dates = datesParam.split(',').sort();
          chainData = await fetchOptionChain(symbol, token, dates[0], dates[dates.length - 1]);
        } else {
          const now = new Date();
          const fromDate = now.toISOString().split('T')[0];
          const nextMonth = new Date(now.getFullYear(), now.getMonth() + 2, 0);
          const toDate = nextMonth.toISOString().split('T')[0];
          chainData = await fetchOptionChain(symbol, token, fromDate, toDate);
        }

        const gex = calculateGEX(chainData, spotPrice);
        if (!gex) throw new Error('GEX计算失败，期权数据可能为空');

        await storeIVHistory(env, symbol, gex.atmIV, gex.callWallIV, gex.putWallIV);

        return jsonRes({
          symbol,
          spotPrice,
          gammaFlip: gex.gammaFlip,
          callWall: gex.callWall,
          putWall: gex.putWall,
          atmIV: gex.atmIV,
          callWallIV: gex.callWallIV,
          putWallIV: gex.putWallIV,
          strikes: gex.strikes,
          aggregateGex: gex.aggregateGex,
          expirationDates: gex.expirationDates,
          updatedAt: getETTimeString(),
          isMarketHours: isMarketHours(),
        });
      }

      // ── /api/doomsday?symbol=SPY ──────────────────────────────────────────
      if (path === '/api/doomsday') {
        const symbol = url.searchParams.get('symbol')?.toUpperCase();
        if (!symbol) return errRes('symbol required', 400);
        if (symbol.startsWith('/')) return errRes('暂不支持期货期权', 400);

        const token = await getAccessToken(env);
        const quoteData = await fetchQuote(symbol, token);
        const spotPrice = quoteData.last;

        const allDates = await fetchExpirationDates(symbol, token);
        if (!allDates.length) return errRes('No expiration dates', 404);

        const nearestDate = allDates[0];
        const chainData = await fetchOptionChain(symbol, token, nearestDate, nearestDate);
        const gex = calculateGEX(chainData, spotPrice);
        if (!gex) throw new Error('GEX计算失败');

        // Max/Min GEX（限制在Spot ±40%范围内）
        const inRange = gex.strikes.filter(s => s.strike >= spotPrice * 0.60 && s.strike <= spotPrice * 1.40);
        let maxGex = { strike: null, value: -Infinity };
        let minGex = { strike: null, value: Infinity };
        for (const s of inRange) {
          if (s.netGex > maxGex.value) maxGex = { strike: s.strike, value: s.netGex };
          if (s.netGex < minGex.value) minGex = { strike: s.strike, value: s.netGex };
        }

        await storeIVHistory(env, symbol + '_doom', gex.atmIV, gex.callWallIV, gex.putWallIV);

        return jsonRes({
          symbol,
          spotPrice,
          expirationDate: nearestDate,
          gammaFlip: gex.gammaFlip,
          callWall: gex.callWall,
          putWall: gex.putWall,
          atmIV: gex.atmIV,
          callWallIV: gex.callWallIV,
          putWallIV: gex.putWallIV,
          strikes: gex.strikes,
          aggregateGex: gex.aggregateGex,
          maxGex,
          minGex,
          updatedAt: getETTimeString(),
          isMarketHours: isMarketHours(),
        });
      }

      // ── /api/iv-history?symbol=SPY ────────────────────────────────────────
      if (path === '/api/iv-history') {
        const symbol = url.searchParams.get('symbol')?.toUpperCase();
        if (!symbol) return errRes('symbol required', 400);
        const suffix = url.searchParams.get('doom') === '1' ? '_doom' : '';
        const history = await getIVHistory(env, symbol + suffix);
        return jsonRes({ symbol, history });
      }

      return errRes('Not found', 404);

    } catch (e) {
      console.error('Worker error:', e.message);
      return errRes(e.message || 'Internal error');
    }
  },

  async scheduled(event, env, ctx) {
    if (!isMarketHours()) return;
    console.log('Scheduled tick:', getETTimeString());
  },
};
