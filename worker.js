/**
 * GEX Monitor - Cloudflare Worker v2
 *
 * 核心修正：
 * 1. 每行权价只有一根净GEX柱（CallGEX正 + PutGEX负 = 净GEX）
 * 2. 聚合GEX = 所有行权价净GEX从低到高累加
 * 3. Gamma Flip = 聚合GEX曲线唯一穿越零轴点（线性插值）
 * 4. Call Wall = 净GEX最大正值行权价
 * 5. Put Wall  = 净GEX绝对值最大负值行权价
 * 6. IV过滤：只取 0 < IV < 500 的有效值
 * 7. 支持期货期权（/ES /GC等）
 */

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Content-Type': 'application/json',
};

const ok  = (d)    => new Response(JSON.stringify(d), { headers: CORS });
const err = (m, s=500) => new Response(JSON.stringify({ error: m }), { status: s, headers: CORS });

function etNow() {
  return new Date().toLocaleString('zh-CN', {
    timeZone: 'America/New_York', hour12: false,
    year:'numeric',month:'2-digit',day:'2-digit',
    hour:'2-digit',minute:'2-digit',second:'2-digit'
  }) + ' ET';
}

function isMarketOpen() {
  const et = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const d = et.getDay(), m = et.getHours()*60 + et.getMinutes();
  return d >= 1 && d <= 5 && m >= 510 && m <= 960; // 8:30-16:00
}

const isFutures = s => s.startsWith('/');

// ── Token ─────────────────────────────────────────────────────────────────────
async function getToken(env) {
  try {
    const c = await env.GEX_KV.get('tok');
    if (c) { const p = JSON.parse(c); if (Date.now() < p.exp - 60000) return p.tok; }
  } catch(_){}

  const res = await fetch('https://api.schwabapi.com/v1/oauth/token', {
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + btoa(`${env.SCHWAB_APP_KEY}:${env.SCHWAB_SECRET}`),
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({ grant_type:'refresh_token', refresh_token: env.SCHWAB_REFRESH_TOKEN }),
  });
  if (!res.ok) throw new Error('Token: ' + res.status + ' ' + await res.text());
  const d = await res.json();
  await env.GEX_KV.put('tok', JSON.stringify({ tok: d.access_token, exp: Date.now() + d.expires_in*1000 }), { expirationTtl: d.expires_in - 60 });
  return d.access_token;
}

// ── Quote ─────────────────────────────────────────────────────────────────────
async function getQuote(symbol, token) {
  const r = await fetch(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=${encodeURIComponent(symbol)}&fields=quote`, {
    headers: { Authorization: 'Bearer ' + token }
  });
  if (!r.ok) throw new Error('Quote ' + r.status);
  const d = await r.json();
  const key = Object.keys(d)[0];
  const q = d[key]?.quote;
  if (!q) throw new Error('No quote for ' + symbol);
  return q.lastPrice || q.mark || q.closePrice || 0;
}

// ── Option Chain ──────────────────────────────────────────────────────────────
async function getChain(symbol, token, fromDate, toDate) {
  const p = new URLSearchParams({
    symbol, contractType:'ALL', includeUnderlyingQuote:'true',
    strategy:'SINGLE', optionType:'ALL',
  });
  if (fromDate) p.set('fromDate', fromDate);
  if (toDate)   p.set('toDate', toDate);

  const r = await fetch(`https://api.schwabapi.com/marketdata/v1/chains?${p}`, {
    headers: { Authorization: 'Bearer ' + token }
  });
  if (!r.ok) throw new Error('Chain ' + r.status + ': ' + await r.text());
  return r.json();
}

// ── GEX 计算核心 ──────────────────────────────────────────────────────────────
function calcGEX(chain, spot) {
  if (!spot || spot <= 0) throw new Error('Invalid spot price');

  const map = {}; // strike -> accumulator

  function validIV(v) { return typeof v === 'number' && v > 0 && v < 500; }

  function process(expMap, isCall) {
    if (!expMap) return;
    for (const [expKey, strikes] of Object.entries(expMap)) {
      for (const [kStr, contracts] of Object.entries(strikes)) {
        const k = parseFloat(kStr);
        const c = Array.isArray(contracts) ? contracts[0] : contracts;
        if (!c) continue;

        const gamma = typeof c.gamma === 'number' ? Math.abs(c.gamma) : 0;
        const oi    = typeof c.openInterest === 'number' ? c.openInterest : 0;
        const vol   = typeof c.totalVolume  === 'number' ? c.totalVolume  : 0;
        const iv    = validIV(c.volatility) ? c.volatility : 0;

        // GEX 单位：美元
        // 对做市商而言：卖出Call → 正GEX；卖出Put → 负GEX
        const gex = gamma * oi * 100 * spot * spot * 0.01;

        if (!map[k]) map[k] = { cGex:0, pGex:0, cOI:0, pOI:0, cVol:0, pVol:0, cIV:0, pIV:0, cIVn:0, pIVn:0 };
        const m = map[k];
        if (isCall) {
          m.cGex += gex;   // 正
          m.cOI  += oi; m.cVol += vol;
          if (iv > 0) { m.cIV += iv; m.cIVn++; }
        } else {
          m.pGex -= gex;   // 负
          m.pOI  += oi; m.pVol += vol;
          if (iv > 0) { m.pIV += iv; m.pIVn++; }
        }
      }
    }
  }

  process(chain.callExpDateMap, true);
  process(chain.putExpDateMap,  false);

  const sortedStrikes = Object.keys(map).map(Number).sort((a,b) => a-b);
  if (!sortedStrikes.length) return { strikes:[], aggregateGex:[], gammaFlip:null, callWall:null, putWall:null, atmIV:0 };

  // 构建 strikes 数组（每个行权价一个净GEX）
  const strikes = sortedStrikes.map(k => {
    const m = map[k];
    const netGex = m.cGex + m.pGex; // 净GEX，正为绿柱，负为红柱
    return {
      strike: k,
      netGex,
      callGex: m.cGex,
      putGex:  m.pGex,
      callOI: m.cOI, putOI: m.pOI,
      callVol: m.cVol, putVol: m.pVol,
      totalOI: m.cOI + m.pOI,
      totalVol: m.cVol + m.pVol,
      volOIRatio: (m.cOI+m.pOI) > 0 ? (m.cVol+m.pVol)/(m.cOI+m.pOI) : 0,
      callVolOI: m.cOI > 0 ? m.cVol/m.cOI : 0,
      putVolOI:  m.pOI > 0 ? m.pVol/m.pOI : 0,
      callIV: m.cIVn > 0 ? m.cIV/m.cIVn : 0,
      putIV:  m.pIVn > 0 ? m.pIV/m.pIVn : 0,
    };
  });

  // 聚合GEX曲线（累加净GEX，从最低行权价开始）
  let cum = 0;
  const aggregateGex = strikes.map(s => { cum += s.netGex; return cum; });

  // Gamma Flip：聚合GEX穿越零轴的第一个点
  let gammaFlip = null;
  for (let i = 1; i < strikes.length; i++) {
    const p = aggregateGex[i-1], c = aggregateGex[i];
    if ((p < 0 && c >= 0) || (p >= 0 && c < 0)) {
      const ratio = Math.abs(p) / (Math.abs(p) + Math.abs(c));
      gammaFlip = parseFloat((strikes[i-1].strike + ratio*(strikes[i].strike - strikes[i-1].strike)).toFixed(2));
      break;
    }
  }
  // 无穿越则取聚合GEX绝对值最小点
  if (gammaFlip === null) {
    let minI = 0;
    aggregateGex.forEach((v,i) => { if (Math.abs(v) < Math.abs(aggregateGex[minI])) minI = i; });
    gammaFlip = strikes[minI].strike;
  }

  // Call Wall：净GEX最大正值行权价
  let callWall = null, maxNet = -Infinity;
  strikes.forEach(s => { if (s.netGex > maxNet) { maxNet = s.netGex; callWall = s.strike; } });

  // Put Wall：净GEX最大负值（绝对值最大）行权价
  let putWall = null, minNet = Infinity;
  strikes.forEach(s => { if (s.netGex < minNet) { minNet = s.netGex; putWall = s.strike; } });

  // ATM IV：距spot最近5个行权价的有效IV均值
  let atmIV = 0;
  const near = [...strikes].sort((a,b) => Math.abs(a.strike-spot) - Math.abs(b.strike-spot)).slice(0,5);
  const ivVals = near.flatMap(s => {
    const vs = [];
    if (s.callIV > 0) vs.push(s.callIV);
    if (s.putIV  > 0) vs.push(s.putIV);
    return vs;
  });
  if (ivVals.length) atmIV = parseFloat((ivVals.reduce((a,b)=>a+b,0)/ivVals.length).toFixed(2));

  return { strikes, aggregateGex, gammaFlip, callWall, putWall, atmIV };
}

// ── 到期日列表 ────────────────────────────────────────────────────────────────
async function getExpDates(symbol, token) {
  const now = new Date();
  const from = now.toISOString().split('T')[0];
  const to   = new Date(now.getFullYear(), now.getMonth()+2, 0).toISOString().split('T')[0];
  const chain = await getChain(symbol, token, from, to);
  const dates = new Set();
  const add = m => { if(m) Object.keys(m).forEach(k => dates.add(k.split(':')[0])); };
  add(chain.callExpDateMap); add(chain.putExpDateMap);
  return [...dates].sort();
}

// ── IV 历史 ───────────────────────────────────────────────────────────────────
async function storeIV(env, symbol, atmIV, cwStrike, cwIV, pwStrike, pwIV) {
  const key = `iv:${symbol.replace('/','_')}:${new Date().toISOString().split('T')[0]}`;
  let hist = [];
  try { const r = await env.GEX_KV.get(key); if(r) hist = JSON.parse(r); } catch(_){}
  hist.push({
    ts: Date.now(),
    time: new Date().toLocaleTimeString('zh-CN',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',hour12:false}),
    atmIV: atmIV > 0 ? atmIV : null,
    cwStrike, cwIV: cwIV > 0 ? cwIV : null,
    pwStrike, pwIV: pwIV > 0 ? pwIV : null,
  });
  hist = hist.slice(-100);
  await env.GEX_KV.put(key, JSON.stringify(hist), { expirationTtl: 172800 });
  return hist;
}

async function getIV(env, symbol) {
  const key = `iv:${symbol.replace('/','_')}:${new Date().toISOString().split('T')[0]}`;
  try { const r = await env.GEX_KV.get(key); return r ? JSON.parse(r) : []; } catch(_){ return []; }
}

// ── 路由 ──────────────────────────────────────────────────────────────────────
export default {
  async fetch(req, env) {
    if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });
    const { pathname: path, searchParams: q } = new URL(req.url);

    try {
      if (path === '/api/health') return ok({ status:'ok', time:etNow(), market:isMarketOpen() });

      // 获取到期日
      if (path === '/api/expirations') {
        const sym = (q.get('symbol')||'').trim().toUpperCase();
        if (!sym) return err('symbol required', 400);
        const token = await getToken(env);
        const dates = await getExpDates(sym, token);
        return ok({ symbol:sym, dates, updatedAt:etNow() });
      }

      // GEX 数据
      if (path === '/api/gex') {
        const sym = (q.get('symbol')||'').trim().toUpperCase();
        if (!sym) return err('symbol required', 400);
        const datesParam = q.get('dates');
        const token = await getToken(env);
        const spot  = await getQuote(sym, token);

        let chain;
        if (datesParam) {
          const ds = datesParam.split(',').sort();
          chain = await getChain(sym, token, ds[0], ds[ds.length-1]);
        } else {
          const now = new Date();
          const from = now.toISOString().split('T')[0];
          const to   = new Date(now.getFullYear(), now.getMonth()+2, 0).toISOString().split('T')[0];
          chain = await getChain(sym, token, from, to);
        }

        const gex = calcGEX(chain, spot);
        const cw = gex.callWall ? gex.strikes.find(s=>s.strike===gex.callWall) : null;
        const pw = gex.putWall  ? gex.strikes.find(s=>s.strike===gex.putWall)  : null;
        await storeIV(env, sym, gex.atmIV, gex.callWall, cw?.callIV||0, gex.putWall, pw?.putIV||0);

        return ok({ symbol:sym, spotPrice:spot, ...gex,
          expirationDates: [...new Set(chain.callExpDateMap ? Object.keys(chain.callExpDateMap).map(k=>k.split(':')[0]) : [])].sort(),
          updatedAt:etNow(), isMarketHours:isMarketOpen() });
      }

      // 末日观察
      if (path === '/api/doomsday') {
        const sym = (q.get('symbol')||'').trim().toUpperCase();
        if (!sym) return err('symbol required', 400);
        const token = await getToken(env);
        const spot  = await getQuote(sym, token);
        const dates = await getExpDates(sym, token);
        if (!dates.length) return err('No expiration dates', 404);

        const nearest = dates[0];
        const chain   = await getChain(sym, token, nearest, nearest);
        const gex     = calcGEX(chain, spot);

        let maxGex = {strike:null, value:-Infinity};
        let minGex = {strike:null, value:Infinity};
        gex.strikes.forEach(s => {
          if (s.netGex > maxGex.value) maxGex = { strike:s.strike, value:s.netGex };
          if (s.netGex < minGex.value) minGex = { strike:s.strike, value:s.netGex };
        });

        const cw = gex.callWall ? gex.strikes.find(s=>s.strike===gex.callWall) : null;
        const pw = gex.putWall  ? gex.strikes.find(s=>s.strike===gex.putWall)  : null;
        const hist = await storeIV(env, sym, gex.atmIV, gex.callWall, cw?.callIV||0, gex.putWall, pw?.putIV||0);

        return ok({ symbol:sym, spotPrice:spot, expirationDate:nearest, ...gex, maxGex, minGex,
          updatedAt:etNow(), isMarketHours:isMarketOpen() });
      }

      // IV历史
      if (path === '/api/iv-history') {
        const sym = (q.get('symbol')||'').trim().toUpperCase();
        if (!sym) return err('symbol required', 400);
        return ok({ symbol:sym, history: await getIV(env, sym) });
      }

      return err('Not found', 404);
    } catch(e) {
      console.error(e.message, e.stack);
      return err(e.message);
    }
  },

  async scheduled(event, env) {
    if (isMarketOpen()) console.log('tick', etNow());
  },
};
