/* ===== Совместная игра: синхронизация фишек через WebSocket =====
 * Синхронизируется:
 *   - позиция и этаж 4 основных персонажей (Агнес, Изабелла, Кармелита, Мэгги);
 *   - свои фишки: создание, удаление, перемещение.
 * Не синхронизируется (у каждого своё): выбранный персонаж, вид карты, масштаб, освещение, список локаций.
 *
 * Адрес сервера берётся с той же страницы. Для теста можно указать другой:  index.html?server=host:port
 */
(function () {
  "use strict";

  var SEND_MS = 50;            /* как часто проверяем и отправляем свои изменения */
  var LOCAL_HOLD = 600;        /* мс: после своего движения фишки чужие обновления для неё игнорируются */
  var SNAP_DIST = 500;         /* дальше этого расстояния фишка «телепортируется», а не плывёт */
  var LERP = 14;               /* скорость сглаживания чужого движения */
  var EPS = 0.4;               /* изменение позиции меньше этого считаем шумом */
  var CHAR_IDS = ["t5", "t7", "t6", "t189"];

  var api = null, rt = null;
  var ws = null, connected = false, retryMs = 500, online = 1;
  var gotSnap = false;          /* снимок мира от сервера применён */
  var pendingSnap = null;       /* пришёл, но игра ещё не готова */
  var known = {};               /* ключ -> {x,y,l}: последнее, что мы применили или отправили */
  var knownCustoms = {};        /* id -> true: какие свои фишки мы знаем */
  var remote = {};              /* ключ -> {x,y,l}: куда плавно двигать чужую фишку */
  var localT = {};              /* ключ -> время последнего своего изменения */

  window.__mpNewId = function () { return Date.now() * 1000 + Math.floor(Math.random() * 1000); };

  /* ---------------- индикатор ---------------- */
  var badge = document.createElement("div");
  badge.style.cssText = "position:fixed;left:12px;top:12px;z-index:60;padding:5px 11px;border-radius:999px;" +
    "background:rgba(18,20,30,.82);border:1px solid rgba(255,255,255,.22);color:#fff;pointer-events:none;" +
    "font:600 13px/1.2 'Segoe UI',Roboto,Arial,sans-serif;box-shadow:0 3px 12px rgba(0,0,0,.4);";
  function showBadge() {
    if (!badge.parentNode) (document.body || document.documentElement).appendChild(badge);
    if (location.protocol === "file:" && !serverParam()) { badge.textContent = "⚪ Открыто как файл — нет сервера"; return; }
    badge.textContent = connected ? "🟢 Онлайн: " + online : "🔴 Нет связи, переподключаюсь…";
  }

  /* ---------------- игра ---------------- */
  function getApi() {
    var a = window.__mpApi;
    if (!a || !a.init()) return null;
    rt = a.rt();
    return a;
  }
  function ready() {
    if (!api) api = getApi();
    return !!(api && rt && api.firstInst("t5"));
  }
  function readChar(id) {
    var o = api.firstInst(id);
    return o ? { x: o.x, y: o.y, l: o.g ? o.g.name : "" } : null;
  }
  function readCustom(c) { return { x: c.x, y: c.y, l: c.layerName || (c.g && c.g.name) || "" }; }
  function changed(a, b) { return Math.abs(a.x - b.x) > EPS || Math.abs(a.y - b.y) > EPS || a.l !== b.l; }
  function setInstLayer(o, L) {
    if (!L || o.g === L) return;
    o.g.vf(o, true); o.g = L; L.Nf(o, true);        /* то же, что делает действие игры «Move to layer» */
    rt.Q = true;
  }
  function setInstPos(o, x, y) {
    o.x = x; o.y = y;
    if (typeof o.R === "function") o.R();
    rt.Q = true;
  }
  function findCustom(id) {
    var list = api.customs();
    for (var i = 0; i < list.length; i++) if (list[i].id === id) return list[i];
    return null;
  }
  function serverParam() { try { return new URLSearchParams(location.search).get("server"); } catch (e) { return null; } }

  /* ---------------- сеть ---------------- */
  function wsUrl() {
    var s = serverParam();
    var proto = location.protocol === "https:" ? "wss://" : "ws://";
    if (s) return (/^wss?:\/\//.test(s) ? "" : proto) + s.replace(/\/+$/, "") + "/ws";
    return proto + location.host + "/ws";
  }
  function send(obj) { if (connected && ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }

  function connect() {
    if (location.protocol === "file:" && !serverParam()) { showBadge(); return; }
    try { ws = new WebSocket(wsUrl()); } catch (e) { scheduleRetry(); return; }
    ws.onopen = function () { connected = true; retryMs = 500; gotSnap = false; showBadge(); };
    ws.onclose = function () { connected = false; gotSnap = false; showBadge(); scheduleRetry(); };
    ws.onerror = function () { try { ws.close(); } catch (e) {} };
    ws.onmessage = function (ev) {
      var m; try { m = JSON.parse(ev.data); } catch (e) { return; }
      onMessage(m);
    };
  }
  function scheduleRetry() {
    setTimeout(connect, retryMs);
    retryMs = Math.min(10000, retryMs * 1.7);
  }

  function onMessage(m) {
    switch (m.t) {
      case "snap": pendingSnap = m; if (m.n) online = m.n; showBadge(); break;
      case "online": online = m.n; showBadge(); break;
      case "pos": if (gotSnap && ready()) applyPos(m.items); break;
      case "cadd": if (gotSnap && ready()) applyCustomAdd(m.c); break;
      case "crem": if (gotSnap && ready()) applyCustomRemove(m.id); break;
    }
  }

  /* ---------------- применение чужих изменений ---------------- */
  function applyPos(items) {
    var now = Date.now();
    for (var i = 0; i < items.length; i++) {
      var it = items[i];
      if (localT[it.k] && now - localT[it.k] < LOCAL_HOLD) continue;   /* эту фишку сейчас двигаю я */
      remote[it.k] = { x: it.x, y: it.y, l: it.l };
    }
  }
  function applyCustomAdd(o) {
    var key = "c:" + o.id;
    if (known[key] || findCustom(o.id)) return;
    var c = api.addRemoteCustom(o);
    if (c) { known[key] = readCustom(c); knownCustoms[o.id] = true; }
  }
  function applyCustomRemove(id) {
    delete known["c:" + id]; delete knownCustoms[id]; delete remote["c:" + id];
    if (findCustom(id)) api.removeCustom(id);
  }

  /* полный снимок мира: при подключении и после сброса на сервере */
  function applySnapshot(m) {
    var k, i;
    if (!m.init) {
      /* сервер пуст: этот клиент задаёт начальное состояние */
      var chars = {}, customs = [];
      for (i = 0; i < CHAR_IDS.length; i++) {
        var p = readChar(CHAR_IDS[i]);
        if (p) { chars["ch:" + CHAR_IDS[i]] = p; known["ch:" + CHAR_IDS[i]] = p; }
      }
      var list = api.customs();
      for (i = 0; i < list.length; i++) {
        var c = list[i];
        customs.push({ id: c.id, name: c.name, color: c.color, layer: c.layerName, x: c.x, y: c.y });
        known["c:" + c.id] = readCustom(c); knownCustoms[c.id] = true;
      }
      send({ t: "init", chars: chars, customs: customs });
      gotSnap = true;
      return;
    }
    remote = {};
    /* персонажи: сразу ставим на место */
    for (i = 0; i < CHAR_IDS.length; i++) {
      var s = m.chars["ch:" + CHAR_IDS[i]], o = api.firstInst(CHAR_IDS[i]);
      if (!s || !o) continue;
      setInstLayer(o, api.layerByName(s.l));
      setInstPos(o, s.x, s.y);
      known["ch:" + CHAR_IDS[i]] = readChar(CHAR_IDS[i]);
    }
    /* свои фишки: приводим список в точное соответствие серверу */
    var want = {};
    for (i = 0; i < m.customs.length; i++) want[m.customs[i].id] = m.customs[i];
    var have = api.customs().slice();
    for (i = 0; i < have.length; i++) {
      if (!want[have[i].id]) { delete known["c:" + have[i].id]; delete knownCustoms[have[i].id]; api.removeCustom(have[i].id); }
    }
    for (k in want) {
      var w = want[k], cu = findCustom(w.id);
      if (!cu) cu = api.addRemoteCustom(w);
      if (!cu) continue;
      var L = api.layerByName(w.layer);
      if (L) { cu.layerName = w.layer; cu.g = L; }
      cu.x = w.x; cu.y = w.y; cu.mv = null;
      known["c:" + w.id] = readCustom(cu); knownCustoms[w.id] = true;
    }
    for (k in knownCustoms) if (!want[k]) delete knownCustoms[k];
    gotSnap = true;
  }

  /* ---------------- цикл: отправка своих изменений ---------------- */
  function tick() {
    if (!connected) return;
    if (!ready()) return;
    if (pendingSnap) { var s = pendingSnap; pendingSnap = null; applySnapshot(s); return; }
    if (!gotSnap) return;

    var now = Date.now(), items = [], i, key, cur, k;
    var sel = api.selectedChar();
    var selKey = sel ? "ch:" + sel.def.ch : null;

    /* персонажи: отправляем только того, которым сейчас управляю я */
    for (i = 0; i < CHAR_IDS.length; i++) {
      key = "ch:" + CHAR_IDS[i];
      cur = readChar(CHAR_IDS[i]); if (!cur) continue;
      k = known[key];
      if (!k) { known[key] = cur; continue; }
      if (changed(cur, k)) {
        known[key] = cur;
        if (key === selKey) { items.push({ k: key, x: cur.x, y: cur.y, l: cur.l }); localT[key] = now; delete remote[key]; }
      }
    }

    /* свои фишки: добавление, перемещение, удаление */
    var list = api.customs(), present = {};
    for (i = 0; i < list.length; i++) {
      var c = list[i]; key = "c:" + c.id; present[c.id] = true;
      cur = readCustom(c); k = known[key];
      if (!k) {
        known[key] = cur; knownCustoms[c.id] = true;
        send({ t: "cadd", c: { id: c.id, name: c.name, color: c.color, layer: cur.l, x: cur.x, y: cur.y } });
      } else if (changed(cur, k)) {
        known[key] = cur;
        if (c.mv || !remote[key]) { items.push({ k: key, x: cur.x, y: cur.y, l: cur.l }); localT[key] = now; delete remote[key]; }
      }
    }
    for (var id in knownCustoms) {
      if (!present[id]) { delete knownCustoms[id]; delete known["c:" + id]; delete remote["c:" + id]; send({ t: "crem", id: +id }); }
    }
    if (items.length) send({ t: "pos", items: items });
  }

  /* ---------------- цикл: плавное движение чужих фишек ---------------- */
  var lastT = 0;
  function frame(t) {
    requestAnimationFrame(frame);
    var dt = Math.min(0.1, Math.max(0, (t - lastT) / 1000)); lastT = t;
    if (!gotSnap || !ready()) return;
    var f = Math.min(1, dt * LERP);
    for (var key in remote) {
      var r = remote[key], o, isCustom = key.charAt(1) === ":";   /* "c:123" — своя фишка, "ch:t5" — персонаж */
      if (isCustom) o = findCustom(+key.slice(2));
      else o = api.firstInst(key.slice(3));
      if (!o) { delete remote[key]; continue; }
      var L = api.layerByName(r.l), curL = isCustom ? (o.layerName || "") : (o.g ? o.g.name : "");
      var layerChanged = r.l && curL !== r.l;
      if (layerChanged && L) {
        if (isCustom) { o.layerName = r.l; o.g = L; } else setInstLayer(o, L);
      }
      var dx = r.x - o.x, dy = r.y - o.y, d = Math.hypot(dx, dy);
      if (layerChanged || d > SNAP_DIST || d < 0.5) {
        if (isCustom) { o.x = r.x; o.y = r.y; o.mv = null; } else setInstPos(o, r.x, r.y);
        delete remote[key];
      } else if (isCustom) { o.x += dx * f; o.y += dy * f; o.mv = null; }
      else setInstPos(o, o.x + dx * f, o.y + dy * f);
      known[key] = isCustom ? readCustom(o) : readChar(key.slice(3));   /* чтобы не отправить это обратно */
    }
  }

  showBadge();
  connect();
  setInterval(tick, SEND_MS);
  requestAnimationFrame(frame);
})();
