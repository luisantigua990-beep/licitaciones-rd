/* ============================================================
   CONTROL DE OBRA — Lógica encapsulada (objeto Obras)
   ============================================================
   Misma arquitectura que BidManager:
   - HTML del módulo en /frontend/obras/obras.html, cargado en #tab-obras
   - API en /api/obras (router_obras.py)
   - Auth: Bearer sb_token con refresh automático
   ============================================================ */

const Obras = {
  loaded: false,
  htmlLoaded: false,
  API: '/api/obras',

  obras: [],            // lista (v_obra_resumen)
  actual: null,         // obra abierta
  tablero: null,
  partidas: [],
  cubicaciones: [],
  gastos: [],
  tabActual: 'tablero',

  // ── Auth / fetch (copiado del patrón BidManager) ──────────
  _token() { return localStorage.getItem('sb_token'); },

  async _refreshToken() {
    const rt = localStorage.getItem('sb_refresh_token');
    if (!rt || typeof SUPABASE_URL === 'undefined' || typeof SUPABASE_ANON_KEY === 'undefined') return null;
    try {
      const r = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=refresh_token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'apikey': SUPABASE_ANON_KEY },
        body: JSON.stringify({ refresh_token: rt })
      });
      const d = await r.json();
      if (d.access_token) {
        localStorage.setItem('sb_token', d.access_token);
        if (d.refresh_token) localStorage.setItem('sb_refresh_token', d.refresh_token);
        if (typeof _syncSesionCookies === 'function') _syncSesionCookies();
        return d.access_token;
      }
    } catch (e) { console.error('Obras: error refrescando token', e); }
    return null;
  },

  async _req(path, method = 'GET', body = null) {
    const doReq = (tok) => fetch(this.API + path, {
      method,
      headers: { 'Authorization': 'Bearer ' + tok, 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined
    });
    let r = await doReq(this._token());
    if (r.status === 401) {
      const nuevo = await this._refreshToken();
      if (nuevo) r = await doReq(nuevo);
    }
    if (!r.ok) {
      let msg = 'Error ' + r.status;
      try { const j = await r.json(); msg = j.detail || msg; } catch (_) {}
      throw new Error(msg);
    }
    return r.json();
  },

  // Subida de archivos (multipart). No fija Content-Type: el navegador pone el boundary.
  async _reqForm(path, form) {
    const doReq = (tok) => fetch(this.API + path, { method: 'POST', headers: { 'Authorization': 'Bearer ' + tok }, body: form });
    let r = await doReq(this._token());
    if (r.status === 401) {
      const nuevo = await this._refreshToken();
      if (nuevo) r = await doReq(nuevo);
    }
    if (!r.ok) {
      let msg = 'Error ' + r.status;
      try { const j = await r.json(); msg = j.detail || msg; } catch (_) {}
      throw new Error(msg);
    }
    return r.json();
  },

  // Abre el selector de archivos y devuelve el File elegido (o null)
  _elegirArchivo(accept) {
    return new Promise(res => {
      const i = document.createElement('input');
      i.type = 'file'; i.accept = accept; i.style.display = 'none';
      i.onchange = () => { res(i.files[0] || null); i.remove(); };
      document.body.appendChild(i); i.click();
    });
  },

  // Reduce fotos grandes del celular antes de subir (máx. 2000 px, JPEG)
  async _reducirImagen(file) {
    if (!file.type.startsWith('image/') || file.size < 600 * 1024) return file;
    try {
      const bmp = await createImageBitmap(file);
      const k = Math.min(1, 2000 / Math.max(bmp.width, bmp.height));
      const c = document.createElement('canvas');
      c.width = Math.round(bmp.width * k); c.height = Math.round(bmp.height * k);
      c.getContext('2d').drawImage(bmp, 0, 0, c.width, c.height);
      const blob = await new Promise(r => c.toBlob(r, 'image/jpeg', 0.85));
      return blob ? new File([blob], (file.name || 'factura').replace(/\.\w+$/, '') + '.jpg', { type: 'image/jpeg' }) : file;
    } catch (_) { return file; }
  },

  _cargando(titulo, texto) {
    this.abrirModal(titulo, `<div class="bm-loading" style="flex-direction:column;gap:12px"><div class="bm-spinner"></div><div class="ob-muted">${texto}</div></div>`, null);
    document.getElementById('ob-modal-footer').innerHTML = '';
  },

  // Datalist de partidas para los campos "Partida" (se escribe y filtra)
  _etiquetaPartida(p) { return `${p.codigo ? p.codigo + ' — ' : ''}${(p.descripcion || '').slice(0, 110)}`; },
  _datalistPartidas() {
    this._mapEtiqueta = {}; this._etiquetaDe = {};
    const opts = this.partidas.map(p => {
      let e = this._etiquetaPartida(p), n = 2;
      while (this._mapEtiqueta[e]) e = `${this._etiquetaPartida(p)} (${n++})`;
      this._mapEtiqueta[e] = p.id; this._etiquetaDe[p.id] = e;
      return `<option value="${this.esc(e)}">${this.esc(p.capitulo || '')}</option>`;
    }).join('');
    return `<datalist id="ob-dl-partidas">${opts}</datalist>`;
  },
  _inputPartida(attrs, pid) {
    const v = pid ? (this._etiquetaDe[pid] || '') : '';
    return `<input class="bm-input ob-input-partida ${pid ? '' : 'ob-falta'}" list="ob-dl-partidas" placeholder="Escribe para buscar la partida…" value="${this.esc(v)}" ${attrs}
      oninput="Obras._marcarPartida(this)">`;
  },
  _marcarPartida(el) {
    el.classList.toggle('ob-falta', !this._mapEtiqueta[el.value]);
    if (typeof this._onPartidaCambio === 'function') this._onPartidaCambio();
  },
  _idPartida(el) { return this._mapEtiqueta[el.value] || null; },

  // ── Utilidades ─────────────────────────────────────────────
  money(n) {
    n = Number(n || 0);
    return 'RD$ ' + n.toLocaleString('es-DO', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  },
  num(n, d = 2) { return Number(n || 0).toLocaleString('es-DO', { minimumFractionDigits: d, maximumFractionDigits: d }); },
  pct(n) { return this.num(n, 1) + '%'; },
  esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); },
  fecha(s) { return s ? new Date(s + 'T00:00:00').toLocaleDateString('es-DO') : '—'; },
  hoy() { return new Date().toISOString().slice(0, 10); },

  toast(msg, ok = true) {
    let t = document.getElementById('ob-toast');
    if (!t) { t = document.createElement('div'); t.id = 'ob-toast'; document.body.appendChild(t); }
    t.textContent = msg;
    t.className = 'bm-toast bm-show ' + (ok ? 'bm-toast-ok' : 'bm-toast-err');
    clearTimeout(this._toastTimer);
    this._toastTimer = setTimeout(() => t.classList.remove('bm-show'), 2500);
  },

  ESTADO_LABEL: {
    planificada: 'Planificada', en_ejecucion: 'En ejecución', suspendida: 'Suspendida',
    terminada: 'Terminada', cerrada: 'Cerrada',
    borrador: 'Borrador', presentada: 'Presentada', aprobada: 'Aprobada', pagada: 'Pagada'
  },
  TIPO_GASTO: {
    material: 'Material', mano_obra: 'Mano de obra', equipo: 'Equipo', subcontrato: 'Subcontrato',
    transporte: 'Transporte', indirecto: 'Indirecto'
  },

  // ── Init ───────────────────────────────────────────────────
  async init() {
    if (!this.htmlLoaded) {
      const container = document.getElementById('tab-obras');
      if (!container) { console.error('#tab-obras no encontrado'); return; }
      try {
        const r = await fetch('/frontend/obras/obras.html?v=3');
        if (!r.ok) throw new Error('HTML no encontrado');
        container.innerHTML = await r.text();
        this.htmlLoaded = true;
        this._bindTabs();
      } catch (e) {
        container.innerHTML = `<div class="bm-container"><div class="bm-empty"><div class="bm-empty-icon">!</div><div class="bm-empty-text">Error cargando el módulo: ${this.esc(e.message)}</div></div></div>`;
        return;
      }
    }
    await this.cargarLista();
    this.loaded = true;
  },

  _bindTabs() {
    document.querySelectorAll('#ob-tabs .bm-tab').forEach(b => {
      b.addEventListener('click', () => this.irTab(b.dataset.panel));
    });
  },

  irTab(panel) {
    this.tabActual = panel;
    document.querySelectorAll('#ob-tabs .bm-tab').forEach(b => b.classList.toggle('bm-active', b.dataset.panel === panel));
    document.querySelectorAll('#ob-vista-detalle .bm-panel').forEach(p => p.classList.toggle('bm-active', p.dataset.panel === panel));
  },

  // ══════════════════════════════════════════════════════════
  // LISTA DE OBRAS
  // ══════════════════════════════════════════════════════════
  async cargarLista() {
    const cont = document.getElementById('ob-lista-obras');
    cont.innerHTML = '<div class="bm-loading"><div class="bm-spinner"></div></div>';
    try {
      this.obras = await this._req('');
    } catch (e) {
      cont.innerHTML = `<div class="bm-empty"><div class="bm-empty-text">${this.esc(e.message)}</div></div>`;
      return;
    }
    this.renderLista();
  },

  renderLista() {
    const cont = document.getElementById('ob-lista-obras');
    const kp = document.getElementById('ob-kpis-globales');

    const activas = this.obras.filter(o => o.estado === 'en_ejecucion');
    const tot = (k) => this.obras.reduce((s, o) => s + Number(o[k] || 0), 0);
    const ejecutado = tot('ejecutado'), gastado = tot('gastado_directo') + tot('gastado_indirecto');
    kp.innerHTML = [
      ['Obras en ejecución', activas.length, ''],
      ['Contratado', this.money(tot('presupuesto')), 'suma de listados cargados'],
      ['Ejecutado', this.money(ejecutado), 'cubicaciones presentadas'],
      ['Margen bruto', this.money(ejecutado - gastado), ejecutado ? this.pct((ejecutado - gastado) / ejecutado * 100) : '']
    ].map(([l, v, d]) => `<div class="bm-indicator"><div class="bm-indicator-label">${l}</div><div class="bm-indicator-value">${v}</div><div class="bm-indicator-desc">${d}</div></div>`).join('');

    if (!this.obras.length) {
      cont.innerHTML = `<div class="bm-empty"><div class="bm-empty-icon">🏗️</div><div class="bm-empty-text">Todavía no hay obras. Crea la primera con el contrato adjudicado.</div></div>`;
      return;
    }

    cont.innerHTML = this.obras.map(o => {
      const gast = Number(o.gastado_directo || 0) + Number(o.gastado_indirecto || 0);
      const margen = Number(o.ejecutado || 0) - gast;
      return `
      <div class="bm-card ob-obra-card" onclick="Obras.abrir('${o.obra_id}')">
        <div class="bm-card-header">
          <div>
            <div class="bm-card-title">${this.esc(o.nombre)}</div>
            <div class="ob-obra-meta">${this.esc(o.entidad || '')} ${o.referencia ? '· ' + this.esc(o.referencia) : ''}</div>
          </div>
          <span class="bm-badge ${o.estado === 'en_ejecucion' ? 'bm-badge-ok' : 'bm-badge-info'}">${this.ESTADO_LABEL[o.estado] || o.estado}</span>
        </div>
        <div class="ob-obra-kpis">
          <div><span>Contratado</span><strong>${this.money(o.presupuesto || o.monto_contrato)}</strong></div>
          <div><span>Ejecutado</span><strong>${this.money(o.ejecutado)}</strong></div>
          <div><span>Margen</span><strong class="${margen < 0 ? 'ob-neg' : 'ob-pos'}">${this.money(margen)}</strong></div>
        </div>
        <div class="ob-bar"><i style="width:${Math.min(100, Number(o.avance_financiero_pct || 0))}%"></i></div>
        <div class="ob-obra-meta">Avance financiero ${this.pct(o.avance_financiero_pct)}</div>
      </div>`;
    }).join('');
  },

  volver() {
    this.actual = null;
    document.getElementById('ob-vista-detalle').style.display = 'none';
    document.getElementById('ob-vista-lista').style.display = '';
    this.cargarLista();
  },

  // ══════════════════════════════════════════════════════════
  // DETALLE DE OBRA
  // ══════════════════════════════════════════════════════════
  async abrir(id) {
    document.getElementById('ob-vista-lista').style.display = 'none';
    document.getElementById('ob-vista-detalle').style.display = '';
    this.actual = { id };
    await this.refrescarDetalle();
    this.irTab('tablero');
  },

  async refrescarDetalle() {
    const id = this.actual.id;
    try {
      const [tab, part, cubs, gastos] = await Promise.all([
        this._req(`/${id}/tablero`),
        this._req(`/${id}/partidas`),
        this._req(`/${id}/cubicaciones`),
        this._req(`/${id}/gastos`)
      ]);
      this.tablero = tab; this.actual = tab.obra;
      this.partidas = part; this.cubicaciones = cubs; this.gastos = gastos;
    } catch (e) { this.toast(e.message, false); return; }

    const o = this.actual;
    document.getElementById('ob-det-nombre').textContent = o.nombre;
    document.getElementById('ob-det-sub').textContent =
      [o.entidad, o.contrato_no ? 'Contrato ' + o.contrato_no : null, o.referencia].filter(Boolean).join(' · ');
    const est = document.getElementById('ob-det-estado');
    est.textContent = this.ESTADO_LABEL[o.estado] || o.estado;
    est.className = 'bm-badge ' + (o.estado === 'en_ejecucion' ? 'bm-badge-ok' : 'bm-badge-info');

    document.getElementById('ob-badge-partidas').textContent = this.partidas.length;
    document.getElementById('ob-badge-cubicaciones').textContent = this.cubicaciones.length;
    document.getElementById('ob-badge-gastos').textContent = this.gastos.length;

    this.renderTablero();
    this.renderPartidas();
    this.renderCubicaciones();
    this.renderGastos();
  },

  // ── Tablero ────────────────────────────────────────────────
  renderTablero() {
    const k = this.tablero.kpis;
    document.getElementById('ob-kpis').innerHTML = [
      ['Contratado', this.money(k.presupuesto), k.monto_contrato && Math.abs(k.monto_contrato - k.presupuesto) > 1 ? 'contrato: ' + this.money(k.monto_contrato) : 'según listado'],
      ['Ejecutado', this.money(k.ejecutado), this.pct(k.avance_financiero_pct) + ' avance financiero'],
      ['Gastado', this.money(k.gastado), 'indirectos: ' + this.money(k.gastado_indirecto)],
      ['Margen bruto', this.money(k.margen_bruto), this.pct(k.margen_pct) + ' sobre ejecutado'],
      ['Cobrado', this.money(k.cobrado), 'por cobrar: ' + this.money(k.por_cobrar)],
      ['Por ejecutar', this.money(k.por_ejecutar), '']
    ].map(([l, v, d]) => `<div class="bm-indicator"><div class="bm-indicator-label">${l}</div><div class="bm-indicator-value ${/Margen/.test(l) && k.margen_bruto < 0 ? 'ob-neg' : ''}">${v}</div><div class="bm-indicator-desc">${d}</div></div>`).join('');

    const al = document.getElementById('ob-alertas');
    al.innerHTML = this.tablero.alertas.length
      ? `<div class="bm-alert"><div class="bm-alert-icon">!</div><div class="bm-alert-text">
           <strong>${this.tablero.alertas.length} partida(s) gastan más de lo que han producido:</strong>
           ${this.tablero.alertas.map(a => `${this.esc(a.codigo || '')} ${this.esc(a.descripcion)} (${this.money(a.margen)})`).join(' · ')}
         </div></div>`
      : '';

    // Por capítulo
    const caps = this.tablero.capitulos;
    document.getElementById('ob-tabla-capitulos').innerHTML = `
      <thead><tr><th>Capítulo</th><th class="ob-num">Contratado</th><th class="ob-num">Ejecutado</th><th class="ob-num">Avance</th><th class="ob-num">Gastado</th><th class="ob-num">Margen</th></tr></thead>
      <tbody>${caps.map(c => `<tr>
        <td class="ob-desc">${this.esc(c.capitulo)}</td>
        <td class="ob-num">${this.money(c.contratado)}</td><td class="ob-num">${this.money(c.ejecutado)}</td>
        <td class="ob-num">${this.pct(c.avance_pct)}</td><td class="ob-num">${this.money(c.gastado)}</td>
        <td class="ob-num ${c.margen < 0 ? 'ob-neg' : ''}">${this.money(c.margen)}</td></tr>`).join('') || '<tr><td colspan="6" class="ob-muted">Sin partidas cargadas</td></tr>'}
      </tbody>`;

    // Por partida
    const ps = this.tablero.partidas;
    let cap = null, rows = '';
    for (const p of ps) {
      if (p.capitulo !== cap) { cap = p.capitulo; rows += `<tr class="ob-capitulo"><td colspan="10">${this.esc(cap || 'SIN CAPÍTULO')}</td></tr>`; }
      const cur = p.costo_unitario_real, apu = p.costo_unitario_apu;
      rows += `<tr>
        <td>${this.esc(p.codigo || '')}</td>
        <td class="ob-desc">${this.esc(p.descripcion)}</td>
        <td>${this.esc(p.unidad || '')}</td>
        <td class="ob-num">${this.num(p.cant_contratada)}</td>
        <td class="ob-num">${this.num(p.cant_ejecutada)}</td>
        <td class="ob-num">${this.pct(p.avance_pct)}</td>
        <td class="ob-num">${this.money(p.monto_ejecutado)}</td>
        <td class="ob-num">${this.money(p.gastado)}</td>
        <td class="ob-num ${p.margen < 0 ? 'ob-neg' : ''}">${this.money(p.margen)}</td>
        <td class="ob-num ${cur && apu && cur > apu ? 'ob-neg' : ''}" title="costo unitario real vs. APU">${cur ? this.num(cur) : '—'}${apu ? ' / ' + this.num(apu) : ''}</td>
      </tr>`;
    }
    document.getElementById('ob-tabla-estado').innerHTML = `
      <thead><tr><th>Cód.</th><th>Partida</th><th>Ud.</th><th class="ob-num">Contratada</th><th class="ob-num">Ejecutada</th><th class="ob-num">Avance</th><th class="ob-num">Ejecutado RD$</th><th class="ob-num">Gastado</th><th class="ob-num">Margen</th><th class="ob-num">CU real / APU</th></tr></thead>
      <tbody>${rows || '<tr><td colspan="10" class="ob-muted">Carga el listado en la pestaña Partidas</td></tr>'}</tbody>`;
  },

  // ══════════════════════════════════════════════════════════
  // PARTIDAS
  // ══════════════════════════════════════════════════════════
  renderPartidas() {
    const t = document.getElementById('ob-tabla-partidas');
    const vacio = document.getElementById('ob-partidas-vacio');
    const hay = this.partidas.length > 0;
    t.parentElement.style.display = hay ? '' : 'none';
    if (vacio) vacio.style.display = hay ? 'none' : '';
    let cap = null, rows = '', total = 0, costo = 0;
    const conCosto = this.partidas.some(p => p.costo_unitario_apu != null);
    const nc = conCosto ? 8 : 7;
    for (const p of this.partidas) {
      if (p.capitulo !== cap) { cap = p.capitulo; rows += `<tr class="ob-capitulo"><td colspan="${nc}">${this.esc(cap || 'SIN CAPÍTULO')}</td></tr>`; }
      total += Number(p.total || 0);
      costo += Number(p.cantidad || 0) * Number(p.costo_unitario_apu || 0);
      rows += `<tr>
        <td>${this.esc(p.codigo || '')}</td><td class="ob-desc">${this.esc(p.descripcion)}</td><td>${this.esc(p.unidad || '')}</td>
        <td class="ob-num">${this.num(p.cantidad)}</td><td class="ob-num">${this.num(p.precio_unitario)}</td>
        ${conCosto ? `<td class="ob-num ${p.costo_unitario_apu > p.precio_unitario ? 'ob-neg' : ''}">${p.costo_unitario_apu != null ? this.num(p.costo_unitario_apu) : '—'}</td>` : ''}
        <td class="ob-num">${this.money(p.total)}</td>
        <td class="ob-acciones">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="Obras.editarPartida('${p.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="Obras.eliminarPartida('${p.id}')">✕</button>
        </td></tr>`;
    }
    t.innerHTML = `
      <thead><tr><th>Cód.</th><th>Descripción</th><th>Ud.</th><th class="ob-num">Cantidad</th><th class="ob-num">P.U.</th>${conCosto ? '<th class="ob-num" title="Costo unitario de ejecución">Costo U.</th>' : ''}<th class="ob-num">Total</th><th></th></tr></thead>
      <tbody>${rows}
      ${hay ? `<tr class="ob-total"><td colspan="${conCosto ? 6 : 5}">TOTAL${conCosto && total ? ` <span class="ob-muted" style="font-weight:400">· costo ${this.money(costo)} · margen esperado ${this.pct((total - costo) / total * 100)}</span>` : ''}</td><td class="ob-num">${this.money(total)}</td><td></td></tr>` : ''}</tbody>`;
  },

  // ══════════════════════════════════════════════════════════
  // SUBIR EXCEL DEL PRESUPUESTO
  // ══════════════════════════════════════════════════════════
  CAMPOS_PRES: [
    ['codigo', 'Código'], ['descripcion', 'Descripción'], ['unidad', 'Unidad'], ['cantidad', 'Cantidad'],
    ['precio_unitario', 'P.U. contratado'], ['costo_unitario', 'Costo unitario (opcional)']
  ],

  async subirPresupuesto(file) {
    file = file || await this._elegirArchivo('.xlsx,.xlsm,.csv');
    if (!file) return;
    this._imp = { file, hoja: null, mapeo: null, datos: null };
    await this._leerPresupuesto();
  },

  async _leerPresupuesto() {
    const imp = this._imp;
    this._cargando('Leyendo el presupuesto', `Buscando partidas en <strong>${this.esc(imp.file.name)}</strong>…`);
    const fd = new FormData();
    fd.append('file', imp.file);
    if (imp.hoja) fd.append('hoja', imp.hoja);
    if (imp.mapeo) fd.append('mapeo', JSON.stringify(imp.mapeo));
    try { imp.datos = await this._reqForm(`/${this.actual.id}/partidas/importar`, fd); }
    catch (e) { this.cerrarModal(); return this.toast(e.message, false); }
    this._vistaPresupuesto();
  },

  _vistaPresupuesto() {
    const d = this._imp.datos, r = d.resumen;
    const optsCol = (sel) => `<option value="">— ninguna —</option>` + d.encabezados.map(h =>
      `<option value="${h.col}" ${sel === h.col ? 'selected' : ''}>${h.letra}${h.texto ? ' · ' + this.esc(h.texto.slice(0, 28)) : ''}${h.grupo ? ' (' + this.esc(h.grupo.slice(0, 14)) + ')' : ''}</option>`).join('');
    const mapeo = this.CAMPOS_PRES.map(([k, l]) => `
      <div class="bm-form-row"><label class="bm-label">${l}</label>
        <select class="bm-input" data-campo="${k}" onchange="Obras._cambiarMapeoPres()">${optsCol(d.columnas[k])}</select></div>`).join('');

    let cap = null, filas = '';
    d.partidas.forEach((p, i) => {
      if (p.capitulo !== cap) { cap = p.capitulo; filas += `<tr class="ob-capitulo"><td></td><td colspan="7">${this.esc(cap || 'SIN CAPÍTULO')}</td></tr>`; }
      filas += `<tr data-i="${i}">
        <td><input type="checkbox" checked data-tipo="p" data-i="${i}" onchange="Obras._totalesPres()"></td>
        <td>${this.esc(p.codigo || '')}</td><td class="ob-desc">${this.esc(p.descripcion)}</td><td>${this.esc(p.unidad || '')}</td>
        <td class="ob-num">${this.num(p.cantidad)}</td><td class="ob-num">${this.num(p.precio_unitario)}</td>
        <td class="ob-num">${p.costo_unitario_apu != null ? this.num(p.costo_unitario_apu) : '—'}</td>
        <td class="ob-num">${this.money(p.cantidad * p.precio_unitario)}</td></tr>`;
    });
    if (d.indirectos.length) {
      filas += `<tr class="ob-capitulo"><td></td><td colspan="7">GASTOS INDIRECTOS</td></tr>` + d.indirectos.map((p, i) => `
        <tr><td><input type="checkbox" checked data-tipo="i" data-i="${i}" onchange="Obras._totalesPres()"></td>
        <td>${this.esc(p.codigo || '')}</td><td class="ob-desc">${this.esc(p.descripcion)}${p.porcentaje ? ` <span class="ob-muted">(${this.num(p.porcentaje * 100, 2)}%)</span>` : ''}</td><td>PA</td>
        <td class="ob-num">1.00</td><td class="ob-num">${this.num(p.precio_unitario)}</td><td class="ob-num">—</td><td class="ob-num">${this.money(p.precio_unitario)}</td></tr>`).join('');
    }

    const avisos = d.avisos.length ? `<div class="bm-alert"><div class="bm-alert-icon">!</div><div class="bm-alert-text">${d.avisos.map(a => this.esc(a)).join('<br>')}</div></div>` : '';
    const hojas = d.hojas.length > 1 ? `<select class="bm-input ob-inline-select" onchange="Obras._imp.hoja=this.value;Obras._imp.mapeo=null;Obras._leerPresupuesto()">${d.hojas.map(h => `<option ${h === d.hoja ? 'selected' : ''}>${this.esc(h)}</option>`).join('')}</select>` : `<strong>${this.esc(d.hoja)}</strong>`;
    const existentes = this.partidas.length;

    const body = `
      <div class="ob-imp-cab">
        <div>Hoja ${hojas} · encabezados en la fila <strong>${d.fila_encabezado}</strong> · <strong>${r.partidas}</strong> partidas en <strong>${r.capitulos}</strong> capítulos${d.indirectos.length ? ` · ${d.indirectos.length} indirectos` : ''}</div>
        <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="document.getElementById('ob-imp-mapeo').classList.toggle('ob-oculto')">Corregir columnas</button>
      </div>
      <div id="ob-imp-mapeo" class="ob-oculto">
        <p class="ob-preview-note">Si alguna columna quedó mal, cámbiala aquí y vuelvo a leer el archivo.</p>
        <div class="bm-form-grid">${mapeo}
          <div class="bm-form-row"><label class="bm-label">Fila de encabezados</label><input class="bm-input" type="number" min="1" id="ob-imp-fila" value="${d.fila_encabezado}" onchange="Obras._cambiarMapeoPres()"></div>
        </div>
      </div>
      ${avisos}
      <div class="ob-table-wrap ob-imp-tabla"><table class="ob-table">
        <thead><tr><th><input type="checkbox" checked onchange="Obras._marcarTodasPres(this.checked)"></th><th>Cód.</th><th>Descripción</th><th>Ud.</th><th class="ob-num">Cantidad</th><th class="ob-num">P.U.</th><th class="ob-num">Costo U.</th><th class="ob-num">Total</th></tr></thead>
        <tbody>${filas || '<tr><td colspan="8" class="ob-muted">No se encontraron partidas. Corrige las columnas.</td></tr>'}</tbody></table></div>
      <div class="ob-imp-totales" id="ob-imp-totales"></div>
      ${existentes ? `<label class="ob-check"><input type="checkbox" id="ob-imp-reemplazar" checked> Reemplazar las ${existentes} partidas que ya tiene la obra</label>` : ''}`;

    this.abrirModal('Revisa el presupuesto antes de cargarlo', body, () => this._guardarPresupuesto(), 'Cargar partidas');
    this._totalesPres();
  },

  _cambiarMapeoPres() {
    const m = {};
    document.querySelectorAll('#ob-imp-mapeo select[data-campo]').forEach(s => m[s.dataset.campo] = s.value === '' ? '' : Number(s.value));
    m.fila_encabezado = Number(document.getElementById('ob-imp-fila').value || 1);
    this._imp.mapeo = m;
    this._leerPresupuesto();
  },
  _marcarTodasPres(v) {
    document.querySelectorAll('#ob-modal input[data-tipo]').forEach(c => c.checked = v);
    this._totalesPres();
  },
  _seleccionPres() {
    const d = this._imp.datos, out = [];
    document.querySelectorAll('#ob-modal input[data-tipo]').forEach(c => {
      if (!c.checked) return;
      const p = (c.dataset.tipo === 'p' ? d.partidas : d.indirectos)[Number(c.dataset.i)];
      out.push({ codigo: p.codigo, capitulo: p.capitulo, descripcion: p.descripcion, unidad: p.unidad,
                 cantidad: p.cantidad, precio_unitario: p.precio_unitario, costo_unitario_apu: p.costo_unitario_apu });
    });
    return out;
  },
  _totalesPres() {
    const sel = this._seleccionPres();
    const tot = sel.reduce((s, p) => s + p.cantidad * p.precio_unitario, 0);
    const costo = sel.reduce((s, p) => s + p.cantidad * (p.costo_unitario_apu || 0), 0);
    const directo = sel.filter(p => p.costo_unitario_apu != null).reduce((s, p) => s + p.cantidad * p.precio_unitario, 0);
    const el = document.getElementById('ob-imp-totales');
    if (el) el.innerHTML = `<span><strong>${sel.length}</strong> seleccionadas</span><span>Contratado <strong>${this.money(tot)}</strong></span>` +
      (costo ? `<span>Costo de ejecución <strong>${this.money(costo)}</strong></span><span>Margen esperado en directos <strong class="${directo - costo < 0 ? 'ob-neg' : 'ob-pos'}">${this.pct((directo - costo) / directo * 100)}</strong></span>` : '');
  },

  async _guardarPresupuesto() {
    const partidas = this._seleccionPres();
    if (!partidas.length) throw new Error('Marca al menos una partida');
    const re = document.getElementById('ob-imp-reemplazar');
    const r = await this._req(`/${this.actual.id}/partidas`, 'POST', { partidas, reemplazar: re ? re.checked : false });
    // Si la obra no tenía monto de contrato, tomar el total cargado
    const o = this.actual, total = partidas.reduce((s, p) => s + p.cantidad * p.precio_unitario, 0);
    if (!Number(o.monto_contrato)) {
      try {
        await this._req(`/${o.id}`, 'PUT', { nombre: o.nombre, referencia: o.referencia, entidad: o.entidad, contrato_no: o.contrato_no,
          monto_contrato: Math.round(total * 100) / 100, fecha_inicio: o.fecha_inicio, fecha_fin: o.fecha_fin,
          plazo_dias: o.plazo_dias, estado: o.estado, notas: o.notas });
      } catch (_) {}
    }
    this.toast(`${r.insertadas} partidas cargadas`); this.cerrarModal();
    await this.refrescarDetalle(); this.irTab('partidas');
  },

  _formPartida(p = {}) {
    return `<div class="bm-form-grid">
      <div class="bm-form-row"><label class="bm-label">Código</label><input class="bm-input" id="f-codigo" value="${this.esc(p.codigo || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">Capítulo</label><input class="bm-input" id="f-capitulo" value="${this.esc(p.capitulo || '')}"></div>
      <div class="bm-form-row" style="grid-column:1/-1"><label class="bm-label">Descripción</label><input class="bm-input" id="f-descripcion" value="${this.esc(p.descripcion || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">Unidad</label><input class="bm-input" id="f-unidad" value="${this.esc(p.unidad || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">Cantidad</label><input class="bm-input" type="number" step="any" id="f-cantidad" value="${p.cantidad ?? ''}"></div>
      <div class="bm-form-row"><label class="bm-label">Precio unitario</label><input class="bm-input" type="number" step="any" id="f-pu" value="${p.precio_unitario ?? ''}"></div>
      <div class="bm-form-row"><label class="bm-label">Costo unitario APU (opcional)</label><input class="bm-input" type="number" step="any" id="f-cu" value="${p.costo_unitario_apu ?? ''}"></div>
    </div>`;
  },
  _leerPartida() {
    const v = id => document.getElementById(id).value.trim();
    const d = {
      codigo: v('f-codigo') || null, capitulo: v('f-capitulo') || null, descripcion: v('f-descripcion'),
      unidad: v('f-unidad') || null, cantidad: Number(v('f-cantidad') || 0), precio_unitario: Number(v('f-pu') || 0),
      costo_unitario_apu: v('f-cu') ? Number(v('f-cu')) : null
    };
    if (!d.descripcion) throw new Error('La descripción es obligatoria');
    return d;
  },

  nuevaPartida() {
    this.abrirModal('Nueva partida', this._formPartida(), async () => {
      await this._req(`/${this.actual.id}/partidas`, 'POST', { partidas: [this._leerPartida()] });
      this.toast('Partida agregada'); this.cerrarModal(); this.refrescarDetalle();
    });
  },
  editarPartida(id) {
    const p = this.partidas.find(x => x.id === id);
    this.abrirModal('Editar partida', this._formPartida(p), async () => {
      await this._req(`/${this.actual.id}/partidas/${id}`, 'PUT', this._leerPartida());
      this.toast('Partida actualizada'); this.cerrarModal(); this.refrescarDetalle();
    });
  },
  async eliminarPartida(id) {
    if (!confirm('¿Eliminar esta partida? Se borran también sus cubicaciones y se desvinculan sus gastos.')) return;
    try { await this._req(`/${this.actual.id}/partidas/${id}`, 'DELETE'); this.refrescarDetalle(); }
    catch (e) { this.toast(e.message, false); }
  },

  // Pegar desde Excel: columnas separadas por TAB.
  // Acepta: [código] descripción unidad cantidad PU   |  filas de capítulo = solo texto, sin números.
  importarPartidas() {
    const body = `
      <p class="ob-preview-note">Copia del Excel las columnas <strong>código · descripción · unidad · cantidad · precio unitario</strong> y pégalas aquí.
      Las filas que solo tienen texto (sin cantidad ni PU) se toman como capítulo de las siguientes.</p>
      <textarea class="bm-textarea ob-paste" id="f-paste" placeholder="1.01&#9;Replanteo y nivelación&#9;m²&#9;1,250.00&#9;35.00"></textarea>
      <label style="display:flex;gap:8px;align-items:center;margin-top:8px;font-size:13px">
        <input type="checkbox" id="f-reemplazar"> Reemplazar el listado actual (solo si la obra no tiene cubicaciones)
      </label>
      <div id="f-preview" class="ob-preview-note"></div>`;
    this.abrirModal('Pegar listado de cantidades', body, async () => {
      const partidas = this._parsearPegado(document.getElementById('f-paste').value);
      if (!partidas.length) throw new Error('No se reconoció ninguna partida');
      const r = await this._req(`/${this.actual.id}/partidas`, 'POST', { partidas, reemplazar: document.getElementById('f-reemplazar').checked });
      this.toast(`${r.insertadas} partidas cargadas`); this.cerrarModal(); this.refrescarDetalle();
    }, 'Cargar');
    document.getElementById('f-paste').addEventListener('input', (e) => {
      const ps = this._parsearPegado(e.target.value);
      const tot = ps.reduce((s, p) => s + p.cantidad * p.precio_unitario, 0);
      document.getElementById('f-preview').textContent = ps.length ? `${ps.length} partidas · total ${this.money(tot)}` : '';
    });
  },

  _parsearPegado(txt) {
    const numero = s => Number(String(s || '').replace(/RD\$|\$|\s/g, '').replace(/,/g, '')) || 0;
    const out = []; let capitulo = null;
    for (const linea of txt.split(/\r?\n/)) {
      if (!linea.trim()) continue;
      const c = linea.split('\t').map(s => s.trim());
      // ¿tiene número al final? -> partida ; si no -> capítulo
      const nums = c.filter(x => /^-?[\d.,]+$/.test(x.replace(/RD\$|\$|\s/g, '')));
      if (nums.length < 2) { capitulo = c.filter(Boolean).join(' '); continue; }
      let codigo = null, desc, unidad = null, cant, pu;
      // Formato esperado: [cod] desc [ud] cant pu [total]
      const textos = c.filter(x => !/^-?[\d.,]+$/.test(x.replace(/RD\$|\$|\s/g, '')) && x);
      if (textos.length >= 3) { codigo = textos[0]; desc = textos[1]; unidad = textos[2]; }
      else if (textos.length === 2) {
        if (/^[\d.]+$/.test(textos[0]) || textos[0].length <= 6) { codigo = textos[0]; desc = textos[1]; }
        else { desc = textos[0]; unidad = textos[1]; }
      } else { desc = textos[0] || '(sin descripción)'; }
      cant = numero(nums[nums.length === 2 ? 0 : nums.length - 3]);
      pu = numero(nums[nums.length === 2 ? 1 : nums.length - 2]);
      if (/^[\d.]+$/.test(desc) && codigo === null) { codigo = desc; desc = '(sin descripción)'; }
      out.push({ codigo, capitulo, descripcion: desc, unidad, cantidad: cant, precio_unitario: pu });
    }
    return out;
  },

  // ══════════════════════════════════════════════════════════
  // CUBICACIONES
  // ══════════════════════════════════════════════════════════
  renderCubicaciones() {
    const c = document.getElementById('ob-lista-cubicaciones');
    if (!this.cubicaciones.length) { c.innerHTML = '<div class="bm-empty"><div class="bm-empty-text">Sin cubicaciones. Crea la No. 1 cuando tengas avance que certificar.</div></div>'; return; }
    const badge = { borrador: '', presentada: 'bm-badge-info', aprobada: 'bm-badge-warn', pagada: 'bm-badge-ok' };
    c.innerHTML = this.cubicaciones.map(k => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div><div class="bm-card-title">Cubicación No. ${k.numero}</div>
          <div class="bm-card-subtitle">${this.fecha(k.periodo_desde)} → ${this.fecha(k.periodo_hasta)}</div></div>
          <span class="bm-badge ${badge[k.estado] || ''}">${this.ESTADO_LABEL[k.estado]}</span>
        </div>
        <div class="bm-card-body">
          <div>Presentado: <strong>${this.money(k.monto_presentado)}</strong></div>
          ${k.monto_aprobado != null ? `<div>Aprobado: <strong>${this.money(k.monto_aprobado)}</strong></div>` : ''}
          ${k.fecha_pago ? `<div>Pagada el ${this.fecha(k.fecha_pago)}</div>` : ''}
        </div>
        <div class="bm-card-actions">
          ${k.estado === 'borrador' ? `<button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="Obras.editarCubicacion('${k.id}')">Editar detalle</button>
            <button class="bm-btn bm-btn-primary bm-btn-sm" onclick="Obras.cambiarEstadoCub('${k.id}','presentada')">Marcar presentada</button>
            <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="Obras.eliminarCubicacion('${k.id}')">✕</button>` : ''}
          ${k.estado === 'presentada' ? `<button class="bm-btn bm-btn-primary bm-btn-sm" onclick="Obras.cambiarEstadoCub('${k.id}','aprobada')">Marcar aprobada</button>` : ''}
          ${k.estado === 'aprobada' ? `<button class="bm-btn bm-btn-primary bm-btn-sm" onclick="Obras.cambiarEstadoCub('${k.id}','pagada')">Marcar pagada</button>` : ''}
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="Obras.verCubicacion('${k.id}')">Ver</button>
        </div>
      </div>`).join('');
  },

  // Tabla editable: por partida -> contratada, acumulada anterior, cantidad en esta cubicación
  _formCubicacion(k = {}, detalle = {}) {
    const acum = {};
    for (const p of this.tablero.partidas) acum[p.partida_id] = Number(p.cant_ejecutada || 0);
    const filas = this.partidas.map(p => {
      const prev = acum[p.id] || 0, rest = Number(p.cantidad) - prev;
      return `<tr>
        <td>${this.esc(p.codigo || '')}</td><td class="ob-desc">${this.esc(p.descripcion)}</td><td>${this.esc(p.unidad || '')}</td>
        <td class="ob-num">${this.num(p.cantidad)}</td><td class="ob-num">${this.num(prev)}</td>
        <td class="ob-num ${rest < 0 ? 'ob-neg' : ''}">${this.num(rest)}</td>
        <td class="ob-num"><input class="bm-input" type="number" step="any" min="0" data-partida="${p.id}" data-pu="${p.precio_unitario}" value="${detalle[p.id] ?? ''}" oninput="Obras._totalCub()"></td>
      </tr>`;
    }).join('');
    return `<div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Desde</label><input class="bm-input" type="date" id="f-desde" value="${k.periodo_desde || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Hasta</label><input class="bm-input" type="date" id="f-hasta" value="${k.periodo_hasta || this.hoy()}"></div>
        <div class="bm-form-row" style="grid-column:1/-1"><label class="bm-label">Notas</label><input class="bm-input" id="f-notas" value="${this.esc(k.notas || '')}"></div>
      </div>
      <div class="ob-table-wrap"><table class="ob-table">
        <thead><tr><th>Cód.</th><th>Partida</th><th>Ud.</th><th class="ob-num">Contratada</th><th class="ob-num">Acum. anterior</th><th class="ob-num">Por ejecutar</th><th class="ob-num">Esta cubicación</th></tr></thead>
        <tbody>${filas}</tbody></table></div>
      <div class="ob-preview-note">Monto de esta cubicación: <strong id="f-total-cub">${this.money(0)}</strong></div>`;
  },
  _totalCub() {
    let t = 0;
    document.querySelectorAll('#ob-modal input[data-partida]').forEach(i => t += Number(i.value || 0) * Number(i.dataset.pu || 0));
    const el = document.getElementById('f-total-cub'); if (el) el.textContent = this.money(t);
  },
  _leerCubicacion() {
    const detalle = [];
    document.querySelectorAll('#ob-modal input[data-partida]').forEach(i => {
      const c = Number(i.value || 0); if (c > 0) detalle.push({ partida_id: i.dataset.partida, cantidad: c });
    });
    return {
      periodo_desde: document.getElementById('f-desde').value || null,
      periodo_hasta: document.getElementById('f-hasta').value || null,
      notas: document.getElementById('f-notas').value || null,
      estado: 'borrador', detalle
    };
  },
  nuevaCubicacion() {
    if (!this.partidas.length) return this.toast('Primero carga el listado de partidas', false);
    this.abrirModal(`Cubicación No. ${this.cubicaciones.length + 1}`, this._formCubicacion(), async () => {
      await this._req(`/${this.actual.id}/cubicaciones`, 'POST', this._leerCubicacion());
      this.toast('Cubicación creada en borrador'); this.cerrarModal(); this.refrescarDetalle();
    });
    this._totalCub();
  },
  async editarCubicacion(id) {
    const k = await this._req(`/${this.actual.id}/cubicaciones/${id}`);
    const det = {}; for (const d of k.detalle) det[d.partida_id] = Number(d.cantidad);
    // El acumulado anterior excluye esta misma cubicación (está en borrador, no cuenta en la vista)
    this.abrirModal(`Editar cubicación No. ${k.numero}`, this._formCubicacion(k, det), async () => {
      await this._req(`/${this.actual.id}/cubicaciones/${id}`, 'PUT', this._leerCubicacion());
      this.toast('Cubicación actualizada'); this.cerrarModal(); this.refrescarDetalle();
    });
    this._totalCub();
  },
  async verCubicacion(id) {
    const k = await this._req(`/${this.actual.id}/cubicaciones/${id}`);
    const rows = k.detalle.map(d => { const p = d.obra_partidas || {}; return `<tr><td>${this.esc(p.codigo || '')}</td><td class="ob-desc">${this.esc(p.descripcion || '')}</td><td>${this.esc(p.unidad || '')}</td><td class="ob-num">${this.num(d.cantidad)}</td><td class="ob-num">${this.num(p.precio_unitario)}</td><td class="ob-num">${this.money(d.cantidad * p.precio_unitario)}</td></tr>`; }).join('');
    this.abrirModal(`Cubicación No. ${k.numero} · ${this.ESTADO_LABEL[k.estado]}`,
      `<div class="ob-table-wrap"><table class="ob-table"><thead><tr><th>Cód.</th><th>Partida</th><th>Ud.</th><th class="ob-num">Cantidad</th><th class="ob-num">P.U.</th><th class="ob-num">Importe</th></tr></thead><tbody>${rows}<tr class="ob-total"><td colspan="5">TOTAL</td><td class="ob-num">${this.money(k.monto_presentado)}</td></tr></tbody></table></div>`,
      null);
  },
  async cambiarEstadoCub(id, estado) {
    const body = { estado };
    if (estado === 'aprobada') {
      const k = this.cubicaciones.find(x => x.id === id);
      const v = prompt('Monto aprobado por la entidad (deja vacío si aprobaron lo presentado):', k.monto_presentado);
      if (v === null) return;
      if (v.trim()) body.monto_aprobado = Number(v.replace(/,/g, ''));
    }
    if (estado === 'pagada') {
      const f = prompt('Fecha de pago (AAAA-MM-DD):', this.hoy());
      if (f === null) return; body.fecha_pago = f;
    }
    try { await this._req(`/${this.actual.id}/cubicaciones/${id}/estado`, 'PATCH', body); this.refrescarDetalle(); }
    catch (e) { this.toast(e.message, false); }
  },
  async eliminarCubicacion(id) {
    if (!confirm('¿Eliminar esta cubicación en borrador?')) return;
    try { await this._req(`/${this.actual.id}/cubicaciones/${id}`, 'DELETE'); this.refrescarDetalle(); }
    catch (e) { this.toast(e.message, false); }
  },

  // ══════════════════════════════════════════════════════════
  // GASTOS
  // ══════════════════════════════════════════════════════════
  renderGastos() {
    const t = document.getElementById('ob-tabla-gastos');
    let total = 0, sinPartida = 0;
    const rows = this.gastos.map(g => {
      total += Number(g.monto || 0);
      const p = g.obra_partidas;
      if (!p) sinPartida++;
      return `<tr>
        <td>${this.fecha(g.fecha)}</td><td>${this.TIPO_GASTO[g.tipo] || g.tipo}</td>
        <td class="ob-desc">${this.esc(g.descripcion)}${g.proveedor ? ` <span class="ob-muted">· ${this.esc(g.proveedor)}</span>` : ''}</td>
        <td class="ob-desc">${p ? this.esc((p.codigo || '') + ' ' + p.descripcion) : '<span class="ob-neg">Sin partida</span>'}</td>
        <td>${this.esc(g.documento || '')}${g.archivo_path ? ` <button class="bm-btn bm-btn-ghost bm-btn-sm" title="Ver factura" onclick="Obras.verFactura('${g.id}')">📄</button>` : ''}</td>
        <td class="ob-num">${this.money(g.monto)}</td>
        <td>${g.pagado ? '<span class="bm-badge bm-badge-ok">Pagado</span>' : '<span class="bm-badge bm-badge-warn">Pendiente</span>'}</td>
        <td class="ob-acciones">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="Obras.editarGasto('${g.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="Obras.eliminarGasto('${g.id}')">✕</button></td>
      </tr>`;
    }).join('');
    t.innerHTML = `
      <thead><tr><th>Fecha</th><th>Tipo</th><th>Descripción</th><th>Partida</th><th>Doc.</th><th class="ob-num">Monto</th><th></th><th></th></tr></thead>
      <tbody>${rows || `<tr><td colspan="8" class="ob-muted">${this.partidas.length ? 'Sin gastos. Toma la foto de una factura o sube la relación de gastos en Excel.' : 'Primero carga el presupuesto en la pestaña Partidas; cada gasto se asigna a una partida.'}</td></tr>`}
      ${rows ? `<tr class="ob-total"><td colspan="5">TOTAL${sinPartida ? ` <span class="ob-neg" style="font-weight:400">· ${sinPartida} sin partida</span>` : ''}</td><td class="ob-num">${this.money(total)}</td><td colspan="2"></td></tr>` : ''}</tbody>`;
  },

  _hayPartidas() {
    if (this.partidas.length) return true;
    this.toast('Primero carga el presupuesto: cada gasto va a una partida', false);
    this.irTab('partidas');
    return false;
  },
  _optsTipo(sel) {
    return Object.entries(this.TIPO_GASTO).map(([k, v]) => `<option value="${k}" ${(sel || 'material') === k ? 'selected' : ''}>${v}</option>`).join('');
  },

  // ── Gasto manual ───────────────────────────────────────────
  _formGasto(g = {}) {
    return `${this._datalistPartidas()}<div class="bm-form-grid">
      <div class="bm-form-row" style="grid-column:1/-1"><label class="bm-label">Partida</label>${this._inputPartida('id="f-partida"', g.partida_id)}</div>
      <div class="bm-form-row"><label class="bm-label">Fecha</label><input class="bm-input" type="date" id="f-fecha" value="${g.fecha || this.hoy()}"></div>
      <div class="bm-form-row"><label class="bm-label">Tipo</label><select class="bm-input" id="f-tipo">${this._optsTipo(g.tipo)}</select></div>
      <div class="bm-form-row" style="grid-column:1/-1"><label class="bm-label">Descripción</label><input class="bm-input" id="f-descripcion" value="${this.esc(g.descripcion || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">Proveedor</label><input class="bm-input" id="f-proveedor" value="${this.esc(g.proveedor || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">RNC / cédula</label><input class="bm-input" id="f-rnc" value="${this.esc(g.rnc || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">NCF / No. factura / conduce</label><input class="bm-input" id="f-documento" value="${this.esc(g.documento || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">Monto (sin ITBIS)</label><input class="bm-input" type="number" step="any" id="f-monto" value="${g.monto ?? ''}"></div>
      <div class="bm-form-row"><label class="bm-label">ITBIS</label><input class="bm-input" type="number" step="any" id="f-itbis" value="${g.itbis ?? 0}"></div>
      <div class="bm-form-row"><label class="ob-check"><input type="checkbox" id="f-pagado" ${g.pagado ? 'checked' : ''}> Pagado</label></div>
    </div>`;
  },
  _leerGasto(g = {}) {
    const v = id => document.getElementById(id).value.trim();
    const d = {
      fecha: v('f-fecha') || null, tipo: v('f-tipo'), partida_id: this._idPartida(document.getElementById('f-partida')),
      descripcion: v('f-descripcion'), proveedor: v('f-proveedor') || null, rnc: v('f-rnc') || null, documento: v('f-documento') || null,
      monto: Number(v('f-monto') || 0), itbis: Number(v('f-itbis') || 0), pagado: document.getElementById('f-pagado').checked,
      archivo_path: g.archivo_path || null, origen: g.origen || 'manual'
    };
    if (!d.partida_id) throw new Error('Elige la partida de la lista');
    if (!d.descripcion) throw new Error('La descripción es obligatoria');
    if (!d.monto) throw new Error('El monto es obligatorio');
    return d;
  },
  nuevoGasto() {
    if (!this._hayPartidas()) return;
    this.abrirModal('Nuevo gasto', this._formGasto(), async () => {
      await this._req(`/${this.actual.id}/gastos`, 'POST', this._leerGasto());
      this.toast('Gasto registrado'); this.cerrarModal(); this.refrescarDetalle();
    });
  },
  editarGasto(id) {
    const g = this.gastos.find(x => x.id === id);
    this.abrirModal('Editar gasto', this._formGasto(g), async () => {
      await this._req(`/${this.actual.id}/gastos/${id}`, 'PUT', this._leerGasto(g));
      this.toast('Gasto actualizado'); this.cerrarModal(); this.refrescarDetalle();
    });
  },
  async verFactura(id) {
    try { const r = await this._req(`/${this.actual.id}/gastos/${id}/archivo`); window.open(r.url, '_blank'); }
    catch (e) { this.toast(e.message, false); }
  },

  // ── Foto de factura ────────────────────────────────────────
  async fotoFactura() {
    if (!this._hayPartidas()) return;
    let file = await this._elegirArchivo('image/*,application/pdf');
    if (!file) return;
    file = await this._reducirImagen(file);
    this._cargando('Leyendo la factura', 'Sacando proveedor, NCF, montos y renglones, y buscando la partida de cada uno…');
    const fd = new FormData(); fd.append('file', file);
    let f;
    try { f = await this._reqForm(`/${this.actual.id}/gastos/factura`, fd); }
    catch (e) { this.cerrarModal(); return this.toast(e.message, false); }
    this._fac = f;
    const vista = file.type.startsWith('image/') ? URL.createObjectURL(file) : null;
    const dl = this._datalistPartidas();

    const filas = f.items.map((it, i) => `<tr>
      <td class="ob-desc"><input class="bm-input" data-k="desc" data-i="${i}" value="${this.esc(it.descripcion)}"></td>
      <td class="ob-num"><input class="bm-input ob-num-input" type="number" step="any" data-k="imp" data-i="${i}" value="${it.importe}" oninput="Obras._totalesFactura()"></td>
      <td class="ob-partida-cel">${this._inputPartida(`data-k="par" data-i="${i}"`, it.partida_id)}
        ${it.fuente === 'palabras' ? '<div class="ob-hint">Sugerida por palabras, confírmala</div>' : ''}</td>
    </tr>`).join('');

    const body = `${dl}
      <div class="ob-fac">
        ${vista ? `<a href="${vista}" target="_blank" class="ob-fac-img"><img src="${vista}" alt="Factura"></a>` : ''}
        <div class="ob-fac-datos">
          ${f.confianza === 'baja' || f.notas ? `<div class="bm-alert"><div class="bm-alert-icon">!</div><div class="bm-alert-text">${f.confianza === 'baja' ? '<strong>Lectura con poca confianza, revisa los datos.</strong> ' : ''}${this.esc(f.notas || '')}</div></div>` : ''}
          <div class="bm-form-grid">
            <div class="bm-form-row"><label class="bm-label">Proveedor</label><input class="bm-input" id="fx-proveedor" value="${this.esc(f.proveedor || '')}"></div>
            <div class="bm-form-row"><label class="bm-label">RNC</label><input class="bm-input" id="fx-rnc" value="${this.esc(f.rnc || '')}"></div>
            <div class="bm-form-row"><label class="bm-label">NCF / No. factura</label><input class="bm-input" id="fx-doc" value="${this.esc(f.documento || '')}"></div>
            <div class="bm-form-row"><label class="bm-label">Fecha</label><input class="bm-input" type="date" id="fx-fecha" value="${f.fecha || this.hoy()}"></div>
            <div class="bm-form-row"><label class="bm-label">Tipo</label><select class="bm-input" id="fx-tipo">${this._optsTipo(f.tipo)}</select></div>
            <div class="bm-form-row"><label class="bm-label">ITBIS de la factura</label><input class="bm-input" type="number" step="any" id="fx-itbis" value="${f.itbis || 0}" oninput="Obras._totalesFactura()"></div>
          </div>
        </div>
      </div>
      <div class="ob-imp-cab"><strong>Renglones y partida</strong>
        <div class="ob-todo-una">${this._inputPartida('id="fx-todo"', null)}<button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="Obras._todoAUna()">Todo a esta partida</button></div>
      </div>
      <div class="ob-table-wrap"><table class="ob-table ob-apilada">
        <thead><tr><th>Renglón</th><th class="ob-num">Importe sin ITBIS</th><th>Partida</th></tr></thead>
        <tbody>${filas}</tbody></table></div>
      <div class="ob-imp-totales" id="fx-totales"></div>
      <label class="ob-check"><input type="checkbox" id="fx-pagado"> Ya está pagada</label>`;
    this.abrirModal('Confirma la factura', body, () => this._guardarFactura(), 'Guardar gasto');
    this._onPartidaCambio = () => this._totalesFactura();
    this._totalesFactura();
  },
  _todoAUna() {
    const v = document.getElementById('fx-todo').value;
    if (!this._mapEtiqueta[v]) return this.toast('Elige una partida de la lista', false);
    document.querySelectorAll('#ob-modal input[data-k="par"]').forEach(el => { el.value = v; this._marcarPartida(el); });
  },
  _totalesFactura() {
    const el = document.getElementById('fx-totales'); if (!el) return;
    let sub = 0, faltan = 0;
    document.querySelectorAll('#ob-modal input[data-k="imp"]').forEach(i => sub += Number(i.value || 0));
    document.querySelectorAll('#ob-modal input[data-k="par"]').forEach(i => { if (!this._mapEtiqueta[i.value]) faltan++; });
    const itb = Number(document.getElementById('fx-itbis').value || 0), f = this._fac;
    const dif = f.subtotal ? sub - f.subtotal : 0;
    el.innerHTML = `<span>Subtotal <strong>${this.money(sub)}</strong></span><span>ITBIS <strong>${this.money(itb)}</strong></span><span>Total <strong>${this.money(sub + itb)}</strong></span>` +
      (Math.abs(dif) > 1 ? `<span class="ob-neg">No cuadra con el subtotal de la factura (${this.money(f.subtotal)})</span>` : '') +
      (faltan ? `<span class="ob-neg">${faltan} renglón(es) sin partida</span>` : '<span class="ob-pos">Todo asignado ✓</span>');
  },
  async _guardarFactura() {
    const v = id => document.getElementById(id).value.trim();
    const renglones = [];
    document.querySelectorAll('#ob-modal input[data-k="par"]').forEach(el => {
      const i = el.dataset.i;
      renglones.push({
        partida_id: this._idPartida(el),
        desc: document.querySelector(`#ob-modal input[data-k="desc"][data-i="${i}"]`).value.trim(),
        imp: Number(document.querySelector(`#ob-modal input[data-k="imp"][data-i="${i}"]`).value || 0)
      });
    });
    if (renglones.some(r => !r.partida_id)) throw new Error('Asigna la partida de todos los renglones');
    const sub = renglones.reduce((s, r) => s + r.imp, 0);
    if (!sub) throw new Error('La factura no tiene importe');
    const itb = Number(v('fx-itbis') || 0);
    // Un gasto por partida: se agrupan los renglones y el ITBIS se reparte en proporción
    const grupos = {};
    for (const r of renglones) {
      const g = grupos[r.partida_id] ||= { partida_id: r.partida_id, desc: [], monto: 0 };
      g.desc.push(r.desc); g.monto += r.imp;
    }
    const base = { fecha: v('fx-fecha') || null, tipo: v('fx-tipo'), proveedor: v('fx-proveedor') || null, rnc: v('fx-rnc') || null,
      documento: v('fx-doc') || null, pagado: document.getElementById('fx-pagado').checked, archivo_path: this._fac.archivo_path, origen: 'foto' };
    const gastos = Object.values(grupos).map(g => ({ ...base, partida_id: g.partida_id,
      descripcion: g.desc.filter(Boolean).join('; ').slice(0, 300) || (base.proveedor || 'Factura'),
      monto: Math.round(g.monto * 100) / 100, itbis: Math.round(itb * g.monto / sub * 100) / 100 }));
    const r = await this._req(`/${this.actual.id}/gastos/lote`, 'POST', { gastos });
    this._onPartidaCambio = null;
    this.toast(r.insertados > 1 ? `Factura repartida en ${r.insertados} partidas` : 'Gasto registrado');
    this.cerrarModal(); await this.refrescarDetalle(); this.irTab('gastos');
  },

  // ── Excel de gastos ────────────────────────────────────────
  CAMPOS_GAS: [
    ['fecha', 'Fecha'], ['proveedor', 'Proveedor'], ['rnc', 'RNC'], ['documento', 'NCF / factura'], ['descripcion', 'Concepto'],
    ['subtotal', 'Monto sin ITBIS'], ['itbis', 'ITBIS'], ['total', 'Total'], ['partida', 'Código de partida']
  ],
  async subirGastosExcel(file) {
    if (!this._hayPartidas()) return;
    file = file || await this._elegirArchivo('.xlsx,.xlsm,.csv');
    if (!file) return;
    this._gx = { file, hoja: null, mapeo: null, datos: null };
    await this._leerGastosExcel();
  },
  async _leerGastosExcel() {
    const gx = this._gx;
    this._cargando('Leyendo los gastos', `Leyendo <strong>${this.esc(gx.file.name)}</strong> y buscando la partida de cada gasto…`);
    const fd = new FormData(); fd.append('file', gx.file);
    if (gx.hoja) fd.append('hoja', gx.hoja);
    if (gx.mapeo) fd.append('mapeo', JSON.stringify(gx.mapeo));
    try { gx.datos = await this._reqForm(`/${this.actual.id}/gastos/importar`, fd); }
    catch (e) { this.cerrarModal(); return this.toast(e.message, false); }
    this._vistaGastosExcel();
  },
  _vistaGastosExcel() {
    const d = this._gx.datos;
    const dl = this._datalistPartidas();
    const optsCol = (sel) => `<option value="">— ninguna —</option>` + d.encabezados.map(h =>
      `<option value="${h.col}" ${sel === h.col ? 'selected' : ''}>${h.letra}${h.texto ? ' · ' + this.esc(h.texto.slice(0, 28)) : ''}</option>`).join('');
    const mapeo = this.CAMPOS_GAS.map(([k, l]) => `
      <div class="bm-form-row"><label class="bm-label">${l}</label>
        <select class="bm-input" data-campo="${k}" onchange="Obras._cambiarMapeoGas()">${optsCol(d.columnas[k])}</select></div>`).join('');
    const filas = d.gastos.map((g, i) => `<tr>
      <td><input type="checkbox" checked data-sel="${i}" onchange="Obras._totalesGastos()"></td>
      <td>${g.fecha ? this.fecha(g.fecha) : '<span class="ob-muted">—</span>'}</td>
      <td class="ob-desc">${this.esc(g.descripcion)}${g.proveedor ? `<div class="ob-muted">${this.esc(g.proveedor)}${g.documento ? ' · ' + this.esc(g.documento) : ''}</div>` : ''}</td>
      <td class="ob-num">${this.money(g.monto)}${g.itbis ? `<div class="ob-muted">ITBIS ${this.num(g.itbis)}</div>` : ''}</td>
      <td><select class="bm-input ob-inline-select" data-tipo="${i}">${this._optsTipo(g.tipo)}</select></td>
      <td class="ob-partida-cel">${this._inputPartida(`data-par="${i}"`, g.partida_id)}
        ${g.fuente === 'palabras' ? '<div class="ob-hint">Sugerida por palabras, confírmala</div>' : ''}</td>
    </tr>`).join('');
    const avisos = d.avisos.length ? `<div class="bm-alert"><div class="bm-alert-icon">!</div><div class="bm-alert-text">${d.avisos.map(a => this.esc(a)).join('<br>')}</div></div>` : '';
    const hojas = d.hojas.length > 1 ? `<select class="bm-input ob-inline-select" onchange="Obras._gx.hoja=this.value;Obras._gx.mapeo=null;Obras._leerGastosExcel()">${d.hojas.map(h => `<option ${h === d.hoja ? 'selected' : ''}>${this.esc(h)}</option>`).join('')}</select>` : `<strong>${this.esc(d.hoja)}</strong>`;
    const body = `${dl}
      <div class="ob-imp-cab">
        <div>Hoja ${hojas} · encabezados en la fila <strong>${d.fila_encabezado}</strong> · <strong>${d.gastos.length}</strong> gastos</div>
        <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="document.getElementById('ob-gx-mapeo').classList.toggle('ob-oculto')">Corregir columnas</button>
      </div>
      <div id="ob-gx-mapeo" class="ob-oculto">
        <div class="bm-form-grid">${mapeo}
          <div class="bm-form-row"><label class="bm-label">Fila de encabezados</label><input class="bm-input" type="number" min="1" id="ob-gx-fila" value="${d.fila_encabezado}" onchange="Obras._cambiarMapeoGas()"></div>
        </div>
      </div>
      ${avisos}
      <div class="ob-imp-cab"><span class="ob-muted">Revisa la partida de cada gasto. Las que no estén claras quedan en blanco.</span>
        <div class="ob-todo-una">${this._inputPartida('id="gx-todo"', null)}<button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="Obras._asignarVacios()">Asignar a las vacías</button></div>
      </div>
      <div class="ob-table-wrap ob-imp-tabla"><table class="ob-table ob-apilada">
        <thead><tr><th><input type="checkbox" checked onchange="document.querySelectorAll('#ob-modal input[data-sel]').forEach(c=>c.checked=this.checked);Obras._totalesGastos()"></th>
          <th>Fecha</th><th>Concepto</th><th class="ob-num">Monto</th><th>Tipo</th><th>Partida</th></tr></thead>
        <tbody>${filas || '<tr><td colspan="6" class="ob-muted">No se encontraron gastos. Corrige las columnas.</td></tr>'}</tbody></table></div>
      <div class="ob-imp-totales" id="gx-totales"></div>
      <label class="ob-check"><input type="checkbox" id="gx-pagado" checked> Marcar como pagados</label>`;
    this.abrirModal('Revisa los gastos antes de guardarlos', body, () => this._guardarGastosExcel(), 'Guardar gastos');
    this._onPartidaCambio = () => this._totalesGastos();
    this._totalesGastos();
  },
  _cambiarMapeoGas() {
    const m = {};
    document.querySelectorAll('#ob-gx-mapeo select[data-campo]').forEach(s => m[s.dataset.campo] = s.value === '' ? '' : Number(s.value));
    m.fila_encabezado = Number(document.getElementById('ob-gx-fila').value || 1);
    this._gx.mapeo = m; this._leerGastosExcel();
  },
  _asignarVacios() {
    const v = document.getElementById('gx-todo').value;
    if (!this._mapEtiqueta[v]) return this.toast('Elige una partida de la lista', false);
    document.querySelectorAll('#ob-modal input[data-par]').forEach(el => {
      const sel = document.querySelector(`#ob-modal input[data-sel="${el.dataset.par}"]`);
      if (sel.checked && !this._mapEtiqueta[el.value]) { el.value = v; el.classList.remove('ob-falta'); }
    });
    this._totalesGastos();
  },
  _totalesGastos() {
    const el = document.getElementById('gx-totales'); if (!el) return;
    const d = this._gx.datos; let n = 0, monto = 0, faltan = 0;
    document.querySelectorAll('#ob-modal input[data-sel]').forEach(c => {
      if (!c.checked) return;
      const i = c.dataset.sel; n++; monto += d.gastos[i].monto;
      if (!this._mapEtiqueta[document.querySelector(`#ob-modal input[data-par="${i}"]`).value]) faltan++;
    });
    el.innerHTML = `<span><strong>${n}</strong> gastos</span><span>Monto <strong>${this.money(monto)}</strong></span>` +
      (faltan ? `<span class="ob-neg">Faltan ${faltan} por asignar</span>` : (n ? '<span class="ob-pos">Todos asignados ✓</span>' : ''));
  },
  async _guardarGastosExcel() {
    const d = this._gx.datos, gastos = [];
    let faltan = 0;
    const pagado = document.getElementById('gx-pagado').checked;
    document.querySelectorAll('#ob-modal input[data-sel]').forEach(c => {
      if (!c.checked) return;
      const i = c.dataset.sel, g = d.gastos[i];
      const pid = this._idPartida(document.querySelector(`#ob-modal input[data-par="${i}"]`));
      if (!pid) { faltan++; return; }
      gastos.push({ fecha: g.fecha, tipo: document.querySelector(`#ob-modal select[data-tipo="${i}"]`).value, partida_id: pid,
        proveedor: g.proveedor, rnc: g.rnc, documento: g.documento, descripcion: g.descripcion,
        monto: g.monto, itbis: g.itbis, pagado, origen: 'excel' });
    });
    if (faltan) throw new Error(`Faltan ${faltan} gasto(s) por asignar a una partida (o desmárcalos)`);
    if (!gastos.length) throw new Error('Marca al menos un gasto');
    const r = await this._req(`/${this.actual.id}/gastos/lote`, 'POST', { gastos });
    this._onPartidaCambio = null;
    this.toast(`${r.insertados} gastos guardados`); this.cerrarModal();
    await this.refrescarDetalle(); this.irTab('gastos');
  },
  async eliminarGasto(id) {
    if (!confirm('¿Eliminar este gasto?')) return;
    try { await this._req(`/${this.actual.id}/gastos/${id}`, 'DELETE'); this.refrescarDetalle(); }
    catch (e) { this.toast(e.message, false); }
  },

  // ══════════════════════════════════════════════════════════
  // OBRA: crear / editar
  // ══════════════════════════════════════════════════════════
  _formObra(o = {}) {
    const estados = Object.keys(this.ESTADO_LABEL).slice(0, 5).map(k => `<option value="${k}" ${(o.estado || 'planificada') === k ? 'selected' : ''}>${this.ESTADO_LABEL[k]}</option>`).join('');
    return `<div class="bm-form-grid">
      <div class="bm-form-row" style="grid-column:1/-1"><label class="bm-label">Nombre de la obra</label><input class="bm-input" id="f-nombre" value="${this.esc(o.nombre || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">Entidad contratante</label><input class="bm-input" id="f-entidad" value="${this.esc(o.entidad || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">Referencia del proceso</label><input class="bm-input" id="f-referencia" value="${this.esc(o.referencia || '')}" placeholder="CEIZTUR-CCC-CP-2026-0033"></div>
      <div class="bm-form-row"><label class="bm-label">No. de contrato</label><input class="bm-input" id="f-contrato" value="${this.esc(o.contrato_no || '')}"></div>
      <div class="bm-form-row"><label class="bm-label">Monto del contrato</label><input class="bm-input" type="number" step="any" id="f-monto" value="${o.monto_contrato ?? ''}"></div>
      <div class="bm-form-row"><label class="bm-label">Inicio</label><input class="bm-input" type="date" id="f-inicio" value="${o.fecha_inicio || ''}"></div>
      <div class="bm-form-row"><label class="bm-label">Fin</label><input class="bm-input" type="date" id="f-fin" value="${o.fecha_fin || ''}"></div>
      <div class="bm-form-row"><label class="bm-label">Plazo (días)</label><input class="bm-input" type="number" id="f-plazo" value="${o.plazo_dias ?? ''}"></div>
      <div class="bm-form-row"><label class="bm-label">Estado</label><select class="bm-input" id="f-estado">${estados}</select></div>
      <div class="bm-form-row" style="grid-column:1/-1"><label class="bm-label">Notas</label><textarea class="bm-textarea" id="f-notas">${this.esc(o.notas || '')}</textarea></div>
    </div>`;
  },
  _leerObra() {
    const v = id => document.getElementById(id).value.trim();
    const d = {
      nombre: v('f-nombre'), entidad: v('f-entidad') || null, referencia: v('f-referencia') || null,
      contrato_no: v('f-contrato') || null, monto_contrato: Number(v('f-monto') || 0),
      fecha_inicio: v('f-inicio') || null, fecha_fin: v('f-fin') || null,
      plazo_dias: v('f-plazo') ? Number(v('f-plazo')) : null, estado: v('f-estado'), notas: v('f-notas') || null
    };
    if (!d.nombre) throw new Error('El nombre es obligatorio');
    return d;
  },
  nuevaObra() {
    this.abrirModal('Nueva obra', this._formObra(), async () => {
      const o = await this._req('', 'POST', this._leerObra());
      this.cerrarModal(); await this.abrir(o.id); this.irTab('partidas');
    });
  },
  editarObra() {
    this.abrirModal('Editar obra', this._formObra(this.actual), async () => {
      await this._req(`/${this.actual.id}`, 'PUT', this._leerObra());
      this.toast('Obra actualizada'); this.cerrarModal(); this.refrescarDetalle();
    });
  },

  // ══════════════════════════════════════════════════════════
  // MODAL
  // ══════════════════════════════════════════════════════════
  abrirModal(titulo, html, onGuardar, labelGuardar = 'Guardar') {
    document.getElementById('ob-modal-title').textContent = titulo;
    document.getElementById('ob-modal-body').innerHTML = html;
    const f = document.getElementById('ob-modal-footer');
    f.innerHTML = `<button class="bm-btn bm-btn-ghost" onclick="Obras.cerrarModal()">Cerrar</button>` +
      (onGuardar ? `<button class="bm-btn bm-btn-primary" id="ob-modal-guardar">${labelGuardar}</button>` : '');
    if (onGuardar) {
      document.getElementById('ob-modal-guardar').onclick = async (ev) => {
        ev.target.disabled = true;
        try { await onGuardar(); } catch (e) { this.toast(e.message, false); }
        finally { ev.target.disabled = false; }
      };
    }
    document.getElementById('ob-modal').classList.add('bm-active');
  },
  cerrarModal() { this._onPartidaCambio = null; document.getElementById('ob-modal').classList.remove('bm-active'); }
};
