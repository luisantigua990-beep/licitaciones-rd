/* ============================================================
   expediente-rail.js — Rail de navegación del Expediente v2
   ============================================================
   Se activa SOLO si localStorage.expediente_v2 === '1' (rollout
   controlado). Con el flag apagado, la UI actual queda intacta.

   Reglas que respeta (RECOMENDACIONES_PRODUCCION.md):
   - D1: el % del anillo y los "N de M" vienen del backend
     (/api/bid/expediente/resumen-v2). Aquí NO se calcula nada.
   - D4: las metas por sección vienen redactadas del backend.
   - Deep-linking: #bid/{seccion}?ref={referencia}
   - Lente y proceso activo persisten vía /api/bid/expediente/prefs.
   ============================================================ */

const ExpedienteRail = {
  resumen: null,
  prefs: {},
  procesos: [],
  montado: false,

  get activo() {
    try { return localStorage.getItem('expediente_v2') === '1'; }
    catch { return false; }
  },

  // Sección backend (id) → data-panel de la UI existente
  PANEL: {
    empresa: 'empresa', representantes: 'representantes', socios: 'socios',
    certificaciones: 'certificaciones', personal: 'personal',
    equipos: 'equipos', experiencia: 'experiencia', financieros: 'financieros',
  },
  LABEL: {
    empresa: 'Empresa', representantes: 'Representantes', socios: 'Socios',
    certificaciones: 'Documentos', personal: 'Personal',
    equipos: 'Equipos', experiencia: 'Experiencia', financieros: 'Financieros',
  },

  // ── Bootstrap (lo llama BidManager.init con el flag prendido) ──
  async init() {
    if (!this.activo || this.montado) return;
    const cont = document.querySelector('.bm-container');
    if (!cont || typeof BidManager === 'undefined') return;

    this._css();
    cont.classList.add('exp-v2');

    // Reorganizar el DOM: rail a la izquierda + columna de contenido.
    // (El orden del DOM viejo no sirve para el layout del prototipo.)
    const layout = document.createElement('div');
    layout.className = 'exp-layout';
    layout.innerHTML =
      '<aside class="exp-rail" id="exp-rail" aria-label="Navegación del expediente"></aside>' +
      '<div class="exp-main" id="exp-main"><div id="exp-bloqueos"></div></div>';
    cont.appendChild(layout);

    const main = layout.querySelector('#exp-main');
    // Mover alertas y todos los paneles viejos dentro de la columna de contenido
    const alertas = cont.querySelector('#bm-alerts-container');
    if (alertas) main.appendChild(alertas);
    cont.querySelectorAll(':scope > .bm-panel').forEach(p => main.appendChild(p));
    this.montado = true;

    // Prefs primero (proceso activo persiste entre sesiones)
    try { this.prefs = await BidManager._get('/expediente/prefs') || {}; }
    catch { this.prefs = {}; }

    await Promise.all([this._cargarProcesos(), this._cargarResumen()]);
    this._render();
    this._hashInicial();
    window.addEventListener('hashchange', () => this._hashInicial());

    // Tras cualquier BidManager.refresh() (guardar/eliminar en modales),
    // repintar tabla y rail para que no queden con datos viejos.
    if (!BidManager._expV2Wrapped) {
      const orig = BidManager.refresh.bind(BidManager);
      BidManager.refresh = async (...a) => {
        const r = await orig(...a);
        try {
          if (window.ExpedienteTabla?.entidadActual) {
            ExpedienteTabla.invalidar(ExpedienteTabla.entidadActual);
            ExpedienteTabla.mostrar(ExpedienteTabla.entidadActual);
          }
          ExpedienteRail.refresh();
        } catch {}
        return r;
      };
      BidManager._expV2Wrapped = true;
    }
  },

  _css() {
    if (document.getElementById('exp-rail-css')) return;
    const l = document.createElement('link');
    l.id = 'exp-rail-css'; l.rel = 'stylesheet';
    l.href = '/frontend/bid-manager/expediente-rail.css';
    document.head.appendChild(l);
  },

  // ── Data (todo viene calculado del backend — regla D1) ──
  async _cargarResumen() {
    const ref = this.prefs.proceso_activo;
    const q = ref ? `?referencia=${encodeURIComponent(ref)}` : '';
    try { this.resumen = await BidManager._get(`/expediente/resumen-v2${q}`); }
    catch (e) { console.error('resumen-v2:', e); this.resumen = null; }
  },

  async _cargarProcesos() {
    try {
      const rows = await BidManager._get('/matching') || [];
      const vistos = new Set();
      this.procesos = rows.filter(r => {
        if (!r.referencia || vistos.has(r.referencia)) return false;
        vistos.add(r.referencia); return true;
      });
    } catch { this.procesos = []; }
  },

  async refresh() {                 // llamable tras subir docs, etc.
    if (!this.montado) return;
    await this._cargarResumen();
    this._render();
  },

  // ── Render ──
  _render() {
    const rail = document.getElementById('exp-rail');
    if (!rail) return;
    const g = this.resumen?.global || { pct: 0, secciones: [] };
    const p = this.resumen?.proceso;

    rail.innerHTML =
      this._htmlAnillo(g, p) +
      `<div class="exp-rail-titulo">Midiendo contra</div>` +
      this._htmlProceso() +
      this._htmlLente() +
      `<div class="exp-rail-titulo">Secciones del expediente</div>` +
      `<nav class="exp-secciones" aria-label="Secciones">` +
      g.secciones.map(s => this._htmlSeccion(s)).join('') +
      `</nav>` +
      `<div class="exp-footer">
         <button class="exp-generar" disabled title="Disponible en la próxima fase">Generar Sobre A</button>
         <div class="exp-footer-nota">Generación granular — próximamente</div>
       </div>`;

    this._bind(rail);
    this._renderBloqueos(p);
    this._renderAlertas();
    this._marcarActiva(this._panelActual());
  },

  // ── Alertas con nombre y apellido: qué documento y a cuánto está ──
  _renderAlertas() {
    const box = document.getElementById('bm-alerts-container');
    const certs = BidManager.data?.certificaciones || [];
    if (!box || !certs.length) return;
    const esc = s => String(s ?? '').replace(/[&<>"']/g, c =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const fmt = f => { try {
      return new Date(f + 'T12:00:00').toLocaleDateString('es-DO', { day: '2-digit', month: 'short' });
    } catch { return f || ''; } };
    let ocultas = {};
    try { ocultas = JSON.parse(localStorage.getItem('exp_alertas_ocultas') || '{}'); } catch {}
    const clave = c => `${c.id}:${c.fecha_vencimiento || ''}`;   // si renueva, la alerta vuelve
    const vencidos = certs.filter(c => c.estado === 'vencido' && !ocultas[clave(c)]);
    const porVencer = certs.filter(c => c.estado === 'por_vencer' && !ocultas[clave(c)]);
    if (!vencidos.length && !porVencer.length) { box.innerHTML = ''; return; }

    const item = (c, tono) => `
      <div class="exp-al-fila">
        <button class="exp-al-item" data-al-filtro="${tono === 'err' ? 'vencido' : 'por_vencer'}">
          <span>${esc(c.nombre_display || c.tipo)}</span>
          <span class="exp-al-fecha">${tono === 'err' ? 'venció' : 'vence'} ${fmt(c.fecha_vencimiento)} →</span>
        </button>
        <button class="exp-al-x" data-al-x="${esc(clave(c))}" title="Descartar esta alerta">✕</button>
      </div>`;
    box.innerHTML =
      (vencidos.length ? `<div class="exp-alerta exp-alerta-err">
          <div class="exp-al-t">Vencido${vencidos.length > 1 ? 's' : ''} — renovar para poder ofertar</div>
          ${vencidos.map(c => item(c, 'err')).join('')}
        </div>` : '') +
      (porVencer.length ? `<div class="exp-alerta exp-alerta-warn">
          <div class="exp-al-t">Por vencer en los próximos días</div>
          ${porVencer.map(c => item(c, 'warn')).join('')}
        </div>` : '');
    box.querySelectorAll('[data-al-filtro]').forEach(b =>
      b.addEventListener('click', () => this.ir('certificaciones', b.dataset.alFiltro)));
    box.querySelectorAll('[data-al-x]').forEach(b =>
      b.addEventListener('click', ev => {
        ev.stopPropagation();
        ocultas[b.dataset.alX] = true;
        try { localStorage.setItem('exp_alertas_ocultas', JSON.stringify(ocultas)); } catch {}
        this._renderAlertas();
      }));
  },

  _htmlAnillo(g, p) {
    const R = 34, C = 2 * Math.PI * R;
    const pct = Math.max(0, Math.min(100, g.pct || 0));
    const off = C * (1 - pct / 100);
    const sub = p && p.analizado
      ? (p.total > 0
        ? `<div class="exp-ring-proceso"><strong>${p.cumplidos} de ${p.total}</strong> requisitos del proceso</div>`
        : `<div class="exp-ring-proceso">Este pliego no tiene requisitos medibles extraídos</div>`)
      : (p && !p.analizado
        ? `<div class="exp-ring-proceso">Pliego sin analizar aún</div>`
        : `<div class="exp-ring-proceso">Elige un proceso para medirte contra su pliego</div>`);
    return `
      <div class="exp-ring-wrap">
        <svg class="exp-ring" viewBox="0 0 84 84" role="img" aria-label="Completitud ${pct}%">
          <circle class="exp-ring-bg" cx="42" cy="42" r="${R}"></circle>
          <circle class="exp-ring-fg" cx="42" cy="42" r="${R}"
                  stroke-dasharray="${C.toFixed(1)}" stroke-dashoffset="${off.toFixed(1)}"></circle>
        </svg>
        <div>
          <div class="exp-ring-pct">${pct}%</div>
          <div class="exp-ring-label">Expediente completo</div>
          ${sub}
        </div>
      </div>`;
  },

  _htmlProceso() {
    const act = this.prefs.proceso_activo || '';
    if (!this.procesos.length) {
      return `<div class="exp-proceso">
        <div class="exp-proceso-vacio">Analiza un pliego desde la pestaña Matching para medir tu expediente contra un proceso.</div>
      </div>`;
    }
    const visibles = this._verMas ? this.procesos : this.procesos.slice(0, 5);
    const items = visibles.map(pr => {
      const on = pr.referencia === act;
      const nom = (pr.nombre_proceso || '').slice(0, 60);
      return `<button class="exp-pr-item ${on ? 'exp-pr-on' : ''}" data-ref="${pr.referencia}">
        <span class="exp-proceso-ref">${pr.referencia}</span>
        <span class="exp-pr-nombre">${nom}</span>
        ${on ? `<span class="exp-pr-ver" data-ver="${pr.referencia}">Ver análisis del pliego →</span>` : ''}
      </button>`;
    }).join('');
    const resto = this.procesos.length - 5;
    return `<div class="exp-proceso">
      ${items}
      ${act ? `<button class="exp-pr-quitar" id="exp-pr-quitar">× Quitar proceso activo</button>` : ''}
      ${resto > 0 ? `<button class="exp-pr-mas" id="exp-pr-mas">${this._verMas ? 'Ver menos' : `Ver ${resto} más`}</button>` : ''}
      <div class="exp-pr-add">
        <input id="exp-pr-codigo" placeholder="Código de otro proceso… (LPN-…)" autocomplete="off">
        <button id="exp-pr-medir" title="Descarga el pliego, extrae requisitos y los cruza con tu expediente">Medir</button>
      </div>
      <div class="exp-proceso-vacio" id="exp-pr-msg"></div>
    </div>`;
  },

  _htmlLente() {
    const on = this.prefs.lente !== false; // default: prendida
    return `<label class="exp-lente">
      <span>Lente del proceso</span>
      <input type="checkbox" id="exp-lente-chk" ${on ? 'checked' : ''}>
    </label>`;
  },

  _htmlSeccion(s) {
    const label = this.LABEL[s.id] || s.id;
    return `
      <button class="exp-sec" data-sec="${s.id}" role="link"
              aria-label="${label}: ${s.meta}">
        <span class="exp-dot ${s.kind}"></span>
        <span class="exp-sec-body">
          <span class="exp-sec-label">${label}</span>
          <span class="exp-sec-meta ${s.kind}">${s.meta}</span>
        </span>
        <span class="exp-sec-count">${s.count}</span>
        <span class="exp-sec-bar"><i style="width:${s.pct}%"></i></span>
      </button>`;
  },

  _renderBloqueos(p) {
    const box = document.getElementById('exp-bloqueos');
    if (!box) return;
    if (!p || !p.analizado || !(p.bloqueos || []).length) { box.innerHTML = ''; return; }
    box.innerHTML = `
      <div class="exp-bloqueos-card" role="alert">
        <div class="exp-bloqueos-titulo">Esto bloquea tu oferta en ${p.referencia}</div>
        ${p.bloqueos.map(b => `
          <div class="exp-bloqueo">
            <span>${b.titulo}</span>
            <button data-sec="${b.seccion}">Resolver ahora</button>
          </div>`).join('')}
      </div>`;
    box.querySelectorAll('button[data-sec]').forEach(btn =>
      btn.addEventListener('click', () => this.ir(btn.dataset.sec)));
  },

  // ── Navegación / deep-linking ──
  _bind(rail) {
    rail.querySelectorAll('.exp-sec').forEach(el =>
      el.addEventListener('click', () => this.ir(el.dataset.sec)));

    const elegir = async (ref) => {
      this.prefs.proceso_activo = ref;
      this._guardarPref({ proceso_activo: ref });
      await this._cargarResumen();
      this._render();
    };
    rail.querySelectorAll('.exp-pr-item').forEach(b =>
      b.addEventListener('click', () => elegir(b.dataset.ref)));
    rail.querySelectorAll('[data-ver]').forEach(v =>
      v.addEventListener('click', ev => {
        ev.stopPropagation();
        if (typeof window.verDetalle !== 'function') return;
        // verDetalle oculta los tabs "clásicos" pero no conoce el del
        // Expediente: lo ocultamos nosotros y limpiamos el hash #bid
        // para que al volver no re-entre en bucle.
        ExpedienteTabla?.cerrarPanel?.();
        const tabBid = document.getElementById('tab-bid');
        if (tabBid) tabBid.style.display = 'none';
        try { history.replaceState(null, '', '#detalle'); } catch {}
        window.verDetalle(v.dataset.ver);
      }));
    rail.querySelector('#exp-pr-quitar')?.addEventListener('click', () => elegir(null));
    rail.querySelector('#exp-pr-mas')?.addEventListener('click', () => {
      this._verMas = !this._verMas; this._render();
    });
    const inputC = rail.querySelector('#exp-pr-codigo');
    const btnM = rail.querySelector('#exp-pr-medir');
    const msg = rail.querySelector('#exp-pr-msg');
    const medir = async () => {
      const codigo = (inputC?.value || '').trim();
      if (!codigo) return;
      btnM.disabled = true; btnM.textContent = 'Midiendo…';
      if (msg) msg.textContent = 'Descargando pliego y cruzando con tu expediente (puede tomar ~1 min)…';
      try {
        const r = await BidManager._fetchAuth('/api/bid/matching/analizar-proceso', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ codigo_proceso: codigo }),
        });
        if (!r.ok) {
          const e = await r.json().catch(() => null);
          if (msg) msg.textContent = e?.detail?.message || e?.detail ||
            'No se pudo medir contra ese proceso. Revisa el código.';
        } else {
          await this._cargarProcesos();
          await elegir(codigo);
          return;
        }
      } catch { if (msg) msg.textContent = 'Error de conexión.'; }
      btnM.disabled = false; btnM.textContent = 'Medir';
    };
    btnM?.addEventListener('click', medir);
    inputC?.addEventListener('keydown', ev => { if (ev.key === 'Enter') medir(); });

    const chk = rail.querySelector('#exp-lente-chk');
    if (chk) chk.addEventListener('change', () => {
      this.prefs.lente = chk.checked;
      this._guardarPref({ lente: chk.checked });
    });
  },

  ir(sec, filtro = null) {
    const panel = this.PANEL[sec] || sec;
    if (window.ExpedienteTabla && ExpedienteTabla.esTabla(panel)) {
      ExpedienteTabla.mostrar(panel, filtro);
    } else {
      if (window.ExpedienteTabla) ExpedienteTabla.ocultar();
      BidManager.switchPanel(panel);
    }
    this._marcarActiva(panel);
    const ref = this.prefs.proceso_activo;
    const hash = `#bid/${panel}` + (ref ? `?ref=${encodeURIComponent(ref)}` : '');
    if (location.hash !== hash) history.replaceState(null, '', hash);
    this._guardarPref({ seccion_activa: panel });
  },

  _panelActual() {
    const m = (location.hash || '').match(/^#bid\/([a-z]+)/);
    return (m && this.PANEL[m[1]]) ? m[1]
      : (this.prefs.seccion_activa || 'empresa');
  },

  _hashInicial() {
    const m = (location.hash || '').match(/^#bid\/([a-z]+)(?:\?ref=([^&]+))?/);
    if (!m) return;
    const [, sec, ref] = m;
    if (ref && decodeURIComponent(ref) !== this.prefs.proceso_activo) {
      this.prefs.proceso_activo = decodeURIComponent(ref);
      this._guardarPref({ proceso_activo: this.prefs.proceso_activo });
      this._cargarResumen().then(() => this._render());
    }
    if (this.PANEL[sec]) {
      const panel = this.PANEL[sec];
      if (window.ExpedienteTabla && ExpedienteTabla.esTabla(panel)) {
        ExpedienteTabla.mostrar(panel);
      } else {
        if (window.ExpedienteTabla) ExpedienteTabla.ocultar();
        BidManager.switchPanel(panel);
      }
      this._marcarActiva(sec);
    }
  },

  _marcarActiva(sec) {
    document.querySelectorAll('.exp-sec').forEach(el =>
      el.classList.toggle('exp-activa', el.dataset.sec === sec));
  },

  _guardarPref(obj) {
    // Optimista con rollback silencioso: si falla, la pref local queda
    // pero no rompe nada (regla §7.3 — prefs no son críticas).
    BidManager._fetchAuth('/api/bid/expediente/prefs', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(obj),
    }).catch(() => {});
  },
};

window.ExpedienteRail = ExpedienteRail;
