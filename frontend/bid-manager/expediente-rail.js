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
    cont.insertAdjacentHTML(
      'beforeend',
      '<aside class="exp-rail" id="exp-rail" aria-label="Navegación del expediente"></aside>' +
      '<div id="exp-bloqueos"></div>'
    );
    this.montado = true;

    // Prefs primero (proceso activo persiste entre sesiones)
    try { this.prefs = await BidManager._get('/expediente/prefs') || {}; }
    catch { this.prefs = {}; }

    await Promise.all([this._cargarProcesos(), this._cargarResumen()]);
    this._render();
    this._hashInicial();
    window.addEventListener('hashchange', () => this._hashInicial());
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
      this._htmlProceso() +
      this._htmlLente() +
      `<div class="exp-rail-titulo">Secciones del expediente</div>` +
      `<nav class="exp-secciones" aria-label="Secciones">` +
      g.secciones.map(s => this._htmlSeccion(s)).join('') +
      `</nav>`;

    this._bind(rail);
    this._renderBloqueos(p);
    this._marcarActiva(this._panelActual());
  },

  _htmlAnillo(g, p) {
    const R = 34, C = 2 * Math.PI * R;
    const pct = Math.max(0, Math.min(100, g.pct || 0));
    const off = C * (1 - pct / 100);
    const sub = p && p.analizado
      ? `<div class="exp-ring-proceso"><strong>${p.cumplidos} de ${p.total}</strong> requisitos del proceso</div>`
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
        <select disabled aria-label="Proceso activo"><option>Sin procesos analizados</option></select>
        <div class="exp-proceso-vacio">Analiza un pliego desde la pestaña Matching para medir tu expediente contra un proceso.</div>
      </div>`;
    }
    const ops = ['<option value="">— Sin proceso activo —</option>']
      .concat(this.procesos.map(pr => {
        const sel = pr.referencia === act ? ' selected' : '';
        const nom = (pr.nombre_proceso || pr.referencia || '').slice(0, 48);
        return `<option value="${pr.referencia}"${sel}>${pr.referencia} · ${nom}</option>`;
      }));
    return `<div class="exp-proceso">
      <select id="exp-proceso-sel" aria-label="Proceso activo">${ops.join('')}</select>
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

    const sel = rail.querySelector('#exp-proceso-sel');
    if (sel) sel.addEventListener('change', async () => {
      const ref = sel.value || null;
      this.prefs.proceso_activo = ref;
      this._guardarPref({ proceso_activo: ref });
      await this._cargarResumen();
      this._render();
    });

    const chk = rail.querySelector('#exp-lente-chk');
    if (chk) chk.addEventListener('change', () => {
      this.prefs.lente = chk.checked;
      this._guardarPref({ lente: chk.checked });
    });
  },

  ir(sec) {
    const panel = this.PANEL[sec] || sec;
    BidManager.switchPanel(panel);
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
      BidManager.switchPanel(this.PANEL[sec]);
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
