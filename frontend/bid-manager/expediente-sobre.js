/* ============================================================
   expediente-sobre.js — Fase 4: Generar Sobre A (granular)
   ============================================================
   Se activa SOLO con localStorage.expediente_v2 === '1'.

   - Activa el botón negro "Generar Sobre A" del rail (Fase 1 lo
     dejó disabled con "próximamente").
   - Modal 2 pestañas (Generar / Historial) según el prototipo del
     handoff: checkboxes por pieza con estado D2 visible, bloqueadas
     explicando por qué, hint de pendientes, formato docx/pdf.
   - POST /sobre-a/generar → job 202 → polling 1.5s con pieza_actual.
   - El job sigue en el servidor aunque se cierre el modal (queda en
     Historial y llega push al terminar).
   - Formularios del portal (Fase 4b): botón para buscar en el portal
     y subida de la versión llenada por formulario.
   - Reglas: tokens CSS solamente, prefijo exp-, sin frameworks,
     todo valor pasa por _esc().
   ============================================================ */

const ExpedienteSobre = {
  piezas: [],
  meta: null,
  referencia: null,
  jobActivo: null,
  pollTimer: null,
  tab: 'generar',

  get activo() {
    try { return localStorage.getItem('expediente_v2') === '1'; }
    catch { return false; }
  },

  _esc(v) {
    return String(v ?? '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
  },

  // ── Bootstrap: CSS propio + activar el botón del rail ─────
  init() {
    if (!this.activo) return;
    if (!document.querySelector('link[href*="expediente-sobre.css"]')) {
      const l = document.createElement('link');
      l.rel = 'stylesheet';
      l.href = '/frontend/bid-manager/expediente-sobre.css';
      document.head.appendChild(l);
    }
    this._activarBoton();
    // El rail se monta async: observar hasta que aparezca el botón
    if (!this._obs) {
      this._obs = new MutationObserver(() => this._activarBoton());
      this._obs.observe(document.body, { childList: true, subtree: true });
    }
  },

  _activarBoton() {
    const btn = document.querySelector('.exp-generar');
    if (!btn || btn.dataset.sobreListo) return;
    btn.dataset.sobreListo = '1';
    btn.disabled = false;
    btn.removeAttribute('title');
    btn.addEventListener('click', () => this.abrir());
    const nota = document.querySelector('.exp-footer-nota');
    if (nota) nota.textContent = 'Generación granular por piezas';
  },

  // ── Modal ─────────────────────────────────────────────────
  abrir() {
    const ref = (window.ExpedienteRail && ExpedienteRail.prefs &&
                 ExpedienteRail.prefs.proceso_activo) || null;
    if (!ref) {
      alert('Primero elige un proceso en "MIDIENDO CONTRA" para generar su Sobre A.');
      return;
    }
    this.referencia = ref;
    this.tab = 'generar';
    this._render();
    this._cargarPiezas();
    this._escHandler = (e) => { if (e.key === 'Escape') this.cerrar(); };
    document.addEventListener('keydown', this._escHandler);
  },

  cerrar() {
    this._pararPoll();
    document.removeEventListener('keydown', this._escHandler || (() => {}));
    const m = document.getElementById('exp-sobre-overlay');
    if (m) m.remove();
    document.body.classList.remove('exp-sobre-abierto');
  },

  _render() {
    let ov = document.getElementById('exp-sobre-overlay');
    if (!ov) {
      ov = document.createElement('div');
      ov.id = 'exp-sobre-overlay';
      ov.addEventListener('click', (e) => { if (e.target === ov) this.cerrar(); });
      document.body.appendChild(ov);
      document.body.classList.add('exp-sobre-abierto');
    }
    ov.innerHTML = `
      <div class="exp-sobre-modal" role="dialog" aria-modal="true"
           aria-label="Generar Sobre A">
        <div class="exp-sobre-head">
          <div>
            <div class="exp-sobre-titulo">Generar Sobre A</div>
            <div class="exp-sobre-ref">${this._esc(this.referencia)}</div>
          </div>
          <button class="exp-sobre-x" aria-label="Cerrar"
                  onclick="ExpedienteSobre.cerrar()">&times;</button>
        </div>
        <div class="exp-sobre-tabs">
          <button class="exp-sobre-tab ${this.tab === 'generar' ? 'act' : ''}"
                  onclick="ExpedienteSobre._irTab('generar')">Generar</button>
          <button class="exp-sobre-tab ${this.tab === 'historial' ? 'act' : ''}"
                  onclick="ExpedienteSobre._irTab('historial')">Historial</button>
        </div>
        <div class="exp-sobre-cuerpo" id="exp-sobre-cuerpo">
          <div class="exp-sobre-cargando">Cargando…</div>
        </div>
      </div>`;
  },

  _irTab(tab) {
    this.tab = tab;
    document.querySelectorAll('.exp-sobre-tab').forEach((b, i) =>
      b.classList.toggle('act', (i === 0) === (tab === 'generar')));
    if (tab === 'generar') {
      this.jobActivo ? this._pintarProgreso() : this._pintarGenerar();
    } else {
      this._cargarHistorial();
    }
  },

  // ── Pestaña Generar ───────────────────────────────────────
  async _cargarPiezas() {
    try {
      const q = `?referencia=${encodeURIComponent(this.referencia)}`;
      const data = await BidManager._get(`/expediente/sobre-a/piezas${q}`);
      this.piezas = data.piezas || [];
      this.meta = data;
    } catch (e) {
      this.piezas = [];
      this.meta = null;
    }
    if (this.tab === 'generar' && !this.jobActivo) this._pintarGenerar();
  },

  _pintarGenerar() {
    const c = document.getElementById('exp-sobre-cuerpo');
    if (!c) return;
    if (!this.meta) {
      c.innerHTML = `<div class="exp-sobre-vacio">No se pudo cargar el catálogo.
        <button class="exp-sobre-link" onclick="ExpedienteSobre._cargarPiezas()">Reintentar</button></div>`;
      return;
    }
    // Agrupar por sección respetando orden de llegada
    const secciones = [];
    const porSec = {};
    for (const p of this.piezas) {
      const s = p.seccion || 'Otros';
      if (!porSec[s]) { porSec[s] = []; secciones.push(s); }
      porSec[s].push(p);
    }
    const warns = this.piezas.filter(p => p.estado === 'warn').length;
    const errs = this.piezas.filter(p => p.estado === 'err').length;
    const pdfOn = !!this.meta.pdf_disponible;

    let html = '';
    for (const s of secciones) {
      const items = porSec[s];
      html += `
        <div class="exp-sobre-seccion">
          <div class="exp-sobre-sec-head">
            <span>${this._esc(s)}</span>
            <button class="exp-sobre-link"
                    onclick="ExpedienteSobre._toggleSeccion('${this._esc(s)}')">
              marcar / desmarcar</button>
          </div>
          ${items.map(p => this._filaPieza(p)).join('')}
        </div>`;
    }

    c.innerHTML = `
      <div class="exp-sobre-scroll">${html}</div>
      <div class="exp-sobre-pie">
        ${warns ? `<div class="exp-sobre-hint warn">⚠ ${warns} requisito${warns > 1 ? 's' : ''} pendiente${warns > 1 ? 's' : ''} — se generará con huecos marcados</div>` : ''}
        ${errs ? `<div class="exp-sobre-hint err">✕ ${errs} pieza${errs > 1 ? 's' : ''} bloqueada${errs > 1 ? 's' : ''} (vencidas o sin archivo válido) — no entran al ZIP</div>` : ''}
        <div class="exp-sobre-opciones">
          <label class="exp-sobre-radio">
            <input type="radio" name="exp-sobre-fmt" value="docx" checked>
            Word editable (.docx)</label>
          <label class="exp-sobre-radio ${pdfOn ? '' : 'off'}">
            <input type="radio" name="exp-sobre-fmt" value="pdf" ${pdfOn ? '' : 'disabled'}>
            PDF ${pdfOn ? '+ Sobre completo fusionado' : '(no disponible en el servidor)'}</label>
          <button class="exp-sobre-sync" title="Buscar en el portal los formularios propios de la institución"
                  onclick="ExpedienteSobre.sincronizar()">↻ Buscar formularios del proceso</button>
        </div>
        <button class="exp-sobre-cta" onclick="ExpedienteSobre.generar()">
          Generar Sobre A</button>
      </div>`;
  },

  _filaPieza(p) {
    const bloqueada = p.estado === 'err' || p.tipo === 'aviso';
    const marcada = !bloqueada;
    const llenado = p.id.startsWith('formproc:')
      ? `<button class="exp-sobre-mini" title="Subir versión llenada"
                 onclick="ExpedienteSobre.subirLlenado('${this._esc(p.registro_id)}')">⬆ llenado</button>`
      : '';
    return `
      <label class="exp-sobre-fila ${bloqueada ? 'bloq' : ''}">
        <input type="checkbox" data-pieza="${this._esc(p.id)}"
               ${marcada ? 'checked' : ''} ${bloqueada ? 'disabled' : ''}>
        <span class="exp-dot ${this._esc(p.estado)}"></span>
        <span class="exp-sobre-nombre">${this._esc(p.nombre)}</span>
        ${llenado}
        ${p.motivo ? `<span class="exp-sobre-motivo">${this._esc(p.motivo)}</span>` : ''}
      </label>`;
  },

  _toggleSeccion(sec) {
    const filas = this.piezas.filter(p => (p.seccion || 'Otros') === sec && p.estado !== 'err' && p.tipo !== 'aviso');
    const boxes = filas.map(p =>
      document.querySelector(`input[data-pieza="${CSS.escape(p.id)}"]`)).filter(Boolean);
    const todos = boxes.every(b => b.checked);
    boxes.forEach(b => { b.checked = !todos; });
  },

  // ── Generar + polling ─────────────────────────────────────
  async generar() {
    const marcadas = [...document.querySelectorAll('input[data-pieza]:checked')]
      .map(i => i.dataset.pieza);
    if (!marcadas.length) { alert('Marca al menos una pieza.'); return; }
    const todasElegibles = this.piezas.filter(p => p.estado !== 'err' && p.tipo !== 'aviso').length;
    const fmt = (document.querySelector('input[name="exp-sobre-fmt"]:checked') || {}).value || 'docx';
    const body = {
      referencia: this.referencia,
      piezas: marcadas.length === todasElegibles ? 'todas' : marcadas,
      formato: fmt,
    };
    try {
      const r = await BidManager._fetchAuth('/api/bid/expediente/sobre-a/generar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (r.status === 429) { alert('Límite alcanzado: máximo 10 generaciones por hora.'); return; }
      if (!r.ok) throw new Error(r.status);
      const data = await r.json();
      this.jobActivo = { id: data.job_id, estado: 'en_cola', progreso: {} };
      this._pintarProgreso();
      this._arrancarPoll();
    } catch (e) {
      alert('No se pudo iniciar la generación. Inténtalo de nuevo.');
    }
  },

  _arrancarPoll() {
    this._pararPoll();
    this.pollTimer = setInterval(() => this._poll(), 1500);
    this._poll();
  },

  _pararPoll() {
    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
  },

  async _poll() {
    if (!this.jobActivo) return;
    try {
      const job = await BidManager._get(`/expediente/sobre-a/jobs/${this.jobActivo.id}`);
      this.jobActivo = job;
      if (job.estado === 'listo' || job.estado === 'error') this._pararPoll();
      if (this.tab === 'generar') this._pintarProgreso();
    } catch (e) { /* siguiente tick */ }
  },

  _pintarProgreso() {
    const c = document.getElementById('exp-sobre-cuerpo');
    if (!c || !this.jobActivo) return;
    const j = this.jobActivo;
    const pr = j.progreso || {};
    const total = pr.total || 0;
    const hechas = pr.completadas || 0;
    const pct = total ? Math.round(hechas / total * 100) : 5;

    if (j.estado === 'listo') {
      const res = j.resultado || {};
      const pend = j.pendientes || [];
      c.innerHTML = `
        <div class="exp-sobre-listo">
          <div class="exp-sobre-check">✓</div>
          <div class="exp-sobre-listo-titulo">Sobre A listo</div>
          <div class="exp-sobre-listo-nombre">${this._esc(res.nombre || '')}</div>
          ${res.url_firmada
            ? `<a class="exp-sobre-cta" href="${this._esc(res.url_firmada)}"
                  target="_blank" rel="noopener">⬇ Descargar ZIP</a>`
            : '<div class="exp-sobre-motivo">El enlace expiró — ábrelo desde Historial.</div>'}
          ${pend.length ? `
            <div class="exp-sobre-pend">
              <div class="exp-sobre-pend-t">Hoja de pendientes (${pend.length})</div>
              ${pend.map(p => `<div class="exp-sobre-pend-i">• ${this._esc(p.nombre)}${p.motivo ? ` — ${this._esc(p.motivo)}` : ''}</div>`).join('')}
            </div>` : ''}
          <button class="exp-sobre-link" onclick="ExpedienteSobre._reiniciar()">
            Generar otro</button>
        </div>`;
      return;
    }
    if (j.estado === 'error') {
      c.innerHTML = `
        <div class="exp-sobre-listo">
          <div class="exp-sobre-check err">✕</div>
          <div class="exp-sobre-listo-titulo">La generación falló</div>
          <div class="exp-sobre-motivo">${this._esc(j.error || 'Error desconocido')}</div>
          <button class="exp-sobre-cta" onclick="ExpedienteSobre._reiniciar()">
            Intentar de nuevo</button>
        </div>`;
      return;
    }
    c.innerHTML = `
      <div class="exp-sobre-prog">
        <div class="exp-sobre-prog-txt">
          ${j.estado === 'en_cola' ? 'En cola…' :
            `Generando: ${this._esc(pr.pieza_actual || '…')}`}
        </div>
        <div class="exp-sobre-barra"><div class="exp-sobre-barra-fill"
             style="width:${pct}%"></div></div>
        <div class="exp-sobre-prog-num">${hechas} de ${total || '…'} piezas</div>
        <div class="exp-sobre-motivo">Puedes cerrar esta ventana: el trabajo sigue
          en el servidor y te llegará una notificación al terminar.</div>
      </div>`;
  },

  _reiniciar() {
    this.jobActivo = null;
    this._pararPoll();
    this._pintarGenerar();
  },

  // ── Pestaña Historial ─────────────────────────────────────
  async _cargarHistorial() {
    const c = document.getElementById('exp-sobre-cuerpo');
    if (!c) return;
    c.innerHTML = '<div class="exp-sobre-cargando">Cargando…</div>';
    let jobs = [];
    try {
      const q = `?referencia=${encodeURIComponent(this.referencia)}`;
      jobs = (await BidManager._get(`/expediente/sobre-a/jobs${q}`)).jobs || [];
    } catch (e) { /* vacío */ }
    if (!jobs.length) {
      c.innerHTML = '<div class="exp-sobre-vacio">Todavía no has generado ningún Sobre A para este proceso.</div>';
      return;
    }
    const ETIQ = { listo: 'Listo', procesando: 'Procesando', en_cola: 'En cola',
                   error: 'Error', expirado: 'Expirado' };
    c.innerHTML = `<div class="exp-sobre-scroll">` + jobs.map(j => `
      <div class="exp-sobre-hjob">
        <span class="exp-dot ${j.estado === 'listo' ? 'ok' : j.estado === 'error' ? 'err' : 'warn'}"></span>
        <div class="exp-sobre-hinfo">
          <div class="exp-sobre-hnombre">${this._esc(j.nombre || `Sobre A (${j.formato})`)}</div>
          <div class="exp-sobre-hfecha">${this._esc((j.creado_en || '').slice(0, 16).replace('T', ' '))}
            · ${ETIQ[j.estado] || this._esc(j.estado)}
            ${j.pendientes ? ` · ${j.pendientes} pendientes` : ''}</div>
        </div>
        ${j.estado === 'listo'
          ? `<button class="exp-sobre-mini" onclick="ExpedienteSobre.descargarJob('${this._esc(j.id)}')">⬇</button>`
          : ''}
      </div>`).join('') + `</div>`;
  },

  async descargarJob(id) {
    try {
      const job = await BidManager._get(`/expediente/sobre-a/jobs/${id}`);
      const url = job.resultado && job.resultado.url_firmada;
      if (url) window.open(url, '_blank', 'noopener');
      else alert('El archivo expiró (los ZIP viven 7 días). Genera uno nuevo.');
    } catch (e) {
      alert('No se pudo obtener el enlace de descarga.');
    }
  },

  // ── Fase 4b: portal ───────────────────────────────────────
  async sincronizar() {
    const btn = document.querySelector('.exp-sobre-sync');
    if (btn) { btn.disabled = true; btn.textContent = 'Buscando en el portal…'; }
    try {
      const q = `?referencia=${encodeURIComponent(this.referencia)}`;
      const r = await BidManager._fetchAuth(
        `/api/bid/expediente/sobre-a/sincronizar-formularios${q}`, { method: 'POST' });
      const data = r.ok ? await r.json() : {};
      const n = (data.descargados || []).length;
      alert(n ? `Se descargaron ${n} formulario(s) del portal.` :
        'No se encontraron formularios nuevos en el portal de este proceso.');
    } catch (e) {
      alert('No se pudo consultar el portal. Inténtalo más tarde.');
    }
    await this._cargarPiezas();
  },

  subirLlenado(formularioId) {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.pdf,.doc,.docx,.xlsx,.jpg,.jpeg,.png';
    input.onchange = async () => {
      const f = input.files && input.files[0];
      if (!f) return;
      const fd = new FormData();
      fd.append('archivo', f);
      try {
        const r = await BidManager._fetchAuth(
          `/api/bid/expediente/sobre-a/formularios/${encodeURIComponent(formularioId)}/llenado`,
          { method: 'POST', body: fd });
        if (r.status === 415) { alert('El archivo no es válido (el contenido no coincide con su tipo).'); return; }
        if (!r.ok) throw new Error(r.status);
        await this._cargarPiezas();
      } catch (e) {
        alert('No se pudo subir el archivo llenado.');
      }
    };
    input.click();
  },
};

// Arranque: mismo patrón del rail (flag + DOM listo)
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => ExpedienteSobre.init());
} else {
  ExpedienteSobre.init();
}
window.ExpedienteSobre = ExpedienteSobre;
