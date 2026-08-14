/* ============================================================
   BID MANAGER — Lógica encapsulada
   ============================================================
   Todo dentro del objeto BidManager para no contaminar el scope
   global (el index.html ya tiene 1584+ declaraciones).
   ============================================================ */

const BidManager = {
  // ── Estado ────────────────────────────────────────────
  loaded: false,
  htmlLoaded: false,
  currentTab: 'empresa',
  currentFinTab: 'indicadores',
  data: {
    empresa: null,
    representantes: [],
    socios: [],
    certificaciones: [],
    personal: [],
    equipos: [],
    experiencia: [],
    financieros: [],
    ir2: [],
    capacidad: [],
    referencias: [],
    alertas: [],
    resumen: null
  },
  editing: null, // {tipo, id} cuando se está editando algo

  // ── Auth ──────────────────────────────────────────────
  _token() {
    return localStorage.getItem('sb_token');
  },

  _authHeaders(json = true) {
    const h = { 'Authorization': 'Bearer ' + this._token() };
    if (json) h['Content-Type'] = 'application/json';
    return h;
  },

  // ── API helpers ───────────────────────────────────────
  async _get(path) {
    const r = await fetch(`/api/bid${path}`, { headers: this._authHeaders(false) });
    if (!r.ok) throw new Error(`GET ${path} → ${r.status}`);
    return r.json();
  },

  async _post(path, body) {
    const r = await fetch(`/api/bid${path}`, {
      method: 'POST',
      headers: this._authHeaders(),
      body: JSON.stringify(body)
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `POST ${path} → ${r.status}`);
    }
    return r.json();
  },

  async _put(path, body) {
    const r = await fetch(`/api/bid${path}`, {
      method: 'PUT',
      headers: this._authHeaders(),
      body: JSON.stringify(body)
    });
    if (!r.ok) throw new Error(`PUT ${path} → ${r.status}`);
    return r.json();
  },

  async _delete(path) {
    const r = await fetch(`/api/bid${path}`, {
      method: 'DELETE',
      headers: this._authHeaders(false)
    });
    if (!r.ok) throw new Error(`DELETE ${path} → ${r.status}`);
    return r.json();
  },

  // ── Init: se llama la primera vez que se abre el tab ─
  async init() {
    if (this.loaded) return;

    // Cargar el HTML del módulo dentro de #tab-bid
    if (!this.htmlLoaded) {
      const container = document.getElementById('tab-bid');
      if (!container) {
        console.error('#tab-bid no encontrado');
        return;
      }
      try {
        const r = await fetch('/frontend/bid-manager/bid-manager.html');
        if (!r.ok) throw new Error('HTML no encontrado');
        container.innerHTML = await r.text();
        this.htmlLoaded = true;
        this._bindTabs();
      } catch (e) {
        container.innerHTML = `<div class="bm-container"><div class="bm-empty"><div class="bm-empty-icon">!</div><div class="bm-empty-text">Error cargando el módulo: ${e.message}</div></div></div>`;
        return;
      }
    }

    await this.refresh();
    this.loaded = true;
  },

  // ── Refrescar toda la data ────────────────────────────
  async refresh() {
    try {
      const [empresa, reps, socios, certs, pers, equip, exp, fin, ir2, cap, refs, alertas, resumen] = await Promise.all([
        this._get('/empresa'),
        this._get('/representantes'),
        this._get('/socios'),
        this._get('/certificaciones'),
        this._get('/personal'),
        this._get('/equipos'),
        this._get('/experiencia'),
        this._get('/financieros'),
        this._get('/ir2'),
        this._get('/capacidad-financiera'),
        this._get('/referencias-comerciales'),
        this._get('/certificaciones/alertas'),
        this._get('/expediente/resumen')
      ]);

      this.data.empresa = empresa.empresa;
      this.data.representantes = reps;
      this.data.socios = socios;
      this.data.certificaciones = certs;
      this.data.personal = pers;
      this.data.equipos = equip;
      this.data.experiencia = exp;
      this.data.financieros = fin;
      this.data.ir2 = ir2;
      this.data.capacidad = cap;
      this.data.referencias = refs;
      this.data.alertas = alertas;
      this.data.resumen = resumen;

      this._renderAll();
    } catch (e) {
      console.error('Error cargando Bid Manager:', e);
      this.toast('Error cargando datos: ' + e.message, 'err');
    }
  },

  // ── Bind de tabs internos ─────────────────────────────
  _bindTabs() {
    // Tabs principales
    document.querySelectorAll('#bm-tabs .bm-tab').forEach(t => {
      t.addEventListener('click', () => this.switchPanel(t.dataset.panel));
    });
    // Sub-tabs financieros
    document.querySelectorAll('[data-fin-panel]').forEach(t => {
      if (t.tagName === 'BUTTON') {
        t.addEventListener('click', () => this.switchFinPanel(t.dataset.finPanel));
      }
    });
    // Cerrar modal con overlay
    const modal = document.getElementById('bm-modal');
    if (modal) {
      modal.addEventListener('click', (e) => {
        if (e.target === modal) this.cerrarModal();
      });
    }
  },

  switchPanel(panel) {
    this.currentTab = panel;
    document.querySelectorAll('#bm-tabs .bm-tab').forEach(t => {
      t.classList.toggle('bm-active', t.dataset.panel === panel);
    });
    document.querySelectorAll('.bm-panel[data-panel]').forEach(p => {
      p.classList.toggle('bm-active', p.dataset.panel === panel);
    });
  },

  switchFinPanel(panel) {
    this.currentFinTab = panel;
    document.querySelectorAll('[data-fin-panel]').forEach(el => {
      if (el.tagName === 'BUTTON') {
        el.classList.toggle('bm-active', el.dataset.finPanel === panel);
      } else {
        el.style.display = el.dataset.finPanel === panel ? 'block' : 'none';
        el.classList.toggle('bm-active', el.dataset.finPanel === panel);
      }
    });
  },

  // ── Render principal ──────────────────────────────────
  _renderAll() {
    this._renderProgress();
    this._renderAlertas();
    this._renderBadges();
    this._renderEmpresa();
    this._renderRepresentantes();
    this._renderSocios();
    this._renderCertificaciones();
    this._renderPersonal();
    this._renderEquipos();
    this._renderExperiencia();
    this._renderFinancieros();
    this._renderIndicadores();
    this._renderIr2();
    this._renderCapacidad();
    this._renderReferencias();
  },

  _renderProgress() {
    // Calcular completitud: 10 secciones, cada una vale 10%
    const m = this.data.resumen?.modulos || {};
    const emp = this.data.empresa;
    let done = 0;
    if (emp?.razon_social && emp?.rnc) done++;
    if (m.representantes > 0) done++;
    if (m.socios > 0) done++;
    if (m.certificaciones >= 3) done++;
    if (m.personal > 0) done++;
    if (m.equipos > 0) done++;
    if (m.experiencia > 0) done++;
    if (m.estados_financieros > 0) done++;
    if (m.capacidad_financiera > 0) done++;
    if (m.referencias_comerciales > 0) done++;
    
    const pct = Math.round(done * 10);
    const pctEl = document.getElementById('bm-progress-pct');
    const fillEl = document.getElementById('bm-progress-fill');
    if (pctEl) pctEl.textContent = pct + '%';
    if (fillEl) fillEl.style.width = pct + '%';
  },

  _renderAlertas() {
    const cont = document.getElementById('bm-alerts-container');
    if (!cont) return;
    const alertas = this.data.alertas || [];
    if (!alertas.length) { cont.innerHTML = ''; return; }
    
    const vencidas = alertas.filter(a => a.estado === 'vencido');
    const porVencer = alertas.filter(a => a.estado === 'por_vencer');
    
    let html = '';
    if (vencidas.length) {
      html += `<div class="bm-alert"><div class="bm-alert-icon">!</div><div class="bm-alert-text"><strong>${vencidas.length} documento(s) vencido(s)</strong> — necesitan renovación urgente para poder ofertar.</div></div>`;
    }
    if (porVencer.length) {
      html += `<div class="bm-alert" style="background:rgba(245,158,11,0.08);border-color:rgba(245,158,11,0.25);"><div class="bm-alert-icon">!</div><div class="bm-alert-text" style="color:var(--text)"><strong style="color:#F59E0B">${porVencer.length} documento(s) por vencer</strong> — vencen en los próximos 15 días.</div></div>`;
    }
    cont.innerHTML = html;
  },

  _renderBadges() {
    const m = this.data.resumen?.modulos || {};
    const map = {
      'bm-badge-representantes': m.representantes,
      'bm-badge-socios': m.socios,
      'bm-badge-certificaciones': m.certificaciones,
      'bm-badge-personal': m.personal,
      'bm-badge-equipos': m.equipos,
      'bm-badge-experiencia': m.experiencia,
      'bm-badge-financieros': m.estados_financieros
    };
    Object.keys(map).forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = map[id] || 0;
    });
  },

  // ── EMPRESA ───────────────────────────────────────────
  _renderEmpresa() {
    const cont = document.getElementById('bm-empresa-view');
    if (!cont) return;
    const e = this.data.empresa;
    if (!e) {
      cont.innerHTML = `<div class="bm-empty"><div class="bm-empty-icon">B</div><div class="bm-empty-text">Aún no tienes datos de empresa. Completa el onboarding.</div></div>`;
      return;
    }
    cont.innerHTML = `
      <div class="bm-card">
        <div class="bm-card-body" style="line-height:1.9">
          <div><strong>Razón social:</strong> ${e.razon_social || '—'}</div>
          <div><strong>RNC:</strong> ${e.rnc || '—'}</div>
          <div><strong>RPE:</strong> ${e.rpe || '—'}</div>
          <div><strong>Registro Mercantil:</strong> ${e.registro_mercantil_no || '—'}</div>
          <div><strong>Tipo:</strong> ${e.tipo_sociedad || '—'}</div>
          <div><strong>Capital social:</strong> ${e.capital_social ? this._money(e.capital_social, e.moneda_capital) : '—'}</div>
          <div><strong>MIPYME:</strong> ${e.clasificacion_mipyme === 'NO' || e.clasificacion_mipyme === 'GRANDE' ? 'No' : (e.clasificacion_mipyme ? 'Sí' : '—')}</div>
          <div><strong>Provee:</strong> ${e.provee || '—'}</div>
          <div><strong>Domicilio:</strong> ${e.domicilio || '—'}</div>
          <div><strong>Teléfono:</strong> ${e.telefono_principal || '—'}</div>
          <div><strong>Email:</strong> ${e.email_empresa || '—'}</div>
          ${e.objeto_social ? `<div style="margin-top:8px"><strong>Objeto social:</strong><br>${e.objeto_social}</div>` : ''}
        </div>
      </div>
    `;
  },

  editarEmpresa() {
    const e = this.data.empresa || {};
    this._openModal('Editar datos de empresa', `
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Razón social</label><input class="bm-input" id="f-razon_social" value="${this._esc(e.razon_social)}"></div>
        <div class="bm-form-row"><label class="bm-label">RNC</label><input class="bm-input" id="f-rnc" value="${this._esc(e.rnc)}"></div>
        <div class="bm-form-row"><label class="bm-label">RPE</label><input class="bm-input" id="f-rpe" value="${this._esc(e.rpe)}"></div>
        <div class="bm-form-row"><label class="bm-label">Registro Mercantil No.</label><input class="bm-input" id="f-registro_mercantil_no" value="${this._esc(e.registro_mercantil_no)}"></div>
        <div class="bm-form-row"><label class="bm-label">Tipo sociedad</label>
          <select class="bm-select" id="f-tipo_sociedad">
            <option value="">—</option>
            ${['SRL','SA','EIRL','Sociedad Anónima Simplificada'].map(t => `<option ${e.tipo_sociedad===t?'selected':''}>${t}</option>`).join('')}
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Capital social (DOP)</label><input class="bm-input" type="number" id="f-capital_social" value="${e.capital_social || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">¿Es MIPYME?</label>
          <select class="bm-select" id="f-clasificacion_mipyme">
            <option value="">—</option>
            <option value="SI" ${e.clasificacion_mipyme==='SI'||['MICRO','PEQUEÑA','MEDIANA'].includes(e.clasificacion_mipyme)?'selected':''}>Sí</option>
            <option value="NO" ${e.clasificacion_mipyme==='NO'||e.clasificacion_mipyme==='GRANDE'?'selected':''}>No</option>
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Provee</label>
          <select class="bm-select" id="f-provee">
            ${['Obras','Bienes','Servicios','Consultoría'].map(t => `<option ${(e.provee||'Obras')===t?'selected':''}>${t}</option>`).join('')}
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Teléfono principal</label><input class="bm-input" id="f-telefono_principal" value="${this._esc(e.telefono_principal)}"></div>
        <div class="bm-form-row"><label class="bm-label">Email</label><input class="bm-input" type="email" id="f-email_empresa" value="${this._esc(e.email_empresa)}"></div>
      </div>
      <div class="bm-form-row"><label class="bm-label">Domicilio</label><input class="bm-input" id="f-domicilio" value="${this._esc(e.domicilio)}"></div>
      <div class="bm-form-row"><label class="bm-label">Objeto social</label><textarea class="bm-textarea" id="f-objeto_social">${this._esc(e.objeto_social)}</textarea></div>
    `, async () => {
      const data = this._collectForm(['razon_social','rnc','rpe','registro_mercantil_no','tipo_sociedad','capital_social','clasificacion_mipyme','provee','telefono_principal','email_empresa','domicilio','objeto_social']);
      await this._put('/empresa', data);
      this.toast('Empresa actualizada', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  // ── REPRESENTANTES ────────────────────────────────────
  _renderRepresentantes() {
    const cont = document.getElementById('bm-representantes-list');
    if (!cont) return;
    if (!this.data.representantes.length) {
      cont.innerHTML = this._emptyState('R', 'No has agregado representantes legales.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.representantes.map(r => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">${r.nombre_completo}</h4>
            <div class="bm-card-subtitle">${r.cargo}${r.es_firmante_principal ? ' · <span class="bm-badge bm-badge-ok">Firmante principal</span>' : ''}</div>
          </div>
        </div>
        <div class="bm-card-body">
          <div>Cédula: <strong>${r.cedula}</strong></div>
          ${r.profesion ? `<div>Profesión: ${r.profesion}</div>` : ''}
          ${r.telefono ? `<div>Tel: ${r.telefono}</div>` : ''}
          ${r.email ? `<div>Email: ${r.email}</div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirRepresentante('${r.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('representantes', '${r.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirRepresentante(id) {
    const r = id ? this.data.representantes.find(x => x.id === id) : {};
    this.editing = { tipo: 'representantes', id };
    this._openModal(id ? 'Editar representante' : 'Nuevo representante', `
      <div class="bm-form-row"><label class="bm-label">Nombre completo *</label><input class="bm-input" id="f-nombre_completo" value="${this._esc(r.nombre_completo)}"></div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Cédula *</label><input class="bm-input" id="f-cedula" value="${this._esc(r.cedula)}"></div>
        <div class="bm-form-row"><label class="bm-label">Cargo *</label><input class="bm-input" id="f-cargo" placeholder="Ej: Gerente General" value="${this._esc(r.cargo)}"></div>
        <div class="bm-form-row"><label class="bm-label">Profesión</label><input class="bm-input" id="f-profesion" value="${this._esc(r.profesion)}"></div>
        <div class="bm-form-row"><label class="bm-label">Estado civil</label>
          <select class="bm-select" id="f-estado_civil">
            <option value="">—</option>
            ${['SOLTERO','CASADO','DIVORCIADO','VIUDO','UNION LIBRE'].map(t => `<option ${r.estado_civil===t?'selected':''}>${t}</option>`).join('')}
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Teléfono</label><input class="bm-input" id="f-telefono" value="${this._esc(r.telefono)}"></div>
        <div class="bm-form-row"><label class="bm-label">Email</label><input class="bm-input" type="email" id="f-email" value="${this._esc(r.email)}"></div>
      </div>
      <div class="bm-form-row"><label class="bm-label">Dirección</label><input class="bm-input" id="f-direccion" value="${this._esc(r.direccion)}"></div>
      <div class="bm-form-row"><label style="display:flex;align-items:center;gap:8px;font-size:13px;color:var(--text)"><input type="checkbox" id="f-es_firmante_principal" ${r.es_firmante_principal?'checked':''}> Firmante principal (aparece por defecto en las ofertas)</label></div>
      <div class="bm-form-row"><label class="bm-label">Documentos del representante</label>
        <div style="font-size:12px;color:var(--text2,#6b7280);margin-bottom:6px">La firma se usará automáticamente en los documentos que la requieran (SNCC, cartas, declaraciones).</div>
        ${this._docCascade('rep', 'personal', [
          {field:'firma_url', label:'Firma (imagen o PDF)'},
          {field:'cedula_url', label:'Cédula (ambos lados)'}
        ], r)}
      </div>
    `, async () => {
      const data = this._collectForm(['nombre_completo','cedula','cargo','profesion','estado_civil','telefono','email','direccion','firma_url','cedula_url']);
      data.es_firmante_principal = document.getElementById('f-es_firmante_principal').checked;
      if (id) await this._put(`/representantes/${id}`, data);
      else await this._post('/representantes', data);
      this.toast(id ? 'Representante actualizado' : 'Representante agregado', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  // ── SOCIOS ────────────────────────────────────────────
  _renderSocios() {
    const cont = document.getElementById('bm-socios-list');
    if (!cont) return;
    if (!this.data.socios.length) {
      cont.innerHTML = this._emptyState('S', 'No has agregado socios.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.socios.map(s => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">${s.nombre_completo}</h4>
            <div class="bm-card-subtitle">${s.es_gerente ? 'Gerente-Socio' : 'Socio'}</div>
          </div>
          <div class="bm-badge bm-badge-info">${s.porcentaje_participacion || 0}%</div>
        </div>
        <div class="bm-card-body">
          ${s.cedula ? `<div>Cédula: <strong>${s.cedula}</strong></div>` : ''}
          ${s.cantidad_cuotas ? `<div>Cuotas: ${s.cantidad_cuotas.toLocaleString()}</div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirSocio('${s.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('socios', '${s.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirSocio(id) {
    const s = id ? this.data.socios.find(x => x.id === id) : {};
    // Suma de participación de los OTROS socios (excluye el que se está editando)
    const totalOtros = (this.data.socios || [])
      .filter(x => x.id !== id)
      .reduce((acc, x) => acc + (Number(x.porcentaje_participacion) || 0), 0);
    const disponible = Math.max(0, 100 - totalOtros);
    this._openModal(id ? 'Editar socio' : 'Nuevo socio', `
      <div class="bm-form-row"><label class="bm-label">Nombre completo *</label><input class="bm-input" id="f-nombre_completo" value="${this._esc(s.nombre_completo)}"></div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Cédula</label><input class="bm-input" id="f-cedula" value="${this._esc(s.cedula)}"></div>
        <div class="bm-form-row"><label class="bm-label">Estado civil</label>
          <select class="bm-select" id="f-estado_civil">
            <option value="">—</option>
            ${['SOLTERO','CASADO','DIVORCIADO','VIUDO'].map(t => `<option ${s.estado_civil===t?'selected':''}>${t}</option>`).join('')}
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Cantidad cuotas</label><input class="bm-input" type="number" id="f-cantidad_cuotas" value="${s.cantidad_cuotas || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">% participación <span style="color:var(--text2);font-weight:400">(disponible: ${disponible.toFixed(2)}%)</span></label><input class="bm-input" type="number" step="0.01" min="0" max="${disponible.toFixed(2)}" id="f-porcentaje_participacion" value="${s.porcentaje_participacion || ''}"></div>
      </div>
      <div class="bm-form-row"><label style="display:flex;align-items:center;gap:8px;font-size:13px;color:var(--text)"><input type="checkbox" id="f-es_gerente" ${s.es_gerente?'checked':''}> Es gerente</label></div>
    `, async () => {
      const data = this._collectForm(['nombre_completo','cedula','estado_civil','cantidad_cuotas','porcentaje_participacion']);
      const pct = Number(data.porcentaje_participacion) || 0;
      if (pct < 0) throw new Error('El porcentaje no puede ser negativo');
      if (totalOtros + pct > 100.0001) {
        throw new Error(`La participación total excedería 100%. Ya hay ${totalOtros.toFixed(2)}% asignado; máximo disponible: ${disponible.toFixed(2)}%`);
      }
      data.es_gerente = document.getElementById('f-es_gerente').checked;
      if (id) await this._put(`/socios/${id}`, data);
      else await this._post('/socios', data);
      this.toast('Guardado', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  // ── CERTIFICACIONES ───────────────────────────────────
  _renderCertificaciones() {
    const cont = document.getElementById('bm-certificaciones-list');
    if (!cont) return;
    if (!this.data.certificaciones.length) {
      cont.innerHTML = this._emptyState('D', 'No has subido documentos aún. Sube DGII, TSS, RPE, Estatutos, etc.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.certificaciones.map(c => {
      // Estado SIEMPRE calculado por fecha real, no por el campo guardado:
      //   sin fecha_vencimiento → NO VENCE (documento permanente)
      //   fecha < hoy → VENCIDO | fecha <= hoy+7d → POR VENCER | resto → VIGENTE
      let estadoCalc, badge;
      if (!c.fecha_vencimiento) {
        estadoCalc = 'NO VENCE';
        badge = 'bm-badge-ok';
      } else {
        const hoy = new Date(); hoy.setHours(0,0,0,0);
        const fv = new Date(c.fecha_vencimiento + 'T00:00:00');
        const limite = new Date(hoy); limite.setDate(limite.getDate() + 7);
        if (fv < hoy) { estadoCalc = 'VENCIDO'; badge = 'bm-badge-err'; }
        else if (fv <= limite) { estadoCalc = 'POR VENCER'; badge = 'bm-badge-warn'; }
        else { estadoCalc = 'VIGENTE'; badge = 'bm-badge-ok'; }
      }
      return `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">${c.nombre_display}</h4>
            <div class="bm-card-subtitle">${c.tipo}</div>
          </div>
          <div class="bm-badge ${badge}">${estadoCalc}</div>
        </div>
        <div class="bm-card-body">
          ${c.numero_certificacion ? `<div>No.: <strong>${c.numero_certificacion}</strong></div>` : ''}
          ${c.fecha_emision ? `<div>Emisión: ${this._fecha(c.fecha_emision)}</div>` : ''}
          ${c.fecha_vencimiento ? `<div>Vence: <strong>${this._fecha(c.fecha_vencimiento)}</strong></div>` : '<div style="color:var(--text2)">Documento sin vencimiento</div>'}
          ${c.archivo_url ? `<div style="margin-top:6px"><a href="javascript:void(0)" onclick="BidManager._openStoragePath('${this._esc(c.archivo_url)}')" style="color:var(--green);text-decoration:none">Ver archivo</a></div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirCertificacion('${c.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('certificaciones', '${c.id}')">Eliminar</button>
        </div>
      </div>`;
    }).join('')}</div>`;
  },

  abrirCertificacion(id) {
    const c = id ? this.data.certificaciones.find(x => x.id === id) : {};
    const tipos = [
      ['DGII','Certificación DGII','30'],
      ['TSS','Certificación TSS','30'],
      ['RPE','Registro de Proveedores del Estado',''],
      ['REGISTRO_MERCANTIL','Registro Mercantil',''],
      ['MIPYME','Certificación MIPYME','180'],
      ['ANTECEDENTES_PENALES','No Antecedentes Penales',''],
      ['ESTATUTOS','Estatutos Sociales',''],
      ['ACTA_ASAMBLEA','Acta de Asamblea',''],
      ['PODER_REPRESENTACION','Poder de Representación',''],
      ['NOMINA_SOCIETARIA','Nómina Societaria',''],
      ['CEDULA_REPRESENTANTE','Cédula del Representante',''],
      ['OTRO','Otro documento','']
    ];
    this._openModal(id ? 'Editar documento' : 'Subir documento', `
      <div class="bm-form-row"><label class="bm-label">Tipo de documento *</label>
        <select class="bm-select" id="f-tipo" onchange="BidManager._autoFillCert(this.value)">
          ${tipos.map(([t,n,d]) => `<option value="${t}" data-nombre="${n}" data-dias="${d}" ${c.tipo===t?'selected':''}>${n}</option>`).join('')}
        </select>
      </div>
      <div class="bm-form-row"><label class="bm-label">Nombre a mostrar</label><input class="bm-input" id="f-nombre_display" value="${this._esc(c.nombre_display)}"></div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">No. certificación</label><input class="bm-input" id="f-numero_certificacion" value="${this._esc(c.numero_certificacion)}"></div>
        <div class="bm-form-row"><label class="bm-label">Fecha emisión</label><input class="bm-input" type="date" id="f-fecha_emision" value="${c.fecha_emision || ''}"></div>
        <div class="bm-form-row" id="bm-cert-fv-row"><label class="bm-label">Fecha vencimiento *</label><input class="bm-input" type="date" id="f-fecha_vencimiento" value="${c.fecha_vencimiento || ''}"></div>
        <div class="bm-form-row" id="bm-cert-vig-row"><label class="bm-label">Vigencia (días)</label><input class="bm-input" type="number" id="f-vigencia_dias" value="${c.vigencia_dias || ''}"></div>
      </div>
      <div class="bm-form-row"><label style="display:flex;align-items:center;gap:8px;font-size:13px;color:var(--text)">
        <input type="checkbox" id="f-no_vence" ${id && !c.fecha_vencimiento ? 'checked' : ''} onchange="BidManager._toggleNoVence(this.checked)"> Este documento no vence
      </label></div>
      <div class="bm-form-row"><label class="bm-label">Archivo</label>
        ${this._uploadWidget('archivo_url', 'certificaciones', c.archivo_url)}
      </div>
      <div class="bm-form-row"><label class="bm-label">Notas</label><textarea class="bm-textarea" id="f-notas">${this._esc(c.notas)}</textarea></div>
    `, async () => {
      const noVence = document.getElementById('f-no_vence').checked;
      const data = this._collectForm(['tipo','nombre_display','numero_certificacion','fecha_emision','fecha_vencimiento','vigencia_dias','archivo_url','notas']);
      if (noVence) {
        data.fecha_vencimiento = null;
        data.vigencia_dias = null;
      } else if (!data.fecha_vencimiento) {
        throw new Error('Indica la fecha de vencimiento, o marca "Este documento no vence"');
      }
      if (!data.nombre_display) data.nombre_display = tipos.find(t => t[0] === data.tipo)?.[1] || data.tipo;
      if (id) await this._put(`/certificaciones/${id}`, data);
      else await this._post('/certificaciones', data);
      this.toast('Documento guardado', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
    // Estado inicial del toggle (al editar un doc sin fecha)
    if (id && !c.fecha_vencimiento) this._toggleNoVence(true);
  },

  _toggleNoVence(checked) {
    const fv = document.getElementById('bm-cert-fv-row');
    const vig = document.getElementById('bm-cert-vig-row');
    if (fv) fv.style.opacity = checked ? '0.4' : '1';
    if (vig) vig.style.opacity = checked ? '0.4' : '1';
    const fvi = document.getElementById('f-fecha_vencimiento');
    const vigi = document.getElementById('f-vigencia_dias');
    if (fvi) { fvi.disabled = checked; if (checked) fvi.value = ''; }
    if (vigi) { vigi.disabled = checked; if (checked) vigi.value = ''; }
  },

  _autoFillCert(tipo) {
    const sel = document.getElementById('f-tipo');
    if (!sel) return;
    const opt = sel.querySelector(`option[value="${tipo}"]`);
    if (!opt) return;
    const nombre = document.getElementById('f-nombre_display');
    const dias = document.getElementById('f-vigencia_dias');
    if (nombre && !nombre.value) nombre.value = opt.dataset.nombre;
    if (dias && opt.dataset.dias) dias.value = opt.dataset.dias;
  },

  // ── PERSONAL ──────────────────────────────────────────
  _renderPersonal() {
    const cont = document.getElementById('bm-personal-list');
    if (!cont) return;
    if (!this.data.personal.length) {
      cont.innerHTML = this._emptyState('P', 'No has agregado personal técnico.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.personal.map(p => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">${p.nombre_completo}</h4>
            <div class="bm-card-subtitle">${p.cargo_empresa || p.profesion || '—'}</div>
          </div>
          ${p.disponible === false ? '<div class="bm-badge bm-badge-warn">No disponible</div>' : ''}
        </div>
        <div class="bm-card-body">
          ${p.profesion ? `<div>${p.profesion}</div>` : ''}
          ${p.experiencia_general_anios ? `<div>Experiencia: <strong>${p.experiencia_general_anios} años</strong></div>` : ''}
          ${p.codia ? `<div>CODIA: ${p.codia}</div>` : ''}
          ${p.tiene_maestria ? '<div>Con maestría</div>' : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirPersonal('${p.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('personal', '${p.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirPersonal(id) {
    const p = id ? this.data.personal.find(x => x.id === id) : {};
    this._openModal(id ? 'Editar personal' : 'Nuevo personal técnico', `
      <div class="bm-form-row"><label class="bm-label">Nombre completo *</label><input class="bm-input" id="f-nombre_completo" value="${this._esc(p.nombre_completo)}"></div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Cédula</label><input class="bm-input" id="f-cedula" value="${this._esc(p.cedula)}"></div>
        <div class="bm-form-row"><label class="bm-label">Cargo en la empresa</label><input class="bm-input" id="f-cargo_empresa" placeholder="Director de Obra" value="${this._esc(p.cargo_empresa)}"></div>
        <div class="bm-form-row"><label class="bm-label">Profesión</label><input class="bm-input" id="f-profesion" placeholder="Ingeniero Civil" value="${this._esc(p.profesion)}"></div>
        <div class="bm-form-row"><label class="bm-label">CODIA</label><input class="bm-input" id="f-codia" value="${this._esc(p.codia)}"></div>
        <div class="bm-form-row"><label class="bm-label">Años de experiencia</label><input class="bm-input" type="number" id="f-experiencia_general_anios" value="${p.experiencia_general_anios || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Teléfono</label><input class="bm-input" id="f-telefono" value="${this._esc(p.telefono)}"></div>
        <div class="bm-form-row"><label class="bm-label">Email</label><input class="bm-input" type="email" id="f-email" value="${this._esc(p.email)}"></div>
      </div>
      <div class="bm-form-row"><label class="bm-label">Especialidades (separadas por coma)</label><input class="bm-input" id="f-especialidades" placeholder="alcantarillado, edificaciones, carreteras" value="${(p.especialidades || []).join(', ')}"></div>
      <div class="bm-form-row"><label style="display:flex;align-items:center;gap:8px;font-size:13px;color:var(--text)"><input type="checkbox" id="f-tiene_maestria" ${p.tiene_maestria?'checked':''}> Tiene maestría</label></div>
      <div class="bm-form-row"><label class="bm-label">Descripción maestría</label><input class="bm-input" id="f-maestria_descripcion" placeholder="Maestría en Gestión de Proyectos" value="${this._esc(p.maestria_descripcion)}"></div>
      <div class="bm-form-row"><label class="bm-label">Documentos</label>
        <div style="font-size:12px;color:var(--text2,#6b7280);margin-bottom:6px">Selecciona el tipo y sube el archivo. Quedan guardados aquí abajo.</div>
        ${this._docCascade('pers', 'personal', [
          {field:'cv_url', label:'Curriculum (CV)'},
          {field:'cedula_url', label:'Cédula'},
          {field:'foto_url', label:'Foto'},
          {field:'firma_url', label:'Firma'},
          {field:'codia_url', label:'Carnet CODIA'},
          {field:'exequatur_url', label:'Exequátur'},
          {field:'maestria_url', label:'Título de maestría'},
          {field:'carta_trabajo_url', label:'Carta de trabajo'},
          {field:'otros_documentos', label:'Otro documento (nombre libre)', custom:true}
        ], p)}
      </div>
    `, async () => {
      const data = this._collectForm(['nombre_completo','cedula','cargo_empresa','profesion','codia','experiencia_general_anios','telefono','email','maestria_descripcion','cv_url','cedula_url','foto_url','firma_url','codia_url','exequatur_url','maestria_url','carta_trabajo_url']);
      const espText = document.getElementById('f-especialidades').value.trim();
      data.especialidades = espText ? espText.split(',').map(s => s.trim().toLowerCase()).filter(Boolean) : [];
      data.tiene_maestria = document.getElementById('f-tiene_maestria').checked;
      data.otros_documentos = this._collectArray('otros_documentos');
      if (id) await this._put(`/personal/${id}`, data);
      else await this._post('/personal', data);
      this.toast('Personal guardado', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  // ── EQUIPOS ───────────────────────────────────────────
  _renderEquipos() {
    const cont = document.getElementById('bm-equipos-list');
    if (!cont) return;
    if (!this.data.equipos.length) {
      cont.innerHTML = this._emptyState('E', 'No has agregado equipos.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.equipos.map(eq => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">${eq.descripcion}</h4>
            <div class="bm-card-subtitle">${eq.marca || ''} ${eq.modelo || ''} ${eq.anio || ''}</div>
          </div>
          <div class="bm-badge ${eq.propiedad==='propio'?'bm-badge-ok':'bm-badge-info'}">${eq.propiedad}</div>
        </div>
        <div class="bm-card-body">
          <div>Cantidad: <strong>${eq.cantidad || 1}</strong></div>
          ${eq.capacidad ? `<div>Capacidad: ${eq.capacidad}</div>` : ''}
          ${eq.propiedad === 'alquilado' && eq.empresa_alquiler ? `<div>Empresa: ${eq.empresa_alquiler}</div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirEquipo('${eq.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('equipos', '${eq.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirEquipo(id) {
    const eq = id ? this.data.equipos.find(x => x.id === id) : { propiedad: 'propio', cantidad: 1 };
    this._openModal(id ? 'Editar equipo' : 'Nuevo equipo', `
      <div class="bm-form-row"><label class="bm-label">Descripción *</label><input class="bm-input" id="f-descripcion" placeholder="Ej: Retroexcavadora" value="${this._esc(eq.descripcion)}"></div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Marca</label><input class="bm-input" id="f-marca" value="${this._esc(eq.marca)}"></div>
        <div class="bm-form-row"><label class="bm-label">Modelo</label><input class="bm-input" id="f-modelo" value="${this._esc(eq.modelo)}"></div>
        <div class="bm-form-row"><label class="bm-label">Año</label><input class="bm-input" type="number" id="f-anio" value="${eq.anio || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Cantidad</label><input class="bm-input" type="number" id="f-cantidad" value="${eq.cantidad || 1}"></div>
        <div class="bm-form-row"><label class="bm-label">Capacidad</label><input class="bm-input" id="f-capacidad" placeholder="1.5 m3, 20 TON" value="${this._esc(eq.capacidad)}"></div>
        <div class="bm-form-row"><label class="bm-label">Matrícula</label><input class="bm-input" id="f-matricula" value="${this._esc(eq.matricula)}"></div>
        <div class="bm-form-row"><label class="bm-label">Propiedad</label>
          <select class="bm-select" id="f-propiedad" onchange="BidManager._togglePropiedad(this.value)">
            <option value="propio" ${eq.propiedad==='propio'?'selected':''}>Propio</option>
            <option value="alquilado" ${eq.propiedad==='alquilado'?'selected':''}>Alquilado</option>
            <option value="leasing" ${eq.propiedad==='leasing'?'selected':''}>Leasing</option>
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Estado</label>
          <select class="bm-select" id="f-estado">
            <option value="operativo" ${eq.estado==='operativo'?'selected':''}>Operativo</option>
            <option value="en_reparacion" ${eq.estado==='en_reparacion'?'selected':''}>En reparación</option>
            <option value="fuera_de_servicio" ${eq.estado==='fuera_de_servicio'?'selected':''}>Fuera de servicio</option>
          </select>
        </div>
      </div>
      <div id="bm-alquiler-section" style="display:${eq.propiedad==='alquilado'?'block':'none'}">
        <div class="bm-form-grid">
          <div class="bm-form-row"><label class="bm-label">Empresa que alquila</label><input class="bm-input" id="f-empresa_alquiler" value="${this._esc(eq.empresa_alquiler)}"></div>
          <div class="bm-form-row"><label class="bm-label">RNC empresa</label><input class="bm-input" id="f-empresa_alquiler_rnc" value="${this._esc(eq.empresa_alquiler_rnc)}"></div>
          <div class="bm-form-row"><label class="bm-label">Teléfono</label><input class="bm-input" id="f-empresa_alquiler_telefono" value="${this._esc(eq.empresa_alquiler_telefono)}"></div>
          <div class="bm-form-row"><label class="bm-label">Contacto</label><input class="bm-input" id="f-empresa_alquiler_contacto" value="${this._esc(eq.empresa_alquiler_contacto)}"></div>
        </div>
      </div>
      <div class="bm-form-row"><label class="bm-label">Documentos del equipo</label>
        <div style="font-size:12px;color:var(--text2,#6b7280);margin-bottom:6px">Matrícula y factura para equipos propios; carta de disponibilidad y contrato para alquilados.</div>
        ${this._docCascade('equipo', 'equipos', [
          {field:'matricula_url', label:'Matrícula'},
          {field:'factura_url', label:'Factura de compra'},
          {field:'foto_url', label:'Foto del equipo'},
          {field:'carta_disponibilidad_equipo_url', label:'Carta de disponibilidad'},
          {field:'contrato_alquiler_url', label:'Contrato de alquiler'},
          {field:'otros_documentos', label:'Otro documento (nombre libre)', custom:true}
        ], eq)}
      </div>
    `, async () => {
      const data = this._collectForm(['descripcion','marca','modelo','anio','cantidad','capacidad','matricula','propiedad','estado','empresa_alquiler','empresa_alquiler_rnc','empresa_alquiler_telefono','empresa_alquiler_contacto','carta_disponibilidad_equipo_url','matricula_url','factura_url','foto_url','contrato_alquiler_url']);
      data.otros_documentos = this._collectArray('otros_documentos');
      if (id) await this._put(`/equipos/${id}`, data);
      else await this._post('/equipos', data);
      this.toast('Equipo guardado', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  _togglePropiedad(v) {
    const alq = document.getElementById('bm-alquiler-section');
    if (alq) alq.style.display = v === 'alquilado' ? 'block' : 'none';
  },

  // ── EXPERIENCIA ───────────────────────────────────────
  _renderExperiencia() {
    const cont = document.getElementById('bm-experiencia-list');
    if (!cont) return;
    if (!this.data.experiencia.length) {
      cont.innerHTML = this._emptyState('X', 'No has agregado proyectos ejecutados.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.experiencia.map(x => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">${x.nombre_proyecto}</h4>
            <div class="bm-card-subtitle">${x.cliente}${x.provincia?' · '+x.provincia:''}</div>
          </div>
          <div class="bm-badge bm-badge-${x.estado==='completado'?'ok':'warn'}">${x.estado || '—'}</div>
        </div>
        <div class="bm-card-body">
          ${x.monto_contrato ? `<div>Monto: <strong>${this._money(x.monto_contrato, x.moneda)}</strong></div>` : ''}
          ${x.tipo_obra ? `<div>Tipo: ${x.tipo_obra}</div>` : ''}
          ${x.fecha_inicio ? `<div>${this._fecha(x.fecha_inicio)}${x.fecha_fin?' → '+this._fecha(x.fecha_fin):''}</div>` : ''}
          ${(x.categorias_obra || []).length ? `<div style="margin-top:6px">${x.categorias_obra.map(c=>`<span class="bm-badge bm-badge-info" style="margin-right:4px">${c}</span>`).join('')}</div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirExperiencia('${x.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('experiencia', '${x.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirExperiencia(id) {
    const x = id ? this.data.experiencia.find(e => e.id === id) : { estado: 'completado', tipo_cliente: 'publico' };
    this._openModal(id ? 'Editar proyecto' : 'Nuevo proyecto', `
      <div class="bm-form-row"><label class="bm-label">Nombre del proyecto *</label><input class="bm-input" id="f-nombre_proyecto" value="${this._esc(x.nombre_proyecto)}"></div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Cliente *</label><input class="bm-input" id="f-cliente" placeholder="Ej: INAPA" value="${this._esc(x.cliente)}"></div>
        <div class="bm-form-row"><label class="bm-label">Tipo cliente</label>
          <select class="bm-select" id="f-tipo_cliente">
            <option value="publico" ${x.tipo_cliente==='publico'?'selected':''}>Público</option>
            <option value="privado" ${x.tipo_cliente==='privado'?'selected':''}>Privado</option>
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">No. contrato</label><input class="bm-input" id="f-numero_contrato" value="${this._esc(x.numero_contrato)}"></div>
        <div class="bm-form-row"><label class="bm-label">Tipo de obra</label><input class="bm-input" id="f-tipo_obra" placeholder="edificación, carretera, etc" value="${this._esc(x.tipo_obra)}"></div>
        <div class="bm-form-row"><label class="bm-label">Provincia</label><input class="bm-input" id="f-provincia" value="${this._esc(x.provincia)}"></div>
        <div class="bm-form-row"><label class="bm-label">Monto contrato (DOP)</label><input class="bm-input" type="number" id="f-monto_contrato" value="${x.monto_contrato || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Fecha inicio</label><input class="bm-input" type="date" id="f-fecha_inicio" value="${x.fecha_inicio || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Fecha fin</label><input class="bm-input" type="date" id="f-fecha_fin" value="${x.fecha_fin || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Estado</label>
          <select class="bm-select" id="f-estado">
            <option value="completado" ${x.estado==='completado'?'selected':''}>Completado</option>
            <option value="en_ejecucion" ${x.estado==='en_ejecucion'?'selected':''}>En ejecución</option>
            <option value="paralizado" ${x.estado==='paralizado'?'selected':''}>Paralizado</option>
          </select>
        </div>
      </div>
      <div class="bm-form-row"><label class="bm-label">Ubicación</label><input class="bm-input" id="f-ubicacion" value="${this._esc(x.ubicacion)}"></div>
      <div class="bm-form-row"><label class="bm-label">Descripción</label><textarea class="bm-textarea" id="f-descripcion">${this._esc(x.descripcion)}</textarea></div>
      <div class="bm-form-row"><label class="bm-label">Alcance detallado <span style="color:var(--text2);font-weight:400">(para matching con IA)</span></label><textarea class="bm-textarea" id="f-alcance_detallado" placeholder="Describe TODO lo que se hizo: movimiento de tierra, hormigón armado, drenaje pluvial, etc. Mientras más detallado, mejor matching">${this._esc(x.alcance_detallado)}</textarea></div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Categorías obra (coma)</label><input class="bm-input" id="f-categorias_obra" placeholder="infraestructura_vial, edificacion" value="${(x.categorias_obra || []).join(', ')}"></div>
        <div class="bm-form-row"><label class="bm-label">Actividades ejecutadas (coma)</label><input class="bm-input" id="f-actividades_ejecutadas" placeholder="movimiento_de_tierra, drenaje, asfalto" value="${(x.actividades_ejecutadas || []).join(', ')}"></div>
      </div>
      <div class="bm-form-row"><label class="bm-label">Documentos del proyecto</label>
        <div style="font-size:12px;color:var(--text2,#6b7280);margin-bottom:6px">Contrato, cubicaciones, recepciones, certificación de experiencia, finiquito y fotos. Puedes subir varias fotos.</div>
        ${this._docCascade('exp', 'experiencia', [
          {field:'contrato_url', label:'Contrato'},
          {field:'cubicaciones_url', label:'Cubicaciones'},
          {field:'recepcion_provisional_url', label:'Recepción provisional'},
          {field:'recepcion_definitiva_url', label:'Recepción definitiva'},
          {field:'acta_recepcion_url', label:'Acta de recepción'},
          {field:'certificacion_url', label:'Certificación de experiencia'},
          {field:'finiquito_url', label:'Finiquito'},
          {field:'fotos_url', label:'Foto del proyecto', array:true},
          {field:'otros_documentos', label:'Otro documento (nombre libre)', custom:true}
        ], x)}
      </div>
    `, async () => {
      const data = this._collectForm(['nombre_proyecto','cliente','tipo_cliente','numero_contrato','tipo_obra','provincia','monto_contrato','fecha_inicio','fecha_fin','estado','ubicacion','descripcion','alcance_detallado','contrato_url','cubicaciones_url','recepcion_provisional_url','recepcion_definitiva_url','acta_recepcion_url','certificacion_url','finiquito_url']);
      const cats = document.getElementById('f-categorias_obra').value.trim();
      const acts = document.getElementById('f-actividades_ejecutadas').value.trim();
      data.categorias_obra = cats ? cats.split(',').map(s => s.trim().toLowerCase().replace(/\s+/g,'_')).filter(Boolean) : [];
      data.actividades_ejecutadas = acts ? acts.split(',').map(s => s.trim().toLowerCase().replace(/\s+/g,'_')).filter(Boolean) : [];
      data.fotos_url = this._collectArray('fotos_url');
      data.otros_documentos = this._collectArray('otros_documentos');
      if (id) await this._put(`/experiencia/${id}`, data);
      else await this._post('/experiencia', data);
      this.toast('Proyecto guardado', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  // ── FINANCIEROS ───────────────────────────────────────
  _renderFinancieros() {
    const cont = document.getElementById('bm-financieros-list');
    if (!cont) return;
    if (!this.data.financieros.length) {
      cont.innerHTML = this._emptyState('F', 'No has agregado estados financieros.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.financieros.map(f => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">Período ${f.periodo}</h4>
            <div class="bm-card-subtitle">${f.tipo} · Cierre: ${this._fecha(f.fecha_cierre)}</div>
          </div>
          ${f.verificado ? '<div class="bm-badge bm-badge-ok">Verificado</div>' : ''}
        </div>
        <div class="bm-card-body">
          ${f.activos_totales ? `<div>Activos: <strong>${this._money(f.activos_totales)}</strong></div>` : ''}
          ${f.patrimonio_neto ? `<div>Patrimonio: <strong>${this._money(f.patrimonio_neto)}</strong></div>` : ''}
          ${f.ingresos ? `<div>Ingresos: ${this._money(f.ingresos)}</div>` : ''}
          ${f.solvencia ? `<div style="margin-top:6px">Solvencia: <strong>${Number(f.solvencia).toFixed(2)}</strong></div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirFinanciero('${f.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('financieros', '${f.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirFinanciero(id) {
    const f = id ? this.data.financieros.find(x => x.id === id) : { tipo: 'anual' };
    this._openModal(id ? 'Editar estado financiero' : 'Nuevo estado financiero', `
      <div style="border:1px solid var(--green,#16a34a);border-radius:8px;padding:14px;margin-bottom:16px;background:rgba(22,163,74,0.05)">
        <div style="font-size:13px;font-weight:600;color:var(--text,#111);margin-bottom:4px">Llenado automático con IA</div>
        <div style="font-size:12px;color:var(--text2,#6b7280);margin-bottom:10px">Sube el PDF de los estados financieros o el IR-2 y la IA extrae los valores. Revisa y ajusta antes de guardar.</div>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <label class="bm-btn bm-btn-primary bm-btn-sm" style="cursor:pointer;margin:0">
            Subir PDF y extraer
            <input type="file" accept="application/pdf,.pdf" style="display:none" onchange="BidManager._extraerFinancierosIA(event, 'fin')">
          </label>
          <span id="bm-ia-status-fin" style="font-size:12px;color:var(--text2,#6b7280)"></span>
        </div>
      </div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Período *</label><input class="bm-input" id="f-periodo" placeholder="2025" value="${this._esc(f.periodo)}"></div>
        <div class="bm-form-row"><label class="bm-label">Tipo</label>
          <select class="bm-select" id="f-tipo">
            <option value="anual" ${f.tipo==='anual'?'selected':''}>Anual</option>
            <option value="interino" ${f.tipo==='interino'?'selected':''}>Interino</option>
            <option value="auditado" ${f.tipo==='auditado'?'selected':''}>Auditado</option>
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Fecha cierre *</label><input class="bm-input" type="date" id="f-fecha_cierre" value="${f.fecha_cierre || ''}"></div>
      </div>
      <h4 style="font-size:13px;color:var(--text2);margin:20px 0 10px;text-transform:uppercase;letter-spacing:0.5px">Balance General</h4>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Activos corrientes</label><input class="bm-input" type="number" id="f-activos_corrientes" value="${f.activos_corrientes || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Activos no corrientes</label><input class="bm-input" type="number" id="f-activos_no_corrientes" value="${f.activos_no_corrientes || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Activos totales</label><input class="bm-input" type="number" id="f-activos_totales" value="${f.activos_totales || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Pasivos corrientes</label><input class="bm-input" type="number" id="f-pasivos_corrientes" value="${f.pasivos_corrientes || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Pasivos no corrientes</label><input class="bm-input" type="number" id="f-pasivos_no_corrientes" value="${f.pasivos_no_corrientes || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Pasivos totales</label><input class="bm-input" type="number" id="f-pasivos_totales" value="${f.pasivos_totales || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Patrimonio neto</label><input class="bm-input" type="number" id="f-patrimonio_neto" value="${f.patrimonio_neto || ''}"></div>
      </div>
      <h4 style="font-size:13px;color:var(--text2);margin:20px 0 10px;text-transform:uppercase;letter-spacing:0.5px">Estado de Resultados</h4>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Ingresos</label><input class="bm-input" type="number" id="f-ingresos" value="${f.ingresos || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Costo de ventas</label><input class="bm-input" type="number" id="f-costo_ventas" value="${f.costo_ventas || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Utilidad bruta</label><input class="bm-input" type="number" id="f-utilidad_bruta" value="${f.utilidad_bruta || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Gastos operativos</label><input class="bm-input" type="number" id="f-gastos_operativos" value="${f.gastos_operativos || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Utilidad operativa</label><input class="bm-input" type="number" id="f-utilidad_operativa" value="${f.utilidad_operativa || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Utilidad neta</label><input class="bm-input" type="number" id="f-utilidad_neta" value="${f.utilidad_neta || ''}"></div>
      </div>
      <div class="bm-form-row"><label class="bm-label">PDF del estado financiero</label>
        ${this._uploadWidget('pdf_url', 'financieros', f.pdf_url)}
      </div>
    `, async () => {
      const data = this._collectForm(['periodo','tipo','fecha_cierre','activos_corrientes','activos_no_corrientes','activos_totales','pasivos_corrientes','pasivos_no_corrientes','pasivos_totales','patrimonio_neto','ingresos','costo_ventas','utilidad_bruta','gastos_operativos','utilidad_operativa','utilidad_neta','pdf_url']);
      if (id) await this._put(`/financieros/${id}`, data);
      else await this._post('/financieros', data);
      this.toast('Estado financiero guardado', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  _renderIndicadores() {
    const cont = document.getElementById('bm-indicadores-view');
    if (!cont) return;
    if (!this.data.financieros.length) {
      cont.innerHTML = this._emptyState('i', 'Agrega un estado financiero para ver los indicadores.');
      return;
    }
    const ult = [...this.data.financieros].sort((a,b) => (b.fecha_cierre||'').localeCompare(a.fecha_cierre||''))[0];
    const ind = [
      { label: 'Solvencia', value: ult.solvencia, desc: 'Activo/Pasivo total', fmt: 'ratio' },
      { label: 'Liquidez corriente', value: ult.liquidez_corriente, desc: 'AC/PC', fmt: 'ratio' },
      { label: 'Endeudamiento', value: ult.endeudamiento, desc: 'Pasivo/Patrimonio', fmt: 'ratio' },
      { label: 'Capital de trabajo', value: ult.capital_trabajo, desc: 'AC - PC', fmt: 'money' },
      { label: 'Margen utilidad', value: ult.margen_utilidad, desc: 'Utilidad/Ingresos', fmt: 'pct' },
      { label: 'ROE', value: ult.roe, desc: 'Utilidad/Patrimonio', fmt: 'pct' }
    ];
    cont.innerHTML = `
      <div style="font-size:12px;color:var(--text2);margin-bottom:12px">Período: <strong style="color:var(--text)">${ult.periodo}</strong> · Cierre: ${this._fecha(ult.fecha_cierre)}</div>
      <div class="bm-indicators-grid">
        ${ind.map(i => `
          <div class="bm-indicator">
            <div class="bm-indicator-label">${i.label}</div>
            <div class="bm-indicator-value">${this._fmtInd(i.value, i.fmt)}</div>
            <div class="bm-indicator-desc">${i.desc}</div>
          </div>
        `).join('')}
      </div>
    `;
  },

  _fmtInd(v, fmt) {
    if (v === null || v === undefined) return '—';
    const n = Number(v);
    if (fmt === 'money') return this._money(n);
    if (fmt === 'pct') return (n * 100).toFixed(1) + '%';
    return n.toFixed(2);
  },

  // ── IR-2 ──────────────────────────────────────────────
  _renderIr2() {
    const cont = document.getElementById('bm-ir2-list');
    if (!cont) return;
    if (!this.data.ir2.length) {
      cont.innerHTML = this._emptyState('IR', 'No has agregado declaraciones IR-2.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.ir2.map(i => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">IR-2 · ${i.periodo_fiscal}</h4>
            <div class="bm-card-subtitle">${i.tipo} · ${this._fecha(i.fecha_cierre_fiscal)}</div>
          </div>
        </div>
        <div class="bm-card-body">
          ${i.ingresos_brutos ? `<div>Ingresos brutos: <strong>${this._money(i.ingresos_brutos)}</strong></div>` : ''}
          ${i.renta_neta_imponible ? `<div>Renta neta: ${this._money(i.renta_neta_imponible)}</div>` : ''}
          ${i.impuesto_liquidado ? `<div>Impuesto: ${this._money(i.impuesto_liquidado)}</div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirIr2('${i.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('ir2', '${i.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirIr2(id) {
    const i = id ? this.data.ir2.find(x => x.id === id) : { tipo: 'anual' };
    this._openModal(id ? 'Editar IR-2' : 'Nueva declaración IR-2', `
      <div style="border:1px solid var(--green,#16a34a);border-radius:8px;padding:14px;margin-bottom:16px;background:rgba(22,163,74,0.05)">
        <div style="font-size:13px;font-weight:600;color:var(--text,#111);margin-bottom:4px">Llenado automático con IA</div>
        <div style="font-size:12px;color:var(--text2,#6b7280);margin-bottom:10px">Sube el PDF del IR-2 y la IA extrae los valores. Revisa y ajusta antes de guardar.</div>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <label class="bm-btn bm-btn-primary bm-btn-sm" style="cursor:pointer;margin:0">
            Subir PDF y extraer
            <input type="file" accept="application/pdf,.pdf" style="display:none" onchange="BidManager._extraerFinancierosIA(event, 'ir2')">
          </label>
          <span id="bm-ia-status-ir2" style="font-size:12px;color:var(--text2,#6b7280)"></span>
        </div>
      </div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Período fiscal *</label><input class="bm-input" id="f-periodo_fiscal" placeholder="2025" value="${this._esc(i.periodo_fiscal)}"></div>
        <div class="bm-form-row"><label class="bm-label">Tipo</label>
          <select class="bm-select" id="f-tipo">
            <option value="anual" ${i.tipo==='anual'?'selected':''}>Anual</option>
            <option value="interino" ${i.tipo==='interino'?'selected':''}>Interino</option>
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Fecha cierre fiscal *</label><input class="bm-input" type="date" id="f-fecha_cierre_fiscal" value="${i.fecha_cierre_fiscal || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Ingresos brutos</label><input class="bm-input" type="number" id="f-ingresos_brutos" value="${i.ingresos_brutos || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Costos y gastos</label><input class="bm-input" type="number" id="f-costos_y_gastos" value="${i.costos_y_gastos || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Renta neta imponible</label><input class="bm-input" type="number" id="f-renta_neta_imponible" value="${i.renta_neta_imponible || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Impuesto liquidado</label><input class="bm-input" type="number" id="f-impuesto_liquidado" value="${i.impuesto_liquidado || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Total activos</label><input class="bm-input" type="number" id="f-total_activos" value="${i.total_activos || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Total pasivos</label><input class="bm-input" type="number" id="f-total_pasivos" value="${i.total_pasivos || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Patrimonio</label><input class="bm-input" type="number" id="f-patrimonio" value="${i.patrimonio || ''}"></div>
      </div>
      <div class="bm-form-row"><label class="bm-label">PDF del IR-2</label>
        ${this._uploadWidget('pdf_url', 'ir2', i.pdf_url)}
      </div>
    `, async () => {
      const data = this._collectForm(['periodo_fiscal','tipo','fecha_cierre_fiscal','ingresos_brutos','costos_y_gastos','renta_neta_imponible','impuesto_liquidado','total_activos','total_pasivos','patrimonio','pdf_url']);
      if (id) await this._put(`/ir2/${id}`, data);
      else await this._post('/ir2', data);
      this.toast('IR-2 guardado', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  // ── CAPACIDAD FINANCIERA ──────────────────────────────
  _renderCapacidad() {
    const cont = document.getElementById('bm-capacidad-list');
    if (!cont) return;
    if (!this.data.capacidad.length) {
      cont.innerHTML = this._emptyState('C', 'No has agregado líneas de crédito o solvencias.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.capacidad.map(c => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">${c.institucion_financiera}</h4>
            <div class="bm-card-subtitle">${c.tipo}</div>
          </div>
          <div class="bm-badge ${c.estado==='vigente'?'bm-badge-ok':c.estado==='por_vencer'?'bm-badge-warn':'bm-badge-err'}">${c.estado || '—'}</div>
        </div>
        <div class="bm-card-body">
          <div>Monto: <strong>${this._money(c.monto, c.moneda)}</strong></div>
          ${c.monto_disponible ? `<div>Disponible: ${this._money(c.monto_disponible, c.moneda)}</div>` : ''}
          ${c.fecha_vencimiento ? `<div>Vence: ${this._fecha(c.fecha_vencimiento)}</div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirCapacidad('${c.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('capacidad-financiera', '${c.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirCapacidad(id) {
    const c = id ? this.data.capacidad.find(x => x.id === id) : { tipo: 'LINEA_CREDITO', moneda: 'DOP' };
    this._openModal(id ? 'Editar capacidad financiera' : 'Nueva capacidad financiera', `
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">Tipo *</label>
          <select class="bm-select" id="f-tipo">
            <option value="LINEA_CREDITO" ${c.tipo==='LINEA_CREDITO'?'selected':''}>Línea de crédito</option>
            <option value="CERTIFICADO_AHORRO" ${c.tipo==='CERTIFICADO_AHORRO'?'selected':''}>Certificado de ahorro</option>
            <option value="SOLVENCIA_BANCARIA" ${c.tipo==='SOLVENCIA_BANCARIA'?'selected':''}>Solvencia bancaria</option>
            <option value="DEPOSITO_A_PLAZO" ${c.tipo==='DEPOSITO_A_PLAZO'?'selected':''}>Depósito a plazo</option>
          </select>
        </div>
        <div class="bm-form-row"><label class="bm-label">Institución *</label><input class="bm-input" id="f-institucion_financiera" placeholder="Banco Popular" value="${this._esc(c.institucion_financiera)}"></div>
        <div class="bm-form-row"><label class="bm-label">Monto *</label><input class="bm-input" type="number" id="f-monto" value="${c.monto || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Monto disponible</label><input class="bm-input" type="number" id="f-monto_disponible" value="${c.monto_disponible || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Fecha emisión</label><input class="bm-input" type="date" id="f-fecha_emision" value="${c.fecha_emision || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Fecha vencimiento</label><input class="bm-input" type="date" id="f-fecha_vencimiento" value="${c.fecha_vencimiento || ''}"></div>
      </div>
      <div class="bm-form-row"><label class="bm-label">Archivo (carta / certificación)</label>
        ${this._uploadWidget('archivo_url', 'capacidad', c.archivo_url)}
      </div>
    `, async () => {
      const data = this._collectForm(['tipo','institucion_financiera','monto','monto_disponible','fecha_emision','fecha_vencimiento','archivo_url']);
      if (id) await this._put(`/capacidad-financiera/${id}`, data);
      else await this._post('/capacidad-financiera', data);
      this.toast('Capacidad guardada', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  // ── REFERENCIAS COMERCIALES ───────────────────────────
  _renderReferencias() {
    const cont = document.getElementById('bm-referencias-list');
    if (!cont) return;
    if (!this.data.referencias.length) {
      cont.innerHTML = this._emptyState('RC', 'No has agregado referencias comerciales.');
      return;
    }
    cont.innerHTML = `<div class="bm-grid">${this.data.referencias.map(r => `
      <div class="bm-card">
        <div class="bm-card-header">
          <div>
            <h4 class="bm-card-title">${r.proveedor_nombre}</h4>
            <div class="bm-card-subtitle">${r.relacion_anios ? r.relacion_anios+' años de relación' : ''}</div>
          </div>
        </div>
        <div class="bm-card-body">
          ${r.monto_credito ? `<div>Crédito: <strong>${this._money(r.monto_credito, r.moneda)}</strong></div>` : ''}
          ${r.plazo_credito_dias ? `<div>Plazo: ${r.plazo_credito_dias} días</div>` : ''}
          ${r.proveedor_telefono ? `<div>Tel: ${r.proveedor_telefono}</div>` : ''}
        </div>
        <div class="bm-card-actions">
          <button class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager.abrirReferencia('${r.id}')">Editar</button>
          <button class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager.eliminar('referencias-comerciales', '${r.id}')">Eliminar</button>
        </div>
      </div>
    `).join('')}</div>`;
  },

  abrirReferencia(id) {
    const r = id ? this.data.referencias.find(x => x.id === id) : { moneda: 'DOP' };
    this._openModal(id ? 'Editar referencia' : 'Nueva referencia comercial', `
      <div class="bm-form-row"><label class="bm-label">Proveedor *</label><input class="bm-input" id="f-proveedor_nombre" placeholder="Ferretería Americana" value="${this._esc(r.proveedor_nombre)}"></div>
      <div class="bm-form-grid">
        <div class="bm-form-row"><label class="bm-label">RNC proveedor</label><input class="bm-input" id="f-proveedor_rnc" value="${this._esc(r.proveedor_rnc)}"></div>
        <div class="bm-form-row"><label class="bm-label">Contacto</label><input class="bm-input" id="f-proveedor_contacto" value="${this._esc(r.proveedor_contacto)}"></div>
        <div class="bm-form-row"><label class="bm-label">Teléfono</label><input class="bm-input" id="f-proveedor_telefono" value="${this._esc(r.proveedor_telefono)}"></div>
        <div class="bm-form-row"><label class="bm-label">Monto crédito</label><input class="bm-input" type="number" id="f-monto_credito" value="${r.monto_credito || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Plazo (días)</label><input class="bm-input" type="number" id="f-plazo_credito_dias" value="${r.plazo_credito_dias || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Años de relación</label><input class="bm-input" type="number" id="f-relacion_anios" value="${r.relacion_anios || ''}"></div>
        <div class="bm-form-row"><label class="bm-label">Fecha carta</label><input class="bm-input" type="date" id="f-fecha_carta" value="${r.fecha_carta || ''}"></div>
      </div>
      <div class="bm-form-row"><label class="bm-label">Carta de referencia</label>
        ${this._uploadWidget('archivo_url', 'referencias', r.archivo_url)}
      </div>
    `, async () => {
      const data = this._collectForm(['proveedor_nombre','proveedor_rnc','proveedor_contacto','proveedor_telefono','monto_credito','plazo_credito_dias','relacion_anios','fecha_carta','archivo_url']);
      if (id) await this._put(`/referencias-comerciales/${id}`, data);
      else await this._post('/referencias-comerciales', data);
      this.toast('Referencia guardada', 'ok');
      this.cerrarModal();
      await this.refresh();
    });
  },

  // ── ELIMINAR (genérico) ───────────────────────────────
  async eliminar(recurso, id) {
    if (!confirm('¿Eliminar este registro? Esta acción no se puede deshacer.')) return;
    try {
      await this._delete(`/${recurso}/${id}`);
      this.toast('Eliminado', 'ok');
      await this.refresh();
    } catch (e) {
      this.toast('Error al eliminar', 'err');
    }
  },

  // ── MODAL genérico ────────────────────────────────────
  _openModal(title, bodyHtml, onSave) {
    document.getElementById('bm-modal-title').textContent = title;
    document.getElementById('bm-modal-body').innerHTML = bodyHtml;
    document.getElementById('bm-modal-footer').innerHTML = `
      <button class="bm-btn bm-btn-ghost" onclick="BidManager.cerrarModal()">Cancelar</button>
      <button class="bm-btn bm-btn-primary" id="bm-modal-save">Guardar</button>
    `;
    document.getElementById('bm-modal').classList.add('bm-active');
    document.getElementById('bm-modal-save').onclick = async (e) => {
      const btn = e.target;
      btn.disabled = true;
      btn.textContent = 'Guardando...';
      try { await onSave(); }
      catch (err) { this.toast('Error: ' + err.message, 'err'); btn.disabled = false; btn.textContent = 'Guardar'; }
    };
  },

  cerrarModal() {
    document.getElementById('bm-modal').classList.remove('bm-active');
  },

  _collectForm(campos) {
    const data = {};
    campos.forEach(c => {
      const el = document.getElementById('f-' + c);
      if (!el) return;
      let v = el.value.trim();
      if (v === '') { data[c] = null; return; }
      if (el.type === 'number') v = Number(v);
      data[c] = v;
    });
    return data;
  },

  _emptyState(icon, text) {
    return `<div class="bm-empty"><div class="bm-empty-icon">${icon}</div><div class="bm-empty-text">${text}</div></div>`;
  },

  // ── Toast ─────────────────────────────────────────────
  toast(msg, type = 'ok') {
    const t = document.getElementById('bm-toast');
    if (!t) return;
    t.textContent = msg;
    t.className = 'bm-toast bm-show bm-toast-' + type;
    setTimeout(() => t.classList.remove('bm-show'), 2500);
  },

  // ── Helpers formato ───────────────────────────────────
  _esc(v) {
    if (v === null || v === undefined) return '';
    return String(v).replace(/"/g, '&quot;').replace(/</g, '&lt;');
  },

  _money(v, moneda = 'DOP') {
    if (v === null || v === undefined) return '—';
    const n = Number(v);
    const simbolo = moneda === 'USD' ? '$' : 'RD$';
    return simbolo + ' ' + n.toLocaleString('es-DO', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  },

  _fecha(f) {
    if (!f) return '—';
    try {
      return new Date(f).toLocaleDateString('es-DO', { day: '2-digit', month: 'short', year: 'numeric' });
    } catch(e) { return f; }
  },

  // ═══════════════════════════════════════════════════════
  // UPLOAD WIDGET — Reemplaza los inputs de URL manuales
  // ═══════════════════════════════════════════════════════
  // Uso dentro de un modal:
  //   ${this._uploadWidget('archivo_url', 'certificaciones', c.archivo_url)}
  //
  // El helper renderiza:
  //   - <input type="hidden" id="f-{field}"> con el path guardado
  //   - Un div visible que muestra o "elegir archivo" o el archivo cargado
  //
  // _collectForm() sigue funcionando sin cambios (lee el hidden por su id).
  // El valor guardado en BD es el path del bucket (ej: "abc-uuid/certificaciones/xyz.pdf"),
  // no una URL directa. Al presionar Ver, se pide una signed URL de 1h.
  //
  // Retro-compatibilidad: si el valor ya guardado empieza con "http", se trata
  // como URL externa legacy y "Ver" abre esa URL directa.
  // ═══════════════════════════════════════════════════════

  _uploadWidget(field, categoria, currentValue) {
    const inputId = `f-${field}`;
    const widgetId = `bm-upload-${field}`;
    const val = currentValue || '';
    return `
      <input type="hidden" id="${inputId}" value="${this._esc(val)}">
      <div class="bm-upload-widget" id="${widgetId}" data-field="${field}" data-categoria="${categoria}">
        ${this._renderUploadWidget(field, val)}
      </div>
    `;
  },

  _renderUploadWidget(field, val) {
    if (val) {
      let displayName;
      if (val.startsWith('http')) {
        displayName = 'Archivo externo (legacy)';
      } else {
        const last = val.split('/').pop() || val;
        displayName = last.replace(/^[0-9a-f]{8}_/, '');
      }
      return `
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:10px 12px;background:var(--bg2,#f8f9fa);border:1px solid var(--border,#e5e7eb);border-radius:6px">
          <span style="flex:1;min-width:120px;font-size:13px;color:var(--text,#111);word-break:break-all">${this._esc(displayName)}</span>
          <button type="button" class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager._viewFile('${field}')">Ver</button>
          <button type="button" class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager._replaceFile('${field}')">Cambiar</button>
          <button type="button" class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager._clearFile('${field}')">Quitar</button>
        </div>
      `;
    }
    return `
      <label style="display:flex;align-items:center;gap:10px;padding:12px 14px;border:1.5px dashed var(--border,#d1d5db);border-radius:6px;cursor:pointer;color:var(--text2,#6b7280);font-size:13px;transition:all .15s;background:var(--bg2,#fafafa)" onmouseover="this.style.borderColor='var(--green,#16a34a)';this.style.color='var(--green,#16a34a)'" onmouseout="this.style.borderColor='var(--border,#d1d5db)';this.style.color='var(--text2,#6b7280)'">
        <span style="font-weight:600">+</span>
        <span>Elegir archivo (máx. 25 MB)</span>
        <input type="file" style="display:none" onchange="BidManager._doUpload(event, '${field}')">
      </label>
    `;
  },

  // ═══════════════════════════════════════════════════════
  // CASCADA DE DOCUMENTOS — un selector de tipo + subir
  // ═══════════════════════════════════════════════════════
  // En vez de un cajón de upload por cada documento posible,
  // muestra: [select tipo de documento] [Subir] y debajo la
  // lista de documentos ya cargados con Ver / Quitar.
  //
  // Uso:
  //   ${this._docCascade('personal', 'personal', [
  //     {field:'cv_url', label:'Curriculum (CV)'},
  //     {field:'cedula_url', label:'Cédula'},
  //     ...
  //     {field:'fotos_url', label:'Foto del proyecto', array:true}
  //   ], p)}
  //
  // - Campos normales: hidden f-{field} con el path (compatible _collectForm)
  // - Campos array:true: hidden f-{field} con JSON '["path1","path2"]'
  //   → el handler de guardado debe parsearlo manualmente.
  // ═══════════════════════════════════════════════════════

  _cascadeReg: {},

  _docCascade(cid, categoria, tipos, record) {
    this._cascadeReg[cid] = { categoria, tipos };
    record = record || {};
    const hiddens = tipos.map(t => {
      let v = record[t.field];
      if (t.array || t.custom) v = JSON.stringify(Array.isArray(v) ? v : (v ? [v] : []));
      else v = v || '';
      return `<input type="hidden" id="f-${t.field}" value="${this._esc(v)}">`;
    }).join('');
    const hayCustom = tipos.some(t => t.custom);
    return `
      ${hiddens}
      <div class="bm-cascade" id="bm-cascade-${cid}" style="border:1px solid var(--border,#e5e7eb);border-radius:8px;padding:14px;background:var(--bg2,#fafafa)">
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <select class="bm-select" id="bm-cascade-sel-${cid}" style="flex:1;min-width:180px" ${hayCustom ? `onchange="BidManager._cascadeSelChange('${cid}')"` : ''}>
            ${tipos.map(t => `<option value="${t.field}">${t.label}</option>`).join('')}
          </select>
          <label class="bm-btn bm-btn-primary bm-btn-sm" style="cursor:pointer;margin:0">
            Subir
            <input type="file" style="display:none" onchange="BidManager._cascadeUpload(event, '${cid}')">
          </label>
        </div>
        ${hayCustom ? `<input class="bm-input" id="bm-cascade-name-${cid}" placeholder="Nombre del documento (ej: Permiso del ayuntamiento)" style="display:none;margin-top:8px">` : ''}
        <div id="bm-cascade-list-${cid}" style="margin-top:10px">
          ${this._cascadeListHtml(cid, record)}
        </div>
      </div>
    `;
  },

  _cascadeSelChange(cid) {
    const reg = this._cascadeReg[cid];
    const sel = document.getElementById(`bm-cascade-sel-${cid}`);
    const nameInput = document.getElementById(`bm-cascade-name-${cid}`);
    if (!reg || !sel || !nameInput) return;
    const tipo = reg.tipos.find(t => t.field === sel.value);
    nameInput.style.display = (tipo && tipo.custom) ? 'block' : 'none';
  },

  _cascadeListHtml(cid, record) {
    const reg = this._cascadeReg[cid];
    if (!reg) return '';
    const filas = [];
    reg.tipos.forEach(t => {
      let val;
      if (record) {
        // Primer render: los hidden aún no están en el DOM → leer del registro
        val = record[t.field];
        if (t.array || t.custom) val = JSON.stringify(Array.isArray(val) ? val : (val ? [val] : []));
        else val = val || '';
      } else {
        const el = document.getElementById(`f-${t.field}`);
        if (!el) return;
        val = el.value;
      }
      if (t.custom) {
        let arr = [];
        try { arr = JSON.parse(val || '[]'); } catch(e) { arr = []; }
        arr.forEach((item, idx) => {
          if (item && item.path) filas.push(this._cascadeRowHtml(cid, t, item.path, idx, item.label));
        });
      } else if (t.array) {
        let arr = [];
        try { arr = JSON.parse(val || '[]'); } catch(e) { arr = []; }
        arr.forEach((p, idx) => filas.push(this._cascadeRowHtml(cid, t, p, idx)));
      } else if (val) {
        filas.push(this._cascadeRowHtml(cid, t, val, null));
      }
    });
    if (!filas.length) {
      return `<div style="font-size:12px;color:var(--text2,#9ca3af);padding:4px 2px">Sin documentos cargados todavía.</div>`;
    }
    return filas.join('');
  },

  _cascadeRowHtml(cid, tipo, path, idx, labelOverride) {
    let name;
    if (path.startsWith('http')) name = 'Archivo externo';
    else name = (path.split('/').pop() || path).replace(/^[0-9a-f]{8}_/, '');
    const idxArg = idx === null ? 'null' : idx;
    const label = labelOverride || tipo.label;
    return `
      <div style="display:flex;align-items:center;gap:8px;padding:8px 10px;margin-top:6px;background:#fff;border:1px solid var(--border,#e5e7eb);border-radius:6px">
        <div style="flex:1;min-width:0">
          <div style="font-size:12px;font-weight:600;color:var(--text,#111)">${this._esc(label)}</div>
          <div style="font-size:12px;color:var(--text2,#6b7280);word-break:break-all">${this._esc(name)}</div>
        </div>
        <button type="button" class="bm-btn bm-btn-ghost bm-btn-sm" onclick="BidManager._openStoragePath('${this._esc(path)}')">Ver</button>
        <button type="button" class="bm-btn bm-btn-danger bm-btn-sm" onclick="BidManager._cascadeRemove('${cid}','${tipo.field}',${idxArg})">Quitar</button>
      </div>
    `;
  },

  _cascadeRefresh(cid) {
    const cont = document.getElementById(`bm-cascade-list-${cid}`);
    if (cont) cont.innerHTML = this._cascadeListHtml(cid);
  },

  async _cascadeUpload(evt, cid) {
    const file = evt.target.files && evt.target.files[0];
    evt.target.value = '';
    if (!file) return;
    const reg = this._cascadeReg[cid];
    if (!reg) return;
    const sel = document.getElementById(`bm-cascade-sel-${cid}`);
    const field = sel ? sel.value : null;
    const tipo = reg.tipos.find(t => t.field === field);
    if (!tipo) return;

    // Documento "Otro": requiere nombre antes de subir
    let customLabel = null;
    if (tipo.custom) {
      const nameInput = document.getElementById(`bm-cascade-name-${cid}`);
      customLabel = nameInput ? nameInput.value.trim() : '';
      if (!customLabel) {
        this.toast('Escribe el nombre del documento antes de subirlo', 'err');
        return;
      }
    }

    const MAX = 25 * 1024 * 1024;
    if (file.size > MAX) { this.toast('Archivo excede 25 MB', 'err'); return; }
    if (file.size === 0) { this.toast('Archivo vacío', 'err'); return; }

    const cont = document.getElementById(`bm-cascade-list-${cid}`);
    if (cont) cont.insertAdjacentHTML('afterbegin', `
      <div id="bm-cascade-uploading-${cid}" style="display:flex;align-items:center;gap:10px;padding:8px 10px;margin-top:6px;background:#fff;border:1px solid var(--border,#e5e7eb);border-radius:6px">
        <div style="width:14px;height:14px;border:2px solid var(--green,#16a34a);border-top-color:transparent;border-radius:50%;animation:bm-spin .7s linear infinite"></div>
        <span style="font-size:12px;color:var(--text2,#6b7280)">Subiendo ${this._esc(file.name)} como <strong>${this._esc(tipo.label)}</strong>...</span>
      </div>
    `);

    try {
      const form = new FormData();
      form.append('categoria', reg.categoria);
      form.append('file', file);
      const r = await fetch('/api/bid/upload', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + this._token() },
        body: form
      });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        throw new Error(err.detail || `Upload → ${r.status}`);
      }
      const res = await r.json();
      const hidden = document.getElementById(`f-${field}`);
      if (hidden) {
        if (tipo.custom) {
          let arr = [];
          try { arr = JSON.parse(hidden.value || '[]'); } catch(e) { arr = []; }
          arr.push({ label: customLabel, path: res.path });
          hidden.value = JSON.stringify(arr);
          const nameInput = document.getElementById(`bm-cascade-name-${cid}`);
          if (nameInput) nameInput.value = '';
        } else if (tipo.array) {
          let arr = [];
          try { arr = JSON.parse(hidden.value || '[]'); } catch(e) { arr = []; }
          arr.push(res.path);
          hidden.value = JSON.stringify(arr);
        } else {
          hidden.value = res.path;
        }
      }
      this.toast('Documento subido', 'ok');
    } catch (e) {
      this.toast('Error subiendo: ' + e.message, 'err');
    } finally {
      const up = document.getElementById(`bm-cascade-uploading-${cid}`);
      if (up) up.remove();
      this._cascadeRefresh(cid);
    }
  },

  async _cascadeRemove(cid, field, idx) {
    const reg = this._cascadeReg[cid];
    const tipo = reg ? reg.tipos.find(t => t.field === field) : null;
    const hidden = document.getElementById(`f-${field}`);
    if (!hidden || !tipo) return;

    let path;
    if (tipo.custom) {
      let arr = [];
      try { arr = JSON.parse(hidden.value || '[]'); } catch(e) { arr = []; }
      path = arr[idx] ? arr[idx].path : null;
      arr.splice(idx, 1);
      hidden.value = JSON.stringify(arr);
    } else if (tipo.array) {
      let arr = [];
      try { arr = JSON.parse(hidden.value || '[]'); } catch(e) { arr = []; }
      path = arr[idx];
      arr.splice(idx, 1);
      hidden.value = JSON.stringify(arr);
    } else {
      path = hidden.value;
      hidden.value = '';
    }

    if (path && !path.startsWith('http')) {
      try {
        await fetch(`/api/bid/upload?path=${encodeURIComponent(path)}`, {
          method: 'DELETE',
          headers: { 'Authorization': 'Bearer ' + this._token() }
        });
      } catch(e) { /* silencioso */ }
    }
    this._cascadeRefresh(cid);
    this.toast('Documento quitado', 'ok');
  },

  // Recoge un hidden array (JSON) y devuelve el array parseado
  _collectArray(field) {
    const el = document.getElementById(`f-${field}`);
    if (!el) return [];
    try { return JSON.parse(el.value || '[]'); } catch(e) { return []; }
  },

  // ═══════════════════════════════════════════════════════
  // EXTRACCIÓN IA — Estados financieros / IR-2
  // ═══════════════════════════════════════════════════════
  // Sube el PDF al storage (categoría financieros/ir2) y en
  // paralelo lo manda a /financieros/extraer para que Gemini
  // devuelva los valores y pre-llenar el formulario.
  // El usuario siempre revisa antes de guardar.
  // ═══════════════════════════════════════════════════════

  // Análisis por LOTE: varios PDFs (estados + IR-2 de distintos años) de una vez.
  // El backend extrae cada uno, detecta el tipo, sube el PDF y CREA los registros.
  // Los indicadores se calculan solos al insertarse (columnas GENERATED).
  _loteAbort: null,

  async _extraerLoteIA(evt) {
    const files = Array.from(evt.target.files || []);
    evt.target.value = '';
    if (!files.length) return;
    if (files.length > 6) { this.toast('Máximo 6 PDFs por lote', 'err'); return; }

    const status = document.getElementById('bm-ia-lote-status');
    const resultCont = document.getElementById('bm-ia-lote-resultados');
    const setStatus = (t) => { if (status) status.textContent = t; };

    const MAX = 25 * 1024 * 1024;
    for (const f of files) {
      if (f.size > MAX) { this.toast(`${f.name} excede 25 MB`, 'err'); return; }
    }

    setStatus(`Analizando ${files.length} PDF(s) con IA... esto puede tomar 1-3 minutos`);
    if (resultCont) {
      resultCont.innerHTML = `
        <button type="button" class="bm-btn bm-btn-danger bm-btn-sm" id="bm-ia-lote-stop"
                onclick="BidManager._detenerLoteIA()">Detener análisis</button>
      `;
    }

    this._loteAbort = new AbortController();

    try {
      const form = new FormData();
      files.forEach(f => form.append('files', f));
      const r = await fetch('/api/bid/financieros/extraer-lote', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + this._token() },
        body: form,
        signal: this._loteAbort.signal
      });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        throw new Error(err.detail || `Lote → ${r.status}`);
      }
      const { resultados } = await r.json();

      const oks = resultados.filter(x => x.ok).length;
      setStatus(`Completado: ${oks}/${resultados.length} procesados correctamente.`);

      if (resultCont) {
        resultCont.innerHTML = resultados.map(res => `
          <div style="display:flex;align-items:center;gap:10px;padding:8px 10px;margin-top:6px;background:#fff;border:1px solid ${res.ok ? 'var(--green,#16a34a)' : '#dc2626'};border-radius:6px">
            <div style="flex:1;min-width:0">
              <div style="font-size:12px;font-weight:600">${this._esc(res.archivo)}</div>
              <div style="font-size:12px;color:var(--text2,#6b7280)">
                ${res.ok
                  ? `${res.tipo} · Período ${this._esc(res.periodo || '?')} · Confianza: ${res.confianza || 'media'}${res.notas ? ' · ' + this._esc(res.notas) : ''}`
                  : `Error: ${this._esc(res.error || 'desconocido')}`}
              </div>
            </div>
          </div>
        `).join('');
      }

      if (oks > 0) {
        this.toast(`${oks} registro(s) creados. Revisa los valores en Estados/IR-2.`, 'ok');
        await this.refresh();
      } else {
        this.toast('Ningún PDF pudo procesarse', 'err');
      }
    } catch (e) {
      if (e.name === 'AbortError') {
        setStatus('Análisis detenido por el usuario.');
        if (resultCont) resultCont.innerHTML = '';
        this.toast('Análisis detenido', 'ok');
      } else {
        setStatus('');
        if (resultCont) resultCont.innerHTML = '';
        this.toast('Error en análisis por lote: ' + e.message, 'err');
      }
    } finally {
      this._loteAbort = null;
    }
  },

  _detenerLoteIA() {
    if (this._loteAbort) this._loteAbort.abort();
  },

  async _extraerFinancierosIA(evt, modo) {
    const file = evt.target.files && evt.target.files[0];
    evt.target.value = '';
    if (!file) return;

    const status = document.getElementById(`bm-ia-status-${modo}`);
    const setStatus = (t) => { if (status) status.textContent = t; };

    const MAX = 25 * 1024 * 1024;
    if (file.size > MAX) { this.toast('PDF excede 25 MB', 'err'); return; }

    setStatus('Analizando con IA... esto toma 20-60 segundos');

    try {
      const categoria = modo === 'ir2' ? 'ir2' : 'financieros';

      // 1) Subir al storage (para que quede guardado como respaldo)
      const formUp = new FormData();
      formUp.append('categoria', categoria);
      formUp.append('file', file);
      const upPromise = fetch('/api/bid/upload', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + this._token() },
        body: formUp
      }).then(r => r.ok ? r.json() : null).catch(() => null);

      // 2) Extraer con IA
      const formIA = new FormData();
      formIA.append('file', file);
      const iaResp = await fetch('/api/bid/financieros/extraer', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + this._token() },
        body: formIA
      });
      if (!iaResp.ok) {
        const err = await iaResp.json().catch(() => ({}));
        throw new Error(err.detail || `Extracción → ${iaResp.status}`);
      }
      const { extraido } = await iaResp.json();

      // 3) Guardar el path del PDF subido en f-pdf_url
      const up = await upPromise;
      if (up && up.path) {
        const hidden = document.getElementById('f-pdf_url');
        if (hidden) {
          hidden.value = up.path;
          const widget = document.getElementById('bm-upload-pdf_url');
          if (widget) widget.innerHTML = this._renderUploadWidget('pdf_url', up.path);
        }
      }

      // 4) Pre-llenar los campos según el modo
      const setVal = (fid, v) => {
        const el = document.getElementById('f-' + fid);
        if (el && v !== null && v !== undefined && v !== 0) el.value = v;
        else if (el && v === 0) el.value = 0;
      };

      if (modo === 'ir2') {
        setVal('periodo_fiscal', extraido.periodo);
        setVal('fecha_cierre_fiscal', extraido.fecha_cierre);
        setVal('ingresos_brutos', extraido.ingresos_brutos ?? extraido.ingresos);
        setVal('costos_y_gastos', extraido.costos_y_gastos);
        setVal('renta_neta_imponible', extraido.renta_neta_imponible);
        setVal('impuesto_liquidado', extraido.impuesto_liquidado);
        setVal('total_activos', extraido.activos_totales);
        setVal('total_pasivos', extraido.pasivos_totales);
        setVal('patrimonio', extraido.patrimonio_neto);
      } else {
        setVal('periodo', extraido.periodo);
        setVal('fecha_cierre', extraido.fecha_cierre);
        setVal('activos_corrientes', extraido.activos_corrientes);
        setVal('activos_no_corrientes', extraido.activos_no_corrientes);
        setVal('activos_totales', extraido.activos_totales);
        setVal('pasivos_corrientes', extraido.pasivos_corrientes);
        setVal('pasivos_no_corrientes', extraido.pasivos_no_corrientes);
        setVal('pasivos_totales', extraido.pasivos_totales);
        setVal('patrimonio_neto', extraido.patrimonio_neto);
        setVal('ingresos', extraido.ingresos ?? extraido.ingresos_brutos);
        setVal('costo_ventas', extraido.costo_ventas);
        setVal('utilidad_bruta', extraido.utilidad_bruta);
        setVal('gastos_operativos', extraido.gastos_operativos);
        setVal('utilidad_operativa', extraido.utilidad_operativa);
        setVal('utilidad_neta', extraido.utilidad_neta);
      }

      const conf = extraido.confianza || 'media';
      setStatus(`Listo. Confianza: ${conf}.${extraido.notas ? ' ' + extraido.notas : ''} Revisa los valores antes de guardar.`);
      this.toast('Valores extraídos. Revísalos antes de guardar.', 'ok');
    } catch (e) {
      setStatus('');
      this.toast('Error extrayendo: ' + e.message, 'err');
    }
  },

  async _doUpload(evt, field) {
    const file = evt.target.files && evt.target.files[0];
    if (!file) return;

    const widget = document.getElementById(`bm-upload-${field}`);
    if (!widget) return;
    const categoria = widget.dataset.categoria;

    // Validación cliente-side (backend valida también)
    const MAX = 25 * 1024 * 1024;
    if (file.size > MAX) {
      this.toast('Archivo excede 25 MB. Comprime o divide.', 'err');
      evt.target.value = '';
      return;
    }
    if (file.size === 0) {
      this.toast('Archivo vacío', 'err');
      evt.target.value = '';
      return;
    }

    // Estado visual: subiendo
    widget.innerHTML = `
      <div style="display:flex;align-items:center;gap:10px;padding:12px 14px;background:var(--bg2,#f8f9fa);border:1px solid var(--border,#e5e7eb);border-radius:6px">
        <div style="width:16px;height:16px;border:2px solid var(--green,#16a34a);border-top-color:transparent;border-radius:50%;animation:bm-spin 0.7s linear infinite"></div>
        <span style="font-size:13px;color:var(--text2,#6b7280);flex:1">Subiendo <strong>${this._esc(file.name)}</strong> (${(file.size/1024/1024).toFixed(2)} MB)...</span>
      </div>
    `;

    try {
      const form = new FormData();
      form.append('categoria', categoria);
      form.append('file', file);

      const r = await fetch('/api/bid/upload', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + this._token() },
        body: form
      });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        throw new Error(err.detail || `Upload → ${r.status}`);
      }
      const res = await r.json();

      // Guardar path en el input hidden (así _collectForm lo recoge)
      const hidden = document.getElementById(`f-${field}`);
      if (hidden) hidden.value = res.path;

      // Re-render widget con el archivo cargado
      widget.innerHTML = this._renderUploadWidget(field, res.path);
      this.toast('Archivo subido', 'ok');
    } catch (e) {
      this.toast('Error subiendo: ' + e.message, 'err');
      widget.innerHTML = this._renderUploadWidget(field, '');
    }
  },

  async _viewFile(field) {
    const path = (document.getElementById(`f-${field}`) || {}).value || '';
    return this._openStoragePath(path);
  },

  // Utilidad pública: abrir cualquier path/URL guardado en storage
  // (uso desde tarjetas fuera del modal, ej: link "Ver archivo" en certificaciones)
  async _openStoragePath(path) {
    if (!path) return;
    if (path.startsWith('http://') || path.startsWith('https://')) {
      window.open(path, '_blank', 'noopener');
      return;
    }
    try {
      const r = await fetch(`/api/bid/signed-url?path=${encodeURIComponent(path)}`, {
        headers: { 'Authorization': 'Bearer ' + this._token() }
      });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        throw new Error(err.detail || `Signed URL → ${r.status}`);
      }
      const { url } = await r.json();
      window.open(url, '_blank', 'noopener');
    } catch (e) {
      this.toast('Error abriendo archivo: ' + e.message, 'err');
    }
  },

  _replaceFile(field) {
    const widget = document.getElementById(`bm-upload-${field}`);
    if (!widget) return;
    // Vaciar el hidden y renderizar el selector; luego auto-click para abrir picker
    const hidden = document.getElementById(`f-${field}`);
    if (hidden) hidden.value = '';
    widget.innerHTML = this._renderUploadWidget(field, '');
    const input = widget.querySelector('input[type="file"]');
    if (input) input.click();
  },

  async _clearFile(field) {
    const hidden = document.getElementById(`f-${field}`);
    const path = hidden ? hidden.value : '';
    if (!path) return;

    // Si es path del bucket (no URL externa), intentar borrar del storage.
    // Silencioso: si falla, seguimos y limpiamos el campo igual.
    if (!path.startsWith('http')) {
      try {
        await fetch(`/api/bid/upload?path=${encodeURIComponent(path)}`, {
          method: 'DELETE',
          headers: { 'Authorization': 'Bearer ' + this._token() }
        });
      } catch(e) { /* silencioso */ }
    }

    if (hidden) hidden.value = '';
    const widget = document.getElementById(`bm-upload-${field}`);
    if (widget) widget.innerHTML = this._renderUploadWidget(field, '');
    this.toast('Archivo quitado', 'ok');
  }
};

// Exponer al scope global para que switchTab2 lo pueda llamar
window.BidManager = BidManager;
