/* ============================================================
   expediente-tabla.js — Tabla densa + panel de detalle (Fase 2)
   ============================================================
   - Cabecera estática (el buscador NO se re-renderiza al teclear:
     solo se repintan chips y filas → el cursor nunca se pierde)
   - Clic en una fila → panel lateral de 392px con los datos del
     registro, sus ARCHIVOS reales (listar/subir/descargar, endpoints
     Fase 0), y trazabilidad desde bid_auditoria.
   - Todo valor de datos pasa por _esc() antes de insertarse en HTML.
   Solo se activa con el flag expediente_v2 (lo monta ExpedienteRail).
   ============================================================ */

const ExpedienteTabla = {
  conteos: {},
  filtro: 'todos',
  q: '',
  entidadActual: null,
  selId: null,

  // ── Utilidades ──
  _esc(v) {
    return String(v ?? '').replace(/[&<>"']/g, c =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  },
  _c2(a, b) {
    return `<div class="exp-c1">${this._esc(a) || '—'}</div>` +
           (b ? `<div class="exp-c2">${this._esc(b)}</div>` : '');
  },
  _badge: (tono, txt) => `<span class="exp-badge ${tono}">${ExpedienteTabla._esc(txt)}</span>`,
  _f(fecha) {
    if (!fecha) return 'Sin vencimiento';
    try {
      return new Date(fecha + 'T12:00:00')
        .toLocaleDateString('es-DO', { day: '2-digit', month: 'short', year: 'numeric' });
    } catch { return fecha; }
  },
  _kb(n) {
    if (!n) return '';
    return n > 1024 * 1024 ? (n / 1048576).toFixed(1) + ' MB' : Math.round(n / 1024) + ' KB';
  },

  DEFS: {
    representantes: {
      titulo: 'Representantes legales', dataKey: 'representantes',
      desc: 'Personas autorizadas a firmar ofertas y contratos.',
      cta: '+ Agregar representante', abrir: 'abrirRepresentante', tablaApi: 'representantes',
      cols: ['Nombre', 'Cargo', 'Documento', 'Adjuntos', 'Estado'],
      fila: r => [
        ExpedienteTabla._c2(r.nombre_completo, r.profesion || ''),
        ExpedienteTabla._esc(r.cargo || '—'),
        `<span class="exp-mono">${ExpedienteTabla._esc(r.cedula || '—')}</span>`,
        null,
        r.es_firmante_principal
          ? ExpedienteTabla._badge('ok', 'Firma habilitada')
          : ExpedienteTabla._badge('neutro', 'Registrado'),
      ],
      buscar: r => `${r.nombre_completo} ${r.cargo} ${r.cedula}`,
      estado: r => r.cedula ? 'ok' : 'sin_verificar',
      panel: r => ({
        badge: r.es_firmante_principal ? ['ok', 'Firma habilitada'] : ['neutro', 'Registrado'],
        titulo: r.nombre_completo,
        sub: [r.cargo, r.cedula].filter(Boolean).join(' · '),
        campos: [['Cargo', r.cargo], ['Cédula', r.cedula], ['Profesión', r.profesion],
                 ['Email', r.email], ['Teléfono', r.telefono], ['Dirección', r.direccion]],
      }),
    },
    socios: {
      titulo: 'Socios y accionistas', dataKey: 'socios',
      desc: 'Estructura societaria declarada.',
      cta: '+ Agregar socio', abrir: 'abrirSocio', tablaApi: 'socios',
      cols: ['Nombre', 'Rol', 'Documento', 'Adjuntos', 'Estado'],
      fila: s => [
        ExpedienteTabla._c2(s.nombre_completo, s.es_gerente ? 'Gerente' : ''),
        `Socio${s.porcentaje_participacion ? ` · ${ExpedienteTabla._esc(s.porcentaje_participacion)}%` : ''}`,
        `<span class="exp-mono">${ExpedienteTabla._esc(s.cedula || '—')}</span>`,
        null,
        s.cedula ? ExpedienteTabla._badge('ok', 'Declarado')
                 : ExpedienteTabla._badge('warn', 'Falta cédula'),
      ],
      buscar: s => `${s.nombre_completo} ${s.cedula}`,
      estado: s => s.cedula ? 'ok' : 'sin_verificar',
      panel: s => ({
        badge: s.cedula ? ['ok', 'Declarado'] : ['warn', 'Falta cédula'],
        titulo: s.nombre_completo,
        sub: `Participación: ${s.porcentaje_participacion || '—'}%`,
        campos: [['Cédula', s.cedula], ['Participación', s.porcentaje_participacion ? s.porcentaje_participacion + '%' : null],
                 ['Gerente', s.es_gerente ? 'Sí' : 'No'], ['Estado civil', s.estado_civil]],
      }),
    },
    certificaciones: {
      titulo: 'Documentos y certificaciones', dataKey: 'certificaciones',
      desc: 'DGII, TSS, RPE, Registro Mercantil, estatutos. La IA extrae número y vencimiento; tú confirmas.',
      cta: '+ Subir documento', abrir: 'abrirCertificacion', tablaApi: 'certificaciones',
      cols: ['Documento', 'Verificación', 'Vencimiento', 'Adjuntos', ''],
      fila: c => [
        ExpedienteTabla._c2(c.nombre_display || c.tipo, c.tipo || ''),
        ({ vigente: ExpedienteTabla._badge('ok', 'Validado'),
           por_vencer: ExpedienteTabla._badge('warn', 'Vence pronto'),
           vencido: ExpedienteTabla._badge('err', 'Vencida'),
         })[c.estado] || ExpedienteTabla._badge('neutro', c.estado || 'Sin estado'),
        ExpedienteTabla._c2(ExpedienteTabla._f(c.fecha_vencimiento),
          c.numero_certificacion ? `Nº ${c.numero_certificacion}` : ''),
        null, '',
      ],
      buscar: c => `${c.nombre_display} ${c.tipo} ${c.numero_certificacion}`,
      estado: c => c.estado === 'vencido' ? 'vencido'
                 : c.estado === 'por_vencer' ? 'por_vencer' : 'ok',
      chipsExtra: true,
      panel: c => ({
        badge: c.estado === 'vencido' ? ['err', 'Vencida']
             : c.estado === 'por_vencer' ? ['warn', 'Vence pronto'] : ['ok', 'Validado'],
        titulo: c.nombre_display || c.tipo,
        sub: c.numero_certificacion ? `Nº ${c.numero_certificacion}` : '',
        campos: [['Tipo', c.tipo], ['Número', c.numero_certificacion],
                 ['Emisión', ExpedienteTabla._f(c.fecha_emision)],
                 ['Vencimiento', ExpedienteTabla._f(c.fecha_vencimiento)],
                 ['Notas', c.notas]],
      }),
    },
    personal: {
      titulo: 'Personal técnico', dataKey: 'personal',
      desc: 'Ingenieros, arquitectos y personal profesional disponible para asignar a la oferta.',
      cta: '+ Agregar persona', abrir: 'abrirPersonal', tablaApi: 'personal',
      cols: ['Profesional', 'Especialidad', 'CODIA', 'Adjuntos', 'Estado'],
      fila: p => [
        ExpedienteTabla._c2(p.nombre_completo, p.profesion || ''),
        ExpedienteTabla._c2(p.especialidades || '—',
          [p.experiencia_general_anios ? `${p.experiencia_general_anios} años` : null,
           p.tiene_maestria ? 'Con maestría' : null].filter(Boolean).join(' · ')),
        `<span class="exp-mono">${ExpedienteTabla._esc(p.codia || '—')}</span>`,
        null,
        p.codia ? ExpedienteTabla._badge('ok', 'Verificado')
                : ExpedienteTabla._badge('info', 'Sin certificado'),
      ],
      buscar: p => `${p.nombre_completo} ${p.profesion} ${p.especialidades} ${p.codia}`,
      estado: p => p.codia ? 'ok' : 'sin_verificar',
      panel: p => ({
        badge: p.codia ? ['ok', 'Verificado'] : ['info', 'Sin certificado'],
        titulo: p.nombre_completo,
        sub: [p.profesion, p.codia ? `CODIA ${p.codia}` : null].filter(Boolean).join(' · '),
        campos: [['Profesión', p.profesion], ['Especialidad', p.especialidades],
                 ['Registro CODIA', p.codia],
                 ['Años de experiencia', p.experiencia_general_anios ? `${p.experiencia_general_anios} años` : null],
                 ['Posgrado', p.tiene_maestria ? (p.maestria_descripcion || 'Con maestría') : 'No'],
                 ['Cargo en la empresa', p.cargo_empresa],
                 ['Email', p.email], ['Teléfono', p.telefono], ['Cédula', p.cedula]],
      }),
    },
    equipos: {
      titulo: 'Equipos y maquinaria', dataKey: 'equipos',
      desc: 'Equipos propios y alquilados con su disponibilidad real.',
      cta: '+ Agregar equipo', abrir: 'abrirEquipo', tablaApi: 'equipos',
      cols: ['Equipo', 'Origen', 'Capacidad', 'Adjuntos', 'Estado'],
      fila: e => [
        ExpedienteTabla._c2(e.descripcion, e.tipo || ''),
        ExpedienteTabla._c2(e.propiedad === 'propio' ? 'Propio' : 'Alquilado',
          [e.marca, e.modelo, e.anio].filter(Boolean).join(' ')),
        ExpedienteTabla._c2(e.capacidad || '—', `Cantidad: ${e.cantidad || 1}`),
        null,
        e.propiedad === 'propio'
          ? ExpedienteTabla._badge('ok', 'Disponible')
          : ExpedienteTabla._badge('warn', e.empresa_alquiler || 'Alquilado'),
      ],
      buscar: e => `${e.descripcion} ${e.tipo} ${e.marca} ${e.modelo}`,
      estado: e => e.propiedad === 'propio' ? 'ok' : 'sin_verificar',
      panel: e => ({
        badge: e.propiedad === 'propio' ? ['ok', 'Disponible'] : ['warn', 'Alquilado'],
        titulo: e.descripcion,
        sub: [e.tipo, [e.marca, e.modelo, e.anio].filter(Boolean).join(' ')].filter(Boolean).join(' · '),
        campos: [['Tipo', e.tipo], ['Propiedad', e.propiedad],
                 ['Marca / Modelo', [e.marca, e.modelo].filter(Boolean).join(' ')],
                 ['Año', e.anio], ['Cantidad', e.cantidad || 1], ['Capacidad', e.capacidad],
                 ['Empresa de alquiler', e.empresa_alquiler]],
      }),
    },
    experiencia: {
      titulo: 'Banco de proyectos', dataKey: 'experiencia',
      desc: 'Obras ejecutadas que se cruzan con la experiencia mínima del pliego.',
      cta: '+ Agregar proyecto', abrir: 'abrirExperiencia', tablaApi: 'experiencia',
      cols: ['Proyecto', 'Cliente y año', 'Monto', 'Adjuntos', 'Estado'],
      fila: x => [
        ExpedienteTabla._c2(x.nombre_proyecto, x.tipo_obra || ''),
        ExpedienteTabla._c2(x.cliente || '—', (x.fecha_fin || '').slice(0, 4)),
        ExpedienteTabla._c2(
          x.monto_contrato ? `${x.moneda || 'RD$'} ${Number(x.monto_contrato).toLocaleString('es-DO')}` : '—',
          x.provincia || ''),
        null,
        x.estado === 'concluida' || x.estado === 'certificada'
          ? ExpedienteTabla._badge('ok', 'Certificada')
          : ExpedienteTabla._badge('info', x.estado || 'En curso'),
      ],
      buscar: x => `${x.nombre_proyecto} ${x.cliente} ${x.tipo_obra}`,
      estado: x => (x.estado === 'concluida' || x.estado === 'certificada') ? 'ok' : 'sin_verificar',
      panel: x => ({
        badge: (x.estado === 'concluida' || x.estado === 'certificada')
          ? ['ok', 'Certificada'] : ['info', x.estado || 'En curso'],
        titulo: x.nombre_proyecto,
        sub: [x.cliente, x.tipo_obra].filter(Boolean).join(' · '),
        campos: [['Cliente', x.cliente], ['Tipo de obra', x.tipo_obra],
                 ['Monto', x.monto_contrato ? `${x.moneda || 'RD$'} ${Number(x.monto_contrato).toLocaleString('es-DO')}` : null],
                 ['Inicio', ExpedienteTabla._f(x.fecha_inicio)], ['Fin', ExpedienteTabla._f(x.fecha_fin)],
                 ['Ubicación', x.ubicacion || x.provincia], ['Nº de contrato', x.numero_contrato]],
      }),
    },
  },

  esTabla(sec) { return !!this.DEFS[sec]; },

  // ── Mostrar entidad ──
  async mostrar(entidad, filtro = null) {
    const def = this.DEFS[entidad];
    if (!def) return this.ocultar();
    const cambia = this.entidadActual !== entidad;
    this.entidadActual = entidad;
    if (cambia) { this.filtro = 'todos'; this.q = ''; this.cerrarPanel(); }
    if (filtro) this.filtro = filtro;

    document.querySelectorAll('.bm-panel').forEach(p => p.classList.remove('bm-active'));
    let cont = document.getElementById('exp-tabla');
    if (!cont) {
      cont = document.createElement('div');
      cont.id = 'exp-tabla';
      (document.getElementById('exp-main')
        || document.querySelector('.bm-container.exp-v2'))?.appendChild(cont);
    }
    cont.style.display = '';
    this._renderCabecera();   // estática: no se toca al teclear
    this._renderCuerpo();     // chips + filas

    if (!this.conteos[entidad]) {
      try {
        this.conteos[entidad] =
          await BidManager._get(`/expediente/archivos/conteos?entidad=${def.tablaApi}`) || {};
      } catch { this.conteos[entidad] = {}; }
      if (this.entidadActual === entidad) this._renderCuerpo();
    }
  },

  ocultar() {
    this.entidadActual = null;
    this.cerrarPanel();
    const cont = document.getElementById('exp-tabla');
    if (cont) cont.style.display = 'none';
  },

  _datos() {
    const def = this.DEFS[this.entidadActual];
    return (BidManager.data?.[def.dataKey] || []);
  },

  _filtrados() {
    const def = this.DEFS[this.entidadActual];
    let rows = this._datos();
    if (this.q) {
      const q = this.q.toLowerCase();
      rows = rows.filter(r => (def.buscar(r) || '').toLowerCase().includes(q));
    }
    if (this.filtro !== 'todos') {
      if (this.filtro === 'sin_archivos') {
        const cts = this.conteos[this.entidadActual] || {};
        rows = rows.filter(r => !(cts[r.id]?.n > 0));
      } else {
        rows = rows.filter(r => def.estado(r) === this.filtro);
      }
    }
    return rows;
  },

  _chips() {
    const def = this.DEFS[this.entidadActual];
    const rows = this._datos();
    const cts = this.conteos[this.entidadActual] || {};
    const chips = [['todos', 'Todos', rows.length]];
    if (def.chipsExtra) {
      chips.push(
        ['vencido', 'Vencidos', rows.filter(r => def.estado(r) === 'vencido').length],
        ['por_vencer', 'Por vencer', rows.filter(r => def.estado(r) === 'por_vencer').length]);
    }
    chips.push(['sin_archivos', 'Sin adjuntos', rows.filter(r => !(cts[r.id]?.n > 0)).length]);
    return chips;
  },

  // ── Render: cabecera UNA vez, cuerpo cuantas veces haga falta ──
  _renderCabecera() {
    const def = this.DEFS[this.entidadActual];
    const cont = document.getElementById('exp-tabla');
    cont.innerHTML = `
      <div class="exp-cab">
        <div>
          <div class="exp-miga">Expediente Digital <span>›</span> ${def.titulo}</div>
          <h2 class="exp-titulo">${def.titulo}</h2>
          <p class="exp-desc">${def.desc}</p>
        </div>
        <div class="exp-cab-der">
          <input class="exp-buscar" id="exp-buscar" placeholder="Filtrar..."
                 aria-label="Filtrar registros" autocomplete="off">
          <button class="exp-cta" id="exp-cta">${def.cta}</button>
        </div>
      </div>
      <div id="exp-cuerpo"></div>`;

    const input = cont.querySelector('#exp-buscar');
    input.value = this.q;
    input.addEventListener('input', () => {          // el input NUNCA se re-crea
      clearTimeout(this._t);
      this._t = setTimeout(() => { this.q = input.value; this._renderCuerpo(); }, 200);
    });
    cont.querySelector('#exp-cta')?.addEventListener('click', () => {
      const fn = BidManager[def.abrir]; if (typeof fn === 'function') fn.call(BidManager);
    });
  },

  _renderCuerpo() {
    const def = this.DEFS[this.entidadActual];
    const cuerpo = document.getElementById('exp-cuerpo');
    if (!cuerpo || !def) return;
    const rows = this._filtrados();
    const cts = this.conteos[this.entidadActual] || {};
    const total = this._datos().length;

    const chipsHtml = this._chips().map(([id, label, n]) => `
      <button class="exp-chip ${this.filtro === id ? 'exp-chip-on' : ''}" data-chip="${id}">
        ${label} <span>${n}</span>
      </button>`).join('');

    const filasHtml = rows.length ? rows.map(r => {
      const celdas = def.fila(r);
      const ct = cts[r.id];
      const adjHtml = ct?.n
        ? `<div class="exp-c2">${this._esc(ct.tipos || '')}</div><span class="exp-adj">${ct.n} archivo${ct.n > 1 ? 's' : ''}</span>`
        : `<div class="exp-c2">Adjunta el respaldo</div><span class="exp-adj exp-adj-warn">Sin archivos</span>`;
      const tds = celdas.map(c => `<td>${c === null ? adjHtml : c}</td>`).join('');
      const sel = r.id === this.selId ? ' class="exp-sel"' : '';
      return `<tr data-id="${this._esc(r.id)}"${sel}>${tds}
        <td class="exp-acciones"><button class="exp-mini" data-editar="${this._esc(r.id)}">Editar</button></td></tr>`;
    }).join('') : `<tr><td colspan="${def.cols.length + 1}" class="exp-vacio">Nada por aquí con este filtro.</td></tr>`;

    cuerpo.innerHTML = `
      <div class="exp-chips">${chipsHtml}
        <span class="exp-conteo">${rows.length} de ${total} registros</span>
      </div>
      <div class="exp-tabla-card">
        <table class="exp-table">
          <thead><tr>${def.cols.map(c => `<th>${c}</th>`).join('')}<th></th></tr></thead>
          <tbody>${filasHtml}</tbody>
        </table>
      </div>`;

    cuerpo.querySelectorAll('[data-chip]').forEach(ch =>
      ch.addEventListener('click', () => { this.filtro = ch.dataset.chip; this._renderCuerpo(); }));
    cuerpo.querySelectorAll('[data-editar]').forEach(b =>
      b.addEventListener('click', ev => {
        ev.stopPropagation();
        const fn = BidManager[def.abrir];
        if (typeof fn === 'function') fn.call(BidManager, b.dataset.editar);
      }));
    cuerpo.querySelectorAll('tr[data-id]').forEach(tr =>
      tr.addEventListener('click', () => this.abrirPanel(tr.dataset.id)));
  },

  // ══════════════════════════════════════════════════════════
  //  PANEL DE DETALLE (392px, como el prototipo)
  // ══════════════════════════════════════════════════════════
  async abrirPanel(id) {
    const def = this.DEFS[this.entidadActual];
    const reg = this._datos().find(r => String(r.id) === String(id));
    if (!def || !reg) return;
    this.selId = id;
    this._renderCuerpo();  // marca la fila seleccionada

    const info = def.panel(reg);
    let panel = document.getElementById('exp-panel');
    if (!panel) {
      const back = document.createElement('div');
      back.id = 'exp-backdrop';
      back.addEventListener('click', () => this.cerrarPanel());
      document.body.appendChild(back);
      panel = document.createElement('aside');
      panel.id = 'exp-panel';
      panel.setAttribute('role', 'dialog');
      panel.setAttribute('aria-label', 'Detalle del registro');
      document.body.appendChild(panel);
      document.addEventListener('keydown', this._escHandler = ev => {
        if (ev.key === 'Escape') this.cerrarPanel();
      });
    }
    document.body.classList.add('exp-panel-abierto');

    const [tono, btxt] = info.badge;
    const camposHtml = info.campos
      .filter(([, v]) => v !== null && v !== undefined && v !== '')
      .map(([k, v]) => `
        <div class="exp-p-campo">
          <div class="exp-p-k">${this._esc(k)}</div>
          <div class="exp-p-v">${this._esc(v)}</div>
        </div>`).join('') || '<div class="exp-p-campo"><div class="exp-p-v">Sin datos adicionales.</div></div>';

    panel.innerHTML = `
      <div class="exp-p-head">
        ${this._badge(tono, btxt)}
        <button class="exp-p-x" aria-label="Cerrar">✕</button>
      </div>
      <h3 class="exp-p-titulo">${this._esc(info.titulo)}</h3>
      <div class="exp-p-sub">${this._esc(info.sub || '')}</div>
      <div class="exp-p-acciones">
        <button class="exp-cta" id="exp-p-editar">Editar registro</button>
      </div>
      <div class="exp-p-campos">${camposHtml}</div>
      <div class="exp-p-sec">Archivos adjuntos <span id="exp-p-narchivos"></span></div>
      <div id="exp-p-archivos"><div class="exp-p-cargando">Cargando archivos…</div></div>
      <label class="exp-p-drop" id="exp-p-drop">
        <input type="file" id="exp-p-file" multiple hidden
               accept=".pdf,.doc,.docx,.xls,.xlsx,image/jpeg,image/png">
        + Subir CV, facturas, actas o fotos · PDF, Word, Excel, JPG
      </label>
      <div class="exp-p-sec">Trazabilidad</div>
      <div id="exp-p-traza"><div class="exp-p-cargando">…</div></div>`;

    panel.querySelector('.exp-p-x').addEventListener('click', () => this.cerrarPanel());
    panel.querySelector('#exp-p-editar').addEventListener('click', () => {
      const fn = BidManager[def.abrir];
      if (typeof fn === 'function') fn.call(BidManager, id);
    });
    panel.querySelector('#exp-p-file').addEventListener('change', ev =>
      this._subirArchivos(def.tablaApi, id, ev.target.files));
    const drop = panel.querySelector('#exp-p-drop');
    ['dragover', 'dragenter'].forEach(evn => drop.addEventListener(evn, ev => {
      ev.preventDefault(); drop.classList.add('exp-p-drop-on');
    }));
    ['dragleave', 'drop'].forEach(evn => drop.addEventListener(evn, ev => {
      ev.preventDefault(); drop.classList.remove('exp-p-drop-on');
    }));
    drop.addEventListener('drop', ev =>
      this._subirArchivos(def.tablaApi, id, ev.dataTransfer?.files));

    this._cargarArchivos(def.tablaApi, id);
    this._cargarTraza(def.tablaApi, id);
  },

  cerrarPanel() {
    this.selId = null;
    document.body.classList.remove('exp-panel-abierto');
    document.getElementById('exp-panel')?.remove();
    document.getElementById('exp-backdrop')?.remove();
    if (this.entidadActual) this._renderCuerpo();
  },

  async _cargarArchivos(entidadApi, id) {
    const box = document.getElementById('exp-p-archivos');
    if (!box) return;
    let archivos = [];
    try {
      archivos = await BidManager._get(`/expediente/${entidadApi}/${id}/archivos`) || [];
    } catch { box.innerHTML = '<div class="exp-p-cargando">No se pudieron cargar.</div>'; return; }
    const n = document.getElementById('exp-p-narchivos');
    if (n) n.textContent = archivos.length ? `${archivos.length} archivo${archivos.length > 1 ? 's' : ''}` : '';
    if (!archivos.length) {
      box.innerHTML = '<div class="exp-p-cargando">Aún no hay archivos en este registro.</div>';
      return;
    }
    const EXT = m => m.includes('pdf') ? 'PDF' : m.includes('image') ? 'IMG'
                 : m.includes('sheet') || m.includes('excel') ? 'XLS' : 'DOC';
    box.innerHTML = archivos.map(a => `
      <div class="exp-p-arch">
        <span class="exp-p-ext">${EXT(a.mime)}</span>
        <div class="exp-p-arch-info">
          <div class="exp-p-arch-nombre">${this._esc(a.nombre)}</div>
          <div class="exp-c2">${this._kb(a.tamano)} · ${a.estado === 'validado' ? 'verificado ✓' : this._esc(a.estado)}</div>
        </div>
        ${a.estado !== 'validado'
          ? `<button class="exp-p-dl" data-ok="${this._esc(a.id)}" title="Marcar como verificado">✓</button>` : ''}
        <button class="exp-p-dl" data-dl="${this._esc(a.id)}" title="Descargar">⬇</button>
        <button class="exp-p-dl exp-p-del" data-del="${this._esc(a.id)}"
                data-nombre="${this._esc(a.nombre)}" title="Eliminar">🗑</button>
      </div>`).join('');
    box.querySelectorAll('[data-ok]').forEach(b =>
      b.addEventListener('click', async () => {
        b.disabled = true;
        try {
          await BidManager._fetchAuth(`/api/bid/expediente/archivos/${b.dataset.ok}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ estado: 'validado' }),
          });
        } catch { /* silencioso */ }
        await this._cargarArchivos(entidadApi, id);
        this._cargarTraza(entidadApi, id);
      }));
    box.querySelectorAll('[data-del]').forEach(b =>
      b.addEventListener('click', async () => {
        if (!confirm(`¿Eliminar "${b.dataset.nombre}"?\nPodrás volver a subirlo si lo necesitas.`)) return;
        b.disabled = true;
        try {
          const r = await BidManager._fetchAuth(`/api/bid/expediente/archivos/${b.dataset.del}`,
            { method: 'DELETE' });
          if (!r.ok) alert('No se pudo eliminar el archivo');
        } catch { alert('Error de conexión al eliminar'); }
        this.invalidar(this.entidadActual);
        await this._cargarArchivos(entidadApi, id);
        this._cargarTraza(entidadApi, id);
        this.mostrar(this.entidadActual);      // conteos de la tabla
        window.ExpedienteRail?.refresh?.();    // anillo y metas
      }));
    box.querySelectorAll('[data-dl]').forEach(b =>
      b.addEventListener('click', async () => {
        b.disabled = true;
        try {
          const r = await BidManager._get(`/expediente/archivos/${b.dataset.dl}/descargar`);
          if (r?.url) {
            const a = document.createElement('a');
            a.href = r.url; a.download = r.nombre || '';
            document.body.appendChild(a); a.click(); a.remove();
          }
        } catch { /* silencioso */ }
        b.disabled = false;
      }));
  },

  DROP_HTML: `<input type="file" id="exp-p-file" multiple hidden
               accept=".pdf,.doc,.docx,.xls,.xlsx,image/jpeg,image/png">
        + Subir CV, facturas, actas o fotos · PDF, Word, Excel, JPG`,

  async _subirArchivos(entidadApi, id, files) {
    if (!files?.length) return;
    const drop = document.getElementById('exp-p-drop');
    if (drop) drop.textContent = 'Subiendo…';
    const fd = new FormData();
    [...files].forEach(f => fd.append('files', f));
    try {
      const r = await BidManager._fetchAuth(`/api/bid/expediente/${entidadApi}/${id}/archivos`,
        { method: 'POST', body: fd });
      if (!r.ok) {
        const e = await r.json().catch(() => null);
        alert(e?.detail?.message || 'No se pudo subir el archivo');
      }
    } catch { alert('Error de conexión al subir'); }
    this.invalidar(this.entidadActual);
    if (drop) {                                 // restaurar la dropzone
      drop.innerHTML = this.DROP_HTML;
      drop.querySelector('#exp-p-file')?.addEventListener('change', ev =>
        this._subirArchivos(entidadApi, id, ev.target.files));
    }
    await this._cargarArchivos(entidadApi, id);
    this._cargarTraza(entidadApi, id);
    this.mostrar(this.entidadActual);          // refresca conteos de la tabla
    window.ExpedienteRail?.refresh?.();        // y el anillo/metas del rail
  },

  async _cargarTraza(entidadApi, id) {
    const box = document.getElementById('exp-p-traza');
    if (!box) return;
    try {
      const todos = await BidManager._get('/expediente/actividad?limit=50') || [];
      const propios = todos.filter(a => String(a.registro_id) === String(id)).slice(0, 5);
      const TXT = {
        subio_archivo: 'Archivo subido', elimino_archivo: 'Archivo eliminado',
        verifico_archivo: 'Archivo verificado',
      };
      box.innerHTML = propios.length ? propios.map(a => `
        <div class="exp-p-tz">
          <span class="exp-dot ok"></span>
          <div>
            <div class="exp-p-tz-t">${this._esc(TXT[a.accion] || a.accion)}${a.detalle?.nombre ? ' · ' + this._esc(a.detalle.nombre) : ''}</div>
            <div class="exp-c2">${this._f((a.creado_en || '').slice(0, 10))}</div>
          </div>
        </div>`).join('')
        : '<div class="exp-p-cargando">Sin actividad registrada aún.</div>';
    } catch { box.innerHTML = ''; }
  },

  invalidar(entidad) { delete this.conteos[entidad]; },
};

window.ExpedienteTabla = ExpedienteTabla;
