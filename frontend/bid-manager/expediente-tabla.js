/* ============================================================
   expediente-tabla.js — Tabla densa del Expediente v2 (Fase 2)
   ============================================================
   Reemplaza las tarjetas viejas por la tabla del prototipo en:
   representantes, socios, certificaciones (Documentos), personal,
   equipos y experiencia. Empresa y Financieros conservan su vista.

   - Cabecera: breadcrumb · título · buscador · CTA
   - Chips de filtro con conteos
   - Columna Adjuntos conectada a /expediente/archivos/conteos (Fase 0)
   - Acciones por fila reutilizan los modales existentes (abrirX)
   Solo se activa con el flag expediente_v2 (lo monta ExpedienteRail).
   ============================================================ */

const ExpedienteTabla = {
  conteos: {},          // { entidad: { registro_id: {n, tipos} } }
  filtro: 'todos',
  q: '',
  entidadActual: null,

  DEFS: {
    representantes: {
      titulo: 'Representantes legales', dataKey: 'representantes',
      desc: 'Personas autorizadas a firmar ofertas y contratos.',
      cta: '+ Agregar representante', abrir: 'abrirRepresentante', tablaApi: 'representantes',
      cols: ['Nombre', 'Cargo', 'Documento', 'Adjuntos', 'Estado'],
      fila: r => [
        ExpedienteTabla._c2(r.nombre_completo, r.profesion || ''),
        r.cargo || '—',
        `<span class="exp-mono">${r.cedula || '—'}</span>`,
        null,
        r.es_firmante_principal
          ? ExpedienteTabla._badge('ok', 'Firma habilitada')
          : ExpedienteTabla._badge('neutro', 'Registrado'),
      ],
      buscar: r => `${r.nombre_completo} ${r.cargo} ${r.cedula}`,
      estado: r => r.cedula ? 'ok' : 'sin_verificar',
    },
    socios: {
      titulo: 'Socios y accionistas', dataKey: 'socios',
      desc: 'Estructura societaria declarada.',
      cta: '+ Agregar socio', abrir: 'abrirSocio', tablaApi: 'socios',
      cols: ['Nombre', 'Rol', 'Documento', 'Adjuntos', 'Estado'],
      fila: s => [
        ExpedienteTabla._c2(s.nombre_completo, s.es_gerente ? 'Gerente' : ''),
        `Socio${s.porcentaje_participacion ? ` · ${s.porcentaje_participacion}%` : ''}`,
        `<span class="exp-mono">${s.cedula || '—'}</span>`,
        null,
        s.cedula ? ExpedienteTabla._badge('ok', 'Declarado')
                 : ExpedienteTabla._badge('warn', 'Falta cédula'),
      ],
      buscar: s => `${s.nombre_completo} ${s.cedula}`,
      estado: s => s.cedula ? 'ok' : 'sin_verificar',
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
        null,
        '',
      ],
      buscar: c => `${c.nombre_display} ${c.tipo} ${c.numero_certificacion}`,
      estado: c => c.estado === 'vencido' ? 'vencido'
                 : c.estado === 'por_vencer' ? 'por_vencer' : 'ok',
      chipsExtra: true,
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
        `<span class="exp-mono">${p.codia || '—'}</span>`,
        null,
        p.codia ? ExpedienteTabla._badge('ok', 'Verificado')
                : ExpedienteTabla._badge('info', 'Sin certificado'),
      ],
      buscar: p => `${p.nombre_completo} ${p.profesion} ${p.especialidades} ${p.codia}`,
      estado: p => p.codia ? 'ok' : 'sin_verificar',
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
    },
    experiencia: {
      titulo: 'Banco de proyectos', dataKey: 'experiencia',
      desc: 'Obras ejecutadas que se cruzan con la experiencia mínima del pliego.',
      cta: '+ Agregar proyecto', abrir: 'abrirExperiencia', tablaApi: 'experiencia',
      cols: ['Proyecto', 'Cliente y año', 'Monto', 'Adjuntos', 'Estado'],
      fila: x => [
        ExpedienteTabla._c2(x.nombre_proyecto, x.tipo_obra || ''),
        ExpedienteTabla._c2(x.cliente || '—',
          (x.fecha_fin || '').slice(0, 4)),
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
    },
  },

  // ── Helpers de celdas ──
  _c2: (a, b) => `<div class="exp-c1">${a || '—'}</div>${b ? `<div class="exp-c2">${b}</div>` : ''}`,
  _badge: (tono, txt) => `<span class="exp-badge ${tono}">${txt}</span>`,
  _f(fecha) {
    if (!fecha) return 'Sin vencimiento';
    try {
      const d = new Date(fecha + 'T12:00:00');
      return d.toLocaleDateString('es-DO', { day: '2-digit', month: 'short', year: 'numeric' });
    } catch { return fecha; }
  },

  esTabla(sec) { return !!this.DEFS[sec]; },

  // ── Mostrar una entidad como tabla ──
  async mostrar(entidad) {
    const def = this.DEFS[entidad];
    if (!def) return this.ocultar();
    this.entidadActual = entidad;
    this.filtro = 'todos'; this.q = '';

    // Ocultar los paneles viejos; la tabla toma la columna de contenido
    document.querySelectorAll('.bm-panel').forEach(p => p.classList.remove('bm-active'));
    let cont = document.getElementById('exp-tabla');
    if (!cont) {
      cont = document.createElement('div');
      cont.id = 'exp-tabla';
      document.querySelector('.bm-container.exp-v2')?.appendChild(cont);
    }
    cont.style.display = '';
    this._render();

    // Adjuntos lazy por entidad (una sola query — endpoint de la Fase 0)
    if (!this.conteos[entidad]) {
      try {
        this.conteos[entidad] =
          await BidManager._get(`/expediente/archivos/conteos?entidad=${def.tablaApi}`) || {};
      } catch { this.conteos[entidad] = {}; }
      if (this.entidadActual === entidad) this._render();
    }
  },

  ocultar() {
    this.entidadActual = null;
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
        ['por_vencer', 'Por vencer', rows.filter(r => def.estado(r) === 'por_vencer').length],
      );
    }
    chips.push(['sin_archivos', 'Sin adjuntos',
      rows.filter(r => !(cts[r.id]?.n > 0)).length]);
    return chips;
  },

  _render() {
    const def = this.DEFS[this.entidadActual];
    const cont = document.getElementById('exp-tabla');
    if (!cont || !def) return;
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
        ? `<div class="exp-c2">${ct.tipos || ''}</div><span class="exp-adj">${ct.n} archivo${ct.n > 1 ? 's' : ''}</span>`
        : `<div class="exp-c2">Adjunta el respaldo</div><span class="exp-adj exp-adj-warn">Sin archivos</span>`;
      const tds = celdas.map(c => `<td>${c === null ? adjHtml : c}</td>`).join('');
      return `<tr data-id="${r.id}">${tds}
        <td class="exp-acciones">
          <button class="exp-mini" data-editar="${r.id}">Editar</button>
        </td></tr>`;
    }).join('') : `<tr><td colspan="${def.cols.length + 1}" class="exp-vacio">
        Nada por aquí con este filtro.</td></tr>`;

    cont.innerHTML = `
      <div class="exp-cab">
        <div>
          <div class="exp-miga">Expediente Digital <span>›</span> ${def.titulo}</div>
          <h2 class="exp-titulo">${def.titulo}</h2>
          <p class="exp-desc">${def.desc}</p>
        </div>
        <div class="exp-cab-der">
          <input class="exp-buscar" placeholder="Filtrar..." value="${this.q}" aria-label="Filtrar registros">
          <button class="exp-cta" id="exp-cta">${def.cta}</button>
        </div>
      </div>
      <div class="exp-chips">${chipsHtml}
        <span class="exp-conteo">${rows.length} de ${total} registros</span>
      </div>
      <div class="exp-tabla-card">
        <table class="exp-table">
          <thead><tr>${def.cols.map(c => `<th>${c}</th>`).join('')}<th></th></tr></thead>
          <tbody>${filasHtml}</tbody>
        </table>
      </div>`;

    // Bind
    cont.querySelector('.exp-buscar')?.addEventListener('input', ev => {
      clearTimeout(this._t);
      this._t = setTimeout(() => { this.q = ev.target.value; this._render();
        cont.querySelector('.exp-buscar')?.focus(); }, 200);
    });
    cont.querySelectorAll('[data-chip]').forEach(ch =>
      ch.addEventListener('click', () => { this.filtro = ch.dataset.chip; this._render(); }));
    cont.querySelector('#exp-cta')?.addEventListener('click', () => {
      const fn = BidManager[def.abrir]; if (typeof fn === 'function') fn.call(BidManager);
    });
    cont.querySelectorAll('[data-editar]').forEach(b =>
      b.addEventListener('click', ev => { ev.stopPropagation();
        const fn = BidManager[def.abrir];
        if (typeof fn === 'function') fn.call(BidManager, b.dataset.editar);
      }));
  },

  // Invalida el cache de adjuntos de una entidad (tras subir/borrar)
  invalidar(entidad) { delete this.conteos[entidad]; },
};

window.ExpedienteTabla = ExpedienteTabla;
