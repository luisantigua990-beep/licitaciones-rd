// Script principal de la app (antes estaba inline en index.html).
// PERF: como archivo aparte el navegador lo guarda en caché (con ?v=<hash>) y
// reutiliza el código ya compilado en visitas siguientes. Se carga en el mismo punto
// del documento y sin defer, así que se ejecuta exactamente igual que antes.
const API = '';
// ── THEME ──
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const isLight = theme === 'light';
  // Drawer móvil
  const track = document.getElementById('drawer-theme-track');
  const icon  = document.getElementById('drawer-theme-icon');
  const label = document.getElementById('drawer-theme-label');
  if (track)  track.classList.toggle('on', isLight);
  if (icon)   icon.textContent  = isLight ? '○' : '●';
  if (label)  label.textContent = isLight ? 'Modo claro' : 'Modo oscuro';
  // Sidebar desktop
  const dsbTrack = document.getElementById('dsb-theme-track');
  const dsbLabel = document.getElementById('dsb-theme-label');
  if (dsbTrack) dsbTrack.classList.toggle('on', isLight);
  if (dsbLabel) dsbLabel.textContent = isLight ? 'Modo claro' : 'Modo oscuro';
  localStorage.setItem('ll_theme', theme);
}
function toggleTheme() {
  const cur = document.documentElement.getAttribute('data-theme') || 'dark';
  applyTheme(cur === 'dark' ? 'light' : 'dark');
}
(function(){ applyTheme(localStorage.getItem('ll_theme') || 'dark'); })();

// ── DRAWER ──
function openDrawer() {
  document.getElementById('drawer').classList.add('open');
  document.getElementById('drawer-overlay').classList.add('visible');
  document.body.classList.add('drawer-open');
  document.body.style.overflow = 'hidden';
}
function closeDrawer() {
  document.getElementById('drawer').classList.remove('open');
  document.getElementById('drawer-overlay').classList.remove('visible');
  document.body.classList.remove('drawer-open');
  document.body.style.overflow = '';
}
document.addEventListener('keydown', e => { if(e.key==='Escape') closeDrawer(); });
const SUPABASE_URL = 'https://yqmwwwqgtkrxtjxaqyxk.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxbXd3d3FndGtyeHRqeGFxeXhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE3ODE2NDYsImV4cCI6MjA4NzM1NzY0Nn0.DbhJpEBYac1DZLUU05lz4rF9qHjDCViaI-FAHtgN2ac';
let currentUser = null;

// ── Respaldo de sesión en cookies ──────────────────────────────────────────
// iOS borra el localStorage de las PWA instaladas (standalone) entre cierres.
// Las cookies sobreviven mejor en ese contexto, así que guardamos la sesión en
// ambos lados y la rehidratamos al arrancar si el localStorage quedó vacío.
// ── JWT: decodificar payload (base64url → base64) ──
// atob() falla con los caracteres '-' y '_' del base64url. Los tokens de
// Google traen avatar_url/nombre en user_metadata y casi siempre los contienen.
function _jwtPayload(token) {
  let b = String(token || '').split('.')[1] || '';
  b = b.replace(/-/g, '+').replace(/_/g, '/');
  while (b.length % 4) b += '=';
  const bin = atob(b);
  const bytes = Uint8Array.from(bin, c => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));
}
function _setCookie(nombre, valor, dias) {
  try {
    const exp = new Date(Date.now() + dias * 864e5).toUTCString();
    document.cookie = `${nombre}=${encodeURIComponent(valor)}; Expires=${exp}; Path=/; SameSite=Lax; Secure`;
  } catch (e) {}
}
function _getCookie(nombre) {
  try {
    const m = document.cookie.match(new RegExp('(?:^|; )' + nombre + '=([^;]*)'));
    return m ? decodeURIComponent(m[1]) : null;
  } catch (e) { return null; }
}
function _delCookie(nombre) {
  try { document.cookie = `${nombre}=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; SameSite=Lax; Secure`; } catch (e) {}
}
function _syncSesionCookies() {
  const t = localStorage.getItem('sb_token');
  const r = localStorage.getItem('sb_refresh_token');
  const u = localStorage.getItem('sb_user');
  if (t) _setCookie('sb_token', t, 30);
  if (r) _setCookie('sb_refresh_token', r, 30);
  if (u) _setCookie('sb_user', u, 30);
}
function _hydratarSesionDesdeCookies() {
  if (!localStorage.getItem('sb_user')) {
    const u = _getCookie('sb_user');
    if (u) {
      localStorage.setItem('sb_user', u);
      const t = _getCookie('sb_token');
      const r = _getCookie('sb_refresh_token');
      if (t) localStorage.setItem('sb_token', t);
      if (r) localStorage.setItem('sb_refresh_token', r);
      console.log('🍪 Sesión rehidratada desde cookies (localStorage estaba vacío)');
    }
  }
}
let searchTimer = null;
let pushSubscription = null;

// ── SEGURIDAD: sanitize contra XSS ──────────────────────────
function sanitize(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#x27;');
}
function safeUrl(url) {
  if (!url) return '';
  const u = String(url).trim();
  return (u.startsWith('http://') || u.startsWith('https://')) ? u : '';
}
// ────────────────────────────────────────────────────────────

// ── AUTH ──
async function supabaseRequest(endpoint, method, body) {
  const token = localStorage.getItem('sb_token');
  const headers = { 'Content-Type': 'application/json', 'apikey': SUPABASE_ANON_KEY };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  try {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/${endpoint}`, {
      method, headers, body: body ? JSON.stringify(body) : undefined
    });
    let data = {};
    try { data = await res.json(); } catch (e) { data = {}; }
    // Si la respuesta es un error HTTP sin cuerpo JSON útil (429, 500, etc.),
    // construimos un error legible para que login()/register() no fallen en silencio.
    if (!res.ok && !data.error && !data.msg && !data.error_code) {
      if (res.status === 429) { data.error_code = 'over_request_rate_limit'; data.msg = 'Demasiados intentos. Espera un momento.'; }
      else { data.error_code = 'http_' + res.status; data.msg = 'Error del servidor (' + res.status + ')'; }
    }
    return data;
  } catch (e) {
    return { error_code: 'network_error', msg: 'Sin conexión. Revisa tu internet e intenta de nuevo.' };
  }
}

// ── SOCIAL AUTH (Supabase OAuth) ──
// Navegación directa a Supabase → Google. Al volver, _procesarCallbackOAuth()
// guarda la sesión que llega en el #hash.
function loginSocial(provider) {
  showMsg('Redirigiendo...', 'info');
  const redirectTo = encodeURIComponent(window.location.origin + window.location.pathname);
  window.location.href = `${SUPABASE_URL}/auth/v1/authorize?provider=${encodeURIComponent(provider)}&redirect_to=${redirectTo}`;
}

async function login() {
  const email = document.getElementById('login-email').value.trim().toLowerCase();
  const password = document.getElementById('login-password').value;
  if (!email || !password) return showMsg('Ingresa correo y contraseña', 'error');
  const btn = document.getElementById('btn-login');
  btn.disabled = true; btn.textContent = 'Entrando...';
  const data = await supabaseRequest('token?grant_type=password', 'POST', { email, password });
  btn.disabled = false; btn.textContent = 'Entrar';
  // Validación a prueba de balas: Supabase SOLO devuelve access_token si las credenciales
  // son válidas. Manejamos ambos formatos de error de GoTrue:
  //   viejo: { error, error_description }   nuevo: { code, error_code, msg }
  if (!data.access_token || data.error || data.error_code || data.msg) {
    const msg = (data.error_description || data.msg || data.error || data.error_code || '').toLowerCase();
    const code = (data.error_code || '').toLowerCase();
    let msgEs = 'Correo o contraseña incorrectos';
    if (code.includes('rate') || msg.includes('rate limit') || msg.includes('too many') || msg.includes('demasiados')) {
      msgEs = '⏳ Demasiados intentos. Espera 1-2 minutos e intenta de nuevo.';
    } else if (code === 'network_error') {
      msgEs = 'Sin conexión. Revisa tu internet e intenta de nuevo.';
    } else if (msg.includes('not confirmed') || msg.includes('confirm')) {
      msgEs = 'Confirma tu correo antes de entrar. Revisa tu email.';
    } else if (msg && !msg.includes('invalid') && !msg.includes('credential') && !msg.includes('grant')) {
      msgEs = data.msg || msgEs;
    }
    return showMsg(msgEs, 'error');
  }
  localStorage.setItem('sb_token', data.access_token);
  if (data.refresh_token) localStorage.setItem('sb_refresh_token', data.refresh_token);
  // Supabase devuelve el user en data.user con user_metadata (nombre, etc.)
  let user = data.user;
  if (!user || !user.id) {
    try {
      const payload = _jwtPayload(data.access_token);
      user = {
        id: payload.sub,
        email: payload.email || email,
        user_metadata: payload.user_metadata || {}
      };
    } catch(e) { console.error('Error decodificando JWT:', e); }
  }
  localStorage.setItem('sb_user', JSON.stringify(user));
  currentUser = user;
  _syncSesionCookies();
  console.log('Login OK, user:', currentUser?.id);
  stopShowcaseRefresh();
  cargarPerfilEmpresa();   // carga perfil para ajuste de score
  cargarSeguimiento();
  showApp();
}

// ═══════════════════════════════════════════════════════════
// PROGRAMA DE REFERIDOS — captura ?ref= y registro post-signup
// ═══════════════════════════════════════════════════════════
(function capturarReferralCode() {
  try {
    const params = new URLSearchParams(window.location.search);
    const ref = params.get('ref');
    if (ref) {
      localStorage.setItem('ll_ref_code', ref.toUpperCase());
      params.delete('ref');
      const newUrl = window.location.pathname + (params.toString() ? '?' + params.toString() : '');
      history.replaceState({}, '', newUrl);
      console.log('🎁 Código de referido capturado:', ref.toUpperCase());
    }
    // Tracking de fuente (agente6, instagram, webinar...)
    const utm = params.get('utm_source');
    if (utm) localStorage.setItem('ll_source', utm);
  } catch(e) {}
})();

async function vincularReferido() {
  const refCode = localStorage.getItem('ll_ref_code');
  if (!refCode) return;
  const token = localStorage.getItem('sb_token');
  if (!token) return;
  try {
    const r = await fetch(`${API}/api/referidos/registrar`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify({ ref_code: refCode })
    });
    const d = await r.json();
    if (d.ok) {
      localStorage.removeItem('ll_ref_code');
      console.log('🎁 Referido vinculado:', d.msg);
    }
  } catch(e) { console.log('Ref error:', e); }
}

// ═══════════════════════════════════════════════════════════
// TOKEN VÁLIDO — refresca si está vencido antes de usarlo
// ═══════════════════════════════════════════════════════════
async function obtenerTokenValido() {
  let token = localStorage.getItem('sb_token');
  if (!token) return null;
  let vencido = false;
  try {
    const payload = _jwtPayload(token);
    vencido = payload.exp && (payload.exp * 1000) < Date.now();
  } catch(e) {}
  if (vencido) {
    const ok = await _tryRefreshToken();
    if (ok) token = localStorage.getItem('sb_token');
    else return null;
  }
  return token;
}

// ═══════════════════════════════════════════════════════════
// FREEMIUM — verificación de plan PRO con cache
// ═══════════════════════════════════════════════════════════
let _esPro = null;
async function esUsuarioPro(forzar = false) {
  if (_esPro !== null && !forzar) return _esPro;
  const token = await obtenerTokenValido();
  if (!token) { _esPro = false; return false; }
  try {
    const r = await fetch(`${API}/api/pagos/mi-suscripcion`, {
      headers: { 'Authorization': 'Bearer ' + token }
    });
    const d = await r.json();
    _esPro = d.activa === true;
  } catch { _esPro = false; }
  return _esPro;
}

async function verificarAccesoPro(featureNombre) {
  if (await esUsuarioPro()) return true;
  mostrarModalUpgrade(featureNombre);
  return false;
}

function mostrarModalUpgrade(feature) {
  const nombres = {
    'analisis': 'el Análisis AI de pliegos',
    'empresas': 'la Inteligencia Competitiva',
    'precio': 'Precio Inteligente',
    'pdf': 'la descarga de análisis en PDF',
    'seguimiento': 'el seguimiento ilimitado de procesos',
    'notificaciones': 'las alertas de todos tus procesos'
  };
  const featureTxt = nombres[feature] || 'esta función';
  const esNotif = feature === 'notificaciones';
  const titulo = esNotif ? '¡Notificaciones activadas!' : 'Función PRO';
  const cuerpo = esNotif
    ? 'Recibirás avisos de fechas de tus procesos seguidos. Con <strong>LicitacionLab PRO</strong> también te llegan alertas de <strong>procesos nuevos de tu rubro</strong> apenas se publican, más análisis con IA ilimitados.'
    : `Para usar ${featureTxt} necesitas una suscripción activa de LicitacionLab PRO.`;
  const cta = esNotif ? 'Ver planes PRO' : 'Ver planes PRO';
  const existente = document.getElementById('modal-upgrade-pro');
  if (existente) existente.remove();
  const modal = document.createElement('div');
  modal.id = 'modal-upgrade-pro';
  modal.style.cssText = 'position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.75);backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;padding:20px;';
  modal.innerHTML = `
    <div style="max-width:380px;width:100%;background:var(--surface);border:1px solid var(--border2);border-radius:18px;padding:28px;text-align:center;position:relative;">
      <button onclick="document.getElementById('modal-upgrade-pro').remove()" style="position:absolute;top:12px;right:14px;background:none;border:none;color:var(--muted);font-size:18px;cursor:pointer;">✕</button>
      <div style="font-size:16px;font-weight:800;color:var(--text);margin-bottom:8px;margin-top:6px;">${titulo}</div>
      <div style="font-size:13px;color:var(--text2);line-height:1.6;margin-bottom:18px;">${cuerpo}</div>
      <button onclick="document.getElementById('modal-upgrade-pro').remove();abrirModalPlanes();" style="width:100%;padding:12px;background:linear-gradient(135deg,#15803d,#16A34A);border:none;border-radius:10px;color:#fff;font-size:14px;font-weight:800;cursor:pointer;font-family:var(--font-body);">${cta}</button>
      <div style="font-size:11px;color:var(--muted);margin-top:10px;">${esNotif ? 'Programa Fundadores activo · Cupos limitados' : 'Programa Fundadores activo · Procesos preparados gratis'}</div>
      ${esNotif ? '<button onclick="document.getElementById(\'modal-upgrade-pro\').remove()" style="width:100%;padding:8px;margin-top:8px;background:none;border:none;color:var(--muted);font-size:12px;font-weight:600;cursor:pointer;">Ahora no</button>' : ''}
    </div>`;
  modal.onclick = (e) => { if (e.target === modal) modal.remove(); };
  document.body.appendChild(modal);
}

// ═══════════════════════════════════════════════════════════
// PANEL DE REFERIDOS — datos del usuario
// ═══════════════════════════════════════════════════════════
async function cargarPanelReferidos(contId = 'referidos-contenido') {
  const cont = document.getElementById(contId);
  if (!cont) return;
  const token = await obtenerTokenValido();
  if (!token) { cont.innerHTML = '<div style="font-size:13px;color:var(--muted);">Tu sesión expiró. Cierra sesión y vuelve a entrar para ver tu programa de referidos.</div>'; return; }
  cont.innerHTML = '<div style="font-size:13px;color:var(--muted);">Cargando...</div>';
  try {
    const h = { 'Authorization': 'Bearer ' + token };
    const [rCodeRes, rBalRes, rRefsRes] = await Promise.all([
      fetch(`${API}/api/referidos/mi-codigo`, { headers: h }),
      fetch(`${API}/api/referidos/balance`, { headers: h }),
      fetch(`${API}/api/referidos/mis-referidos`, { headers: h })
    ]);
    if (!rCodeRes.ok || !rBalRes.ok || !rRefsRes.ok) {
      cont.innerHTML = '<div style="font-size:13px;color:var(--muted);">No se pudo cargar tu programa de referidos. Recarga la página o vuelve a iniciar sesión.</div>';
      return;
    }
    const rCode = await rCodeRes.json();
    const rBal = await rBalRes.json();
    const rRefs = await rRefsRes.json();
    window._refLink = rCode.link;
    window._refMsgWA = rCode.mensaje_whatsapp;
    const puedeRetirar = rBal.puede_retirar_efectivo;
    cont.innerHTML = `
      <div style="background:linear-gradient(135deg,#0D1B2A,#12321f);border-radius:14px;padding:16px;margin-bottom:12px;text-align:center;">
        <div style="font-size:10px;font-weight:800;color:#4ade80;letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px;">Tu código de referido</div>
        <div style="font-size:26px;font-weight:800;color:#fff;letter-spacing:.15em;font-family:var(--font-mono);">${rCode.code}</div>
        <button onclick="compartirRefLink()" style="margin-top:10px;padding:9px 22px;background:#25D366;border:none;border-radius:8px;color:#fff;font-size:13px;font-weight:800;cursor:pointer;">Compartir por WhatsApp</button>
        <button onclick="copiarRefLink()" style="margin-top:8px;margin-left:6px;padding:9px 16px;background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.25);border-radius:8px;color:#fff;font-size:13px;font-weight:700;cursor:pointer;">Copiar link</button>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:12px;">
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:12px;text-align:center;">
          <div style="font-size:18px;font-weight:800;color:var(--text);">${rRefs.total_referidos}</div>
          <div style="font-size:10px;color:var(--muted);">Referidos</div>
        </div>
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:12px;text-align:center;">
          <div style="font-size:18px;font-weight:800;color:#16A34A;">US$${rBal.balance_pendiente.toFixed(2)}</div>
          <div style="font-size:10px;color:var(--muted);">Balance</div>
        </div>
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:12px;text-align:center;">
          <div style="font-size:18px;font-weight:800;color:var(--text);">${rBal.tasa_actual}%</div>
          <div style="font-size:10px;color:var(--muted);">Tu tasa</div>
        </div>
      </div>
      ${!rBal.es_suscriptor ? `<div style="font-size:11px;color:var(--text2);background:rgba(22,163,74,.08);border:1px solid var(--green-border);border-radius:8px;padding:10px;margin-bottom:12px;line-height:1.5;"><strong>Suscríbete y tu tasa sube a ${rBal.tasa_actual < 15 ? '15' : '20'}%</strong> en TODOS tus referidos — los de antes y los nuevos. Tu balance puede pagar tu suscripción.</div>` : ''}
      <div style="font-size:11px;color:var(--text2);background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:12px;line-height:1.5;">Ganas <strong style="color:#16A34A;">US$${rBal.bono_bienvenida.toFixed(0)} de bono</strong> cuando tu referido hace su primer pago, más <strong style="color:#16A34A;">${rBal.tasa_actual}% cada mes</strong> que siga suscrito.</div>
      ${rBal.balance_pendiente > 0 ? `
        <div style="display:flex;gap:8px;">
          <button onclick="solicitarCobroReferidos('credito')" style="flex:1;padding:10px;background:var(--green-dim);border:1px solid var(--green-border);border-radius:8px;color:var(--green);font-size:12px;font-weight:800;cursor:pointer;">Usar como crédito</button>
          <button onclick="solicitarCobroReferidos('transferencia')" ${!puedeRetirar ? 'disabled title="Mínimo US$'+rBal.umbral_efectivo+'"' : ''} style="flex:1;padding:10px;background:${puedeRetirar ? '#16A34A' : 'var(--bg2)'};border:${puedeRetirar ? 'none' : '1px solid var(--border)'};border-radius:8px;color:${puedeRetirar ? '#fff' : 'var(--muted)'};font-size:12px;font-weight:800;cursor:${puedeRetirar ? 'pointer' : 'not-allowed'};">Retirar (mín US$${rBal.umbral_efectivo})</button>
        </div>` : ''}
      ${rRefs.referidos.length > 0 ? `
        <div style="margin-top:14px;">
          <div style="font-size:11px;font-weight:800;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;">Tus referidos</div>
          ${rRefs.referidos.map(r => `
            <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);">
              <div>
                <div style="font-size:12px;font-weight:600;color:var(--text);">${r.email}</div>
                <div style="font-size:10px;color:var(--muted);">${r.status === 'active' ? 'Suscrito' : 'Registrado (aún no paga)'}</div>
              </div>
              <div style="font-size:12px;font-weight:800;color:#16A34A;">+US$${r.comision_generada.toFixed(2)}</div>
            </div>`).join('')}
        </div>` : '<div style="font-size:12px;color:var(--muted);text-align:center;margin-top:10px;">Aún no tienes referidos. Comparte tu link.</div>'}
      <div id="referido-banco-box" style="margin-top:18px;border-top:1px solid var(--border);padding-top:16px;"></div>`;
    cargarCuentaBancaria();
  } catch(e) {
    cont.innerHTML = '<div style="font-size:13px;color:var(--muted);">No se pudo cargar. Intenta de nuevo.</div>';
  }
}

async function cargarCuentaBancaria() {
  const box = document.getElementById('referido-banco-box');
  if (!box) return;
  const token = await obtenerTokenValido();
  if (!token) return;
  try {
    const r = await fetch(`${API}/api/referidos/cuenta-bancaria`, { headers: { 'Authorization': 'Bearer ' + token } });
    const d = await r.json();
    if (d.tiene_cuenta) {
      box.innerHTML = `
        <div style="font-size:11px;font-weight:800;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;">Cuenta para depósito</div>
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:12px;">
          <div style="font-size:13px;font-weight:700;color:var(--text);">${d.bank_name}</div>
          <div style="font-size:12px;color:var(--text2);margin-top:2px;">Cuenta: ${d.account_number}${d.account_type ? ' · ' + d.account_type : ''}</div>
          <div style="font-size:12px;color:var(--text2);">Titular: ${d.account_holder}</div>
          <button onclick="mostrarFormBanco()" style="margin-top:8px;padding:6px 14px;background:none;border:1px solid var(--border2);border-radius:7px;color:var(--text2);font-size:12px;font-weight:700;cursor:pointer;">Editar</button>
        </div>`;
    } else {
      box.innerHTML = `
        <div style="font-size:11px;font-weight:800;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;">Cuenta para depósito</div>
        <div style="font-size:12px;color:var(--text2);margin-bottom:10px;line-height:1.5;">Registra tu cuenta bancaria para que podamos depositarte tus comisiones.</div>
        <button onclick="mostrarFormBanco()" style="width:100%;padding:11px;background:var(--green-dim);border:1px solid var(--green-border);border-radius:9px;color:var(--green);font-size:13px;font-weight:800;cursor:pointer;">Agregar cuenta bancaria</button>`;
    }
  } catch(e) {}
}

async function mostrarFormBanco() {
  const box = document.getElementById('referido-banco-box');
  if (!box) return;
  const token = await obtenerTokenValido();
  let actual = {};
  try {
    const r = await fetch(`${API}/api/referidos/cuenta-bancaria`, { headers: { 'Authorization': 'Bearer ' + token } });
    actual = await r.json();
  } catch(e) {}
  box.innerHTML = `
    <div style="font-size:11px;font-weight:800;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px;">Datos bancarios</div>
    <div style="display:flex;flex-direction:column;gap:8px;">
      <input id="banco-nombre" class="ll-input" placeholder="Banco (ej: Banco Popular)" value="${actual.bank_name || ''}" style="font-size:13px;">
      <input id="banco-cuenta" class="ll-input" placeholder="Número de cuenta" value="${actual.account_number || ''}" style="font-size:13px;">
      <input id="banco-titular" class="ll-input" placeholder="Nombre del titular" value="${actual.account_holder || ''}" style="font-size:13px;">
      <select id="banco-tipo" class="ll-input" style="font-size:13px;">
        <option value="">Tipo de cuenta</option>
        <option value="Ahorros" ${actual.account_type === 'Ahorros' ? 'selected' : ''}>Ahorros</option>
        <option value="Corriente" ${actual.account_type === 'Corriente' ? 'selected' : ''}>Corriente</option>
      </select>
      <input id="banco-documento" class="ll-input" placeholder="Cédula o RNC del titular" value="${actual.document || ''}" style="font-size:13px;">
      <div style="display:flex;gap:8px;margin-top:4px;">
        <button onclick="guardarCuentaBancaria()" style="flex:1;padding:11px;background:#16A34A;border:none;border-radius:9px;color:#fff;font-size:13px;font-weight:800;cursor:pointer;">Guardar</button>
        <button onclick="cargarCuentaBancaria()" style="padding:11px 16px;background:none;border:1px solid var(--border2);border-radius:9px;color:var(--text2);font-size:13px;font-weight:700;cursor:pointer;">Cancelar</button>
      </div>
    </div>`;
}

async function guardarCuentaBancaria() {
  const token = await obtenerTokenValido();
  if (!token) { alert('Tu sesión expiró. Vuelve a iniciar sesión.'); return; }
  const body = {
    bank_name: document.getElementById('banco-nombre').value.trim(),
    account_number: document.getElementById('banco-cuenta').value.trim(),
    account_holder: document.getElementById('banco-titular').value.trim(),
    account_type: document.getElementById('banco-tipo').value,
    document: document.getElementById('banco-documento').value.trim()
  };
  if (!body.bank_name || !body.account_number || !body.account_holder) {
    alert('Banco, número de cuenta y titular son obligatorios');
    return;
  }
  try {
    const r = await fetch(`${API}/api/referidos/cuenta-bancaria`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (r.ok) { alert(d.mensaje); cargarCuentaBancaria(); }
    else alert(d.detail || 'Error guardando la cuenta');
  } catch(e) { alert('Error de conexión'); }
}

function compartirRefLink() {
  const msg = window._refMsgWA || ('Regístrate en LicitacionLab: ' + (window._refLink || ''));
  window.open('https://wa.me/?text=' + encodeURIComponent(msg), '_blank');
}

function copiarRefLink() {
  if (!window._refLink) return;
  navigator.clipboard.writeText(window._refLink).then(() => {
    alert('Link copiado: ' + window._refLink);
  });
}

async function solicitarCobroReferidos(tipo) {
  const token = await obtenerTokenValido();
  if (!token) { alert('Tu sesión expiró. Vuelve a iniciar sesión.'); return; }
  let datos_bancarios = null;
  // Para transferencia, el backend usa la cuenta guardada automáticamente.
  // Si no tiene cuenta guardada, se la pedimos.
  if (tipo === 'transferencia') {
    const rc = await fetch(`${API}/api/referidos/cuenta-bancaria`, { headers: { 'Authorization': 'Bearer ' + token } });
    const dc = await rc.json();
    if (!dc.tiene_cuenta) {
      alert('Primero registra tu cuenta bancaria abajo para poder retirar.');
      mostrarFormBanco();
      return;
    }
  }
  try {
    const r = await fetch(`${API}/api/referidos/solicitar-cobro`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify({ tipo, datos_bancarios })
    });
    const d = await r.json();
    if (r.ok) {
      alert(d.mensaje);
      const contId = document.getElementById('referidos-tab-contenido') && window._lastActiveTab === 'referidos' ? 'referidos-tab-contenido' : 'referidos-contenido';
      cargarPanelReferidos(contId);
    }
    else alert(d.detail || 'Error procesando la solicitud');
  } catch(e) { alert('Error de conexión'); }
}

// ═══════════════════════════════════════════════════════════
// BANNER FUNDADORES — cupos en vivo en el modal de planes
// ═══════════════════════════════════════════════════════════
async function cargarBannerFundadores() {
  try {
    const r = await fetch(`${API}/api/referidos/fundadores/estado`);
    const d = await r.json();
    const banners = document.querySelectorAll('.founder-banner-slot');
    if (!d.activo) { banners.forEach(b => b.style.display = 'none'); return; }
    banners.forEach(b => {
      b.style.display = 'block';
      b.innerHTML = `
        <div style="background:linear-gradient(135deg,#0D1B2A,#14532d);border:1px solid #16A34A;border-radius:12px;padding:14px 16px;margin-bottom:12px;text-align:center;">
          <div style="font-size:10px;font-weight:800;color:#4ade80;letter-spacing:.1em;text-transform:uppercase;">PROGRAMA FUNDADORES</div>
          <div style="font-size:19px;font-weight:800;color:#fff;margin:4px 0;">Quedan ${d.cupos_disponibles} de ${d.cupos_total} cupos</div>
          <div style="font-size:11px;color:rgba(255,255,255,.75);line-height:1.5;">Suscríbete y <strong style="color:#4ade80;">te preparamos procesos de licitación GRATIS</strong>:<br>Mensual: 1 proceso · Semestral: 3 procesos · Anual: 5 procesos<br><span style="font-size:10px;">(Construcción: plan o Sobre A según suscripción)</span></div>
        </div>`;
    });
  } catch(e) {}
}

async function register() {
  const nombre   = document.getElementById('reg-name').value.trim();
  const email    = document.getElementById('reg-email').value.trim();
  const password = document.getElementById('reg-password').value;
  if (!nombre)              return showMsg('Ingresa tu nombre o empresa', 'error');
  if (!email || !password)  return showMsg('Completa todos los campos', 'error');
  if (password.length < 6)  return showMsg('La contraseña debe tener al menos 6 caracteres', 'error');
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) return showMsg('Ingresa un correo válido', 'error');
  const btn = document.getElementById('btn-register');
  btn.disabled = true; btn.textContent = 'Creando cuenta...';
  // Guardar nombre en user_metadata de Supabase
  const data = await supabaseRequest('signup', 'POST', {
    email, password,
    data: { nombre }          // queda en raw_user_meta_data
  });
  btn.disabled = false; btn.textContent = 'Crear cuenta';
  if (data.error || data.error_code || data.msg) {
    const msg = (data.error_description || data.msg || data.error || data.error_code || '').toLowerCase();
    let msgEs = 'Error al registrarse';
    if (msg.includes('already registered') || msg.includes('already exists') || msg.includes('already been registered')) {
      msgEs = 'Este correo ya está registrado. Intenta iniciar sesión.';
    } else if (msg.includes('password')) {
      msgEs = 'La contraseña debe tener al menos 6 caracteres';
    } else if (msg) { msgEs = '❌ ' + msg; }
    return showMsg(msgEs, 'error');
  }
  showMsg('Cuenta creada. Revisa tu correo para confirmar.', 'success');
  // Enviar correo de bienvenida en background (silencioso)
  fetch(`${API}/api/bienvenida`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ email, nombre, user_id: data.user?.id || '' })
  }).catch(() => {});
}

function logout() {
  localStorage.removeItem('sb_token');
  localStorage.removeItem('sb_refresh_token');
  localStorage.removeItem('sb_user');
  _delCookie('sb_token');
  _delCookie('sb_refresh_token');
  _delCookie('sb_user');
  currentUser = null;
  document.documentElement.classList.remove('ll-has-session');
  document.getElementById('screen-app').classList.remove('active');
  document.getElementById('screen-auth').classList.add('active');
}

function showApp() {
  document.documentElement.classList.add('ll-has-session');
  document.getElementById('screen-auth').classList.remove('active');
  document.getElementById('screen-app').classList.add('active');

  // Extraer nombre real del usuario desde user_metadata
  const nombre = currentUser?.user_metadata?.nombre
    || currentUser?.user_metadata?.full_name
    || currentUser?.email?.split('@')[0]
    || 'Usuario';

  // Iniciales para el avatar (máximo 2 letras)
  const partes = nombre.trim().split(/\s+/);
  const iniciales = partes.length >= 2
    ? (partes[0][0] + partes[1][0]).toUpperCase()
    : nombre.substring(0, 2).toUpperCase();

  // Actualizar navbar y drawer
  const dn = document.getElementById('drawer-name'); if(dn) dn.textContent = nombre;
  const da = document.getElementById('drawer-avatar'); if(da) da.textContent = iniciales;
  const emailDisplay = document.getElementById('user-email-display'); if(emailDisplay) emailDisplay.textContent = nombre;

  // Sincronizar sidebar desktop
  const dsbName   = document.getElementById('dsb-name');    if(dsbName)   dsbName.textContent   = nombre;
  const dsbAvatar = document.getElementById('dsb-avatar');  if(dsbAvatar) dsbAvatar.textContent  = iniciales;

  const dr = document.getElementById('drawer-role');
  if (dr && currentUser?.email) {
    const emailLower = currentUser.email.toLowerCase();
    dr.textContent = emailLower.includes('licitacionlab') ? 'Admin · LicitacionLab' : 'LicitacionLab';
    const dsbRole = document.getElementById('dsb-role');
    if (dsbRole) dsbRole.textContent = dr.textContent;
  }

  // Perfil: mostrar nombre + correo
  const perfilEmail = document.getElementById('perfil-email');
  if (perfilEmail) perfilEmail.textContent = nombre + (currentUser?.email ? ' — ' + currentUser.email : '');

  // Poblar input de nombre en perfil
  const perfilNombreInput = document.getElementById('perfil-nombre-input');
  if (perfilNombreInput) perfilNombreInput.value = nombre;

  switchTab2('dashboard');
  cargarStats();
  cargarProcesos(1);
  checkPushStatus();
  cargarInteresesGuardados();
  actualizarSaludo();
  initBanner();
  initOnboarding();
  vincularReferido();       // vincula ?ref= capturado al registrarse
  esUsuarioPro(true);       // pre-carga el estado PRO (cache freemium)
}

function showMsg(msg, type) {
  const el = document.getElementById('auth-msg');
  el.textContent = msg;
  el.className = `auth-msg ${type}`;
}

function switchTab(tab) {
  document.getElementById('form-login').style.display     = tab === 'login'    ? 'block' : 'none';
  document.getElementById('form-register').style.display  = tab === 'register' ? 'block' : 'none';
  document.getElementById('form-recovery').style.display  = tab === 'recovery' ? 'block' : 'none';
  const fnp = document.getElementById('form-newpassword');
  if (fnp) fnp.style.display = tab === 'newpassword' ? 'block' : 'none';
  document.querySelectorAll('.auth-tab').forEach((t, i) => {
    t.classList.toggle('active', (i===0 && tab==='login') || (i===1 && tab==='register'));
  });
  // En modo "nueva contraseña" ocultamos los tabs login/registro para no confundir
  const tabsBar = document.getElementById('auth-tabs-bar');
  if (tabsBar) tabsBar.style.display = (tab === 'newpassword') ? 'none' : 'flex';
  document.getElementById('auth-msg').className = 'auth-msg';
  const rr = document.getElementById('recovery-result');
  if (rr) { rr.style.display = 'none'; rr.textContent = ''; }
}

async function sendRecovery() {
  const email = document.getElementById('recovery-email').value.trim().toLowerCase();
  const result = document.getElementById('recovery-result');
  const btn = document.getElementById('btn-recovery');

  if (!email) {
    result.style.display = 'block';
    result.style.background = 'var(--red-dim)';
    result.style.color = 'var(--red)';
    result.textContent = 'Ingresa tu correo';
    return;
  }

  btn.disabled = true;
  btn.textContent = 'Enviando...';

  try {
    // redirect_to: a dónde vuelve el usuario tras click en el link del correo.
    // Añadimos ?recovery=1 para detectar el regreso aunque el token venga como
    // 'code' (flujo PKCE) en vez de hash. DEBE estar en la lista blanca de Supabase.
    const redirectTo = window.location.origin + window.location.pathname + '?recovery=1';
    const res = await fetch(`${SUPABASE_URL}/auth/v1/recover`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': SUPABASE_ANON_KEY },
      body: JSON.stringify({ email, redirect_to: redirectTo })
    });

    btn.disabled = false;
    btn.textContent = 'Enviar enlace';
    result.style.display = 'block';

    // Por seguridad, Supabase puede devolver un status de error aunque SÍ envíe
    // el correo (para no revelar qué emails están registrados). Salvo error de
    // servidor real (5xx) o rate limit, mostramos siempre "enviado".
    if (res.ok || res.status === 400 || res.status === 422) {
      result.style.background = 'var(--green-dim)';
      result.style.color = 'var(--green)';
      result.textContent = 'Si ese correo está registrado, te enviamos el enlace. Revisa tu bandeja y spam.';
      document.getElementById('recovery-email').value = '';
    } else if (res.status === 429) {
      result.style.background = 'var(--red-dim)';
      result.style.color = 'var(--red)';
      result.textContent = 'Demasiados intentos. Espera unos minutos.';
    } else {
      result.style.background = 'var(--red-dim)';
      result.style.color = 'var(--red)';
      result.textContent = 'Error al enviar. Intenta de nuevo.';
    }
  } catch(e) {
    btn.disabled = false;
    btn.textContent = 'Enviar enlace';
    result.style.display = 'block';
    result.style.background = 'var(--red-dim)';
    result.style.color = 'var(--red)';
    result.textContent = 'Error de conexión.';
  }
}

// ── STATS ──
async function cargarStats() {
  try {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), 10000);
    const res = await fetch(`${API}/api/stats`, { signal: controller.signal });
    clearTimeout(tid);
    const data = await res.json();
    document.getElementById('stat-total').textContent     = (data.total_procesos||0).toLocaleString();
    document.getElementById('stat-activos').textContent   = (data.procesos_activos||0).toLocaleString();
    document.getElementById('stat-obras').textContent     = (data.obras_activas||0).toLocaleString();
    document.getElementById('stat-bienes').textContent    = (data.bienes_activos||0).toLocaleString();
    document.getElementById('stat-servicios').textContent = (data.servicios_activos||0).toLocaleString();
    // Actualizar counts en chips
    const activos = data.procesos_activos || 0;
    const obras   = data.obras_activas    || 0;
    const bienes  = data.bienes_activos   || 0;
    const serv    = data.servicios_activos|| 0;
    const cc = id => document.getElementById(id);
    if (cc('chip-count-all'))      cc('chip-count-all').textContent      = activos;
    if (cc('chip-count-obras'))    cc('chip-count-obras').textContent    = obras;
    if (cc('chip-count-bienes'))   cc('chip-count-bienes').textContent   = bienes;
    if (cc('chip-count-servicios'))cc('chip-count-servicios').textContent= serv;
  } catch(e) {
    // Si falla, mostrar — en lugar de dejar el spinner infinito
    ['chip-count-all','chip-count-obras','chip-count-bienes','chip-count-servicios']
      .forEach(id => { const el = document.getElementById(id); if(el) el.textContent = '—'; });
  }
}

// ── SHOWCASE EN VIVO (login) ──────────────────────────────────────────────
async function iniciarShowcase() {
  const fechaEl = document.getElementById('auth-showcase-fecha');
  if (fechaEl) {
    const ahora = new Date();
    fechaEl.textContent = ahora.toLocaleDateString('es-DO', { day: '2-digit', month: 'short' }) + ' · ' +
      ahora.toLocaleTimeString('es-DO', { hour: '2-digit', minute: '2-digit' });
  }

  const ticker = document.getElementById('auth-ticker');
  if (!ticker) return;

  try {
    // Timeout de 8 segundos para no dejar el spinner infinito
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 8000);

    const res = await fetch(`${API}/api/procesos?page=1&limit=20&order=detectado_en.desc`, {
      signal: controller.signal
    });
    clearTimeout(timeoutId);

    if (!res.ok) throw new Error('fetch failed ' + res.status);
    const data = await res.json();
    const procesos = Array.isArray(data) ? data : (data.procesos || data.items || []);
    if (!procesos.length) throw new Error('sin procesos');

    const todos = [...procesos, ...procesos];
    ticker.innerHTML = todos.map(p => renderTickerRow(p)).join('');
  } catch(e) {
    console.warn('Showcase error, usando fallback:', e.message);
    ticker.innerHTML = generarTickerFallback();
  }
}

function renderTickerRow(p) {
  const tipo = p.objeto_proceso || '';
  const badgeClass = tipo.includes('Obra') ? 'obras' : tipo.includes('Bien') ? 'bienes' : 'servicios';
  const monto = p.monto_estimado
    ? (Number(p.monto_estimado) >= 1_000_000
        ? `RD$ ${(Number(p.monto_estimado)/1_000_000).toFixed(1)}M`
        : `RD$ ${(Number(p.monto_estimado)/1_000).toFixed(0)}K`)
    : '—';

  // Extraer entidad corta (primera sigla del nombre)
  const entidad = p.unidad_compra || '—';
  const entidadCorta = entidad.length > 30 ? entidad.substring(0, 30) + '…' : entidad;

  // Detectar si es nuevo (menos de 24h)
  const esNuevo = p.detectado_en && (Date.now() - new Date(p.detectado_en)) < 86400000;

  return `
  <div class="auth-ticker-row" onclick="void(0)">
    <div class="auth-ticker-meta">
      <span class="auth-ticker-codigo">${p.codigo_proceso || '—'}</span>
      <span class="auth-ticker-entidad">${entidadCorta}</span>
    </div>
    <div class="auth-ticker-titulo">${p.titulo || 'Sin título'}</div>
    <div class="auth-ticker-footer">
      <span class="auth-ticker-monto">${monto}</span>
      <div style="display:flex;gap:4px;">
        ${tipo ? `<span class="auth-ticker-badge ${badgeClass}">${tipo}</span>` : ''}
        ${esNuevo ? '<span class="auth-ticker-badge nuevo">Nuevo</span>' : ''}
      </div>
    </div>
  </div>`;
}

function generarTickerFallback() {
  const ejemplos = [
    { codigo: 'INAPA-CCC-LPN-2026-0021', titulo: 'Red de agua Bayaguana sector norte', monto: 'RD$ 92.3M', tipo: 'obras', entidad: 'INAPA' },
    { codigo: 'MOPC-2026-0198', titulo: 'Puente río Iguamo H.Mayor – El Seibo', monto: 'RD$ 556.7M', tipo: 'obras', entidad: 'MOPC' },
    { codigo: 'MINERD-2026-0087', titulo: 'Construcción Politécnico Bávaro Fase III', monto: 'RD$ 412.1M', tipo: 'obras', entidad: 'DIE' },
    { codigo: 'CAASD-2026-0033', titulo: 'Alcantarillado Los Alcarrizos T-4', monto: 'RD$ 198.4M', tipo: 'obras', entidad: 'CAASD' },
    { codigo: 'INAPA-2026-0029', titulo: 'Tanque elevado 1,000 m³ San Pedro', monto: 'RD$ 62.0M', tipo: 'obras', entidad: 'INAPA' },
  ];
  const todos = [...ejemplos, ...ejemplos];
  return todos.map(e => `
  <div class="auth-ticker-row">
    <div class="auth-ticker-meta">
      <span class="auth-ticker-codigo">${e.codigo}</span>
      <span class="auth-ticker-entidad">${e.entidad}</span>
    </div>
    <div class="auth-ticker-titulo">${e.titulo}</div>
    <div class="auth-ticker-footer">
      <span class="auth-ticker-monto">${e.monto}</span>
      <span class="auth-ticker-badge ${e.tipo}">${e.tipo}</span>
    </div>
  </div>`).join('');
}

// Refrescar showcase cada 4 minutos
let _showcaseTimer = null;
function startShowcaseRefresh() {
  iniciarShowcase();
  _showcaseTimer = setInterval(iniciarShowcase, 4 * 60 * 1000);
}
function stopShowcaseRefresh() {
  if (_showcaseTimer) { clearInterval(_showcaseTimer); _showcaseTimer = null; }
}
// ──────────────────────────────────────────────────────────────────────────

function filtrarRubro(rubro) {
  document.getElementById('filtro-objeto').value = rubro;
  cargarProcesos(1);
  switchTab2('dashboard');
}

// Rubro activo — se guarda aquí, no en un select oculto
let rubroActivo = '';

function filtrarRubroChip(rubro) {
  haptic(); // UX v13
  rubroActivo = rubro;
  // Sincronizar el select oculto por compatibilidad
  const sel = document.getElementById('filtro-objeto');
  if (sel) sel.value = rubro;
  // Actualizar chip activo visualmente
  ['chip-all','chip-obras','chip-bienes','chip-servicios'].forEach(id => {
    document.getElementById(id)?.classList.remove('is-active');
  });
  const chipMap = { '': 'chip-all', 'Obras': 'chip-obras', 'Bienes': 'chip-bienes', 'Servicios': 'chip-servicios' };
  document.getElementById(chipMap[rubro] || 'chip-all')?.classList.add('is-active');
  cargarProcesos(1);
}

function actualizarSaludo() {
  const h = new Date().getHours();
  const saludo = h < 12 ? 'Buenos días' : h < 19 ? 'Buenas tardes' : 'Buenas noches';
  const nombre = currentUser?.user_metadata?.nombre
    || currentUser?.user_metadata?.full_name
    || currentUser?.email?.split('@')[0] || '';
  const primerNombre = nombre.split(' ')[0];
  const g = document.getElementById('dash-greeting');
  if (g) g.textContent = primerNombre ? `${saludo}, ${primerNombre}` : saludo;
}

// ── PROCESOS ──
let unspscFiltro = null;

// ═══════════════ MEJORAS UX v13 ═══════════════

// Haptic feedback (no-op en iOS Safari, vibra en Android)
function haptic(ms = 10) { try { if (navigator.vibrate) navigator.vibrate(ms); } catch(e) {} }

// Skeleton screens — percepción de carga más rápida que un spinner
function skeletonCards(n = 6) {
  let html = '';
  for (let i = 0; i < n; i++) {
    html += `<div class="skel-card">
      <div class="skel-line w40"></div>
      <div class="skel-line w90"></div>
      <div class="skel-line w70"></div>
      <div class="skel-line w55"></div>
    </div>`;
  }
  return html;
}

// Persistencia de filtros — el usuario configura una vez y la app lo recuerda
const _FILTROS_KEY = 'll_filtros_procesos_v1';
let _filtrosRestaurados = false;

function guardarFiltrosProcesos() {
  try {
    localStorage.setItem(_FILTROS_KEY, JSON.stringify({
      busqueda:    document.getElementById('busqueda')?.value || '',
      rubro:       rubroActivo || '',
      institucion: document.getElementById('filtro-institucion')?.value || '',
      montoMin:    document.getElementById('filtro-monto-min')?.value || '',
      montoMax:    document.getElementById('filtro-monto-max')?.value || '',
      orden:       _ordenProcesos,
      unspsc:      unspscFiltro
    }));
  } catch(e) {}
}

function restaurarFiltrosProcesos() {
  if (_filtrosRestaurados) return;
  _filtrosRestaurados = true;
  try {
    const f = JSON.parse(localStorage.getItem(_FILTROS_KEY) || 'null');
    if (!f) return;
    const set = (id, v) => { const el = document.getElementById(id); if (el && v) el.value = v; };
    set('busqueda', f.busqueda);
    set('filtro-institucion', f.institucion);
    set('filtro-monto-min', f.montoMin);
    set('filtro-monto-max', f.montoMax);
    if (f.busqueda) { try { toggleClearBtn(); } catch(e) {} }
    if (f.rubro) {
      rubroActivo = f.rubro;
      const sel = document.getElementById('filtro-objeto'); if (sel) sel.value = f.rubro;
      ['chip-all','chip-obras','chip-bienes','chip-servicios'].forEach(id => document.getElementById(id)?.classList.remove('is-active'));
      const chipMap = { '': 'chip-all', 'Obras': 'chip-obras', 'Bienes': 'chip-bienes', 'Servicios': 'chip-servicios' };
      document.getElementById(chipMap[f.rubro] || 'chip-all')?.classList.add('is-active');
    }
    if (f.orden) {
      _ordenProcesos = f.orden;
      const selO = document.getElementById('filtro-orden-visible'); if (selO) selO.value = f.orden;
      const act = document.getElementById('filtro-activos'); if (act) act.value = f.orden === 'todos' ? 'false' : 'true';
    }
    if (f.unspsc && f.unspsc.codigo) {
      unspscFiltro = f.unspsc;
      const inp = document.getElementById('filtro-unspsc-text'); if (inp) inp.value = f.unspsc.nombre || '';
      const tag = document.getElementById('unspsc-activo');
      if (tag) { tag.style.display = 'flex'; const lbl = document.getElementById('unspsc-activo-label'); if (lbl) lbl.textContent = `${f.unspsc.nombre} (${f.unspsc.codigo})`; }
    }
    try { actualizarBadgeFiltros(); actualizarFiltrosActivos(); } catch(e) {}
  } catch(e) {}
}

// Pull-to-refresh genérico para contenedores con scroll
function attachPullToRefresh(elId, refreshFn) {
  const el = document.getElementById(elId);
  if (!el || el._ptrAttached) return;
  el._ptrAttached = true;
  const ind = document.createElement('div');
  ind.className = 'ptr-indicator';
  ind.innerHTML = '<div class="ptr-spinner"></div>';
  el.prepend(ind);
  let startY = 0, pulling = false;
  el.addEventListener('touchstart', e => {
    if (el.scrollTop <= 0) { startY = e.touches[0].clientY; pulling = true; }
    else pulling = false;
  }, { passive: true });
  el.addEventListener('touchmove', e => {
    if (!pulling) return;
    const dist = e.touches[0].clientY - startY;
    if (dist > 10 && el.scrollTop <= 0) {
      const d = Math.min(dist * 0.4, 70);
      ind.style.height = d + 'px';
      ind.classList.toggle('ready', d >= 58);
    }
  }, { passive: true });
  el.addEventListener('touchend', async () => {
    if (!pulling) return;
    pulling = false;
    if (ind.classList.contains('ready')) {
      ind.classList.remove('ready');
      ind.classList.add('loading');
      ind.style.height = '44px';
      haptic(15);
      try { await refreshFn(); } catch(e) {}
    }
    ind.classList.remove('ready', 'loading');
    ind.style.height = '0px';
  }, { passive: true });
}

// Botón flotante "volver arriba"
function initBackToTop() {
  if (document.getElementById('btn-back-top')) return;
  const btn = document.createElement('button');
  btn.id = 'btn-back-top';
  btn.setAttribute('aria-label', 'Volver arriba');
  btn.textContent = '↑';
  btn.onclick = () => {
    haptic();
    const tab = window._lastActiveTab || 'dashboard';
    const el = document.getElementById('tab-' + tab);
    if (el) el.scrollTo({ top: 0, behavior: 'smooth' });
    btn.classList.remove('visible');
  };
  document.body.appendChild(btn);
  ['tab-dashboard','tab-mercado','tab-seguimiento','tab-inteligencia','tab-perfil','tab-detalle'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('scroll', () => {
      btn.classList.toggle('visible', el.scrollTop > 1200);
    }, { passive: true });
  });
}

// Inicializar mejoras UX (el script corre al final del body, el DOM ya existe)
try {
  initBackToTop();
  attachPullToRefresh('tab-dashboard',   () => cargarProcesos(1));
  attachPullToRefresh('tab-mercado',     () => cargarMercado());
  attachPullToRefresh('tab-seguimiento', () => cargarSeguimiento());
} catch(e) { console.warn('UX init:', e); }
// ═══════════════ FIN MEJORAS UX v13 ═══════════════

// Orden de la lista de procesos (FIX: antes "Cierra primero" no ordenaba nada)
let _ordenProcesos = 'cierre';
function cambiarOrdenProcesos(v) {
  _ordenProcesos = v;
  const act = document.getElementById('filtro-activos');
  if (act) act.value = (v === 'todos') ? 'false' : 'true';
  cargarProcesos(1);
}
async function cargarProcesos(page = 1) {
  restaurarFiltrosProcesos(); // UX v13: restaurar filtros guardados (solo la primera vez)
  document.getElementById('procesos-container').innerHTML = skeletonCards(6);
  const busqueda   = document.getElementById('busqueda').value;
  const objeto     = (typeof rubroActivo !== 'undefined' ? rubroActivo : null)
    || document.getElementById('filtro-objeto')?.value || '';
  const activos    = document.getElementById('filtro-activos').value;
  const institucion = document.getElementById('filtro-institucion')?.value || '';
  const montoMin   = document.getElementById('filtro-monto-min')?.value || '';
  const montoMax   = document.getElementById('filtro-monto-max')?.value || '';
  // Mapear el selector visible al parámetro de orden del backend
  const ordenMap = { cierre: 'cierre', recientes: 'publicacion', monto: 'monto', todos: 'publicacion' };
  const ordenBackend = ordenMap[_ordenProcesos] || 'cierre';

  let url = `${API}/api/procesos?page=${page}&limit=15&solo_activos=${activos}&orden=${ordenBackend}`;
  if (busqueda)    url += `&busqueda=${encodeURIComponent(busqueda)}`;
  if (objeto)      url += `&objeto=${encodeURIComponent(objeto)}`;
  if (institucion) url += `&institucion=${encodeURIComponent(institucion)}`;
  if (montoMin)    url += `&monto_min=${montoMin}`;
  if (montoMax)    url += `&monto_max=${montoMax}`;
  if (unspscFiltro) url += `&familia_unspsc=${encodeURIComponent(unspscFiltro.codigo)}`;
  guardarFiltrosProcesos(); // UX v13: recordar filtros entre sesiones

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 15000);
    const res  = await fetch(url, { signal: controller.signal });
    clearTimeout(timeoutId);
    if (!res.ok) throw new Error(`Error del servidor (${res.status})`);
    const data = await res.json();
    renderProcesos(data.procesos || [], data, page);
  } catch(e) {
    const esTimeout = e.name === 'AbortError';
    // Si es un error de red (no de servidor), intentar de nuevo en 5 segundos
    const esErrorRed = esTimeout || e.message === 'Load failed' || e.message === 'Failed to fetch' || e.message.includes('NetworkError');
    document.getElementById('procesos-container').innerHTML = `
      <div class="empty">
        <div class="empty-icon" style="font-size:28px;opacity:.4">📡</div>
        <div class="empty-text">${esTimeout ? 'El servidor tardó demasiado' : esErrorRed ? 'Sin conexión al servidor' : 'No se pudieron cargar las licitaciones'}</div>
        <div style="font-size:12px;color:var(--muted);margin-top:6px">${esErrorRed ? 'Reconectando automáticamente...' : e.message}</div>
        <button onclick="cargarProcesos(${page})" style="margin-top:14px;padding:8px 20px;background:var(--green-dim);border:1px solid var(--green-border);border-radius:var(--r-sm);color:var(--green);font-weight:700;font-size:13px;cursor:pointer;font-family:var(--font-body)">Reintentar ahora</button>
      </div>`;
    document.getElementById('pagination').innerHTML = '';
    // Retry automático a los 5 segundos si es error de red
    if (esErrorRed) {
      setTimeout(() => {
        const contenedor = document.getElementById('procesos-container');
        if (contenedor?.innerHTML?.includes('Sin conexión')) cargarProcesos(page);
      }, 5000);
    }
  }
  // Al terminar la primera carga, verificar si hay un proceso pendiente de abrir
  _abrirProcesoDeNotificacion();
}

function formatMonto(v) {
  if (!v) return 'Monto N/D';
  const n = Number(v);
  if (n >= 1_000_000) return `RD$${(n/1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `RD$${(n/1_000).toFixed(0)}K`;
  return `RD$${n.toLocaleString('es-DO')}`;
}

function formatMontoFull(v) {
  if (!v) return 'No especificado';
  return `RD$${Number(v).toLocaleString('es-DO', {minimumFractionDigits:2})}`;
}

function renderProcesos(procesos, data, page) {
  const container = document.getElementById('procesos-container');
  if (!procesos.length) {
    container.innerHTML = '<div class="empty"><div class="empty-icon">🔍</div><div class="empty-text">No se encontraron licitaciones</div></div>';
    document.getElementById('pagination').innerHTML = '';
    return;
  }

  // Actualizar el contador del breadcrumb
  const bcCount = document.getElementById('dash-bc-count');
  if (bcCount) bcCount.textContent = (data.total || data.procesos?.length || procesos.length) + ' activas';

  // Desktop: cards densas / Móvil: swipe cards
  const isDesktop = window.innerWidth >= 900;

  if (isDesktop) {
    container.innerHTML = procesos.map(p => {
      const inst = sanitize((p.unidad_compra||'').split(' ').slice(0,1).join(' ').substring(0,10).toUpperCase()) || '—';
      const monto = formatMonto(p.monto_estimado);
      const cd = calcularCountdown(p.fecha_fin_recepcion_ofertas);
      // Barra de progreso: tiempo restante vs total publicado→cierre
      let pct = 50;
      if (p.fecha_publicacion && p.fecha_fin_recepcion_ofertas) {
        const total = new Date(p.fecha_fin_recepcion_ofertas) - new Date(p.fecha_publicacion);
        const elapsed = Date.now() - new Date(p.fecha_publicacion);
        pct = Math.max(3, Math.min(97, Math.round((elapsed/total)*100)));
      }
      const cdCls = cd.clase || '';
      const unspsc = (p.codigo_proceso||'').split('-')[0] || '—';
      const inst2 = (p.unidad_compra||'').substring(0,40);
      return `<div class="proceso-dense-card" data-codigo="${p.codigo_proceso}" onclick="verDetalle('${p.codigo_proceso}')">
        <div class="pdc-row1">
          <span class="pdc-code">${p.codigo_proceso||'—'}</span>
          <span class="pdc-inst-chip">${inst}</span>
          <span class="pdc-estado-chip abierto">${sanitize(p.estado_proceso)||'Publicado'}</span>
          <button class="pdc-bookmark" onclick="event.stopPropagation();toggleSeguimiento('${p.codigo_proceso}')" id="bm-${p.codigo_proceso}">🔖</button>
        </div>
        <div class="pdc-title">${sanitize(p.titulo)||'Sin título'}</div>
        <div class="pdc-meta">
          <span class="pdc-meta-item">
            <svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>
            ${monto}
          </span>
          ${p.provincia ? `<span class="pdc-meta-item">
            <svg viewBox="0 0 24 24"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>
            ${sanitize(p.provincia)}
          </span>` : ''}
          <span class="pdc-meta-item">
            <svg viewBox="0 0 24 24"><polyline points="20.59 13.41 15 19 9 13 14.59 7.41"/><line x1="9" y1="6" x2="20" y2="6"/><line x1="9" y1="20" x2="9" y2="6"/></svg>
            ${sanitize(unspsc)}${p.objeto_proceso ? ' · '+sanitize(p.objeto_proceso) : ''}
          </span>
          <span class="pdc-meta-score" id="score-badge-${p.codigo_proceso}" style="display:none;"></span>
        </div>
        <div class="pdc-row3">
          <span class="pdc-countdown ${cdCls}">
            <span class="dot"></span>
            Cierra en ${cd.texto}
          </span>
          <div class="pdc-progress">
            <div class="pdc-progress-fill ${cdCls}" style="width:${pct}%"></div>
          </div>
          <div class="pdc-actions">
            <button class="pdc-btn-save" onclick="event.stopPropagation();toggleSeguimiento('${p.codigo_proceso}')">
              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>
              Guardar
            </button>
            <button class="pdc-btn-analisis" onclick="event.stopPropagation();verDetalle('${p.codigo_proceso}');setTimeout(()=>cambiarTabDetalle('${p.codigo_proceso}','score'),300)">
              ⚡ Ver análisis
            </button>
          </div>
        </div>
      </div>`;
    }).join('');
  } else {
    // MÓVIL: cards originales con swipe
    container.innerHTML = procesos.map(p => {
      const tipo = p.objeto_proceso || '';
      const chipTipo = tipo === 'Obras' ? 'll-chip ll-chip--info'
        : tipo === 'Bienes' ? 'll-chip ll-chip--warning'
        : tipo === 'Servicios' ? 'll-chip ll-chip--success' : 'll-chip';
      const cierre = p.fecha_fin_recepcion_ofertas ? new Date(p.fecha_fin_recepcion_ofertas).toLocaleDateString('es-DO',{day:'2-digit',month:'short'}) : '—';
      return `
        <div class="proceso-swipe-wrap" data-codigo="${p.codigo_proceso}" data-titulo="${(p.titulo||'').replace(/"/g,'&quot;').substring(0,80)}">
          <div class="proceso-swipe-action" id="swipe-action-${p.codigo_proceso}">
            <span class="proceso-swipe-action-label">Quiero ganar</span>
          </div>
          <div class="ll-card ll-card--hover ll-card--accent" data-codigo="${p.codigo_proceso}">
            <div class="ll-card__header">
              <span class="ll-mono ll-caption">${p.codigo_proceso||'—'}</span>
              <span class="ll-num">${formatMonto(p.monto_estimado)}</span>
            </div>
            <h3 class="ll-card__title" style="margin:6px 0;">${sanitize(p.titulo)||'Sin título'}</h3>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px;">
              <span class="ll-body-sm ll-u-text-subtle">${sanitize(p.unidad_compra)||'—'}</span>
              <span class="ll-mono ll-caption">${cierre}</span>
            </div>
            <div style="display:flex;align-items:center;gap:5px;flex-wrap:wrap;margin-top:8px;">
              ${tipo ? `<span class="${chipTipo}">${sanitize(tipo)}</span>` : ''}
              ${p.modalidad ? `<span class="ll-chip">${sanitize(p.modalidad)}</span>` : ''}
              ${p.dirigido_mipymes ? '<span class="ll-chip ll-chip--warning">MIPYMES</span>' : ''}
              <span class="card-score-badge" id="score-badge-${p.codigo_proceso}" style="display:none;margin-left:auto;"></span>
            </div>
          </div>
        </div>`;
    }).join('');
  }

  // Fetch batch de scores (aplica para móvil y desktop)
  if (procesos.length) {
    const codigos = procesos.map(p => p.codigo_proceso);
    const inFilter = codigos.map(c => `"${c}"`).join(',');
    fetch(
      `${SUPABASE_URL}/rest/v1/analisis_pliego?select=proceso_id,estado,evaluacion_competitividad,alertas_fraude&proceso_id=in.(${inFilter})`,
      { headers: { 'apikey': SUPABASE_ANON_KEY } }
    ).then(r => r.ok ? r.json() : []).then(lista => {
      lista.forEach(a => {
        if (a.estado !== 'completado' || !a.evaluacion_competitividad) return;
        const ev = a.evaluacion_competitividad;
        const alertas = a.alertas_fraude || [];
        let score = ev.score_base != null ? Math.max(5, Math.min(100, Number(ev.score_base)))
          : Math.max(5, Math.round((({'Baja':80,'Media':58,'Alta':30}[ev.nivel_dificultad]??50)
            - alertas.filter(x=>x.riesgo==='Alto').length*15
            - alertas.filter(x=>x.riesgo==='Medio').length*7)));
        const cls = score >= 70 ? 'baja' : score >= 45 ? 'media' : 'alta';
        const badge = document.getElementById(`score-badge-${a.proceso_id}`);
        if (badge) {
          if (isDesktop) {
            badge.textContent = `⚡ ${score}`;
            badge.className = `pdc-meta-score ${cls}`;
            badge.style.display = 'flex';
          } else {
            badge.textContent = `⚡ ${score}`;
            badge.className = `card-score-badge ${cls}`;
            badge.style.display = 'inline-flex';
          }
        }
      });
    }).catch(() => {});
  }

  const pages = data.pages || 1;
  if (pages <= 1) { document.getElementById('pagination').innerHTML = ''; return; }
  let pag = '';
  if (page > 1) pag += `<button class="page-btn" onclick="cargarProcesos(${page-1})">← Anterior</button>`;
  pag += `<button class="page-btn active">${page} / ${pages}</button>`;
  if (page < pages) pag += `<button class="page-btn" onclick="cargarProcesos(${page+1})">Siguiente →</button>`;
  document.getElementById('pagination').innerHTML = pag;
}

function debounceSearch() {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => cargarProcesos(1), 400);
}

function toggleClearBtn() {
  const input = document.getElementById('busqueda');
  const btn = document.getElementById('btn-clear-busqueda');
  if (!btn) return;
  btn.style.display = input.value.length > 0 ? 'flex' : 'none';
}

function limpiarBusqueda() {
  const input = document.getElementById('busqueda');
  if (input) { input.value = ''; input.focus(); }
  toggleClearBtn();
  clearTimeout(searchTimer);
  cargarProcesos(1);
}

function limpiarFiltros() {
  ['busqueda','filtro-institucion','filtro-monto-min','filtro-monto-max'].forEach(id => {
    const el = document.getElementById(id); if(el) el.value = '';
  });
  rubroActivo = '';
  const sel = document.getElementById('filtro-objeto');
  if (sel) sel.value = '';
  // Resetear chip activo a Todos
  ['chip-all','chip-obras','chip-bienes','chip-servicios'].forEach(id => {
    document.getElementById(id)?.classList.remove('is-active');
  });
  document.getElementById('chip-all')?.classList.add('is-active');
  limpiarUnspsc();
  actualizarBadgeFiltros();
  cargarProcesos(1);
}

// ── FILTROS AVANZADOS (móvil) ──
function toggleFiltrosAvanzados() {
  const panel = document.getElementById('filtros-avanzados-panel');
  const btn   = document.getElementById('btn-filtros-avanzados');
  if (!panel || !btn) return;
  const abierto = panel.classList.toggle('abierto');
  btn.classList.toggle('activo', abierto);
}

function actualizarBadgeFiltros() {
  const montoMin = document.getElementById('filtro-monto-min')?.value || '';
  const montoMax = document.getElementById('filtro-monto-max')?.value || '';
  const unspsc   = unspscFiltro ? 1 : 0;
  const count    = (montoMin ? 1 : 0) + (montoMax ? 1 : 0) + unspsc;
  const badge    = document.getElementById('filtros-badge');
  const btn      = document.getElementById('btn-filtros-avanzados');
  if (!badge) return;
  badge.textContent = count;
  badge.classList.toggle('visible', count > 0);
  if (btn) btn.classList.toggle('activo', count > 0);
}

// ── ANÁLISIS IA — solicitar con estado de carga ──
async function solicitarAnalisis(codigo) {
  const pro = await esUsuarioPro();

  // Si NO es suscriptor, verificar/reclamar su análisis gratis de cortesía
  if (!pro) {
    const token = await obtenerTokenValido();
    if (!token) { mostrarModalUpgrade('analisis'); return; }
    try {
      const est = await fetch(`${API}/api/referidos/analisis-gratis/estado`, {
        headers: { 'Authorization': 'Bearer ' + token }
      }).then(r => r.json());
      if (est.aplica && !est.disponible) {
        // Ya usó su gratis → mostrar upgrade
        mostrarModalUpgrade('analisis');
        return;
      }
      if (est.aplica && est.disponible) {
        // Confirmar que quiere usar su único análisis gratis
        const ok = confirm('Este es tu análisis GRATIS de cortesía. Analizaremos el pliego con IA y te enviaremos el resultado a tu correo. ¿Continuar?');
        if (!ok) return;
        const rec = await fetch(`${API}/api/referidos/analisis-gratis/reclamar`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
          body: JSON.stringify({ codigo_proceso: codigo })
        });
        if (!rec.ok) {
          const d = await rec.json().catch(() => ({}));
          mostrarToast(d.detail || 'No se pudo reclamar el análisis gratis');
          mostrarModalUpgrade('analisis');
          return;
        }
      }
    } catch(e) {
      mostrarModalUpgrade('analisis');
      return;
    }
  }

  const btn = document.getElementById(`btn-analizar-${codigo}`);
  if (!btn || btn.disabled) return;
  btn.disabled = true;
  btn.classList.add('cargando');
  try {
    const res  = await fetch(`${API}/api/procesos/${codigo}/analizar`, { method: 'POST', headers: { 'Content-Type': 'application/json' } });
    const data = await res.json();
    if (res.ok) {
      if (!pro) mostrarToast('Análisis en proceso. Te llegará a tu correo en unos minutos.');
      await verDetalle(codigo);  // recarga la vista con el nuevo análisis
    } else {
      showMsg(data.detail || 'Error al solicitar análisis', 'error');
      btn.disabled = false;
      btn.classList.remove('cargando');
    }
  } catch(e) {
    showMsg('Error de conexión al solicitar análisis', 'error');
    btn.disabled = false;
    btn.classList.remove('cargando');
  }
}

function limpiarUnspsc() {
  unspscFiltro = null;
  const el = document.getElementById('filtro-unspsc-text'); if(el) el.value='';
  document.getElementById('unspsc-activo').style.display = 'none';
  actualizarBadgeFiltros();
  cargarProcesos(1);
}

// ── UNSPSC BUSCAR ──
async function buscarUnspsc() {
  const q  = document.getElementById('filtro-unspsc-text').value.trim();
  const dd = document.getElementById('unspsc-dropdown');
  if (q.length < 2) { dd.style.display = 'none'; return; }
  const res  = await fetch(`${API}/api/unspsc/buscar?q=${encodeURIComponent(q)}`);
  const data = await res.json();
  if (!data.length) { dd.style.display = 'none'; return; }
  const esCodigo = /^\d+$/.test(q);
  dd.innerHTML = data.map(f =>
    `<div class="unspsc-dropdown-item" onclick="seleccionarUnspsc('${f.familia}','${f.descripcion_familia.replace(/'/g,"\\'")}')">${
      esCodigo
        ? `<span class="code" style="margin-left:0;margin-right:6px">${f.familia}</span>${f.descripcion_familia}`
        : `${f.descripcion_familia}<span class="code">${f.familia}</span>`
    }</div>`).join('');
  dd.style.display = 'block';
}

function seleccionarUnspsc(codigo, nombre) {
  unspscFiltro = {codigo, nombre};
  document.getElementById('filtro-unspsc-text').value = nombre;
  document.getElementById('unspsc-dropdown').style.display = 'none';
  const tag = document.getElementById('unspsc-activo');
  tag.style.display = 'flex';
  document.getElementById('unspsc-activo-label').textContent = `${nombre} (${codigo})`;
  actualizarBadgeFiltros();
  cargarProcesos(1);
}

// ── UNSPSC PERFIL ──
let familiasPerfil = JSON.parse(localStorage.getItem('familias_interes') || '[]');

async function buscarUnspscPerfil() {
  const q  = document.getElementById('perfil-unspsc-text').value.trim();
  const dd = document.getElementById('perfil-unspsc-dropdown');
  if (q.length < 2) { dd.style.display = 'none'; return; }
  const res  = await fetch(`${API}/api/unspsc/buscar?q=${encodeURIComponent(q)}`);
  const data = await res.json();
  if (!data.length) { dd.style.display = 'none'; return; }
  dd.innerHTML = data.map(f =>
    `<div class="unspsc-dropdown-item" onclick="agregarFamiliaPerfil('${f.familia}','${f.descripcion_familia.replace(/'/g,"\\'")}')">
      ${f.descripcion_familia}<span class="code">${f.familia}</span>
    </div>`).join('');
  dd.style.display = 'block';
}

function agregarFamiliaPerfil(codigo, nombre) {
  if (!familiasPerfil.find(f => f.codigo === codigo)) {
    familiasPerfil.push({codigo, nombre});
    localStorage.setItem('familias_interes', JSON.stringify(familiasPerfil));
    renderFamiliasPerfil();
    guardarIntereses();
  }
  document.getElementById('perfil-unspsc-text').value = '';
  document.getElementById('perfil-unspsc-dropdown').style.display = 'none';
}

function removerFamiliaPerfil(codigo) {
  familiasPerfil = familiasPerfil.filter(f => f.codigo !== codigo);
  localStorage.setItem('familias_interes', JSON.stringify(familiasPerfil));
  renderFamiliasPerfil();
  guardarIntereses();
}

function renderFamiliasPerfil() {
  const c = document.getElementById('familias-seleccionadas');
  if (!c) return;
  c.innerHTML = familiasPerfil.map(f =>
    `<span class="familia-tag">${f.nombre} <span style="font-size:10px;opacity:0.7">${f.codigo}</span>
     <button onclick="removerFamiliaPerfil('${f.codigo}')">✕</button></span>`
  ).join('');
}

async function guardarNombrePerfil() {
  const input = document.getElementById('perfil-nombre-input');
  const status = document.getElementById('perfil-nombre-status');
  const nombre = input?.value.trim();
  if (!nombre) { if(status) status.textContent = '❌ Ingresa un nombre'; return; }

  const token = localStorage.getItem('sb_token');
  try {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${token}`
      },
      body: JSON.stringify({ data: { nombre } })
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    // Actualizar en memoria y localStorage
    if (currentUser) {
      currentUser.user_metadata = { ...(currentUser.user_metadata || {}), nombre };
      localStorage.setItem('sb_user', JSON.stringify(currentUser));
    }
    // Reflejar en navbar y drawer sin recargar
    const partes = nombre.trim().split(/\s+/);
    const iniciales = partes.length >= 2
      ? (partes[0][0] + partes[1][0]).toUpperCase()
      : nombre.substring(0, 2).toUpperCase();
    const dn = document.getElementById('drawer-name'); if(dn) dn.textContent = nombre;
    const da = document.getElementById('drawer-avatar'); if(da) da.textContent = iniciales;
    const ed = document.getElementById('user-email-display'); if(ed) ed.textContent = nombre;
    // Actualizar saludo del dashboard de inmediato
    actualizarSaludo();

    if (status) { status.textContent = 'Nombre actualizado'; setTimeout(()=>{ status.textContent=''; }, 2500); }
  } catch(e) {
    if (status) status.textContent = 'Error al guardar';
  }
}

// Debe coincidir con las claves de CATEGORIAS en clasificador_interes.py (backend)
const CATEGORIAS_INTERES = [
  ['salud_farmacia',          'Salud, farmacia y laboratorio'],
  ['construccion_obras',      'Construcción y obras'],
  ['vehiculos_taller',        'Vehículos, talleres y lubricantes'],
  ['combustibles',            'Combustibles'],
  ['tecnologia',              'Tecnología e informática'],
  ['alimentos',               'Alimentos y catering'],
  ['mobiliario',              'Mobiliario y eq. de oficina'],
  ['uniformes_textil',        'Uniformes y textiles'],
  ['limpieza',                'Limpieza e higiene'],
  ['papeleria',               'Papelería y útiles'],
  ['servicios_profesionales', 'Serv. profesionales y consultoría'],
  ['seguridad',               'Seguridad y vigilancia'],
  ['energia_electricidad',    'Energía y electricidad'],
];
let categoriasInteresSel = JSON.parse(localStorage.getItem('categorias_interes') || '[]');

function renderCategoriasInteres() {
  const grid = document.getElementById('categorias-interes-grid');
  if (!grid) return;
  grid.innerHTML = CATEGORIAS_INTERES.map(([clave, nombre]) => `
    <label class="rubro-chip" style="font-size:12px">
      <input type="checkbox" id="cat-${clave}" value="${clave}" ${categoriasInteresSel.includes(clave)?'checked':''} onchange="toggleCategoriaInteres('${clave}')">
      ${nombre}
    </label>`).join('');
}

function toggleCategoriaInteres(clave) {
  const el = document.getElementById(`cat-${clave}`);
  if (el?.checked) { if (!categoriasInteresSel.includes(clave)) categoriasInteresSel.push(clave); }
  else { categoriasInteresSel = categoriasInteresSel.filter(c => c !== clave); }
  localStorage.setItem('categorias_interes', JSON.stringify(categoriasInteresSel));
  guardarIntereses();
}

async function guardarIntereses() {
  const rubros = ['obras','bienes','servicios']
    .filter(r => document.getElementById(`interes-${r}`)?.checked)
    .map(r => r.charAt(0).toUpperCase()+r.slice(1));
  const familias   = familiasPerfil.map(f => ({codigo: f.codigo, nombre: f.nombre}));
  const textoLibre = document.getElementById('interes-texto-libre')?.value.trim() || '';
  const montoMinEl = document.getElementById('interes-monto-min')?.value;
  const montoMaxEl = document.getElementById('interes-monto-max')?.value;
  const montoMin   = montoMinEl ? parseFloat(montoMinEl) : null;
  const montoMax   = montoMaxEl ? parseFloat(montoMaxEl) : null;

  localStorage.setItem('rubros_interes', JSON.stringify(rubros));
  localStorage.setItem('categorias_interes', JSON.stringify(categoriasInteresSel));

  try {
    await fetch(`${API}/api/notificaciones/intereses`, {
      method:'PUT', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        endpoint: pushSubscription?.endpoint || null,
        user_id: currentUser?.id || null,
        rubros, familias,
        categorias_interes: categoriasInteresSel,
        texto_libre_interes: textoLibre,
        umbral_monto_min: montoMin,
        umbral_monto_max: montoMax,
        analisis_automatico: document.getElementById('interes-auto-analisis')?.checked || false
      })
    });
    const st = document.getElementById('intereses-status');
    if(st){ st.textContent='Preferencias guardadas'; setTimeout(()=>{st.textContent=''},2000); }
  } catch(e) {
    const st = document.getElementById('intereses-status');
    if(st){ st.textContent='Error al guardar — revisa tu conexión'; setTimeout(()=>{st.textContent=''},3000); }
  }
}

async function cargarInteresesGuardados() {
  renderCategoriasInteres();
  renderFamiliasPerfil();

  // Fuente de verdad: el perfil en el servidor (sincroniza entre dispositivos)
  if (currentUser?.id) {
    try {
      const res  = await fetch(`${API}/api/notificaciones/intereses?user_id=${currentUser.id}`);
      const data = await res.json();
      const p = data?.intereses;
      if (p) {
        (p.rubros || []).forEach(r => {
          const el = document.getElementById(`interes-${String(r).toLowerCase()}`);
          if (el) el.checked = true;
        });
        categoriasInteresSel = p.categorias_interes || [];
        localStorage.setItem('categorias_interes', JSON.stringify(categoriasInteresSel));
        renderCategoriasInteres();
        const tl = document.getElementById('interes-texto-libre');
        if (tl) tl.value = p.texto_libre_interes || '';
        const mn = document.getElementById('interes-monto-min');
        if (mn && p.umbral_monto_min != null) mn.value = p.umbral_monto_min;
        const mx = document.getElementById('interes-monto-max');
        if (mx && p.umbral_monto_max != null) mx.value = p.umbral_monto_max;
        const aa = document.getElementById('interes-auto-analisis');
        if (aa) aa.checked = p.analisis_automatico === true;
        if (Array.isArray(p.categorias_unspsc) && p.categorias_unspsc.length) {
          familiasPerfil = p.categorias_unspsc.map(c =>
            typeof c === 'object' ? {codigo:c.codigo, nombre:c.nombre||c.codigo} : {codigo:String(c), nombre:String(c)});
          localStorage.setItem('familias_interes', JSON.stringify(familiasPerfil));
          renderFamiliasPerfil();
        }
        return;
      }
    } catch(e) { /* fallback localStorage abajo */ }
  }
  const rubros = JSON.parse(localStorage.getItem('rubros_interes') || '[]');
  rubros.forEach(r => {
    const el = document.getElementById(`interes-${r.toLowerCase()}`);
    if (el) el.checked = true;
  });
}

// ── DETALLE ──
// ── HELPERS DETALLE ──────────────────────────────────────────────
function calcularCountdown(fechaStr) {
  if (!fechaStr) return { texto: '—', sub: '', clase: '' };
  const ahora = Date.now();
  const fin   = new Date(fechaStr).getTime();
  const diff  = fin - ahora;
  if (diff <= 0) return { texto: 'Vencido', sub: new Date(fechaStr).toLocaleDateString('es-DO'), clase: 'vencido' };
  const d = Math.floor(diff / 86400000);
  const h = Math.floor((diff % 86400000) / 3600000);
  const m = Math.floor((diff % 3600000) / 60000);
  let texto = '', clase = '';
  if (d > 0)      { texto = `${d}d ${h}h`;  clase = d <= 3 ? 'urgente' : ''; }
  else if (h > 0) { texto = `${h}h ${m}m`;  clase = 'urgente'; }
  else            { texto = `${m}m`;         clase = 'urgente'; }
  const sub = new Date(fechaStr).toLocaleDateString('es-DO', {day:'2-digit',month:'short'})
            + ' · ' + new Date(fechaStr).toLocaleTimeString('es-DO', {hour:'2-digit',minute:'2-digit'});
  return { texto, sub, clase };
}

function _scoreFallback(ev, alertas) {
  // Estimación para análisis viejos sin score_base — se marca como estimado
  const niveles = { 'Baja': 80, 'Media': 58, 'Alta': 30 };
  let score = niveles[ev.nivel_dificultad] ?? 50;
  const altasCount  = alertas.filter(a => a.riesgo === 'Alto').length;
  const mediosCount = alertas.filter(a => a.riesgo === 'Medio').length;
  score = Math.max(5, score - altasCount * 15 - mediosCount * 7);
  return Math.round(score);
}

function _ajustePerfil(proceso, perfil) {
  // Retorna { delta, factores } — ajuste del score según compatibilidad con el perfil
  if (!perfil) return { delta: 0, factores: [] };
  let delta = 0;
  const factores = [];

  // Sector / tipo de proceso
  const sectores = perfil.sectores || [];
  const tipoProc = (proceso.objeto_proceso || '').toLowerCase();
  const matchSector = sectores.some(s => tipoProc.includes((s||'').toLowerCase()) || (s||'').toLowerCase().includes(tipoProc));
  if (sectores.length) {
    if (matchSector) {
      delta += 10; factores.push({ texto: `Sector ${proceso.objeto_proceso} dentro de tu perfil`, positivo: true });
    } else {
      delta -= 15; factores.push({ texto: `Sector ${proceso.objeto_proceso} fuera de tu perfil`, positivo: false });
    }
  }

  // Rango de monto
  const monto = Number(proceso.monto_estimado) || 0;
  const montoMin = Number(perfil.monto_minimo) || 0;
  const montoMax = Number(perfil.monto_maximo) || 0;
  if (montoMin || montoMax) {
    const dentroRango = (!montoMin || monto >= montoMin) && (!montoMax || monto <= montoMax);
    if (dentroRango && monto > 0) {
      delta += 8; factores.push({ texto: `Monto dentro de tu rango habitual`, positivo: true });
    } else if (monto > 0) {
      delta -= 20; factores.push({ texto: `Monto fuera de tu rango (${formatMonto(monto)})`, positivo: false });
    }
  }

  // Provincia
  const provincias = perfil.provincias || [];
  const provProc = (proceso.provincia || '').toLowerCase();
  if (provincias.length && provProc) {
    const matchProv = provincias.some(pv => (pv||'').toLowerCase() === provProc);
    if (matchProv) {
      delta += 5; factores.push({ texto: `Provincia ${proceso.provincia} en tu área de operación`, positivo: true });
    }
  }

  // Documentación legal
  if (perfil.tiene_rpe) {
    delta += 5; factores.push({ texto: 'Tienes RPE/RNCE registrado', positivo: true });
  }
  if (perfil.tiene_estados_financieros) {
    delta += 5; factores.push({ texto: 'Tienes estados financieros disponibles', positivo: true });
  }

  return { delta: Math.round(delta), factores };
}

function calcularScoreCompetitividad(analisis, p) {
  if (!analisis || analisis.estado !== 'completado') return null;
  const ev = analisis.evaluacion_competitividad;
  if (!ev) return null;

  const alertas = analisis.alertas_fraude || [];

  // ── Score base: Gemini real si existe, fallback estimado si no ──
  let scoreBase, esEstimado;
  if (ev.score_base != null) {
    scoreBase  = Number(ev.score_base);
    esEstimado = false;
  } else {
    scoreBase  = _scoreFallback(ev, alertas);
    esEstimado = true;
  }
  scoreBase = Math.max(5, Math.min(100, scoreBase));

  // ── Ajuste por perfil del usuario ──
  const perfil = window._perfilEmpresa || null;
  const { delta, factores } = _ajustePerfil(p, perfil);
  const scoreFinal = Math.max(5, Math.min(100, scoreBase + delta));

  return {
    score:         scoreFinal,
    scoreBase,
    delta,
    esEstimado,
    tienePerfil:   !!perfil,
    factoresPerfil: factores,
    nivel:         ev.nivel_dificultad,
    recomendacion: ev.recomendacion,
    razon:         ev.razon,
    faltasGraves:  ev.faltas_graves || [],
  };
}

function renderGaugeScoreSVG(score, nivel) {
  const ARC = 251;
  const offset = ARC - (score / 100) * ARC;
  // Color basado en score numérico, no en nivel_dificultad (más preciso)
  const color = score >= 70 ? 'var(--green)' : score >= 45 ? 'var(--amber)' : 'var(--red)';
  const label = score >= 70 ? 'VALE LA PENA' : score >= 45 ? 'EVALUAR CON CUIDADO' : 'NO RECOMENDADO';
  return `<svg width="200" height="110" viewBox="0 0 200 110" xmlns="http://www.w3.org/2000/svg">
    <path d="M 18 105 A 82 82 0 0 1 182 105" fill="none" stroke="var(--border2)" stroke-width="11" stroke-linecap="round"/>
    <path d="M 18 105 A 82 82 0 0 1 182 105" fill="none" stroke="${color}" stroke-width="11" stroke-linecap="round"
          stroke-dasharray="${ARC}" stroke-dashoffset="${offset.toFixed(1)}" style="transition:stroke-dashoffset .8s cubic-bezier(.4,0,.2,1)"/>
    <text x="100" y="88" text-anchor="middle" font-family="var(--font-mono)" font-size="36" font-weight="800" fill="var(--text)">${score}</text>
    <text x="100" y="104" text-anchor="middle" font-family="var(--font-mono)" font-size="9" fill="var(--muted)" letter-spacing="1">${label}</text>
  </svg>`;
}

function renderTabScore(analisis, p) {
  const scoreData = calcularScoreCompetitividad(analisis, p);
  if (!scoreData) {
    const enCola = ['procesando','pendiente','pendiente_analisis'].includes(analisis?.estado);
    return `<div class="score-gauge-card">
      <div class="score-no-analisis">
        <div class="score-no-icon">${enCola ? '⏳' : '📋'}</div>
        <div style="font-weight:700;margin-bottom:6px;color:var(--text2)">
          ${enCola ? 'Análisis en proceso…' : 'Sin análisis disponible'}
        </div>
        <div style="font-size:12px;color:var(--muted);line-height:1.5;">
          ${enCola
            ? 'La puntuación estará disponible cuando termine el análisis del pliego.'
            : 'Sigue este proceso para que el sistema analice automáticamente el pliego y calcule la puntuación de competitividad.'}
        </div>
      </div>
    </div>`;
  }

  const { score, scoreBase, delta, esEstimado, tienePerfil, factoresPerfil, nivel, recomendacion, razon, faltasGraves } = scoreData;
  const badgeClass = nivel === 'Baja' ? 'baja' : nivel === 'Media' ? 'media' : 'alta';
  const alertas = analisis.alertas_fraude || [];

  // ── Bloque de desglose score ──
  let desgloseHTML = '';
  if (tienePerfil && delta !== 0) {
    const signo = delta > 0 ? `+${delta}` : `${delta}`;
    const colorDelta = delta > 0 ? 'var(--green)' : 'var(--red)';
    desgloseHTML = `
    <div style="display:flex;align-items:center;justify-content:center;gap:12px;margin:-4px 0 10px;font-size:12px;color:var(--muted);">
      <span>Base <strong style="color:var(--text2)">${scoreBase}</strong></span>
      <span style="color:${colorDelta};font-weight:800;">${signo} perfil</span>
      <span>= <strong style="color:var(--text)">${score}</strong></span>
    </div>`;
  }

  // ── Factores del perfil ──
  let perfilHTML = '';
  if (tienePerfil && factoresPerfil.length) {
    perfilHTML = `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header"><h2 class="ll-card__title">Tu compatibilidad</h2></div>
      <div class="ll-card__body" style="padding-top:4px;">
        ${factoresPerfil.map(f => `
        <div style="display:flex;align-items:flex-start;gap:8px;padding:7px 0;border-bottom:1px solid var(--border);">
          <span style="flex-shrink:0;margin-top:1px;">${f.positivo ? '✅' : '⚠️'}</span>
          <span style="font-size:13px;color:var(--text2);">${f.texto}</span>
        </div>`).join('')}
      </div>
    </div>`;
  } else if (!tienePerfil) {
    perfilHTML = `
    <div style="margin-bottom:12px;padding:12px 14px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--r-md);font-size:12px;color:var(--muted);line-height:1.6;">
      <strong style="color:var(--text2);">💡 Completa tu perfil de empresa</strong><br>
      La puntuación se ajustará según tu sector, rango de monto y provincia de operación.
    </div>`;
  }

  // ── Aviso de estimado ──
  const estimadoHTML = esEstimado
    ? `<div style="margin-bottom:10px;padding:8px 12px;background:var(--bg2);border-left:3px solid var(--amber);border-radius:0 var(--r-sm) var(--r-sm) 0;font-size:11px;color:var(--muted);">
        Puntuación estimada — este análisis fue generado antes de la versión actual. Re-analiza el pliego para obtener la puntuación exacta.
      </div>` : '';

  // ── Alertas ──
  const alertasHTML = alertas.length ? `
    <div class="alertas-card">
      <div class="alertas-header">🚨 Alertas del análisis <span style="font-size:10px;font-weight:800;margin-left:6px;background:rgba(231,76,60,.12);color:var(--red);padding:2px 7px;border-radius:3px;">${alertas.length}</span></div>
      ${alertas.map(a => {
        const r = (a.riesgo||'').toLowerCase();
        const iconSVG = r === 'alto'
          ? `<svg viewBox="0 0 24 24" fill="none" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.46 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`
          : r === 'bajo'
          ? `<svg viewBox="0 0 24 24" fill="none" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`
          : `<svg viewBox="0 0 24 24" fill="none" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m10.29 3.86-8.1 14.06A2 2 0 0 0 4 21h16a2 2 0 0 0 1.81-2.86L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`;
        return `<div class="alerta-item">
          <div class="alerta-icon ${r}">${iconSVG}</div>
          <div class="alerta-body">
            <div class="alerta-titulo">${a.categoria || 'Alerta'}
              <span class="alerta-riesgo-tag ${r}">${a.riesgo}</span>
            </div>
            <div class="alerta-desc">${a.hallazgo || ''}</div>
            ${a.articulo_ley ? `<div class="alerta-ley">📋 ${a.articulo_ley}</div>` : ''}
          </div>
        </div>`;
      }).join('')}
    </div>` : '';

  // ── Requisitos ──
  const req = analisis.requisitos_experiencia;
  const fin = analisis.requisitos_financieros;
  const reqRows = [];
  if (req?.anos_experiencia_empresa && req.anos_experiencia_empresa !== 'N/A')
    reqRows.push({ label: 'Experiencia empresa', val: req.anos_experiencia_empresa });
  if (req?.contratos_similares && req.contratos_similares !== 'N/A')
    reqRows.push({ label: 'Contratos similares', val: req.contratos_similares });
  if (req?.sector_especialidad && req.sector_especialidad !== 'N/A')
    reqRows.push({ label: 'Especialidad requerida', val: req.sector_especialidad });
  if (fin?.patrimonio_minimo && fin.patrimonio_minimo !== 'N/A')
    reqRows.push({ label: 'Patrimonio mínimo', val: fin.patrimonio_minimo });
  if (fin?.indice_liquidez && fin.indice_liquidez !== 'N/A')
    reqRows.push({ label: 'Índice de liquidez', val: fin.indice_liquidez });
  if (fin?.otros && fin.otros !== 'N/A')
    reqRows.push({ label: 'Otros requisitos', val: fin.otros });

  const requisitosHTML = reqRows.length ? `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header"><h2 class="ll-card__title">Requisitos para participar</h2></div>
      <div class="ll-card__body" style="padding-top:4px;">
        ${reqRows.map(r => `
        <div style="padding:8px 0;border-bottom:1px solid var(--border);">
          <div class="ll-caption ll-u-text-subtle" style="margin-bottom:3px;">${r.label}</div>
          <div class="ll-body-sm">${r.val}</div>
        </div>`).join('')}
      </div>
    </div>` : '';

  return `
    <div class="score-gauge-card">
      <div class="score-gauge-header">
        <div class="score-gauge-title">¿Vale la pena?</div>
        <span class="score-gauge-badge score-badge-${badgeClass}">Dificultad ${nivel}</span>
      </div>
      <div class="score-gauge-svg-wrap">${renderGaugeScoreSVG(score, nivel)}</div>
      ${desgloseHTML}
      ${estimadoHTML}
      ${recomendacion ? `<div class="score-gauge-rec">💬 ${recomendacion}</div>` : ''}
      ${razon ? `<div class="score-gauge-razon">${razon}</div>` : ''}
    </div>
    ${perfilHTML}
    ${alertasHTML}
    ${requisitosHTML}`;
}


function renderTabScore(analisis, p) {
  const scoreData = calcularScoreCompetitividad(analisis, p);
  if (!scoreData) {
    const enCola = ['procesando','pendiente','pendiente_analisis'].includes(analisis?.estado);
    return `<div class="score-gauge-card">
      <div class="score-no-analisis">
        <div class="score-no-icon">${enCola ? '⏳' : '🤖'}</div>
        <div style="font-weight:700;margin-bottom:6px;color:var(--text2)">
          ${enCola ? 'Análisis en proceso…' : 'Sin análisis disponible'}
        </div>
        <div style="font-size:12px;color:var(--muted);line-height:1.5;">
          ${enCola
            ? 'El score estará disponible cuando termine el análisis del pliego.'
            : 'Sigue este proceso para que el sistema analice automáticamente el pliego y calcule el score de competitividad.'}
        </div>
      </div>
    </div>`;
  }
  const { score, nivel, recomendacion, razon } = scoreData;
  const badgeClass = nivel === 'Baja' ? 'baja' : nivel === 'Media' ? 'media' : 'alta';
  const alertas = analisis.alertas_fraude || [];
  return `
    <div class="score-gauge-card">
      <div class="score-gauge-header">
        <div class="score-gauge-title">⚡ ¿Vale la pena?</div>
        <span class="score-gauge-badge score-badge-${badgeClass}">Dificultad ${nivel}</span>
      </div>
      <div class="score-gauge-svg-wrap">${renderGaugeScoreSVG(score, nivel)}</div>
      ${recomendacion ? `<div class="score-gauge-rec">💬 ${recomendacion}</div>` : ''}
      ${razon ? `<div class="score-gauge-razon">${razon}</div>` : ''}
    </div>
    ${alertas.length ? `
    <div class="alertas-card">
      <div class="alertas-header">🚨 Alertas del análisis <span style="font-size:10px;font-weight:800;margin-left:6px;background:rgba(231,76,60,.12);color:var(--red);padding:2px 7px;border-radius:3px;">${alertas.length}</span></div>
      ${alertas.map(a => {
        const r = (a.riesgo||'').toLowerCase();
        const iconSVG = r === 'alto'
          ? `<svg viewBox="0 0 24 24" fill="none" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.46 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`
          : r === 'bajo'
          ? `<svg viewBox="0 0 24 24" fill="none" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`
          : `<svg viewBox="0 0 24 24" fill="none" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m10.29 3.86-8.1 14.06A2 2 0 0 0 4 21h16a2 2 0 0 0 1.81-2.86L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`;
        return `<div class="alerta-item">
          <div class="alerta-icon ${r}">${iconSVG}</div>
          <div class="alerta-body">
            <div class="alerta-titulo">${a.categoria || 'Alerta'}
              <span class="alerta-riesgo-tag ${r}">${a.riesgo}</span>
            </div>
            <div class="alerta-desc">${a.hallazgo || ''}</div>
            ${a.articulo_ley ? `<div class="alerta-ley">📋 ${a.articulo_ley}</div>` : ''}
          </div>
        </div>`;
      }).join('')}
    </div>` : ''}
    ${renderRequisitosCompletos(analisis)}`;
}

// Renderiza TODOS los requisitos del pliego (mismo detalle que el correo):
// experiencia, financieros, personal clave, equipos, garantías, restricciones.
function renderRequisitosCompletos(analisis) {
  const exp = analisis.requisitos_experiencia || {};
  const fin = analisis.requisitos_financieros || {};
  const pye = analisis.personal_y_equipos || {};
  const personal = pye.personal_clave || [];
  const equipos = pye.equipos_minimos || [];
  const garantias = analisis.garantias_exigidas || [];
  const restricciones = analisis.restricciones_participacion || [];

  const val = (v) => v && String(v).trim().toUpperCase() !== 'N/A' && String(v).trim() !== '' ? v : null;
  const fila = (label, v) => val(v) ? `
    <div style="margin-bottom:10px;">
      <div class="ll-caption ll-u-text-subtle" style="margin-bottom:3px;">${label}</div>
      <div class="ll-body-sm">${v}</div>
    </div>` : '';

  // ── Experiencia ──
  let expHtml = '';
  const expFilas = [
    fila('Años de experiencia (empresa)', exp.anos_experiencia_empresa),
    fila('Contratos similares', exp.contratos_similares || exp.obras_similares),
    fila('Sector / especialidad', exp.sector_especialidad),
    fila('Período de referencia', exp.periodo_referencia),
    fila('Ámbito', exp.ambito),
    fila('Montos facturados', exp.montos_facturados),
    fila('Otros', exp.otros),
  ].filter(Boolean).join('');
  if (expFilas) expHtml = `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header"><h2 class="ll-card__title">Experiencia requerida</h2></div>
      <div class="ll-card__body" style="padding-top:8px;">${expFilas}</div>
    </div>`;

  // ── Financieros ──
  let finHtml = '';
  const finFilas = [
    fila('Índice de liquidez', fin.indice_liquidez),
    fila('Índice de endeudamiento', fin.indice_endeudamiento),
    fila('Patrimonio mínimo', fin.patrimonio_minimo),
    fila('Otros', fin.otros),
  ].filter(Boolean).join('');
  if (finFilas) finHtml = `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header"><h2 class="ll-card__title">Requisitos financieros</h2></div>
      <div class="ll-card__body" style="padding-top:8px;">${finFilas}</div>
    </div>`;

  // ── Personal clave ──
  let persHtml = '';
  if (personal.length) {
    persHtml = `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header"><h2 class="ll-card__title">Personal clave exigido <span style="font-size:10px;font-weight:800;margin-left:6px;background:var(--bg2);color:var(--muted);padding:2px 7px;border-radius:4px;">${personal.length}</span></h2></div>
      <div class="ll-card__body" style="padding-top:8px;">
        ${personal.map(pc => `
          <div style="padding:10px 0;border-bottom:1px solid var(--border);">
            <div class="ll-body-sm" style="font-weight:700;color:var(--text);">${pc.posicion || 'Cargo'}${pc.es_subsanable === false ? '<span style="font-size:9px;font-weight:800;margin-left:6px;background:rgba(231,76,60,.12);color:var(--red);padding:1px 6px;border-radius:3px;">NO SUBSANABLE</span>' : ''}</div>
            ${val(pc.titulo) ? `<div class="ll-caption" style="margin-top:3px;">${pc.titulo}</div>` : ''}
            <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:5px;">
              ${val(pc.anos_experiencia_general) ? `<span class="ll-chip">Exp. gral: ${pc.anos_experiencia_general}</span>` : ''}
              ${val(pc.anos_experiencia_especifica) ? `<span class="ll-chip">Exp. específica: ${pc.anos_experiencia_especifica}</span>` : ''}
              ${val(pc.registro_profesional) ? `<span class="ll-chip">${pc.registro_profesional}</span>` : ''}
              ${val(pc.dedicacion) ? `<span class="ll-chip">${pc.dedicacion}</span>` : ''}
            </div>
            ${val(pc.nota) ? `<div class="ll-caption ll-u-text-subtle" style="margin-top:4px;">${pc.nota}</div>` : ''}
          </div>`).join('')}
      </div>
    </div>`;
  }

  // ── Equipos mínimos ──
  let equipHtml = '';
  if (equipos.length) {
    equipHtml = `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header"><h2 class="ll-card__title">Equipos mínimos <span style="font-size:10px;font-weight:800;margin-left:6px;background:var(--bg2);color:var(--muted);padding:2px 7px;border-radius:4px;">${equipos.length}</span></h2></div>
      <div class="ll-card__body" style="padding-top:8px;">
        ${equipos.map(eq => `
          <div style="padding:9px 0;border-bottom:1px solid var(--border);">
            <div class="ll-body-sm" style="font-weight:700;color:var(--text);">${eq.equipo || 'Equipo'}${val(eq.cantidad) ? ` <span style="color:var(--muted);font-weight:600;">×${eq.cantidad}</span>` : ''}</div>
            <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:4px;">
              ${val(eq.especificaciones) ? `<span class="ll-chip">${eq.especificaciones}</span>` : ''}
              ${val(eq.antiguedad_maxima) ? `<span class="ll-chip">Máx. ${eq.antiguedad_maxima}</span>` : ''}
              ${val(eq.modalidad) ? `<span class="ll-chip">${eq.modalidad}</span>` : ''}
            </div>
          </div>`).join('')}
      </div>
    </div>`;
  }

  // ── Garantías ──
  let garHtml = '';
  if (garantias.length) {
    garHtml = `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header"><h2 class="ll-card__title">Garantías exigidas</h2></div>
      <div class="ll-card__body" style="padding-top:8px;">
        ${garantias.map(g => `
          <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);">
            <div class="ll-body-sm">${g.tipo || 'Garantía'}</div>
            <div class="ll-body-sm ll-mono" style="font-weight:700;">${val(g.monto_o_porcentaje) || '—'}</div>
          </div>`).join('')}
      </div>
    </div>`;
  }

  // ── Restricciones ──
  let restrHtml = '';
  if (restricciones.length) {
    restrHtml = `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header"><h2 class="ll-card__title">Restricciones de participación</h2></div>
      <div class="ll-card__body" style="padding-top:8px;">
        ${restricciones.map(r => `
          <div style="padding:7px 0;border-bottom:1px solid var(--border);">
            ${val(r.tipo) ? `<div class="ll-caption ll-u-text-subtle">${r.tipo}</div>` : ''}
            <div class="ll-body-sm">${r.descripcion || r}</div>
          </div>`).join('')}
      </div>
    </div>`;
  }

  const todo = expHtml + finHtml + persHtml + equipHtml + garHtml + restrHtml;
  return todo || '';
}

function renderTabPlazos(analisis, p) {
  const plazos = analisis?.plazos_clave;
  const fCierre = p.fecha_fin_recepcion_ofertas;

  // Helper: parsear fecha del análisis (DD/MM/YYYY HH:MM o ISO)
  function parseFechaPlazos(str) {
    if (!str || str === 'N/A') return null;
    if (/^\d{2}\/\d{2}\/\d{4}/.test(str)) {
      const [datePart, timePart] = str.split(' ');
      const [d, m, y] = datePart.split('/');
      return new Date(`${y}-${m}-${d}T${timePart || '00:00'}:00`);
    }
    return new Date(str);
  }

  // Validar visita_sitio: descartarla si Gemini la inventó
  // Criterios: hora exactamente 00:00 (sin hora real en pliego)
  // O fecha posterior al cierre del proceso (imposible lógicamente)
  function esVisitaValida(fechaStr) {
    if (!fechaStr || fechaStr === 'N/A') return false;
    const fecha = parseFechaPlazos(fechaStr);
    if (!fecha || isNaN(fecha)) return false;
    // Hora 00:00 = Gemini interpoló sin dato real en el pliego
    const horas = fecha.getHours();
    const mins  = fecha.getMinutes();
    if (horas === 0 && mins === 0) return false;
    // Posterior al cierre = fecha incoherente
    if (fCierre) {
      const cierre = new Date(fCierre);
      if (!isNaN(cierre) && fecha > cierre) return false;
    }
    return true;
  }

  const entradas = [];
  if (plazos) {
    if (plazos.consultas && plazos.consultas !== 'N/A')
      entradas.push({ nombre: 'Último día de consultas', fecha: plazos.consultas, key: false });
    if (esVisitaValida(plazos.visita_sitio))
      entradas.push({ nombre: 'Visita al sitio', fecha: plazos.visita_sitio, key: true });
    if (plazos.presentacion_ofertas && plazos.presentacion_ofertas !== 'N/A')
      entradas.push({ nombre: 'Presentación de ofertas', fecha: plazos.presentacion_ofertas, key: true });
    if (plazos.apertura_ofertas && plazos.apertura_ofertas !== 'N/A')
      entradas.push({ nombre: 'Apertura de ofertas', fecha: plazos.apertura_ofertas, key: true });
  }
  // Cierre desde el proceso si no vino del análisis
  if (fCierre) {
    const yaEsta = entradas.some(e => e.nombre === 'Presentación de ofertas');
    if (!yaEsta) entradas.push({ nombre: 'Cierre recepción de ofertas', fecha: fCierre, key: true });
  }
  if (!entradas.length) {
    return `<div class="plazos-card"><div class="plazos-header">📅 Plazos del proceso</div><div class="plazo-na">Sin fechas disponibles para este proceso.</div></div>`;
  }

  // Ordenar por fecha
  entradas.sort((a, b) => {
    const fa = parseFechaPlazos(a.fecha) || new Date(a.fecha);
    const fb = parseFechaPlazos(b.fecha) || new Date(b.fecha);
    return fa - fb;
  });

  const ahora = Date.now();
  return `<div class="plazos-card">
    <div class="plazos-header">📅 Plazos del proceso</div>
    ${entradas.map(e => {
      const fechaObj = parseFechaPlazos(e.fecha) || new Date(e.fecha);
      const vencido = !isNaN(fechaObj) && fechaObj.getTime() < ahora;
      const urgente = !vencido && !isNaN(fechaObj) && (fechaObj.getTime() - ahora) < 3 * 86400000;
      const dotClass = vencido ? '' : urgente ? 'urgente' : e.key ? 'key' : '';
      const fechaLabel = isNaN(fechaObj) ? e.fecha
        : fechaObj.toLocaleDateString('es-DO', {day:'2-digit',month:'short',year:'numeric'});
      const horaLabel = isNaN(fechaObj) ? ''
        : (fechaObj.getHours() || fechaObj.getMinutes())
          ? fechaObj.toLocaleTimeString('es-DO', {hour:'2-digit',minute:'2-digit'}) : '';
      return `<div class="plazo-item">
        <div class="plazo-fecha-col">
          <div class="plazo-fecha" style="${vencido?'color:var(--muted);':''}${urgente?'color:var(--amber);':''}">${fechaLabel}</div>
          ${horaLabel ? `<div class="plazo-hora">${horaLabel}</div>` : ''}
        </div>
        <div class="plazo-dot ${dotClass}"></div>
        <div>
          <div class="plazo-nombre" style="${vencido?'color:var(--muted);text-decoration:line-through;':''}">${e.nombre}</div>
          ${vencido ? '<div class="plazo-sub" style="color:var(--muted)">Vencido</div>' : urgente ? '<div class="plazo-sub" style="color:var(--amber)">⚡ Próximo</div>' : ''}
        </div>
      </div>`;
    }).join('')}
  </div>
  ${p.duracion_contrato ? `<div class="ll-card" style="margin-bottom:12px;padding:12px 16px;">
    <div class="ll-caption ll-u-text-subtle" style="margin-bottom:4px;">Duración del contrato</div>
    <div class="ll-body-sm" style="font-weight:700;">${p.duracion_contrato}</div>
  </div>` : ''}`;
}

function renderAsidePanel(analisis, p, scoreData, totalItems) {
  const ahora = new Date();
  const horaStr = ahora.toLocaleTimeString('es-DO', {hour:'2-digit', minute:'2-digit'});
  const estadoLabel = analisis?.estado === 'completado' ? 'Actualizado' : analisis?.estado === 'procesando' ? 'Analizando…' : 'Sin análisis';

  // ── Gauge compacto ──
  let gaugeHTML = '';
  if (scoreData) {
    const { score, nivel, recomendacion, esEstimado, delta, tienePerfil, factoresPerfil } = scoreData;
    const ARC = 200;
    const offset = ARC - (score / 100) * ARC;
    const color = score >= 70 ? 'var(--green)' : score >= 45 ? 'var(--amber)' : 'var(--red)';
    const label = score >= 70 ? 'VALE LA PENA' : score >= 45 ? 'EVALUAR' : 'NO RECOMENDADO';
    const badgeCls = nivel === 'Baja' ? 'baja' : nivel === 'Media' ? 'media' : 'alta';

    let desgloseStr = '';
    if (tienePerfil && delta !== 0) {
      const signo = delta > 0 ? `+${delta}` : `${delta}`;
      const colorDelta = delta > 0 ? 'var(--green)' : 'var(--red)';
      desgloseStr = `<div style="text-align:center;font-size:11px;color:var(--muted);margin-top:-6px;margin-bottom:8px;">
        <span style="color:${colorDelta};font-weight:800;">${signo} perfil</span> · Total <strong style="color:var(--text)">${score}</strong>
      </div>`;
    }

    let factoresHTML = '';
    if (tienePerfil && factoresPerfil.length) {
      factoresHTML = `<div class="aside-factores">
        ${factoresPerfil.map(f => `<div class="aside-factor-item"><span>${f.positivo ? '✅' : '⚠️'}</span><span>${f.texto}</span></div>`).join('')}
      </div>`;
    } else if (!tienePerfil) {
      factoresHTML = `<div style="font-size:11px;color:var(--muted);padding:6px 8px;background:var(--bg2);border-radius:var(--r-sm);margin-bottom:10px;line-height:1.5;">
        💡 Completa tu perfil para ver compatibilidad personalizada
      </div>`;
    }

    gaugeHTML = `
      <div style="border-bottom:1px solid var(--border);padding-bottom:14px;margin-bottom:14px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
          <div style="font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);">¿Vale la pena?</div>
          <span class="score-gauge-badge score-badge-${badgeCls}" style="font-size:9px;">Dificultad ${nivel}</span>
        </div>
        <div class="aside-gauge-wrap">
          <svg width="160" height="88" viewBox="0 0 160 88" xmlns="http://www.w3.org/2000/svg">
            <path d="M 14 84 A 66 66 0 0 1 146 84" fill="none" stroke="var(--border2)" stroke-width="9" stroke-linecap="round"/>
            <path d="M 14 84 A 66 66 0 0 1 146 84" fill="none" stroke="${color}" stroke-width="9" stroke-linecap="round"
                  stroke-dasharray="${ARC}" stroke-dashoffset="${(ARC-(score/100)*ARC).toFixed(1)}"
                  style="transition:stroke-dashoffset .8s cubic-bezier(.4,0,.2,1)"/>
            <text x="80" y="72" text-anchor="middle" font-family="var(--font-mono)" font-size="30" font-weight="800" fill="var(--text)">${score}</text>
            <text x="80" y="84" text-anchor="middle" font-family="var(--font-mono)" font-size="7.5" fill="var(--muted)" letter-spacing="1">${label}</text>
          </svg>
        </div>
        ${desgloseStr}
        ${recomendacion ? `<div class="aside-score-rec">💬 ${recomendacion}</div>` : ''}
        ${esEstimado ? `<div style="font-size:10px;color:var(--muted);text-align:center;margin-top:4px;">Puntuación estimada · Re-analiza para exactitud</div>` : ''}
        ${factoresHTML}
      </div>`;
  } else if (analisis?.estado === 'procesando' || analisis?.estado === 'pendiente_analisis') {
    gaugeHTML = `<div style="text-align:center;padding:16px 0;border-bottom:1px solid var(--border);margin-bottom:14px;">
      <div style="font-size:20px;margin-bottom:6px;">⏳</div>
      <div style="font-size:12px;font-weight:700;color:var(--text2);">Analizando pliego…</div>
      <div style="font-size:11px;color:var(--muted);margin-top:4px;">La puntuación estará lista en minutos</div>
    </div>`;
  } else {
    gaugeHTML = `<div style="text-align:center;padding:14px 0;border-bottom:1px solid var(--border);margin-bottom:14px;">
      <div style="font-size:11px;color:var(--muted);line-height:1.6;">Sin análisis disponible.<br>Sigue el proceso para activarlo.</div>
    </div>`;
  }

  // ── Alertas compactas ──
  const alertas = analisis?.alertas_fraude || [];
  let alertasHTML = '';
  if (alertas.length) {
    alertasHTML = `
      <div style="border-bottom:1px solid var(--border);padding-bottom:12px;margin-bottom:12px;">
        <div style="font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:8px;">
          Alertas <span style="background:rgba(231,76,60,.12);color:var(--red);padding:1px 6px;border-radius:3px;font-size:9px;">${alertas.length}</span>
        </div>
        ${alertas.slice(0,4).map(a => {
          const r = (a.riesgo||'').toLowerCase();
          return `<div class="aside-alerta">
            <div class="aside-alerta-dot ${r}"></div>
            <div>
              <div class="aside-alerta-titulo">${a.categoria || 'Alerta'} · <span style="font-size:10px;color:var(--muted);font-weight:400;">${a.riesgo}</span></div>
              <div class="aside-alerta-desc">${(a.hallazgo||'').substring(0,120)}${(a.hallazgo||'').length > 120 ? '…' : ''}</div>
            </div>
          </div>`;
        }).join('')}
        ${alertas.length > 4 ? `<div style="font-size:11px;color:var(--muted);padding-top:6px;">+${alertas.length-4} más · ver en ¿Vale la pena?</div>` : ''}
      </div>`;
  }

  // ── Checklist resumido ──
  let checklistHTML = '';
  if (totalItems > 0) {
    checklistHTML = `
      <div>
        <div class="aside-checklist-prog">
          <span class="aside-cl-label">Checklist</span>
          <span class="aside-cl-count" id="aside-cl-count-${p.codigo_proceso}">0 / ${totalItems}</span>
        </div>
        <div class="checklist-progreso-bar" style="margin-bottom:8px;">
          <div class="checklist-progreso-fill" id="aside-cl-fill-${p.codigo_proceso}" style="width:0%"></div>
        </div>
        <div style="font-size:11px;color:var(--muted);">Ver detalle completo en la pestaña Checklist</div>
      </div>`;
  }

  return `
    <div class="detalle-aside-panel">
      <div class="detalle-aside-header">
        <div class="detalle-aside-header-left">
          <div class="detalle-aside-icon">📋</div>
          <div>
            <div class="detalle-aside-title">Análisis del pliego</div>
            <div class="detalle-aside-sub">${horaStr} · ${estadoLabel}</div>
          </div>
        </div>
        ${analisis?.estado === 'completado'
          ? `<span style="font-size:9px;font-weight:800;padding:3px 8px;border-radius:var(--r-full);background:var(--green-dim);color:var(--green);border:1px solid var(--green-border);">Fresco</span>`
          : ''}
      </div>
      <div class="detalle-aside-body">
        ${gaugeHTML}
        ${alertasHTML}
        ${checklistHTML}
      </div>
    </div>`;
}

function actualizarAsideChecklist(codigo, totalItems) {
  if (!totalItems) return;
  // Contar cuántos están marcados en el estado actual
  let marcados = 0;
  ['legal','tecnica','financiera'].forEach(sec => {
    for (let i = 0; i < 100; i++) {
      if (checklistEstado[`${codigo}-${sec}_${i}`]) marcados++;
      else if (!document.getElementById(`cli-${codigo}-${sec}_${i}`)) break;
    }
  });
  const pct = totalItems ? Math.round(marcados / totalItems * 100) : 0;
  const fill  = document.getElementById(`aside-cl-fill-${codigo}`);
  const count = document.getElementById(`aside-cl-count-${codigo}`);
  if (fill)  fill.style.width  = `${pct}%`;
  if (count) count.textContent = `${marcados} / ${totalItems}`;
}

// ── Precio Inteligente ─────────────────────────────────────────────────────
let _precioCache = {};

async function cargarPrecioInteligente(codigo) {
  if (!(await verificarAccesoPro('precio'))) return;
  const cont = document.getElementById('dtab-oferta-content');
  if (!cont) return;

  // No recargar si ya se cargó para este proceso
  if (_precioCache[codigo]) { renderPrecioInteligente(cont, _precioCache[codigo]); return; }

  cont.innerHTML = `<div style="padding:32px;text-align:center;">
    <div class="spinner" style="margin:0 auto 12px;"></div>
    <div style="font-size:13px;color:var(--muted);">Analizando ${''} contratos similares...</div></div>`;

  try {
    const token = localStorage.getItem('sb_token');
    const headers = { 'Content-Type': 'application/json' };
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const res = await fetch(`${API}/api/procesos/${codigo}/precio-inteligente`, { headers });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    _precioCache[codigo] = data;
    renderPrecioInteligente(cont, data);
  } catch(e) {
    cont.innerHTML = `<div style="padding:24px;text-align:center;">
      <div style="font-size:28px;margin-bottom:8px;">📊</div>
      <div style="font-size:13px;color:var(--muted);">No se pudo cargar el análisis de precio. Intenta de nuevo.</div>
      <button onclick="delete _precioCache['${codigo}'];cargarPrecioInteligente('${codigo}')"
        class="ll-btn ll-btn--secondary" style="margin-top:12px;font-size:12px;">Reintentar</button></div>`;
  }
}

function renderPrecioInteligente(cont, data) {
  if (!data || !data.disponible) {
    cont.innerHTML = `<div style="padding:24px;text-align:center;">
      <div style="font-size:28px;margin-bottom:8px;">🔍</div>
      <div style="font-size:13px;color:var(--muted);">${data?.mensaje || 'No hay suficientes contratos similares aún.'}</div></div>`;
    return;
  }

  const fmt = (n) => n != null ? 'RD$' + Number(n).toLocaleString('es-DO', {maximumFractionDigits:0}) : '—';
  const pct = (n) => n != null ? (n >= 0 ? `-${Math.abs(n)}%` : `+${Math.abs(n)}%`) : '—';
  const pctColor = (n) => n != null && n > 0 ? 'var(--success)' : n < 0 ? 'var(--danger)' : 'var(--muted)';

  const {
    nivel_analisis, muestra_contratos,
    monto_referencial, divisa,
    precio_sugerido, precio_rango_min, precio_rango_max,
    descuento_mediana, descuento_promedio,
    top_competidores, narrativa_ia
  } = data;

  // --- KPI cards ---
  const kpis = [
    { label: 'Precio sugerido', value: fmt(precio_sugerido), sub: 'Mediana histórica de ganadores', big: true, color: 'var(--primary)' },
    { label: 'Rango competitivo', value: `${fmt(precio_rango_min)} – ${fmt(precio_rango_max)}`, sub: 'P25 – P75 de ganadores similares', color: 'var(--text)' },
    { label: 'Descuento típico', value: pct(descuento_mediana), sub: `Promedio ${pct(descuento_promedio)} sobre el referencial`, color: pctColor(descuento_mediana) },
    { label: 'Referencial', value: fmt(monto_referencial), sub: `Monto estimado por la institución · ${divisa}`, color: 'var(--muted)' },
  ];

  const kpiHtml = kpis.map(k => `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px 16px;flex:1;min-width:140px;">
      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:6px;">${k.label}</div>
      <div style="font-size:${k.big?'20px':'16px'};font-weight:800;color:${k.color};line-height:1.2;">${k.value}</div>
      <div style="font-size:11px;color:var(--muted);margin-top:4px;">${k.sub}</div>
    </div>`).join('');

  // --- Competidores ---
  const compHtml = (top_competidores || []).length > 0
    ? (top_competidores || []).map((c, i) => `
      <div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border);">
        <div style="width:24px;height:24px;border-radius:50%;background:var(--primary-10);display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;color:var(--primary);">${i+1}</div>
        <div style="flex:1;min-width:0;">
          <div style="font-size:13px;font-weight:600;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${sanitize(c.nombre)}</div>
          <div style="font-size:11px;color:var(--muted);">${c.victorias_similares} contratos ganados · Promedio ${fmt(c.monto_promedio)}</div>
        </div>
      </div>`).join('')
    : `<div style="font-size:13px;color:var(--muted);padding:8px 0;">Sin datos de competidores para este tipo.</div>`;

  // --- Narrativa IA ---
  const narrativaHtml = narrativa_ia ? `
    <div class="ll-card" style="margin-bottom:12px;border-left:3px solid var(--primary);">
      <div class="ll-card__header">
        <h2 class="ll-card__title">🤖 Recomendación IA</h2>
      </div>
      <div class="ll-card__body">
        <p style="font-size:13px;color:var(--text2);line-height:1.75;margin:0;">${sanitize(narrativa_ia)}</p>
      </div>
    </div>` : '';

  cont.innerHTML = `
    <!-- Badge de fuente -->
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:14px;padding:8px 12px;background:var(--surface);border:1px solid var(--border);border-radius:8px;">
      <span style="font-size:16px;">📊</span>
      <div>
        <div style="font-size:12px;font-weight:700;color:var(--text);">Basado en ${muestra_contratos} contratos similares</div>
        <div style="font-size:11px;color:var(--muted);">${sanitize(nivel_analisis || '')}</div>
      </div>
    </div>

    <!-- KPI grid -->
    <div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px;">
      ${kpiHtml}
    </div>

    ${narrativaHtml}

    <!-- Competidores -->
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__header">
        <h2 class="ll-card__title">🏆 Competidores frecuentes</h2>
        <span class="ll-chip" style="font-size:11px;">${(top_competidores||[]).length} empresas</span>
      </div>
      <div class="ll-card__body" style="padding-top:0;">
        ${compHtml}
      </div>
    </div>

    <!-- Aviso metodológico -->
    <div style="font-size:11px;color:var(--muted);text-align:center;padding:8px;">
      Análisis estadístico basado en contratos adjudicados del DGCP. Úsalo como referencia, no como garantía.
    </div>`;
}

function cambiarTabDetalle(codigo, tabId) {
  document.querySelectorAll(`#detalle-container .detalle-tab-btn`).forEach(b => b.classList.remove('active'));
  document.querySelectorAll(`#detalle-container .detalle-tab-panel`).forEach(p => p.classList.remove('active'));
  const btn   = document.getElementById(`dtab-btn-${tabId}`);
  const panel = document.getElementById(`dtab-panel-${tabId}`);
  if (btn)   btn.classList.add('active');
  if (panel) panel.classList.add('active');
  // Lazy-load del tab Precio Inteligente
  if (tabId === 'oferta' && codigo) {
    cargarPrecioInteligente(codigo);
  }
}

// Volver desde el detalle al dashboard (alias seguro)
function volverDeDetalle() {
  try { mostrarDashboard(); } catch(e) { try { switchTab2('dashboard'); } catch(e2) {} }
}

// ── PROCESO HISTÓRICO: participantes, montos y ganadores ──
// Para procesos adjudicados que ya no están en la tabla de procesos activos.
async function verProcesoHistorico(codigo) {
  const cont = document.getElementById('detalle-container');
  cont.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
  try {
    const res = await fetch(`${API}/api/v1/proveedores/proceso/${encodeURIComponent(codigo)}/historico`);
    if (!res.ok) throw new Error('sin datos');
    const d = await res.json();

    // El identificador era un OCID y el proceso SÍ está activo con su código real
    // → mostrar el detalle completo normal (una sola redirección, sin loops)
    if (d.existe_en_procesos && d.codigo_proceso && d.codigo_proceso !== codigo) {
      verDetalle(d.codigo_proceso);
      return;
    }

    const fmtRD = (n) => {
      if (n === null || n === undefined || isNaN(Number(n))) return '—';
      const v = Number(n);
      return v >= 1_000_000 ? `RD$${(v/1_000_000).toFixed(1)}M` : `RD$${v.toLocaleString('es-DO')}`;
    };

    const ofs = (d.oferentes?.oferentes || []).slice().sort((a,b) => {
      if (a.es_ganador !== b.es_ganador) return a.es_ganador ? -1 : 1;
      return (a.valor_oferta || 0) - (b.valor_oferta || 0);
    });

    const filasOferentes = ofs.map(o => `
      <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;padding:11px 0;border-bottom:1px solid var(--border);">
        <div style="flex:1;min-width:0;">
          <div style="font-size:13px;font-weight:700;color:var(--text);display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
            ${sanitize(o.razon_social || 'Empresa')}
            ${o.es_ganador ? '<span style="font-size:10px;font-weight:800;padding:2px 8px;background:var(--green-dim);color:var(--green);border:1px solid var(--green-border);border-radius:var(--r-full);">🏆 GANADOR</span>' : ''}
          </div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px;">${sanitize(o.estado_evaluacion || o.estado_oferta || '')}${o.rpe ? ' · RPE ' + sanitize(o.rpe) : ''}</div>
        </div>
        <div style="text-align:right;flex-shrink:0;">
          <div style="font-size:13px;font-weight:800;font-family:var(--font-mono);color:${o.es_ganador ? 'var(--green)' : 'var(--text)'};">${fmtRD(o.valor_oferta)}</div>
          ${o.es_ganador && o.monto_adjudicado ? `<div style="font-size:10px;color:var(--muted);">Adjudicado: ${fmtRD(o.monto_adjudicado)}</div>` : ''}
        </div>
      </div>`).join('');

    const filasContratos = (d.contratos || []).map(c => `
      <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 0;border-bottom:1px solid var(--border);">
        <div style="flex:1;min-width:0;">
          <div style="font-size:13px;font-weight:700;color:var(--text);">${sanitize(c.empresa_nombre || 'Empresa adjudicada')}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px;">${c.fecha_contrato ? new Date(c.fecha_contrato).toLocaleDateString('es-DO') : (c.anio || '')}${c.empresa_rnc ? ' · RPE ' + sanitize(c.empresa_rnc) : ''}</div>
        </div>
        <div style="font-size:13px;font-weight:800;font-family:var(--font-mono);color:var(--green);flex-shrink:0;">${fmtRD(c.monto_adjudicado)}</div>
      </div>`).join('');

    const totalOf = d.oferentes?.total_oferentes || ofs.length;

    cont.innerHTML = `
      <div style="padding:16px;">
        <button onclick="volverDeDetalle()" class="ll-btn ll-btn--ghost ll-btn--sm" style="margin-bottom:12px;">← Volver</button>
        <span style="font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;padding:3px 10px;background:var(--surface);border:1px solid var(--border);border-radius:var(--r-full);color:var(--muted);">Proceso histórico · Adjudicado</span>
        <h2 style="font-size:18px;font-weight:900;color:var(--text);margin:10px 0 4px;line-height:1.3;">${sanitize(d.titulo || codigo)}</h2>
        <div style="font-size:12px;color:var(--muted);margin-bottom:16px;">${sanitize(codigo)}${d.modalidad ? ' · ' + sanitize(d.modalidad) : ''}${d.anio ? ' · ' + d.anio : ''}</div>

        ${filasContratos ? `
        <div class="ll-card" style="margin-bottom:14px;">
          <div style="font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--green);margin-bottom:6px;">🏆 Adjudicación</div>
          ${filasContratos}
        </div>` : ''}

        <div class="ll-card">
          <div style="font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:6px;">Participantes (${totalOf})</div>
          ${filasOferentes || '<div style="font-size:13px;color:var(--muted);padding:10px 0;">No hay registro de ofertas para este proceso — solo la adjudicación final.</div>'}
        </div>
      </div>`;
  } catch(e) {
    cont.innerHTML = `
      <div style="padding:32px 16px;text-align:center;">
        <div style="font-size:32px;margin-bottom:12px;">🔍</div>
        <div style="font-weight:700;color:var(--text);margin-bottom:6px;">Sin datos históricos para este proceso.</div>
        <div style="font-size:13px;color:var(--text2);margin-bottom:14px;">Este proceso no está en el portal actual ni en el archivo de adjudicaciones.</div>
        <button onclick="volverDeDetalle()" class="ll-btn ll-btn--secondary">← Volver</button>
      </div>`;
  }
}

async function verDetalle(codigo) {
  // Ocultar TODOS los tabs antes de mostrar el detalle
  ['tab-dashboard','tab-perfil','tab-seguimiento','tab-mercado','tab-alertas','tab-analisis','tab-reportes','tab-calendario','tab-inteligencia'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = 'none';
  });
  document.getElementById('tab-detalle').style.display = 'block';
  document.getElementById('detalle-container').innerHTML = '<div class="loading"><div class="spinner"></div></div>';

  try {

  let res, resEnm;
  try {
    [res, resEnm] = await Promise.all([
      fetch(`${API}/api/procesos/${codigo}`),
      fetch(`${API}/api/procesos/${codigo}/historial`).catch(() => null)
    ]);
  } catch (fetchErr) {
    document.getElementById('detalle-container').innerHTML = `
      <div style="padding:32px 16px;text-align:center;">
        <div style="font-size:32px;margin-bottom:12px;">📡</div>
        <div style="font-weight:700;color:var(--text);margin-bottom:6px;">Sin conexión</div>
        <div style="font-size:13px;color:var(--text2);margin-bottom:16px;">No se pudo conectar al servidor. Verifica tu internet e intenta de nuevo.</div>
        <button onclick="verDetalle('${codigo}')" class="ll-btn ll-btn--secondary">Reintentar</button>
      </div>`;
    return;
  }

  if (!res.ok) {
    if (res.status === 404) {
      // Proceso histórico (no está en la tabla de procesos activos):
      // mostrar participantes, montos y ganadores desde el archivo de ofertas
      await verProcesoHistorico(codigo);
      return;
    }
    document.getElementById('detalle-container').innerHTML = `
      <div style="padding:32px 16px;text-align:center;">
        <div style="font-size:32px;margin-bottom:12px;">⚠️</div>
        <div style="font-weight:700;color:var(--text);margin-bottom:6px;">Error del servidor (${res.status}). Intenta de nuevo.</div>
        <button onclick="verDetalle('${codigo}')" class="ll-btn ll-btn--secondary" style="margin-top:8px;">Reintentar</button>
      </div>`;
    return;
  }

  let data;
  try {
    data = await res.json();
  } catch (parseErr) {
    document.getElementById('detalle-container').innerHTML = `
      <div style="padding:32px 16px;text-align:center;">
        <div style="font-size:32px;margin-bottom:12px;">⚠️</div>
        <div style="font-weight:700;color:var(--text);margin-bottom:6px;">Error leyendo respuesta del servidor.</div>
        <button onclick="verDetalle('${codigo}')" class="ll-btn ll-btn--secondary" style="margin-top:8px;">Reintentar</button>
      </div>`;
    return;
  }

  const p    = data.proceso;
  if (!p) {
    await verProcesoHistorico(codigo);
    return;
  }
  const arts = data.articulos || [];
  const analisis = data.analisis;

  // Extraer enmiendas del historial
  let enmiendas = [];
  try {
    const dataEnm = await resEnm?.json();
    enmiendas = (dataEnm?.eventos || []).filter(e => e.tipo === 'enmienda');
  } catch(e) {}
  const ultimaEnmienda = enmiendas[enmiendas.length - 1] || null;

  const fPub = p.fecha_publicacion
    ? new Date(p.fecha_publicacion).toLocaleDateString('es-DO') : '—';

  // ── GRID MÉTRICAS ──
  const countdown = calcularCountdown(p.fecha_fin_recepcion_ofertas);
  const scoreData = calcularScoreCompetitividad(analisis, p);

  // Contar items checklist
  const checklist = analisis?.checklist_categorizado;
  const totalItems = checklist
    ? (checklist.legal||[]).length + (checklist.tecnica||[]).length + (checklist.financiera||[]).length : 0;

  // UNSPSC del primer artículo
  const unspscCode = arts[0]?.clase_unspsc || p.codigo_proceso?.split('-')[0] || '—';

  // ── ESTADO ANÁLISIS BANNER ──
  let estadoAnalisisBanner = '';
  // Cargar participantes en background (no bloquea el render)
  window._semaforoInst = p.unidad_compra;
  cargarSemaforoPago(p.unidad_compra);

  if (['procesando','pendiente','pendiente_analisis'].includes(analisis?.estado)) {
    estadoAnalisisBanner = `<div class="analisis-procesando">
      <div class="analisis-procesando-spinner"></div>
      <div class="analisis-procesando-texto">${analisis?.estado === 'pendiente_analisis'
        ? 'En cola para análisis. El sistema procesará el pliego en los próximos minutos.'
        : 'Analizando el pliego … Puedes cerrar la app, te notificamos cuando esté listo.'}</div>
    </div>`;
  } else if (analisis?.estado === 'error') {
    estadoAnalisisBanner = `<div class="analisis-procesando" style="background:rgba(231,76,60,0.08);border-color:rgba(231,76,60,0.2)">
      <div class="analisis-procesando-texto" style="color:var(--red)">Hubo un problema al analizar este proceso. Contacta soporte por WhatsApp.</div>
    </div>`;
  }

  // ── SCORE BADGE para el tab ──
  const scoreBadge = scoreData
    ? `<span class="tab-badge" style="color:${scoreData.nivel==='Baja'?'var(--green)':scoreData.nivel==='Media'?'var(--amber)':'var(--red)'}">·${scoreData.score}</span>`
    : '';

  // ── CHECKLIST BADGE ──
  const clBadge = totalItems
    ? `<span class="tab-badge">${totalItems}</span>` : '';

  // ── ARTÍCULOS BADGE ──
  const artsBadge = arts.length ? `<span class="tab-badge">${arts.length}</span>` : '';

  document.getElementById('detalle-container').innerHTML = `
    <!-- HEADER (ancho completo) -->
    <div class="detalle-header-full">
      <div class="ll-card" style="margin-bottom:12px;">
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px;">
          ${p.objeto_proceso ? `<span class="ll-chip ll-chip--info">${p.objeto_proceso}</span>` : ''}
          ${p.modalidad      ? `<span class="ll-chip">${p.modalidad}</span>` : ''}
          ${p.estado_proceso ? `<span class="ll-chip ll-chip--success">${p.estado_proceso}</span>` : ''}
        </div>
        <h1 class="ll-h2" style="margin-bottom:6px;line-height:1.3;">${p.titulo || 'Sin título'}</h1>
        <div style="font-size:12px;color:var(--muted);margin-bottom:12px;">${p.unidad_compra || ''} · Publicado ${fPub}</div>
        <div class="detalle-metrics-grid">
          <div class="detalle-metric">
            <div class="detalle-metric-label">Monto estimado</div>
            <div class="detalle-metric-value" style="font-size:15px;">${formatMontoFull(p.monto_estimado)}</div>
            <div class="detalle-metric-sub">${p.divisa || 'DOP'}</div>
          </div>
          <div class="detalle-metric">
            <div class="detalle-metric-label">Cierre</div>
            <div class="detalle-metric-value ${countdown.clase}">${countdown.texto}</div>
            <div class="detalle-metric-sub">${countdown.sub}</div>
          </div>
          <div class="detalle-metric">
            <div class="detalle-metric-label">Provincia</div>
            <div class="detalle-metric-value" style="font-size:13px;font-family:var(--font-body);">${p.provincia || '—'}</div>
            <div class="detalle-metric-sub">${p.codigo_proceso}</div>
          </div>
          <div class="detalle-metric">
            <div class="detalle-metric-label">UNSPSC</div>
            <div class="detalle-metric-value" style="font-size:14px;">${unspscCode}</div>
            <div class="detalle-metric-sub">${p.objeto_proceso || '—'}</div>
          </div>
        </div>
        <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
          ${p.url ? `<a href="${p.url}" target="_blank" class="ll-btn ll-btn--ghost ll-btn--sm" style="display:inline-flex;">Ver en DGCP ↗</a>` : ''}
          <button id="btn-seguir-${p.codigo_proceso}" class="ll-btn ll-btn--primary ll-btn--sm btn-seguir no-siguiendo" style="flex:1;min-width:120px;" onclick="toggleSeguimiento('${p.codigo_proceso}')">Seguir proceso</button>
          ${analisis?.estado === 'completado' ? `
          <button class="ll-btn ll-btn--secondary ll-btn--sm btn-pdf-analisis" id="btn-pdf-analisis-${p.codigo_proceso}" onclick="descargarAnalisisPDF('${p.codigo_proceso}')">
            <div class="btn-pdf-spinner"></div>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            PDF
          </button>` : ''}
          ${ultimaEnmienda ? `
          <button id="btn-reanalizar-${p.codigo_proceso}"
            class="ll-btn ll-btn--sm"
            style="background:rgba(245,158,11,.15);border:1.5px solid rgba(245,158,11,.4);color:var(--amber);font-weight:700;gap:6px;"
            onclick="solicitarReanalisis('${p.codigo_proceso}')">
            ⚠ Re-analizar con enmienda
            <span style="background:rgba(245,158,11,.25);padding:1px 6px;border-radius:10px;font-size:10px;font-weight:800;">#${enmiendas.length}</span>
          </button>` : ''}
        </div>
      </div>
      ${estadoAnalisisBanner}
    </div>

    <!-- LAYOUT 2 COLUMNAS -->
    <div class="detalle-layout">

      <!-- COLUMNA PRINCIPAL -->
      <div class="detalle-col-main">
        <div class="detalle-tabs-nav">
          <button class="detalle-tab-btn active" id="dtab-btn-resumen"   onclick="cambiarTabDetalle('${codigo}','resumen')">Resumen</button>
          <button class="detalle-tab-btn"        id="dtab-btn-checklist" onclick="cambiarTabDetalle('${codigo}','checklist')">Checklist${clBadge}</button>
          <button class="detalle-tab-btn"        id="dtab-btn-plazos"    onclick="cambiarTabDetalle('${codigo}','plazos')">Plazos</button>
          <button class="detalle-tab-btn"        id="dtab-btn-docs"      onclick="cambiarTabDetalle('${codigo}','docs')">Artículos${artsBadge}</button>
          <button class="detalle-tab-btn"        id="dtab-btn-score"     onclick="cambiarTabDetalle('${codigo}','score')">¿Vale la pena?${scoreBadge}</button>
          <button class="detalle-tab-btn"        id="dtab-btn-semaforo"  onclick="cambiarTabDetalle('${codigo}','semaforo');cargarSemaforoPago(window._semaforoInst)">🚦 Pago</button>
        </div>

        <!-- TAB: RESUMEN -->
        <div class="detalle-tab-panel active" id="dtab-panel-resumen">
          ${analisis?.resumen_ejecutivo ? `
          <div class="ll-card" style="margin-bottom:12px;">
            <div class="ll-card__header"><h2 class="ll-card__title">Resumen ejecutivo</h2></div>
            <div class="ll-card__body"><p style="font-size:13px;color:var(--text2);line-height:1.7;">${analisis.resumen_ejecutivo}</p></div>
          </div>` : `
          <div class="ll-card" style="margin-bottom:12px;padding:14px 16px;">
            <div class="ll-body-sm ll-u-text-subtle" style="text-align:center;padding:12px 0;">
              ${analisis ? 'Sin resumen ejecutivo.' : 'Sigue el proceso para activar el análisis del pliego.'}
            </div>
          </div>`}
          <div id="compat-exp-${p.codigo_proceso}"></div>
          <div class="ll-card" style="margin-bottom:12px;">
            <div class="ll-card__header"><h2 class="ll-card__title">Datos del proceso</h2></div>
            <div class="ll-card__body">
              <dl style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
                <div><dt class="ll-caption ll-u-text-subtle">Institución</dt><dd class="ll-body-sm" style="margin:2px 0 0;">${p.unidad_compra || '—'}</dd></div>
                <div><dt class="ll-caption ll-u-text-subtle">Código</dt><dd class="ll-mono ll-body-sm" style="margin:2px 0 0;">${p.codigo_proceso}</dd></div>
                <div><dt class="ll-caption ll-u-text-subtle">Publicación</dt><dd class="ll-mono ll-body-sm" style="margin:2px 0 0;">${fPub}</dd></div>
                <div><dt class="ll-caption ll-u-text-subtle">Cierre</dt><dd class="ll-mono ll-body-sm" style="margin:2px 0 0;">${countdown.sub || '—'}</dd></div>
                <div><dt class="ll-caption ll-u-text-subtle">Divisa</dt><dd class="ll-body-sm" style="margin:2px 0 0;">${p.divisa || 'DOP'}</dd></div>
                ${p.duracion_contrato ? `<div><dt class="ll-caption ll-u-text-subtle">Duración contrato</dt><dd class="ll-body-sm" style="margin:2px 0 0;">${p.duracion_contrato}</dd></div>` : ''}
              </dl>
            </div>
          </div>
        </div>

        <!-- TAB: CHECKLIST -->
        <div class="detalle-tab-panel" id="dtab-panel-checklist"></div>

        <!-- TAB: PLAZOS -->
        <div class="detalle-tab-panel" id="dtab-panel-plazos">
          ${renderTabPlazos(analisis, p)}
        </div>

        <!-- TAB: ARTÍCULOS -->
        <div class="detalle-tab-panel" id="dtab-panel-docs">
          <div class="ll-card" style="margin-bottom:12px;">
            <div class="ll-card__header">
              <h2 class="ll-card__title">Artículos del proceso <span class="ll-chip" style="margin-left:4px;">${arts.length}</span></h2>
              ${arts.length ? `<button class="ll-btn ll-btn--ghost ll-btn--sm" onclick="descargarArticulosPDF('${p.codigo_proceso}')">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                PDF
              </button>` : ''}
            </div>
            <div class="ll-card__body">
              ${arts.length ? arts.map(a => `
              <div style="display:flex;justify-content:space-between;align-items:flex-start;padding:10px 0;border-bottom:1px solid var(--color-border);">
                <div style="flex:1;margin-right:12px;">
                  <div class="ll-body-sm">${a.descripcion_articulo || a.descripcion_usuario || 'Sin descripción'}</div>
                  <div class="ll-caption ll-mono ll-u-text-subtle" style="margin-top:3px;">${a.cantidad||'—'} ${a.unidad_medida||''} · UNSPSC: ${a.clase_unspsc||'—'}</div>
                </div>
                <div class="ll-num ll-body-sm">${a.precio_total_estimado ? 'RD$'+Number(a.precio_total_estimado).toLocaleString() : '—'}</div>
              </div>`).join('') : '<div class="ll-body-sm ll-u-text-subtle" style="padding:16px 0;">Sin artículos registrados</div>'}
            </div>
          </div>
        </div>

        <!-- TAB: SCORE (solo visible en móvil, en desktop se ve en el aside) -->
        <div class="detalle-tab-panel" id="dtab-panel-score">
          ${renderTabScore(analisis, p)}
        </div>

        <!-- TAB: SEMÁFORO DE PAGO -->
        <div class="detalle-tab-panel" id="dtab-panel-semaforo">
          <div class="ll-card" style="margin-bottom:12px;">
            <div class="ll-card__header">
              <h2 class="ll-card__title">🚦 ¿Esta institución paga a tiempo?</h2>
            </div>
            <div class="ll-card__body">
              <div id="dtab-semaforo-content">
                <div style="font-size:13px;color:var(--muted);padding:12px 0;">Consultando historial de pagos (InfoPago DGCP)...</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- ASIDE (desktop) -->
      <div class="detalle-col-aside" id="detalle-aside-col">
        ${renderAsidePanel(analisis, p, scoreData, totalItems)}
      </div>

    </div>`;

  // Post-render: checklist + botón seguir + aside
  setTimeout(async () => {
    const clPanel = document.getElementById('dtab-panel-checklist');
    if (clPanel) clPanel.innerHTML = renderChecklistHTML(analisis, p.codigo_proceso);

    actualizarBtnSeguir(p.codigo_proceso);
    ['legal','tecnica','financiera'].forEach(secKey => {
      const sec = document.getElementById(`cl-sec-${p.codigo_proceso}-${secKey}`);
      if (sec && !sec.classList.contains('abierta')) sec.classList.add('abierta');
    });
    await cargarEstadoChecklist(p.codigo_proceso);

    // Sincronizar barra de progreso del aside con el checklist
    actualizarAsideChecklist(p.codigo_proceso, totalItems);

    // Compatibilidad Expediente Digital vs pliego (Bid Manager)
    cargarCompatibilidadExpediente(p.codigo_proceso);
  }, 10);

  if (['procesando','pendiente','pendiente_analisis'].includes(analisis?.estado)) {
    iniciarPollingAnalisis(p.codigo_proceso);
  }
  } catch (unexpectedErr) {
    console.error('[verDetalle] Error inesperado:', unexpectedErr);
    document.getElementById('detalle-container').innerHTML = `
      <div style="padding:32px 16px;text-align:center;">
        <div style="font-size:32px;margin-bottom:12px;">⚠️</div>
        <div style="font-weight:700;color:var(--text);margin-bottom:6px;">Error al cargar el proceso</div>
        <div style="font-size:13px;color:var(--text2);margin-bottom:16px;">${unexpectedErr.message || 'Error desconocido'}</div>
        <button onclick="verDetalle('${codigo}')" class="ll-btn ll-btn--secondary">Reintentar</button>
      </div>`;
  }
}

let _pollingTimer = null;
function iniciarPollingAnalisis(codigo) {
  if (_pollingTimer) { clearInterval(_pollingTimer); _pollingTimer = null; }
  let intentos = 0;
  const MAX = 24;
  // Primeros 6 intentos cada 5s (30s), luego cada 15s hasta 6 min total
  const intervalo = () => intentos < 6 ? 5000 : 15000;

  const tick = async () => {
    intentos++;
    const detalleVisible = document.getElementById('tab-detalle')?.style.display !== 'none';
    if (!detalleVisible || intentos > MAX) {
      clearTimeout(_pollingTimer); _pollingTimer = null; return;
    }
    try {
      const res  = await fetch(`${API}/api/procesos/${codigo}`);
      const data = await res.json();
      const estado = data.analisis?.estado;
      if (estado === 'completado') {
        clearTimeout(_pollingTimer); _pollingTimer = null;
        mostrarBannerAnalisisListo(codigo, data.proceso?.titulo || codigo);
        verDetalle(codigo);
        return;
      } else if (estado === 'error') {
        clearTimeout(_pollingTimer); _pollingTimer = null;
        const spinner = document.querySelector('.analisis-procesando');
        if (spinner) {
          spinner.style.background = 'rgba(231,76,60,0.08)';
          spinner.style.borderColor = 'rgba(231,76,60,0.2)';
          const sp = spinner.querySelector('.analisis-procesando-spinner');
          if (sp) sp.style.display = 'none';
          const tx = spinner.querySelector('.analisis-procesando-texto');
          if (tx) { tx.style.color = 'var(--red)'; tx.textContent = 'Hubo un problema al analizar este proceso. Contacta soporte por WhatsApp.'; }
        }
        return;
      }
    } catch(e) { /* silencioso */ }
    _pollingTimer = setTimeout(tick, intervalo());
  };
  _pollingTimer = setTimeout(tick, 5000);
}

function mostrarBannerAnalisisListo(codigo, titulo) {
  // Eliminar banner anterior si existe
  const anterior = document.getElementById('banner-analisis-listo');
  if (anterior) anterior.remove();

  const banner = document.createElement('div');
  banner.id = 'banner-analisis-listo';
  banner.className = 'analisis-listo-banner';
  banner.innerHTML = `
    <div class="analisis-listo-banner-icon">✅</div>
    <div class="analisis-listo-banner-texto">
      <div class="analisis-listo-banner-titulo">¡Análisis listo!</div>
      <div class="analisis-listo-banner-sub">Te enviamos los detalles por correo</div>
    </div>
    <div class="analisis-listo-banner-btns">
      <button class="analisis-listo-banner-btn primary" onclick="descargarAnalisisPDF('${codigo}');cerrarBannerAnalisis()">📄 PDF</button>
      <button class="analisis-listo-banner-btn secondary" onclick="cerrarBannerAnalisis()">✕</button>
    </div>`;
  document.body.appendChild(banner);

  // Animar entrada
  requestAnimationFrame(() => {
    requestAnimationFrame(() => banner.classList.add('visible'));
  });

  // Auto-cerrar después de 12 segundos
  banner._timer = setTimeout(cerrarBannerAnalisis, 12000);

  // Vibrar si el dispositivo lo soporta
  if (navigator.vibrate) navigator.vibrate([100, 50, 100]);
}

function cerrarBannerAnalisis() {
  const banner = document.getElementById('banner-analisis-listo');
  if (!banner) return;
  clearTimeout(banner._timer);
  banner.classList.remove('visible');
  setTimeout(() => banner.remove(), 400);
}

function mostrarDashboard() {
  if (_pollingTimer) { clearTimeout(_pollingTimer); _pollingTimer = null; }
  // Ocultar TODOS los tabs y mostrar dashboard
  ['tab-detalle','tab-perfil','tab-seguimiento','tab-mercado','tab-alertas','tab-analisis','tab-reportes','tab-calendario','tab-inteligencia'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = 'none';
  });
  document.getElementById('tab-dashboard').style.display = 'block';
  // Sidebar desktop — restaurar el tab activo previo
  const prevTab = window._lastActiveTab || 'dashboard';
  document.querySelectorAll('.dsb-nav-item').forEach(b => b.classList.remove('active'));
  const dsbId = prevTab === 'seguimiento' ? 'dsb-nav-seguimiento' : 'dsb-nav-dashboard';
  document.getElementById(dsbId)?.classList.add('active');
}

// ── NAV DASHBOARD TAP — comportamiento tipo Instagram ──
// • Si ya estás en dashboard y en el top: refresca datos
// • Si ya estás en dashboard pero scrolleado: sube suave al top
// • Si estás en otra pantalla: navega al dashboard
function navDashboardTap() {
  const dash = document.getElementById('tab-dashboard');
  const yaEnDashboard = dash && dash.style.display !== 'none';

  if (yaEnDashboard) {
    if (dash.scrollTop <= 10) {
      // Ya está arriba — refrescar como Instagram
      cargarStats();
      cargarProcesos(1);
      mostrarToast('Actualizando...');
    } else {
      // Scrollear suave al top
      dash.scrollTo({ top: 0, behavior: 'smooth' });
    }
  } else {
    switchTab2('dashboard');
  }
}

// ── NAV ──
function switchTab2(tab) {
  window._lastActiveTab = tab;
  _pushNavState({ tab });
  // FIX: blur del elemento activo (evita que un input enfocado en el tab anterior
  // bloquee la navegación, especialmente en el tab Empresas/Inteligencia).
  try { document.activeElement && document.activeElement.blur && document.activeElement.blur(); } catch(e){}
  const allTabs = ['dashboard','detalle','perfil','seguimiento','mercado','alertas','analisis','reportes','calendario','inteligencia','semaforo','referidos','bid'];
  allTabs.forEach(t => {
    const el = document.getElementById(`tab-${t}`);
    if (!el) return;
    if (t === tab) {
      // setProperty con 'important' gana sobre cualquier style inline residual
      el.style.setProperty('display', 'block', 'important');
      // Reset scroll del tab que se abre para no aterrizar a mitad
      try { el.scrollTop = 0; } catch(e){}
    } else {
      el.style.setProperty('display', 'none', 'important');
    }
  });
  if (tab==='seguimiento') cargarSeguimiento();
  if (tab==='mercado')     cargarMercado();
  if (tab==='inteligencia') {
    // Bloqueo freemium: al abrir Empresas, verificar PRO
    esUsuarioPro().then(pro => { if (!pro) mostrarModalUpgrade('empresas'); });
    // FIX: solo resetear el mensaje si no hay resultados ni dashboard visibles
    const dash = document.getElementById('ic-dashboard');
    const resC = document.getElementById('ic-resultados');
    const hayContenido = (dash && dash.style.display !== 'none') || (resC && resC.innerHTML.trim());
    if (!hayContenido) icRenderEstado('Busca una empresa por nombre o RNC para ver su historial de contratos con el Estado.');
  }
  if (tab==='perfil')      { cargarEstadoPlan(); cargarPanelReferidos(); }
  if (tab==='referidos')   cargarPanelReferidos('referidos-tab-contenido');
  if (tab==='alertas')     cargarAlertas();
  if (tab==='analisis')    cargarAnalisisTab();
  if (tab==='reportes')    cargarReportes();
  if (tab==='calendario')  cargarCalendario();
  if (tab==='semaforo')    { if (typeof spInit === 'function') spInit(); }
  if (tab==='bid')         { if (typeof BidManager !== 'undefined') BidManager.init(); }
  // Bottom-nav (móvil)
  document.querySelectorAll('.ll-bottom-nav__item').forEach(b => b.classList.remove('is-active'));
  const navMap = { 'dashboard':'nav-dashboard','perfil':'nav-perfil','seguimiento':'nav-seguimiento','mercado':'nav-mercado','inteligencia':'nav-inteligencia' };
  if(navMap[tab]) document.getElementById(navMap[tab])?.classList.add('is-active');
  // Sidebar desktop
  document.querySelectorAll('.dsb-nav-item').forEach(b => b.classList.remove('active'));
  const dsbMap = {
    'dashboard':'dsb-nav-dashboard','perfil':'dsb-nav-perfil',
    'seguimiento':'dsb-nav-seguimiento','mercado':'dsb-nav-mercado',
    'analisis':'dsb-nav-analisis',
    'inteligencia':'dsb-nav-inteligencia',
    'semaforo':'dsb-nav-semaforo',
    'reportes':'dsb-nav-reportes','calendario':'dsb-nav-calendario',
    'bid':'dsb-nav-bid'
  };
  if(dsbMap[tab]) document.getElementById(dsbMap[tab])?.classList.add('active');
}

// ── NAVEGACIÓN CON BOTÓN ATRÁS (History API) ─────────────────────────────
// FIX: antes la app no manejaba el historial; el gesto/flecha atrás sacaba
// al usuario de la vista actual y lo mandaba a la página principal.
let _navFromPop = false;

function _pushNavState(state) {
  if (_navFromPop) return;
  try {
    const hash = '#' + (state.view ? `${state.tab}/${state.view}` : state.tab);
    const cur = history.state;
    if (!cur || !cur.tab) {
      history.replaceState(state, '', hash);
    } else if (cur.tab === state.tab && cur.view === state.view && cur.id === state.id) {
      // Mismo estado: no duplicar entradas en el historial
    } else {
      history.pushState(state, '', hash);
    }
  } catch(e) {}
}

window.addEventListener('popstate', (ev) => {
  const st = ev.state;
  if (!st || !st.tab) return;
  _navFromPop = true;
  try {
    switchTab2(st.tab);
    if (st.tab === 'inteligencia') {
      if (st.view === 'dashboard' && st.id) {
        icVerDashboard(st.id);
      } else {
        icMostrarResultados();
      }
    }
  } catch(e) {} 
  _navFromPop = false;
});

// ── FILTROS ACTIVOS (chips removibles en topbar) ──────────────────────────
function actualizarFiltrosActivos() {
  const inst = document.getElementById('filtro-institucion')?.value?.trim() || '';
  const mMin = document.getElementById('filtro-monto-min')?.value || '';
  const mMax = document.getElementById('filtro-monto-max')?.value || '';
  const chips = [];
  if (inst) chips.push({ label: inst, clear: () => { document.getElementById('filtro-institucion').value=''; debounceSearch(); actualizarFiltrosActivos(); } });
  if (mMin) chips.push({ label: `Monto > RD$${Number(mMin).toLocaleString()}`, clear: () => { document.getElementById('filtro-monto-min').value=''; debounceSearch(); actualizarFiltrosActivos(); } });
  if (mMax) chips.push({ label: `Monto < RD$${Number(mMax).toLocaleString()}`, clear: () => { document.getElementById('filtro-monto-max').value=''; debounceSearch(); actualizarFiltrosActivos(); } });
  const container = document.getElementById('active-filter-chips');
  const limpiarBtn = document.getElementById('btn-limpiar-filtros');
  if (!container) return;
  container.innerHTML = chips.map((c, i) => `
    <span class="active-filter-chip" id="afc-${i}">${c.label}
      <button onclick="(${c.clear.toString()})()" style="background:none;border:none;color:var(--muted);cursor:pointer;padding:0;font-size:13px;line-height:1;">✕</button>
    </span>`).join('');
  if (limpiarBtn) limpiarBtn.style.display = chips.length ? 'inline-flex' : 'none';
}

// ── TAB: ALERTAS ─────────────────────────────────────────────────────────
async function cargarAlertas() {
  const lista = document.getElementById('alertas-lista');
  if (!lista || !currentUser?.id) return;
  lista.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
  try {
    // Traer procesos seguidos con fechas próximas
    const res = await sbRest(`seguimiento_procesos?user_id=eq.${currentUser.id}&select=proceso_codigo&limit=100`);
    const seg  = await res.json();
    if (!seg.length) { lista.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);">No sigues ningún proceso aún.</div>'; return; }
    const ids = seg.map(s => `"${s.proceso_codigo}"`).join(',');
    const resProcesos = await fetch(`${SUPABASE_URL}/rest/v1/procesos?codigo_proceso=in.(${ids})&select=codigo_proceso,titulo,fecha_fin_recepcion_ofertas,unidad_compra&order=fecha_fin_recepcion_ofertas.asc`, { headers: { 'apikey': SUPABASE_ANON_KEY } });
    const procesos = await resProcesos.json();
    const ahora = Date.now();
    const activos = procesos.filter(p => p.fecha_fin_recepcion_ofertas && new Date(p.fecha_fin_recepcion_ofertas) > new Date(ahora));
    if (!activos.length) { lista.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);">No hay fechas próximas de cierre.</div>'; return; }
    lista.innerHTML = activos.map(p => {
      const cd = calcularCountdown(p.fecha_fin_recepcion_ofertas);
      const urgente = cd.clase === 'urgente' || cd.clase === 'vencido';
      return `<div class="ll-card ll-card--hover" style="margin-bottom:8px;cursor:pointer;border-left:3px solid ${urgente?'var(--amber)':'var(--border)'};" onclick="verDetalle('${p.codigo_proceso}')">
        <div style="display:flex;align-items:center;justify-content:space-between;">
          <div style="flex:1;min-width:0;">
            <div style="font-size:13px;font-weight:700;color:var(--text);margin-bottom:3px;">${p.titulo||'Sin título'}</div>
            <div style="font-size:11px;color:var(--muted);">${p.unidad_compra||''} · ${p.codigo_proceso}</div>
          </div>
          <div style="text-align:right;flex-shrink:0;margin-left:16px;">
            <div style="font-size:14px;font-weight:800;color:${urgente?'var(--amber)':'var(--green)'};">${cd.texto}</div>
            <div style="font-size:11px;color:var(--muted);">${cd.sub}</div>
          </div>
        </div>
      </div>`;
    }).join('');
  } catch(e) {
    lista.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);">Error cargando alertas.</div>';
  }
}

// ── TAB: ANÁLISIS ────────────────────────────────────────────────────────
async function cargarAnalisisTab() {
  const lista = document.getElementById('analisis-tab-lista');
  if (!lista || !currentUser?.id) return;
  lista.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
  try {
    const res  = await fetch(`${API}/api/mis-analisis?user_id=${currentUser.id}`);
    const data = await res.json();
    const items = data.analisis || [];
    if (!items.length) {
      lista.innerHTML = `<div style="text-align:center;padding:60px 20px;">
        <div style="font-size:40px;margin-bottom:12px;">🤖</div>
        <div style="font-weight:700;color:var(--text2);margin-bottom:6px;">Sin análisis aún</div>
        <div style="font-size:13px;color:var(--muted);">Sigue un proceso para que LicitacionLab analice su pliego automáticamente.</div>
      </div>`; return;
    }
    const etiquetas = { completado:'Completado', procesando:'Analizando…', pendiente_analisis:'En cola', error:'Error' };
    const badgesCls = { completado:'completado', procesando:'procesando', pendiente_analisis:'pendiente_analisis', error:'error' };

    lista.innerHTML = items.map(a => {
      const fecha = a.actualizado_en ? new Date(a.actualizado_en).toLocaleDateString('es-DO',{day:'2-digit',month:'short',year:'numeric'}) : '—';
      const monto = a.monto_estimado ? (Number(a.monto_estimado)>=1e6 ? `RD$${(Number(a.monto_estimado)/1e6).toFixed(1)}M` : `RD$${(Number(a.monto_estimado)/1e3).toFixed(0)}K`) : '';
      const estado = a.estado || 'pendiente_analisis';
      // Score desde evaluacion_competitividad (objeto JSON)
      let score = null;
      try {
        const ev = a.evaluacion_competitividad || {};
        const nivel = (ev.nivel_dificultad || '').toLowerCase();
        const nivMap = { baja: 75, media: 50, alta: 25 };
        score = nivMap[nivel] ?? null;
      } catch(e) {}
      const scoreCls = score >= 65 ? 'baja' : score >= 40 ? 'media' : 'alta';
      const scoreColor = score >= 65 ? 'var(--green)' : score >= 40 ? 'var(--amber)' : 'var(--red)';

      return `<div class="ll-card ll-card--hover" style="margin-bottom:8px;cursor:pointer;" onclick="verDetalle('${a.proceso_id || a.proceso_codigo}')">
        <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px;">
          <div style="flex:1;min-width:0;">
            <div style="font-size:14px;font-weight:700;color:var(--text);margin-bottom:4px;line-height:1.3;">${sanitize(a.titulo||'Sin título')}</div>
            <div style="font-size:12px;color:var(--muted);">${sanitize(a.unidad_compra||'')}${monto?' · '+monto:''}</div>
          </div>
          <div style="flex-shrink:0;text-align:right;min-width:80px;">
            ${score != null ? `<div style="font-size:20px;font-weight:800;color:${scoreColor};line-height:1;">⚡ ${score}</div>` : ''}
            <span class="historial-card-estado ${badgesCls[estado]||estado}" style="display:inline-block;margin-top:4px;">${etiquetas[estado]||estado}</span>
            <div style="font-size:11px;color:var(--muted);margin-top:3px;">${fecha}</div>
          </div>
        </div>
      </div>`;
    }).join('');

    const badge = document.getElementById('dsb-badge-analisis');
    if (badge) badge.textContent = items.filter(a=>a.estado==='completado').length || '';
  } catch(e) {
    lista.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);">Error cargando análisis.</div>';
  }
}

// ── TAB: REPORTES ────────────────────────────────────────────────────────
async function cargarReportes() {
  const cont = document.getElementById('reportes-contenido');
  if (!cont || !currentUser?.id) return;
  cont.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
  try {
    const [resSeg, resAnal] = await Promise.all([
      fetch(`${API}/api/seguimiento?user_id=${currentUser.id}`).catch(() => null),
      fetch(`${API}/api/mis-analisis?user_id=${currentUser.id}`)
    ]);
    const anal  = (await resAnal.json()).analisis || [];
    // Usar mis-analisis para contar procesos si el endpoint de seguimiento no existe
    const completados = anal.filter(a => a.estado === 'completado');
    const enCola = anal.filter(a => ['procesando','pendiente_analisis'].includes(a.estado));
    const conError = anal.filter(a => a.estado === 'error');

    // Distribución por institución
    const instMap = {};
    anal.forEach(a => {
      const inst = (a.unidad_compra || 'Otras').split(' ').slice(0,3).join(' ');
      instMap[inst] = (instMap[inst] || 0) + 1;
    });
    const topInst = Object.entries(instMap).sort((a,b)=>b[1]-a[1]).slice(0,5);

    cont.innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:20px;">
        <div class="ll-card" style="text-align:center;padding:24px 16px;">
          <div style="font-size:36px;font-weight:800;color:var(--green);line-height:1;">${anal.length}</div>
          <div style="font-size:12px;color:var(--muted);margin-top:6px;">Análisis totales</div>
        </div>
        <div class="ll-card" style="text-align:center;padding:24px 16px;">
          <div style="font-size:36px;font-weight:800;color:var(--green);line-height:1;">${completados.length}</div>
          <div style="font-size:12px;color:var(--muted);margin-top:6px;">Completados</div>
        </div>
        <div class="ll-card" style="text-align:center;padding:24px 16px;">
          <div style="font-size:36px;font-weight:800;color:var(--amber);line-height:1;">${enCola.length}</div>
          <div style="font-size:12px;color:var(--muted);margin-top:6px;">En proceso</div>
        </div>
      </div>
      ${topInst.length ? `
      <div class="ll-card">
        <div class="ll-card__header"><h2 class="ll-card__title">Análisis por institución</h2></div>
        <div class="ll-card__body" style="padding-top:8px;">
          ${topInst.map(([inst, cnt]) => {
            const pct = Math.round((cnt / anal.length) * 100);
            return `<div style="margin-bottom:12px;">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
                <span style="font-size:13px;color:var(--text2);font-weight:600;">${sanitize(inst)}</span>
                <span style="font-size:12px;font-weight:800;font-family:var(--font-mono);color:var(--text);">${cnt}</span>
              </div>
              <div style="height:4px;background:var(--border);border-radius:2px;">
                <div style="height:100%;width:${pct}%;background:var(--green);border-radius:2px;transition:width .4s;"></div>
              </div>
            </div>`;
          }).join('')}
        </div>
      </div>` : ''}
      ${conError.length ? `
      <div class="ll-card" style="margin-top:12px;border-color:var(--red-dim);">
        <div class="ll-card__header"><h2 class="ll-card__title" style="color:var(--red);">⚠ Análisis con error (${conError.length})</h2></div>
        <div class="ll-card__body" style="padding-top:8px;">
          ${conError.map(a=>`<div style="font-size:13px;color:var(--text2);padding:6px 0;border-bottom:1px solid var(--border);cursor:pointer;" onclick="verDetalle('${a.proceso_id}')">${sanitize(a.titulo||a.proceso_id)}</div>`).join('')}
        </div>
      </div>` : ''}`;
  } catch(e) {
    cont.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);">Error cargando reportes.</div>';
  }
}

// ── TAB: CALENDARIO ──────────────────────────────────────────────────────
async function cargarCalendario() {
  const lista = document.getElementById('calendario-lista');
  if (!lista || !currentUser?.id) return;
  lista.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
  try {
    const res = await fetch(`${API}/api/mis-analisis?user_id=${currentUser.id}`);
    const data = await res.json();
    const analisis = data.analisis || [];

    if (!analisis.length) {
      lista.innerHTML = `<div style="text-align:center;padding:60px 20px;">
        <div style="font-size:40px;margin-bottom:12px;">📅</div>
        <div style="font-weight:700;color:var(--text2);margin-bottom:6px;">Sin fechas aún</div>
        <div style="font-size:13px;color:var(--muted);">Sigue procesos para ver sus fechas aquí.</div>
      </div>`; return;
    }

    // Construir eventos — las fechas ya vienen en mis-analisis
    const eventos = [];
    analisis.forEach(a => {
      const fechas = [
        { fecha: a.fecha_fin_recepcion_ofertas,  label: '📅 Cierre ofertas',  color: 'var(--red)'   },
        { fecha: a.fecha_apertura_ofertas,        label: '🔓 Apertura ofertas',color: 'var(--amber)' },
        { fecha: a.fecha_estimada_adjudicacion,   label: '🏆 Adjudicación',    color: 'var(--purple)'},
      ];
      fechas.forEach(f => {
        if (!f.fecha) return;
        eventos.push({
          fecha:   new Date(f.fecha),
          label:   f.label,
          color:   f.color,
          titulo:  a.titulo || a.proceso_id,
          codigo:  a.proceso_id,
          entidad: a.unidad_compra || '',
          monto:   a.monto_estimado,
        });
      });
    });

    if (!eventos.length) {
      lista.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);">Sin fechas registradas en los procesos seguidos.</div>'; return;
    }

    eventos.sort((a, b) => a.fecha - b.fecha);

    const porMes = {};
    const ahora = new Date();
    eventos.forEach(ev => {
      const key = ev.fecha.toLocaleDateString('es-DO', { month:'long', year:'numeric' });
      if (!porMes[key]) porMes[key] = [];
      porMes[key].push(ev);
    });

    lista.innerHTML = Object.entries(porMes).map(([mes, evs]) => {
      const esPasado = evs.every(e => e.fecha < ahora);
      return `<div style="margin-bottom:28px;${esPasado?'opacity:.5':''}">
        <div style="font-size:11px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);padding:0 0 10px;display:flex;align-items:center;gap:8px;">
          ${mes}
          ${esPasado ? '<span style="font-size:9px;background:var(--border);padding:2px 6px;border-radius:3px;">Pasado</span>' : ''}
        </div>
        ${evs.map(ev => {
          const cd = calcularCountdown(ev.fecha.toISOString());
          const esProximo = !esPasado && cd.clase !== 'vencido';
          const diaNum = ev.fecha.getDate();
          const diaNom = ev.fecha.toLocaleDateString('es-DO', { weekday:'short' });
          const hora   = ev.fecha.toLocaleTimeString('es-DO', { hour:'2-digit', minute:'2-digit' });
          const montoFmt = ev.monto ? (Number(ev.monto)>=1e6 ? `RD$${(Number(ev.monto)/1e6).toFixed(1)}M` : `RD$${(Number(ev.monto)/1e3).toFixed(0)}K`) : '';
          return `<div style="display:flex;gap:14px;margin-bottom:8px;cursor:pointer;" onclick="verDetalle('${ev.codigo}')">
            <div style="flex-shrink:0;width:44px;text-align:center;padding:8px 0;">
              <div style="font-size:22px;font-weight:800;line-height:1;color:${esProximo?ev.color:'var(--muted)'};">${diaNum}</div>
              <div style="font-size:10px;text-transform:uppercase;color:var(--muted);letter-spacing:.04em;">${diaNom}</div>
            </div>
            <div style="width:2px;background:${esProximo?ev.color:'var(--border)'};border-radius:1px;flex-shrink:0;margin:8px 0;"></div>
            <div class="ll-card" style="flex:1;padding:12px 14px;margin-bottom:0;border-left:3px solid ${esProximo?ev.color:'var(--border)'};">
              <div style="font-size:10px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;color:${esProximo?ev.color:'var(--muted)'};margin-bottom:4px;">${ev.label} · ${hora}</div>
              <div style="font-size:13px;font-weight:700;color:var(--text);line-height:1.3;margin-bottom:3px;">${sanitize(ev.titulo)}</div>
              <div style="font-size:11px;color:var(--muted);">${sanitize(ev.entidad)}${montoFmt?' · '+montoFmt:''}</div>
              ${esProximo && cd.texto ? `<div style="font-size:11px;font-weight:700;color:${ev.color};margin-top:5px;">⏱ ${cd.texto}</div>` : ''}
            </div>
          </div>`;
        }).join('')}
      </div>`;
    }).join('');

  } catch(e) {
    lista.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);">Error cargando calendario.</div>';
  }
}
async function checkPushStatus() {
  if (!('serviceWorker' in navigator)) return;
  const reg = await navigator.serviceWorker.getRegistration();
  if (!reg) return;
  const sub    = await reg.pushManager.getSubscription();
  const toggle = document.getElementById('toggle-push');
  const status = document.getElementById('push-status');
  const icono  = document.getElementById('dash-notif-btn');
  if (sub) {
    if (toggle) toggle.classList.add('on');
    pushSubscription = sub;
    if (status) status.style.display = 'flex';
    if (icono)  icono.classList.add('activa');
    // Corregir user_id si quedó guardado como 'user' en vez del UUID real
    if (currentUser?.id) {
      fetch(`${API}/api/notificaciones/actualizar-userid`, {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ endpoint: sub.endpoint, user_id: currentUser.id })
      }).catch(() => {});
    }
  } else {
    if (toggle) toggle.classList.remove('on');
    if (status) status.style.display = 'none';
    if (icono)  icono.classList.remove('activa');
  }
}

async function togglePushDesdeIcono() {
  await togglePush();
  // Sincronizar el estado visual del icono tras el toggle
  const sub   = pushSubscription;
  const icono = document.getElementById('dash-notif-btn');
  if (!icono) return;
  if (sub) {
    icono.classList.add('activa');
    mostrarToast('Notificaciones activadas');
  } else {
    icono.classList.remove('activa');
    mostrarToast('Notificaciones desactivadas');
  }
}

async function togglePush() {
  const toggle = document.getElementById('toggle-push');
  const status = document.getElementById('push-status');
  const icono  = document.getElementById('dash-notif-btn');
  if (toggle.classList.contains('on')) {
    if (pushSubscription) {
      await pushSubscription.unsubscribe();
      await fetch(`${API}/api/notificaciones/desuscribirse`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({endpoint: pushSubscription.endpoint})
      });
      pushSubscription = null;
    }
    toggle.classList.remove('on');
    status.style.display = 'none';
    if (icono) icono.classList.remove('activa');
  } else {
    try {
      const keyRes    = await fetch(`${API}/api/vapid-public-key`);
      const {publicKey} = await keyRes.json();
      const reg       = await navigator.serviceWorker.register('/sw.js');
      const permission = await Notification.requestPermission();
      if (permission !== 'granted') return;
      pushSubscription = await reg.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(publicKey)
      });
      const userId = currentUser?.id;
      if (!userId) { console.error('Push: no hay user_id'); return; }
      await fetch(`${API}/api/notificaciones/suscribirse`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({user_id: userId, subscription: pushSubscription.toJSON(), rubros:[]})
      });
      toggle.classList.add('on');
      status.style.display = 'flex';
      if (icono) icono.classList.add('activa');

      // Gancho: si es freemium, invitar a PRO tras activar notificaciones
      esUsuarioPro().then(pro => {
        if (!pro) setTimeout(() => mostrarModalUpgrade('notificaciones'), 900);
      });
    } catch(e) {
      console.error('Push error:', e);
    }
  }
}

function urlBase64ToUint8Array(b64) {
  const pad = '='.repeat((4-b64.length%4)%4);
  const base64 = (b64+pad).replace(/-/g,'+').replace(/_/g,'/');
  const raw = atob(base64);
  return new Uint8Array([...raw].map(c=>c.charCodeAt(0)));
}

// ── SWIPE PARA VOLVER ──
let touchStartX = 0, touchStartY = 0;
document.addEventListener('touchstart', e=>{
  touchStartX = e.touches[0].clientX;
  touchStartY = e.touches[0].clientY;
}, {passive:true});
document.addEventListener('touchend', e=>{
  const dx = e.changedTouches[0].clientX - touchStartX;
  const dy = Math.abs(e.changedTouches[0].clientY - touchStartY);
  const detalleVisible = document.getElementById('tab-detalle').style.display !== 'none';
  if (detalleVisible && dx > 80 && dy < 60) mostrarDashboard();
}, {passive:true});

// ── SWIPE EN CARDS — tipo WhatsApp ──
(function initCardSwipe() {
  let swipeCard = null, swipeStartX = 0, swipeStartY = 0;
  let swipeMoved = false, swipeOpen = null;
  const THRESHOLD = 60;

  function getWrap(el) { return el.closest('.proceso-swipe-wrap'); }
  function getCard(wrap) { return wrap?.querySelector('.proceso-card'); }
  function getAction(wrap) { return wrap?.querySelector('.proceso-swipe-action'); }
  function resetCard(wrap, animate) {
    const card = getCard(wrap);
    const action = getAction(wrap);
    if (!card) return;
    card.style.transition = animate ? 'transform .25s cubic-bezier(.4,0,.2,1)' : 'none';
    card.style.transform = 'translateX(0)';
    if (action) action.classList.remove('visible');
    wrap.dataset.swiped = '';
  }
  function openCard(wrap) {
    const card = getCard(wrap);
    const action = getAction(wrap);
    if (!card) return;
    card.style.transition = 'transform .25s cubic-bezier(.4,0,.2,1)';
    card.style.transform = 'translateX(-90px)';
    if (action) action.classList.add('visible');
    wrap.dataset.swiped = '1';
    swipeOpen = wrap;
  }

  document.addEventListener('touchstart', e => {
    const wrap = getWrap(e.target);
    if (!wrap) { if (swipeOpen) { resetCard(swipeOpen, true); swipeOpen = null; } return; }
    // Si se toca el botón de acción abierto
    if (e.target.closest('.proceso-swipe-action')) return;
    // Cerrar cualquier otro abierto
    if (swipeOpen && swipeOpen !== wrap) { resetCard(swipeOpen, true); swipeOpen = null; }
    swipeCard = wrap;
    swipeStartX = e.touches[0].clientX;
    swipeStartY = e.touches[0].clientY;
    swipeMoved = false;
  }, { passive: true });

  document.addEventListener('touchmove', e => {
    if (!swipeCard) return;
    const dx = e.touches[0].clientX - swipeStartX;
    const dy = Math.abs(e.touches[0].clientY - swipeStartY);
    if (!swipeMoved && dy > 10) { swipeCard = null; return; }
    if (Math.abs(dx) > 6) swipeMoved = true;
    if (!swipeMoved) return;
    const card = getCard(swipeCard);
    const action = getAction(swipeCard);
    if (!card) return;
    const already = swipeCard.dataset.swiped === '1';
    const offset = already ? -90 + dx : dx;
    const clamped = Math.max(-90, Math.min(0, offset));
    card.style.transition = 'none';
    card.style.transform = `translateX(${clamped}px)`;
    if (action) action.classList.toggle('visible', clamped < -20);
  }, { passive: true });

  document.addEventListener('touchend', e => {
    if (!swipeCard) return;
    const dx = e.changedTouches[0].clientX - swipeStartX;
    const wrap = swipeCard;
    swipeCard = null;
    if (!swipeMoved) {
      // Fue un tap — si está abierto cerramos, si no, abrimos detalle
      if (wrap.dataset.swiped === '1') { resetCard(wrap, true); swipeOpen = null; }
      else {
        const codigo = wrap.dataset.codigo;
        if (codigo) verDetalle(codigo);
      }
      return;
    }
    const already = wrap.dataset.swiped === '1';
    const totalDx = already ? -90 + dx : dx;
    if (totalDx < -THRESHOLD) { openCard(wrap); }
    else { resetCard(wrap, true); swipeOpen = null; }
  }, { passive: true });

  // Click en botón de acción
  document.addEventListener('click', e => {
    const btn = e.target.closest('.proceso-swipe-action');
    if (!btn) return;
    const wrap = btn.closest('.proceso-swipe-wrap');
    const codigo = wrap?.dataset.codigo;
    if (!codigo) return;
    resetCard(wrap, true);
    swipeOpen = null;
    quieroGanarProceso(codigo);
  });
})();

function quieroGanarProceso(codigo) {
  const wrap = document.querySelector(`.proceso-swipe-wrap[data-codigo="${codigo}"]`);
  const titulo = wrap?.dataset.titulo || codigo;
  const WS_NUMBER = '18098154457';
  const msg = encodeURIComponent(`Hola, estoy interesado en participar en el proceso ${codigo} — ${titulo}. ¿Pueden ayudarme?`);
  window.open(`https://wa.me/${WS_NUMBER}?text=${msg}`, '_blank');
}

function toggleTimelineFechas(codigo) {
  const wrap = document.getElementById(`tl-wrap-${codigo}`);
  const btn  = document.getElementById(`toggle-fechas-${codigo}`);
  if (!wrap || !btn) return;
  const abierto = wrap.classList.toggle('abierto');
  btn.classList.toggle('abierto', abierto);
}

// ── PULL TO REFRESH ──
(function initPullToRefresh() {
  let ptrStartY = 0;
  let ptrActive = false;
  const THRESHOLD = 65;
  // El scroller es #tab-dashboard

  document.addEventListener('touchstart', e => {
    const dash = document.getElementById('tab-dashboard');
    if (!dash || dash.style.display === 'none') return;
    if (dash.scrollTop > 2) return;
    ptrStartY = e.touches[0].clientY;
    ptrActive = true;
  }, { passive: true });

  document.addEventListener('touchmove', e => {
    if (!ptrActive) return;
    const dy = e.touches[0].clientY - ptrStartY;
    const indicator = document.getElementById('ptr-indicator');
    if (!indicator) return;
    if (dy > 15) indicator.classList.add('visible');
    else indicator.classList.remove('visible');
  }, { passive: true });

  document.addEventListener('touchend', e => {
    if (!ptrActive) return;
    ptrActive = false;
    const dy = e.changedTouches[0].clientY - ptrStartY;
    const indicator = document.getElementById('ptr-indicator');
    if (!indicator) return;
    if (dy >= THRESHOLD) {
      cargarStats();
      cargarProcesos(1);
    }
    setTimeout(() => indicator.classList.remove('visible'), 500);
  }, { passive: true });
})();

// ── CERRAR DROPDOWNS AL TOCAR FUERA ──
document.addEventListener('click', e=>{
  if (!e.target.closest('#filtro-unspsc-text') && !e.target.closest('#unspsc-dropdown'))
    document.getElementById('unspsc-dropdown').style.display='none';
  if (!e.target.closest('#perfil-unspsc-text') && !e.target.closest('#perfil-unspsc-dropdown')) {
    const dd=document.getElementById('perfil-unspsc-dropdown'); if(dd) dd.style.display='none';
  }
});

// ── INIT ──
async function _tryRefreshToken() {
  const refreshToken = localStorage.getItem('sb_refresh_token');
  if (!refreshToken) return false;
  try {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=refresh_token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': SUPABASE_ANON_KEY },
      body: JSON.stringify({ refresh_token: refreshToken })
    });
    const data = await res.json();
    if (data.access_token) {
      localStorage.setItem('sb_token', data.access_token);
      if (data.refresh_token) localStorage.setItem('sb_refresh_token', data.refresh_token);
      if (data.user) localStorage.setItem('sb_user', JSON.stringify(data.user));
      currentUser = data.user || currentUser;
      _syncSesionCookies();
      return true;
    }
  } catch(e) { console.warn('Refresh falló (sin conexión?):', e); }
  return false;
}

// ── CALLBACK OAUTH (Google) ──
// Tras elegir la cuenta, Supabase regresa a la app con
//   #access_token=...&refresh_token=...&expires_in=...&token_type=bearer
// Esa sesión hay que guardarla ANTES de decidir si se muestra login o app.
// Sin esto, el usuario vuelve a la pantalla de login aunque Google lo autenticó.
async function _procesarCallbackOAuth() {
  const hash = window.location.hash || '';
  if (hash.indexOf('access_token') === -1 && hash.indexOf('error') === -1) return false;
  const p  = new URLSearchParams(hash.replace(/^#/, ''));
  const qs = new URLSearchParams(window.location.search);
  // Recuperación de contraseña: la maneja detectarRecovery()
  if (p.get('type') === 'recovery' || qs.get('recovery') === '1') return false;

  const limpiarUrl = () => {
    try { history.replaceState({}, '', window.location.pathname + window.location.search); } catch (e) {}
  };

  const err = p.get('error_description') || p.get('error');
  if (err) {
    limpiarUrl();
    console.warn('OAuth error:', err);
    setTimeout(() => { try { showMsg('No se pudo entrar con Google. Intenta de nuevo.', 'error'); } catch (e) {} }, 300);
    return false;
  }

  const accessToken  = p.get('access_token');
  const refreshToken = p.get('refresh_token');
  if (!accessToken) return false;

  localStorage.setItem('sb_token', accessToken);
  if (refreshToken) localStorage.setItem('sb_refresh_token', refreshToken);

  // Usuario completo (id, email, user_metadata con nombre/avatar de Google)
  let user = null;
  try {
    const r = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
      headers: { 'apikey': SUPABASE_ANON_KEY, 'Authorization': `Bearer ${accessToken}` }
    });
    if (r.ok) user = await r.json();
  } catch (e) { console.warn('No se pudo leer /auth/v1/user:', e); }
  if (!user || !user.id) {
    try {
      const pl = _jwtPayload(accessToken);
      user = { id: pl.sub, email: pl.email, user_metadata: pl.user_metadata || {} };
    } catch (e) { console.error('JWT de Google ilegible:', e); }
  }
  limpiarUrl();
  if (!user || !user.id) return false;

  localStorage.setItem('sb_user', JSON.stringify(user));
  currentUser = user;
  _syncSesionCookies();
  console.log('✅ Login con Google OK, user:', user.id);
  try { vincularReferido(); } catch (e) {}
  try { cargarPerfilEmpresa(); } catch (e) {}
  return true;
}

(async function iniciarApp() {
  console.log('%c🟢 LicitacionLab BUILD v13 · orden-real+nav-historial+ux-pack (ptr, skeletons, filtros persistentes)', 'color:#16a34a;font-weight:bold');
  await _procesarCallbackOAuth();   // ← guarda la sesión si venimos de Google
  _hydratarSesionDesdeCookies();
  const savedToken   = localStorage.getItem('sb_token');
  const savedUser    = localStorage.getItem('sb_user');
  // Acepta si hay al menos el usuario guardado (token puede estar vencido)
  if (!savedUser) {
    // Sin sesión → mostrar showcase en vivo + asegurar que la clase de sesión esté limpia
    document.documentElement.classList.remove('ll-has-session');
    startShowcaseRefresh();
    return;
  }

  // Restaurar currentUser desde localStorage primero (siempre)
  try {
    currentUser = JSON.parse(savedUser);
    if (!currentUser?.id && savedToken) {
      const payload = _jwtPayload(savedToken);
      currentUser = { ...currentUser, id: payload.sub };
    }
  } catch(e) {}
  if (!currentUser?.id) return; // Sin id no hay sesión válida

  // Verificar si el token está vencido e intentar refrescarlo
  let tokenVencido = true;
  try {
    if (savedToken) {
      const payload = _jwtPayload(savedToken);
      tokenVencido = payload.exp && (payload.exp * 1000) < Date.now();
    }
  } catch(e) {}

  if (tokenVencido) {
    const ok = await _tryRefreshToken();
    if (!ok) {
      // Refresh falló pero hay sb_user — mostrar app de todas formas
      console.warn('⚠️ Token vencido y refresh falló — mostrando app con sesión guardada');
    }
  }

  console.log('✅ Sesión restaurada, user:', currentUser?.id);
  stopShowcaseRefresh();
  showApp();
  cargarSeguimiento();
})();

// ── VISIBILITYCHANGE: refrescar token cuando Chrome Android reactiva el tab ──
document.addEventListener('visibilitychange', async () => {
  if (document.visibilityState !== 'visible') return;
  if (!currentUser?.id) return;
  _syncSesionCookies();
  const savedToken = localStorage.getItem('sb_token');
  let vencido = true;
  try {
    if (savedToken) {
      const payload = _jwtPayload(savedToken);
      vencido = payload.exp && (payload.exp * 1000) < Date.now();
    }
  } catch(e) {}
  if (vencido) {
    console.log('🔄 Tab visible de nuevo — refrescando token...');
    await _tryRefreshToken();
  }
});

// ── ABRIR PROCESO DESDE NOTIFICACIÓN ──
// _abrirProcesoDeNotificacion() se llama al terminar cargarProcesos().
// Lee el parámetro ?proceso= de la URL en ese momento — cuando los datos ya están listos.
// También funciona si la URL tiene el parámetro al momento de cargar (app cerrada).
let _procesoNotifPendiente = null;

(function capturarParamNotificacion() {
  const params = new URLSearchParams(window.location.search);
  const codigo = params.get('proceso');
  if (codigo) {
    _procesoNotifPendiente = codigo;
    // Limpiar la URL para que no se repita si el usuario recarga
    window.history.replaceState({}, '', window.location.pathname);
  }
})();

async function _abrirProcesoDeNotificacion() {
  if (!_procesoNotifPendiente) return;
  const codigo = _procesoNotifPendiente;
  _procesoNotifPendiente = null;  // limpiar para no repetir
  await verDetalle(codigo);
}

// ── RECUPERAR CONTRASEÑA ──
// Cuando el usuario hace click en el link del correo, Supabase lo regresa a la app.
// Según la configuración, el token puede llegar de 3 formas distintas:
//   A) Hash:  #access_token=...&type=recovery         (implicit flow)
//   B) Query: ?recovery=1  + hash con access_token     (nuestro marcador)
//   C) Query: ?code=XXXXX                              (PKCE flow nuevo)
// Detectamos las 3 ANTES de que la app trate esto como login normal.
let _recoveryToken = null;

function _entrarModoRecovery() {
  // Borrar cualquier rastro de sesión vieja
  try { localStorage.removeItem('sb_user'); localStorage.removeItem('sb_token'); } catch(e) {}
  try {
    document.cookie = 'sb_user=; Max-Age=0; path=/';
    document.cookie = 'sb_token=; Max-Age=0; path=/';
  } catch(e) {}
  document.documentElement.classList.remove('ll-has-session');
  const _mostrar = () => {
    try {
      const auth = document.getElementById('screen-auth');
      const app  = document.getElementById('screen-app');
      if (app)  app.classList.remove('active');
      if (auth) auth.classList.add('active');
      if (typeof switchTab === 'function') switchTab('newpassword');
    } catch(e) { console.error('recovery UI error:', e); }
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _mostrar);
  } else {
    _mostrar();
  }
}

(async function detectarRecovery() {
  try {
    const hash   = window.location.hash || '';
    const search = window.location.search || '';
    const hashParams   = new URLSearchParams(hash.replace(/^#/, ''));
    const queryParams  = new URLSearchParams(search);

    const hashType  = hashParams.get('type');
    const hashToken = hashParams.get('access_token');
    const code      = queryParams.get('code');
    const marcador  = queryParams.get('recovery');
    const tokenHash = queryParams.get('token_hash') || hashParams.get('token_hash');
    const typeParam = queryParams.get('type') || hashType;
    const errDesc   = hashParams.get('error_description') || queryParams.get('error_description');

    // Si el link expiró, Supabase manda error en vez de token
    if (errDesc && (marcador === '1' || hashType === 'recovery')) {
      window.history.replaceState({}, '', window.location.pathname);
      _entrarModoRecovery();
      setTimeout(() => {
        const out = document.getElementById('newpass-result');
        if (out) {
          out.style.display = 'block';
          out.style.background = 'var(--red-dim)';
          out.style.color = 'var(--red)';
          out.textContent = 'El enlace expiró o ya se usó. Solicita uno nuevo.';
        }
      }, 300);
      return;
    }

    // CASO 0 (PREFERIDO): token_hash + type=recovery.
    // Es el patrón agnóstico al flujo (funciona en PKCE o implicit) porque
    // /verify devuelve el access_token directo, sin necesidad de code_verifier.
    // La plantilla del correo debe apuntar a:
    //   {{ .SiteURL }}/?recovery=1&token_hash={{ .TokenHash }}&type=recovery
    if (tokenHash && typeParam === 'recovery') {
      try {
        const res = await fetch(`${SUPABASE_URL}/auth/v1/verify`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'apikey': SUPABASE_ANON_KEY },
          body: JSON.stringify({ type: 'recovery', token_hash: tokenHash })
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data.access_token) {
          _recoveryToken = data.access_token;
        }
      } catch(e) { console.error('verify token_hash error:', e); }
      window.history.replaceState({}, '', window.location.pathname);
      _entrarModoRecovery();
      if (!_recoveryToken) {
        setTimeout(() => {
          const out = document.getElementById('newpass-result');
          if (out) {
            out.style.display = 'block';
            out.style.background = 'var(--red-dim)';
            out.style.color = 'var(--red)';
            out.textContent = 'El enlace expiró o ya se usó. Solicita uno nuevo.';
          }
        }, 300);
      }
      return;
    }

    // CASO A y B: token viene en el hash
    if (hashToken && (hashType === 'recovery' || marcador === '1')) {
      _recoveryToken = hashToken;
      window.history.replaceState({}, '', window.location.pathname);
      _entrarModoRecovery();
      return;
    }

    // CASO C: flujo PKCE — viene ?code=XXX y hay que intercambiarlo por un token
    if (code && marcador === '1') {
      try {
        const res = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=pkce`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'apikey': SUPABASE_ANON_KEY },
          body: JSON.stringify({ auth_code: code })
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data.access_token) {
          _recoveryToken = data.access_token;
        }
      } catch(e) { console.error('pkce exchange error:', e); }
      window.history.replaceState({}, '', window.location.pathname);
      _entrarModoRecovery();
      if (!_recoveryToken) {
        setTimeout(() => {
          const out = document.getElementById('newpass-result');
          if (out) {
            out.style.display = 'block';
            out.style.background = 'var(--red-dim)';
            out.style.color = 'var(--red)';
            out.textContent = 'No se pudo validar el enlace. Solicita uno nuevo.';
          }
        }, 300);
      }
      return;
    }

    // CASO C alternativo: ?code= sin marcador, pero sin sesión activa.
    // Puede ser un recovery con Site URL sin parámetro. Lo tratamos como recovery
    // solo si NO hay sesión previa (para no interferir con OAuth de Google).
    if (code && !marcador) {
      const tieneSesion = (() => {
        try { return !!localStorage.getItem('sb_token'); } catch(e) { return false; }
      })();
      if (!tieneSesion) {
        try {
          const res = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=pkce`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'apikey': SUPABASE_ANON_KEY },
            body: JSON.stringify({ auth_code: code })
          });
          const data = await res.json().catch(() => ({}));
          // Solo entrar a modo recovery si el token es de tipo recovery
          if (res.ok && data.access_token) {
            try {
              const payload = _jwtPayload(data.access_token);
              // amr/aal no siempre indican recovery; en duda, mostramos el form igual
            } catch(e) {}
            _recoveryToken = data.access_token;
            window.history.replaceState({}, '', window.location.pathname);
            _entrarModoRecovery();
          }
        } catch(e) { /* si falla, seguir flujo normal */ }
      }
    }
  } catch(e) { console.error('detectarRecovery error:', e); }
})();

async function guardarNuevaPassword() {
  const p1  = document.getElementById('newpass-1').value;
  const p2  = document.getElementById('newpass-2').value;
  const out = document.getElementById('newpass-result');
  const btn = document.getElementById('btn-newpass');

  const showErr = (msg) => {
    out.style.display = 'block';
    out.style.background = 'var(--red-dim)';
    out.style.color = 'var(--red)';
    out.textContent = msg;
  };

  if (!p1 || p1.length < 6) { showErr('La contraseña debe tener al menos 6 caracteres.'); return; }
  if (p1 !== p2)            { showErr('Las contraseñas no coinciden.'); return; }
  if (!_recoveryToken)      { showErr('El enlace expiró. Solicita uno nuevo.'); return; }

  btn.disabled = true;
  btn.textContent = 'Guardando...';

  try {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${_recoveryToken}`,
      },
      body: JSON.stringify({ password: p1 })
    });

    btn.disabled = false;
    btn.textContent = 'Guardar contraseña';

    if (res.ok) {
      _recoveryToken = null;
      out.style.display = 'block';
      out.style.background = 'var(--green-dim)';
      out.style.color = 'var(--green)';
      out.textContent = '✅ Contraseña actualizada. Ya puedes iniciar sesión.';
      document.getElementById('newpass-1').value = '';
      document.getElementById('newpass-2').value = '';
      // Volver al login tras 2 segundos
      setTimeout(() => { if (typeof switchTab === 'function') switchTab('login'); }, 2000);
    } else {
      const data = await res.json().catch(() => ({}));
      const msg = (data?.msg || data?.error_description || '').toLowerCase();
      if (msg.includes('expired') || msg.includes('invalid')) {
        showErr('El enlace expiró. Solicita uno nuevo desde "Olvidé mi contraseña".');
      } else if (msg.includes('same') || msg.includes('different from')) {
        showErr('La nueva contraseña no puede ser igual a la anterior.');
      } else {
        showErr('No se pudo actualizar. Intenta de nuevo.');
      }
    }
  } catch(e) {
    btn.disabled = false;
    btn.textContent = 'Guardar contraseña';
    showErr('Error de conexión. Intenta de nuevo.');
  }
}


// ── SUPABASE REST (tablas) ──
async function sbRest(path, method = 'GET', body = null, extraHeaders = {}) {
  let token = localStorage.getItem('sb_token');

  const doRequest = async (tok) => {
    const headers = {
      'Content-Type': 'application/json',
      'apikey': SUPABASE_ANON_KEY,
      'Prefer': 'return=representation',
    };
    if (tok) headers['Authorization'] = `Bearer ${tok}`;
    Object.assign(headers, extraHeaders);
    return fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
      method, headers,
      body: body ? JSON.stringify(body) : undefined
    });
  };

  let res = await doRequest(token);

  // Si 401, intentar refrescar el token una vez
  if (res.status === 401) {
    console.warn('401 en sbRest, intentando refresh de token...');
    const refreshToken = localStorage.getItem('sb_refresh_token');
    if (refreshToken) {
      try {
        const refreshRes = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=refresh_token`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'apikey': SUPABASE_ANON_KEY },
          body: JSON.stringify({ refresh_token: refreshToken })
        });
        const refreshData = await refreshRes.json();
        if (refreshData.access_token) {
          token = refreshData.access_token;
          localStorage.setItem('sb_token', token);
          if (refreshData.refresh_token) {
            localStorage.setItem('sb_refresh_token', refreshData.refresh_token);
          }
          _syncSesionCookies();
          console.log('✅ Token refrescado OK');
          res = await doRequest(token);
        }
      } catch(e) {
        console.error('❌ Error refrescando token:', e);
      }
    } else {
      console.warn('⚠️ Sin refresh_token — el request falló con 401 pero mantenemos sesión');
      // NO borramos currentUser ni redirigimos: el usuario sigue logueado visualmente.
      // Si el token está verdaderamente muerto, _tryRefreshToken() o visibilitychange lo resolverá.
      // Solo forzamos logout si el propio usuario lo pide.
    }
  }

  if (res.status === 204) return [];
  const data = await res.json();
  if (data?.code === 'PGRST116' || res.status === 401) {
    console.error('❌ sbRest error:', data);
  }
  return data;
}

// ═══════════════════════════════════════════════════════════════
// SEGUIMIENTO DE PROCESOS
// ═══════════════════════════════════════════════════════════════

let seguimientoData = [];
let segFiltroActivo = 'todos';
let segModalCodigo  = null;

const ESTADOS_CONFIG = {
  'analizando': { label: 'Analizando',  icon: '', cls: 'analizando' },
  'preparando' : { label: 'Preparando', icon: '', cls: 'preparando' },
  'presentado' : { label: 'Presentado', icon: '', cls: 'presentado' },
  'adjudicado' : { label: 'Adjudicado', icon: '', cls: 'adjudicado' },
  'no-adj'     : { label: 'No adj.',    icon: '', cls: 'no-adj' },
};

function extraerFechasClaves(p) {
  const fechas = [];
  const ahora = new Date();
  const agregar = (label, valor, destacada = false, sublabel = null) => {
    if (!valor || valor === 'N/A') return;
    // Soportar formato DD/MM/YYYY HH:MM (del análisis) e ISO (de la BD)
    let dt;
    if (typeof valor === 'string' && /^\d{2}\/\d{2}\/\d{4}/.test(valor)) {
      const [fecha, hora] = valor.split(' ');
      const [d, m, y] = fecha.split('/');
      dt = hora ? new Date(`${y}-${m}-${d}T${hora}`) : new Date(`${y}-${m}-${d}`);
    } else {
      dt = new Date(valor);
    }
    if (isNaN(dt.getTime())) return;
    const dias = Math.ceil((dt - ahora) / 86400000);
    let estado = 'normal';
    if (dias < 0) estado = 'vencida';
    else if (dias === 0) estado = 'hoy';
    else if (dias <= 2) estado = 'critico';
    else if (dias <= 7) estado = 'proximo';
    fechas.push({ label, sublabel, valor: dt, dias, estado, destacada });
  };

  // Fechas de la tabla procesos (scraper)
  agregar('Publicación',             p.fecha_publicacion,           false, 'Inicio del proceso');
  agregar('Presentación de ofertas', p.fecha_fin_recepcion_ofertas, true,  'Fecha límite de entrega');
  agregar('Apertura de sobres',      p.fecha_apertura_ofertas,      false, 'Evaluación de ofertas');
  agregar('Adjudicación estimada',   p.fecha_estimada_adjudicacion, false, 'Resultado esperado');
  agregar('Cierre de preguntas',     p.fecha_enmienda,              false, 'Último día para preguntas');
  agregar('Suscripción contrato',    p.fecha_suscripcion,           false, 'Firma del contrato');

  // Plazos del análisis (más completos — vienen del pliego)
  const ia = p._plazos_ia || {};
  agregar('Visita al sitio',         ia.visita_sitio,               false, 'Reconocimiento del lugar');
  agregar('Cierre de consultas',     ia.consultas,                  false, 'Último día para preguntas');
  agregar('Presentación de ofertas', ia.presentacion_ofertas,       true,  'Fecha límite de entrega');
  agregar('Apertura de sobres',      ia.apertura_ofertas,           false, 'Evaluación de ofertas');

  // Deduplicar por label — si ya existe una con ese nombre, no agregar duplicado
  const vistos = new Set();
  const unicas = fechas.filter(f => {
    if (vistos.has(f.label)) return false;
    vistos.add(f.label);
    return true;
  });

  return unicas.sort((a, b) => a.valor - b.valor);
}

function urgenciaClase(fechas) {
  const pres = fechas.find(f => f.label === 'Presentación de ofertas');
  if (!pres) return 'normal';
  if (pres.estado === 'vencida' || pres.estado === 'hoy' || pres.estado === 'critico') return 'critico';
  if (pres.estado === 'proximo') return 'proximo';
  return 'normal';
}

function diasRestantesTexto(fechas) {
  const pres = fechas.find(f => f.label === 'Presentación de ofertas');
  if (!pres) return { texto: '—', cls: 'cerrado' };
  if (pres.dias < 0)  return { texto: 'Cerrado', cls: 'cerrado' };
  if (pres.dias === 0) return { texto: '¡Hoy cierra!', cls: 'critico' };
  if (pres.dias === 1) return { texto: '¡Mañana cierra!', cls: 'critico' };
  if (pres.dias <= 3)  return { texto: `${pres.dias} días`, cls: 'critico' };
  if (pres.dias <= 7)  return { texto: `${pres.dias} días`, cls: 'proximo' };
  return { texto: `${pres.dias} días`, cls: 'normal' };
}

function renderFechaRow(f) {
  const iconos = {
    'Publicación':               '·',
    'Preguntas y subsanaciones': '·',
    'Presentación de ofertas':   '·',
    'Apertura de sobres':        '·',
    'Adjudicación estimada':     '·',
    'Suscripción del contrato':  '·',
  };
  const icono = iconos[f.label] || '·';
  const fechaStr = f.valor.toLocaleDateString('es-DO', { day:'2-digit', month:'short', year:'numeric' });
  const horaStr  = f.valor.toLocaleTimeString('es-DO', { hour:'2-digit', minute:'2-digit' });
  const rowCls = f.destacada ? (f.estado === 'vencida' ? 'vencida' : 'destacada') : f.estado;
  const valCls = f.destacada && f.estado !== 'vencida' ? 'destacada' : f.estado;

  let diasLabel = '';
  if (f.estado === 'vencida')       diasLabel = `<div class="seg-fecha-dias">Vencida</div>`;
  else if (f.estado === 'hoy')      diasLabel = `<div class="seg-fecha-dias critico">HOY ${horaStr}</div>`;
  else if (f.estado === 'critico')  diasLabel = `<div class="seg-fecha-dias critico">${f.dias}d</div>`;
  else if (f.estado === 'proximo')  diasLabel = `<div class="seg-fecha-dias proximo">${f.dias}d</div>`;
  else                               diasLabel = `<div class="seg-fecha-dias">${f.dias}d</div>`;

  return `
    <div class="seg-fecha-row ${rowCls}">
      <span class="seg-fecha-icono">${icono}</span>
      <div class="seg-fecha-info">
        <span class="seg-fecha-label">${f.label}</span>
        ${f.sublabel ? `<span class="seg-fecha-sublabel">${f.sublabel}</span>` : ''}
      </div>
      <div class="seg-fecha-right">
        <span class="seg-fecha-valor ${valCls}">${fechaStr}</span>
        ${diasLabel}
      </div>
    </div>`;
}

function renderSegCard(item) {
  const p      = item.proceso || {};
  const estado = item.estado || 'analizando';
  const eCfg   = ESTADOS_CONFIG[estado] || ESTADOS_CONFIG['analizando'];
  const fechas = extraerFechasClaves(p);
  const urg    = urgenciaClase(fechas);
  const dias   = diasRestantesTexto(fechas);
  const monto  = p.monto_estimado
    ? (Number(p.monto_estimado) >= 1_000_000
        ? `RD$${(Number(p.monto_estimado)/1_000_000).toFixed(1)}M`
        : `RD$${(Number(p.monto_estimado)/1_000).toFixed(0)}K`)
    : '';
  const codigo = p.codigo_proceso || item.proceso_codigo;

  // Badge de enmienda si existe
  const enmienda = p._plazos_ia?._enmienda;
  let enmiendaHtml = '';
  if (enmienda) {
    const dias_enm = enmienda.detectada_en
      ? Math.floor((Date.now() - new Date(enmienda.detectada_en)) / 86400000)
      : null;
    const cuandoEnm = dias_enm === 0 ? 'Hoy' : dias_enm === 1 ? 'Ayer' : dias_enm !== null ? `Hace ${dias_enm}d` : '';
    enmiendaHtml = `<div class="seg-enmienda-badge">⚠ Enmienda detectada${cuandoEnm ? ' · '+cuandoEnm : ''}</div>`;
  }

  // Badge de días compacto
  let diasHtml = '';
  if (dias.cls === 'cerrado') {
    diasHtml = `<span class="seg-dias-badge cerrado">Cerrado</span>`;
  } else if (dias.cls === 'critico') {
    diasHtml = `<span class="seg-dias-badge urgente">${dias.texto}</span>`;
  } else if (dias.cls === 'proximo') {
    diasHtml = `<span class="seg-dias-badge proximo">${dias.texto}</span>`;
  } else {
    diasHtml = `<span class="seg-dias-badge normal">${dias.texto}</span>`;
  }

  return `
  <div class="ll-card ll-card--hover" id="segcard-${codigo}" style="margin:0 16px 10px;overflow:hidden;">
    <div class="seg-urgencia-bar ${urg}" style="height:3px;margin:-12px -12px 12px;"></div>
    <div class="ll-card__header">
      <span class="ll-chip ll-chip--${eCfg.cls === 'analizando' ? 'info' : eCfg.cls === 'preparando' ? 'warning' : eCfg.cls === 'presentado' ? 'success' : eCfg.cls === 'adjudicado' ? 'success' : 'error'}">${eCfg.label}</span>
      ${diasHtml}
    </div>
    <h3 class="ll-card__title" onclick="verDetalle('${codigo}')" style="cursor:pointer;margin:8px 0;">${p.titulo || 'Sin título'}</h3>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
      <span class="ll-body-sm" style="color:var(--blue,#3498db);font-weight:600;">${p.unidad_compra || '—'}</span>
      ${monto ? `<span class="ll-num ll-body-sm">${monto}</span>` : ''}
    </div>
    ${enmiendaHtml}
    ${fechas.length > 0 ? `
    <button class="ll-btn ll-btn--ghost" style="width:100%;justify-content:space-between;padding:8px 0;font-size:12px;" id="toggle-fechas-${codigo}" onclick="toggleTimelineFechas('${codigo}')">
      <span>Cronograma</span><span class="seg-toggle-chevron">▼</span>
    </button>
    <div class="seg-timeline-wrap" id="tl-wrap-${codigo}">
      <div class="seg-timeline">
        ${fechas.map(f => {
          const fechaStr = f.valor.toLocaleDateString('es-DO', { day:'2-digit', month:'short', year:'numeric' });
          const horaStr  = f.valor.toLocaleTimeString('es-DO', { hour:'2-digit', minute:'2-digit' });
          let diasTxt = '';
          if (f.estado === 'vencida')      diasTxt = 'Vencida';
          else if (f.estado === 'hoy')     diasTxt = `HOY · ${horaStr}`;
          else if (f.estado === 'critico') diasTxt = `${f.dias} día${f.dias !== 1 ? 's' : ''}`;
          else if (f.estado === 'proximo') diasTxt = `${f.dias} días`;
          else                              diasTxt = `${f.dias} días`;
          return `
          <div class="seg-tl-row">
            <div class="seg-tl-dot ${f.estado}"></div>
            <div class="seg-tl-info">
              <div class="seg-tl-label">${f.label}</div>
              ${f.sublabel ? `<div class="seg-tl-sub">${f.sublabel}</div>` : ''}
            </div>
            <div class="seg-tl-right">
              <div class="seg-tl-fecha ${f.estado}">${fechaStr}</div>
              <div class="seg-tl-dias ${f.estado}">${diasTxt}</div>
            </div>
          </div>`;
        }).join('')}
      </div>
    </div>` : ''}
    <div style="display:flex;gap:8px;margin-top:10px;">
      <button class="ll-btn ll-btn--secondary" style="flex:1;" onclick="verDetalle('${codigo}')">Ver detalle</button>
      <button class="ll-btn ll-btn--ghost" id="pdfbtn-${codigo}" title="Descargar análisis en PDF" onclick="descargarPdfAnalisis('${codigo}')">📄 PDF</button>
      <button class="ll-btn ll-btn--ghost" onclick="abrirModalEstado('${codigo}','${estado}')">Estado</button>
      <button class="ll-btn ll-btn--danger-ghost ll-btn--icon" onclick="quitarSeguimiento('${codigo}')">✕</button>
    </div>
  </div>`;
}

async function descargarPdfAnalisis(codigo) {
  haptic(12);
  const btn = document.getElementById(`pdfbtn-${codigo}`);
  const original = btn ? btn.innerHTML : '';
  if (btn) { btn.innerHTML = '⏳'; btn.disabled = true; }
  try {
    const res = await fetch(`${API}/analisis/${encodeURIComponent(codigo)}/pdf`);
    if (res.status === 404) {
      mostrarToast('El análisis aún no está listo. Ábrelo en "Ver detalle" para generarlo.');
      return;
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const blob = await res.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url;
    a.download = `Analisis_${codigo.replace(/[\/\s]/g, '-')}.pdf`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 2000);
    mostrarToast('Análisis descargado');
  } catch (e) {
    console.error('Error descargando PDF:', e);
    mostrarToast('No se pudo descargar el PDF. Intenta de nuevo.');
  } finally {
    if (btn) { btn.innerHTML = original; btn.disabled = false; }
  }
}

async function cargarSeguimiento() {
  if (!currentUser) {
    // Sesión aún no lista: mostrar estado vacío en vez de dejar la lista en blanco
    seguimientoData = [];
    renderSeguimiento();
    return;
  }
  const lista = document.getElementById('seg-lista');
  if (lista) lista.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
  try {
    console.log('cargarSeguimiento - user_id:', currentUser?.id);
    // 1. Traer registros de seguimiento del usuario
    const segs = await sbRest(
      `seguimiento_procesos?user_id=eq.${currentUser.id}&order=created_at.desc`
    );
    console.log('seguimientos raw:', segs);
    if (!Array.isArray(segs) || segs.length === 0) {
      seguimientoData = [];
      renderSeguimiento();
      return;
    }

    // 2. Traer los procesos correspondientes — EN LOTES de 50, PERO EN PARALELO.
    // Antes se hacían secuencialmente (uno tras otro), lo que con 800+ seguimientos
    // tomaba muchísimo. Ahora se lanzan todos los lotes a la vez con Promise.all.
    const todosCodigos = segs.map(s => s.proceso_codigo);
    const SELECT_PROC = 'select=codigo_proceso,titulo,unidad_compra,monto_estimado,modalidad,objeto_proceso,url,fecha_publicacion,fecha_fin_recepcion_ofertas,fecha_apertura_ofertas,fecha_estimada_adjudicacion,fecha_enmienda,fecha_suscripcion';

    // Construir todos los lotes de códigos
    const lotes = [];
    for (let i = 0; i < todosCodigos.length; i += 50) {
      lotes.push(todosCodigos.slice(i, i + 50).join(','));
    }

    // Lanzar procesos y plazos EN PARALELO
    const [procesosLotes, plazosLotes] = await Promise.all([
      Promise.all(lotes.map(lote =>
        sbRest(`procesos?codigo_proceso=in.(${lote})&${SELECT_PROC}`)
          .then(r => Array.isArray(r) ? r : [])
          .catch(() => [])
      )),
      Promise.all(lotes.map(lote =>
        fetch(`${API}/api/plazos-bulk?codigos=${encodeURIComponent(lote)}`)
          .then(r => r.ok ? r.json() : {})
          .catch(() => ({}))
      ))
    ]);

    const procesos = procesosLotes.flat();
    const analisisMap = {};
    plazosLotes.forEach(pd => Object.assign(analisisMap, pd));

    // 4. Combinar proceso + plazos IA
    const procesosMap = {};
    (Array.isArray(procesos) ? procesos : []).forEach(p => {
      procesosMap[p.codigo_proceso] = p;
    });

    seguimientoData = segs.map(s => {
      const proc = procesosMap[s.proceso_codigo] || { codigo_proceso: s.proceso_codigo, titulo: 'Cargando...' };
      const plazos = analisisMap[s.proceso_codigo] || {};
      // Fusionar plazos IA en el objeto proceso como fechas adicionales
      proc._plazos_ia = plazos;
      return { ...s, proceso: proc };
    });

    renderSeguimiento();
  } catch(e) {
    console.error('Error cargando seguimiento:', e);
    const esTimeout = e.name === 'AbortError';
    if (lista) lista.innerHTML = `
      <div class="empty">
        <div class="empty-icon">⚠️</div>
        <div class="empty-text">${esTimeout ? 'El servidor tardó demasiado' : 'Error cargando. Intenta de nuevo.'}</div>
        <button onclick="cargarSeguimiento()" style="margin-top:14px;padding:8px 20px;background:var(--green-dim);border:1px solid var(--green-border);border-radius:var(--r-sm);color:var(--green);font-weight:700;font-size:13px;cursor:pointer;font-family:var(--font-body)">Reintentar</button>
      </div>`;
  }
}

// ── DASHBOARD MÉTRICAS PERSONALES ────────────────────────
function actualizarMetricas() {
  const panel = document.getElementById('seg-metricas');
  if (!panel || !seguimientoData.length) {
    if (panel) panel.style.display = 'none';
    const gp = document.getElementById('seg-graficas-panel');
    if (gp) gp.style.display = 'none';
    return;
  }
  panel.style.display = 'grid';
  // Mostrar panel de gráficas
  const grafPanel = document.getElementById('seg-graficas-panel');
  if (grafPanel) grafPanel.style.display = 'block';

  const todos      = seguimientoData;
  const activos    = todos.filter(d => !['adjudicado','no-adj'].includes(d.estado || 'analizando'));
  const ganados    = todos.filter(d => (d.estado || '') === 'adjudicado');
  const noAdj      = todos.filter(d => (d.estado || '') === 'no-adj');
  const terminados = ganados.length + noAdj.length;
  const tasa       = terminados > 0 ? Math.round((ganados.length / terminados) * 100) : null;

  // Monto total de contratos ganados
  const montoTotal = ganados.reduce((sum, d) => {
    const m = Number(d.proceso?.monto_estimado || 0);
    return sum + m;
  }, 0);

  // Tiempo promedio de preparación (días entre created_at y fecha_fin_recepcion_ofertas)
  const tiempos = todos
    .filter(d => d.created_at && d.proceso?.fecha_fin_recepcion_ofertas)
    .map(d => {
      const inicio  = new Date(d.created_at);
      const fin     = new Date(d.proceso.fecha_fin_recepcion_ofertas);
      const dias    = Math.round((fin - inicio) / (1000 * 60 * 60 * 24));
      return dias > 0 ? dias : null;
    })
    .filter(Boolean);
  const tiempoProm = tiempos.length
    ? Math.round(tiempos.reduce((a, b) => a + b, 0) / tiempos.length)
    : null;

  // Formato monto
  const fmtMonto = (v) => {
    if (!v) return '—';
    if (v >= 1_000_000_000) return `RD$${(v/1_000_000_000).toFixed(1)}B`;
    if (v >= 1_000_000)     return `RD$${(v/1_000_000).toFixed(1)}M`;
    if (v >= 1_000)         return `RD$${(v/1_000).toFixed(0)}K`;
    return `RD$${v.toLocaleString('es-DO')}`;
  };

  // Actualizar DOM
  const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  set('met-activos',      activos.length || '0');
  set('met-activos-sub',  `${todos.length} total seguidos`);
  set('met-ganados',      ganados.length || '0');
  set('met-tasa',         tasa !== null ? `${tasa}% tasa de éxito` : 'sin historial aún');
  set('met-monto',        montoTotal > 0 ? fmtMonto(montoTotal) : '—');
  set('met-tiempo',       tiempoProm ? `${tiempoProm}d` : '—');
}
// ─────────────────────────────────────────────────────────

function renderSeguimiento() {
  const lista = document.getElementById('seg-lista');
  if (!lista) return;
  // Actualizar dashboard de métricas
  actualizarMetricas();
  let datos = seguimientoData;
  if (segFiltroActivo !== 'todos') {
    datos = datos.filter(d => (d.estado || 'analizando') === segFiltroActivo);
  }
  const activos = seguimientoData.filter(d => !['adjudicado','no-adj'].includes(d.estado || 'analizando'));
  const countEl = document.getElementById('seg-count');
  if (countEl) countEl.textContent = `${activos.length} activo${activos.length !== 1 ? 's' : ''}`;
  if (!datos.length) {
    lista.innerHTML = `
            <div style="text-align:center;padding:40px 24px;">
        <div style="font-size:32px;margin-bottom:12px;">📋</div>
        <h3 class="ll-h4" style="margin-bottom:8px;">${segFiltroActivo === 'todos' ? 'Aún no sigues ningún proceso' : 'Sin procesos en este estado'}</h3>
        <p class="ll-body-sm ll-u-text-subtle" style="margin-bottom:16px;">${segFiltroActivo === 'todos'
          ? 'Entra al detalle de una licitación y presiona <b>\"Seguir proceso\"</b> para agregarlo aquí y recibir alertas automáticas.'
          : 'Cambia el filtro para ver otros estados.'}</p>
        ${segFiltroActivo === 'todos' ? `<button class="ll-btn ll-btn--primary" onclick="navTo('licitaciones')">Ver licitaciones activas →</button>` : ''}
      </div>`;
    return;
  }
  lista.innerHTML = datos.map(d => renderSegCard(d)).join('');
}

function filtrarSeguimiento(filtro) {
  segFiltroActivo = filtro;
  document.querySelectorAll('#sfbtn-todos, #sfbtn-analizando, #sfbtn-preparando, #sfbtn-presentado, #sfbtn-adjudicado, #sfbtn-no-adj').forEach(b => b.classList.remove('is-active'));
  const btn = document.getElementById(`sfbtn-${filtro}`);
  if (btn) btn.classList.add('is-active');
  renderSeguimiento();
}

async function toggleSeguimiento(codigo) {
  haptic(12); // UX v13
  if (!currentUser) { mostrarToast('Debes iniciar sesión primero'); return; }
  const yaEsta = estaEnSeguimiento(codigo);
  if (yaEsta) {
    await quitarSeguimiento(codigo);
  } else {
    await agregarSeguimiento(codigo);
  }
}

async function agregarSeguimiento(codigo) {
  // Freemium: máximo 3 procesos en seguimiento
  if (!(await esUsuarioPro())) {
    const actuales = (seguimientoData || []).length;
    if (actuales >= 3) {
      mostrarModalUpgrade('seguimiento');
      return;
    }
  }

  // ── ACTUALIZACIÓN OPTIMISTA: el usuario ve el cambio AL INSTANTE ──
  // Agregamos localmente primero, actualizamos el botón, y luego sincronizamos.
  const yaLocal = (seguimientoData || []).some(d =>
    (d.proceso?.codigo_proceso || d.proceso_codigo) === codigo
  );
  if (!yaLocal) {
    seguimientoData = seguimientoData || [];
    seguimientoData.push({ proceso_codigo: codigo, estado: 'analizando', _optimista: true });
  }
  actualizarBtnSeguir(codigo);           // botón cambia YA
  mostrarToast('Proceso agregado al seguimiento');

  try {
    await sbRest('seguimiento_procesos', 'POST', {
      user_id: currentUser.id,
      proceso_codigo: codigo,
      estado: 'analizando'
    });
    // Sincronizar con el servidor en segundo plano (sin bloquear la UI)
    cargarSeguimiento().then(() => actualizarBtnSeguir(codigo));

    // ── Análisis IA automático al seguir: SOLO para suscriptores PRO ──
    // (los freemium tienen su análisis gratis manual, no automático)
    esUsuarioPro().then(pro => {
      if (!pro) return;
      // Compatibilidad Expediente vs pliego, en paralelo al analisis general
      evaluarCompatibilidadExpediente(codigo, false).catch(() => {});
      fetch(`${API}/api/procesos/${codigo}/analizar`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }).then(r => {
        if (r.ok) console.log(`✅ Análisis solicitado para ${codigo}`);
      }).catch(() => {});
    });

  } catch(e) {
    // Falló el guardado → revertir el cambio optimista
    console.error('Error agregando seguimiento:', e);
    seguimientoData = (seguimientoData || []).filter(d =>
      (d.proceso?.codigo_proceso || d.proceso_codigo) !== codigo
    );
    actualizarBtnSeguir(codigo);
    mostrarToast('Error al agregar. Intenta de nuevo.');
  }
}

// ════════════════════════════════════════════════════════════
// COMPATIBILIDAD EXPEDIENTE DIGITAL vs PLIEGO (Bid Manager)
// Se muestra en el tab Resumen del detalle del proceso.
// ════════════════════════════════════════════════════════════

async function _bidFetch(path, options = {}) {
  const doReq = (tok) => fetch(`/api/bid${path}`, Object.assign({}, options, {
    headers: Object.assign({}, options.headers || {}, { 'Authorization': 'Bearer ' + tok })
  }));
  let r = await doReq(localStorage.getItem('sb_token'));
  if (r.status === 401) {
    // reusar la maquinaria de refresh de sbRest: un request dummy refresca el token
    try { await sbRest('perfiles_empresa?select=id&limit=1'); } catch(e) {}
    r = await doReq(localStorage.getItem('sb_token'));
  }
  return r;
}

function _compatSemaforo(v) {
  if (v === true) return '<span style="color:var(--green);font-weight:800">CUMPLE</span>';
  if (v === false) return '<span style="color:var(--red);font-weight:800">NO CUMPLE</span>';
  return '<span style="color:var(--amber);font-weight:800">SIN DATOS</span>';
}

function _compatFmtRD(n) {
  return 'RD$ ' + Number(n).toLocaleString('es-DO', { maximumFractionDigits: 2 });
}

function _compatDetails(titulo, cumple, inner, abierta) {
  const col = cumple === true ? 'var(--green)' : cumple === false ? 'var(--red)' : 'var(--amber)';
  const estado = cumple === true ? 'Cumple' : cumple === false ? 'No cumple' : 'Sin datos';
  return `<details ${abierta ? 'open' : ''} style="border:1px solid var(--border);border-radius:8px;margin-bottom:6px;background:var(--bg)">
    <summary style="padding:9px 12px;cursor:pointer;display:flex;align-items:center;gap:8px;font-size:12px;font-weight:700;list-style:none;color:var(--text)">
      <span style="width:9px;height:9px;border-radius:50%;background:${col};flex-shrink:0"></span>${titulo}
      <span style="margin-left:auto;font-size:11px;font-weight:600;color:${col}">${estado}</span>
    </summary>
    <div style="padding:2px 12px 10px">${inner}</div>
  </details>`;
}

function _renderCompatCard(codigo, m) {
  const cont = document.getElementById(`compat-exp-${codigo}`);
  if (!cont) return;
  const res = (m && m.resultado) || {};
  const cumple = res.cumple_global !== undefined ? res.cumple_global : res.cumple_financiero;
  const faltantes = res.faltantes || [];
  const sec = res.secciones || {};
  const pj = res.puntaje;

  // ── Faltantes (accionable, arriba) ──
  const topFalt = faltantes.slice(0, 5).map(f =>
    `<div style="font-size:12px;color:var(--text2);padding:3px 0;display:flex;gap:6px"><span>&ndash;</span><span>${f.texto}${f.subsanable === true ? ' <strong style="color:var(--amber)">(subsanable)</strong>' : f.subsanable === false ? ' <strong style="color:var(--red)">(NO subsanable)</strong>' : ''}</span></div>`
  ).join('') + (faltantes.length > 5 ? `<div style="font-size:11px;color:var(--muted);padding-top:4px">+${faltantes.length - 5} más abajo en el detalle por sección</div>` : '');

  const pjHtml = pj ? `
    <div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin:8px 0 4px">
      <span style="font-size:12px;font-weight:800;color:var(--text2)">Puntaje técnico estimado</span>
      <span style="font-size:18px;font-weight:900;color:${pj.cumple_umbral === true ? 'var(--green)' : pj.cumple_umbral === false ? 'var(--red)' : 'var(--amber)'}">${pj.estimado}${pj.puntaje_total ? ' / ' + pj.puntaje_total : ''}</span>
      ${pj.puntaje_minimo ? `<span style="font-size:11px;color:var(--muted)">mínimo para Sobre B: <strong>${pj.puntaje_minimo}</strong></span>` : ''}
      <span style="font-size:11px;color:var(--muted)">(${pj.verificado_duros} verificados de tu expediente + ${pj.asumido_blandos} asumidos por redacción)</span>
    </div>` : `<div style="font-size:11px;color:var(--muted);margin:6px 0 2px">Metodología: ${res.metodologia === 'combinada' ? 'combinada (con puntaje)' : 'Cumple / No Cumple'}</div>`;

  // ── Seccion: financieros ──
  const fin = sec.financieros || {};
  const finInner = fin.sin_estados
    ? '<div style="font-size:12px;color:var(--amber)">No hay estados financieros en tu expediente.</div>'
    : ((fin.detalle || []).map(d => {
        const val = d.valor !== null && d.valor !== undefined ? (Math.abs(d.valor) >= 1000 ? _compatFmtRD(d.valor) : Number(d.valor).toFixed(2)) : '—';
        return `<div style="display:flex;justify-content:space-between;gap:8px;font-size:12px;padding:3px 0;border-bottom:1px dashed var(--border)">
          <span>${d.nombre || d.tipo}${d.base_calculo === 'promedio' ? ' <span style="color:var(--muted);font-size:11px">(sobre la media)</span>' : ''}</span>
          <span>pide <strong>${d.limite_desc || '—'}</strong> · tienes <strong>${val}</strong> ${_compatSemaforo(d.cumple)}</span>
        </div>`;
      }).join('') || '<div style="font-size:12px;color:var(--muted)">El pliego no trae índices evaluables.</div>');

  // ── Seccion: lineas ──
  const lin = sec.lineas_credito || {};
  const linEsc = (lin.escenarios_lote && lin.escenarios_lote.length) ? lin.escenarios_lote
    : (lin.detalle && lin.detalle.length ? [{ lote: null, requisitos: lin.detalle }] : []);
  const linInner = linEsc.length ? linEsc.map(e => `
    ${e.lote ? `<div style="font-size:12px;font-weight:700;margin:5px 0 2px">${e.lote}</div>` : ''}
    ${(e.requisitos || []).map(r => `
      <div style="display:flex;justify-content:space-between;gap:8px;font-size:12px;padding:3px 0;border-bottom:1px dashed var(--border)">
        <span>Línea ${r.tipo}${r.pct_aplicado ? ` (${Math.round(r.pct_aplicado * 100)}%${r.es_mipyme ? ' MIPYME' : ''})` : ''}</span>
        <span>${r.requerido != null ? `necesitas <strong>${_compatFmtRD(r.requerido)}</strong> · tienes ${_compatFmtRD(r.disponible || 0)}` : 'monto base no disponible'} ${_compatSemaforo(r.cumple)}</span>
      </div>
      ${(r.fuentes || []).length ? `<div style="font-size:11px;color:var(--muted);padding:1px 0 4px 8px">Suman: ${(r.fuentes || []).join(' · ')}</div>` : ''}`).join('')}`).join('')
    : '<div style="font-size:12px;color:var(--muted)">El pliego no exige líneas de crédito.</div>';

  // ── Seccion: personal ──
  const per = sec.personal || {};
  const perInner = (per.detalle || []).length ? `
    ${(per.detalle || []).map(d => `
      <div style="display:flex;justify-content:space-between;gap:8px;font-size:12px;padding:3px 0;border-bottom:1px dashed var(--border)">
        <span><strong>${d.cargo}</strong> <span style="color:var(--muted)">${(d.titulaciones || []).join(' o ')}${d.anios_min ? ` · ${d.anios_min}+ años` : ''}${d.requiere_maestria ? ' · maestría' + (d.area_maestria ? ' en ' + d.area_maestria : '') : ''}</span></span>
        <span>cubierto ${d.cubiertos}/${d.puestos} ${_compatSemaforo(d.cumple)}</span>
      </div>
      ${(d.candidatos || []).length ? `<div style="font-size:11px;color:var(--text2);padding:2px 0 5px 8px">Puedes usar a: ${(d.candidatos || []).map(c => `<strong>${c.nombre}</strong>${c.anios ? ` (${c.anios} años)` : ''}`).join(', ')}</div>` : (d.cumple === false ? '<div style="font-size:11px;color:var(--red);padding:2px 0 5px 8px">Nadie en tu personal califica para este cargo.</div>' : '')}`).join('')}
    ${(per.asignaciones || []).length ? `<div style="font-size:11px;color:var(--muted);margin-top:5px">Asignación propuesta: ${(per.asignaciones || []).map(a => `${a.nombre} → ${a.cargo}`).join(' · ')}</div>` : ''}
    ${(per.checklist_documentos || []).length ? `<div style="margin-top:5px;padding:6px 8px;background:var(--bg2);border-radius:6px">${(per.checklist_documentos || []).map(c => `<div style="font-size:11px;color:var(--text2)">Docs: ${c}</div>`).join('')}</div>` : ''}`
    : '<div style="font-size:12px;color:var(--muted)">El pliego no detalla personal mínimo.</div>';

  // ── Seccion: equipos ──
  const eq = sec.equipos || {};
  const eqInner = (eq.detalle || []).length ? (eq.detalle || []).map(d => `
    <div style="display:flex;justify-content:space-between;gap:8px;font-size:12px;padding:3px 0;border-bottom:1px dashed var(--border)">
      <span><strong>${d.tipo}</strong>${d.capacidad_min ? ` (${d.capacidad_min})` : ''}${d.admite_alquilado === false ? ' · solo propios' : ''}</span>
      <span>tienes <strong>${d.disponible}</strong> de ${d.requerido} ${_compatSemaforo(d.cumple)}</span>
    </div>
    ${(d.usables || []).length ? `<div style="font-size:11px;color:var(--text2);padding:1px 0 4px 8px">Cuentan: ${(d.usables || []).join(' · ')}</div>` : ''}
    ${(d.revision_manual || []).map(r => `<div style="font-size:11px;color:var(--amber);padding-left:8px">${r}</div>`).join('')}`).join('')
    : '<div style="font-size:12px;color:var(--muted)">El pliego no lista equipos mínimos.</div>';

  // ── Seccion: experiencia ──
  const exp = sec.experiencia || {};
  const expInner = ((exp.checks || []).length || exp.obras_calificadas !== undefined) ? `
    <div style="font-size:12px;margin-bottom:3px">Obras tuyas que califican: <strong>${exp.obras_calificadas ?? 0}</strong>${exp.monto_acumulado ? ` · acumulado ${_compatFmtRD(exp.monto_acumulado)}` : ''}${exp.naturaleza && exp.naturaleza.length ? ` · naturaleza pedida: ${exp.naturaleza.map(n => n.replace(/_/g,' ')).join(', ')}` : ''}</div>
    ${(exp.obras_usables || []).length ? `<div style="font-size:11px;color:var(--text2);padding:1px 0 5px">Puedes presentar: ${(exp.obras_usables || []).map(o => `<strong>${o.nombre}</strong>${o.cliente ? ` (${o.cliente}` : ''}${o.monto ? `${o.cliente ? ', ' : ' ('}${_compatFmtRD(o.monto)})` : (o.cliente ? ')' : '')}${o.en_curso ? ' [en curso]' : ''}`).join(' · ')}</div>` : ''}
    ${(exp.checks || []).map(c => `<div style="display:flex;justify-content:space-between;font-size:12px;padding:2px 0"><span>${c.criterio}</span><span>${_compatSemaforo(c.cumple)}</span></div>`).join('')}
    ${(exp.escenarios_volumen || []).map(v => `<div style="display:flex;justify-content:space-between;font-size:12px;padding:2px 0"><span>${v.lote}: ${v.categoria}</span><span>${Number(v.ejecutado).toLocaleString('es-DO')} de ${Number(v.requerido).toLocaleString('es-DO')} ${v.unidad || ''} ${_compatSemaforo(v.cumple)}</span></div>`).join('')}
    ${(exp.obras_descartadas || []).length ? `<div style="margin-top:5px">${(exp.obras_descartadas || []).map(o => o.obra ? `<div style="font-size:11px;color:var(--muted)">${o.obra}: ${(o.descartada_por || []).join('; ')}</div>` : `<div style="font-size:11px;color:var(--amber)">${o.nota || ''}</div>`).join('')}</div>` : ''}`
    : '<div style="font-size:12px;color:var(--muted)">El pliego no detalla experiencia mínima evaluable.</div>';

  const otros = res.otros_requisitos || [];
  const otrosHtml = otros.length ? _compatDetails('Otros requisitos (revisión manual)', null,
    otros.map(o => `<div style="font-size:12px;padding:2px 0"><strong>${o.nombre}</strong>${o.texto_original ? `<div style="font-size:11px;color:var(--muted)">${o.texto_original}</div>` : ''}</div>`).join(''), false) : '';

  cont.innerHTML = `
  <div class="ll-card" style="margin-bottom:12px;border-left:3px solid ${cumple === true ? 'var(--green)' : cumple === false ? 'var(--red)' : 'var(--amber)'}">
    <div class="ll-card__header" style="display:flex;justify-content:space-between;align-items:center;gap:8px">
      <h2 class="ll-card__title">Compatibilidad con tu Expediente</h2>
      <div>${_compatSemaforo(cumple)}</div>
    </div>
    <div class="ll-card__body">
      ${pjHtml}
      ${faltantes.length ? `<div style="border:1px solid var(--border);border-left:3px solid var(--red);border-radius:8px;padding:8px 10px;margin:6px 0 10px;background:var(--bg2)"><div style="font-size:12px;font-weight:800;color:var(--text2);margin-bottom:3px">Lo que te falta (${faltantes.length})</div>${topFalt}</div>` : (cumple === true ? '<div style="font-size:12px;color:var(--green);font-weight:700;margin:6px 0 10px">Tu expediente cumple todo lo evaluable de este pliego.</div>' : '')}
      ${_compatDetails('Financieros', fin.cumple, finInner, fin.cumple === false)}
      ${_compatDetails('Líneas de crédito', lin.cumple, linInner, lin.cumple === false)}
      ${_compatDetails('Personal', per.cumple, perInner, per.cumple === false)}
      ${_compatDetails('Equipos', eq.cumple, eqInner, eq.cumple === false)}
      ${_compatDetails('Experiencia', exp.cumple, expInner, exp.cumple === false)}
      ${otrosHtml}
      <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap">
        <button class="ll-btn ll-btn--ghost ll-btn--sm" onclick="evaluarCompatibilidadExpediente('${codigo}', false)">Actualizar con mi expediente</button>
        <button class="ll-btn ll-btn--ghost ll-btn--sm" onclick="evaluarCompatibilidadExpediente('${codigo}', true)">Re-analizar pliego</button>
      </div>
      <div style="font-size:11px;color:var(--muted);margin-top:6px">Este análisis también quedó guardado en Expediente Digital → Financieros → Matching. Completa tu expediente y dale "Actualizar" — no gasta análisis nuevo.</div>
    </div>
  </div>`;
}

function _renderCompatCTA(codigo) {
  const cont = document.getElementById(`compat-exp-${codigo}`);
  if (!cont) return;
  cont.innerHTML = `
  <div class="ll-card" style="margin-bottom:12px;">
    <div class="ll-card__body" style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap">
      <div>
        <div style="font-size:13px;font-weight:800;color:var(--text)">¿Cumples los requisitos de este pliego?</div>
        <div style="font-size:12px;color:var(--muted)">Cruza el pliego contra tu Expediente Digital: financieros, líneas de crédito, personal, equipos y experiencia.</div>
      </div>
      <button class="ll-btn ll-btn--primary ll-btn--sm" onclick="evaluarCompatibilidadExpediente('${codigo}', false)">Evaluar compatibilidad</button>
    </div>
  </div>`;
}

async function cargarCompatibilidadExpediente(codigo) {
  const cont = document.getElementById(`compat-exp-${codigo}`);
  if (!cont || !currentUser?.id) return;
  try {
    const r = await _bidFetch(`/matching/por-referencia/${encodeURIComponent(codigo)}`);
    if (r.ok) {
      _renderCompatCard(codigo, await r.json());
    } else {
      _renderCompatCTA(codigo);
    }
  } catch (e) {
    _renderCompatCTA(codigo);
  }
}

async function evaluarCompatibilidadExpediente(codigo, forzar) {
  if (!currentUser?.id) { mostrarToast('Inicia sesión para evaluar compatibilidad'); return; }
  const cont = document.getElementById(`compat-exp-${codigo}`);
  if (cont) cont.innerHTML = `
  <div class="ll-card" style="margin-bottom:12px;">
    <div class="ll-card__body" style="display:flex;align-items:center;gap:10px">
      <div class="spinner" style="width:18px;height:18px;flex-shrink:0"></div>
      <div style="font-size:12px;color:var(--text2)">${forzar ? 'Re-analizando el pliego contra tu expediente (1-2 min)...' : 'Cruzando el pliego contra tu Expediente Digital... si es la primera vez toma 1-2 min.'}</div>
    </div>
  </div>`;
  try {
    const r = await _bidFetch('/matching/analizar-proceso', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ codigo_proceso: codigo, forzar: !!forzar })
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `Error ${r.status}`);
    }
    _renderCompatCard(codigo, await r.json());
  } catch (e) {
    if (cont) cont.innerHTML = `
    <div class="ll-card" style="margin-bottom:12px;">
      <div class="ll-card__body" style="font-size:12px;color:var(--text2)">
        No se pudo evaluar la compatibilidad: ${e.message}.
        ${String(e.message).includes('perfil') || String(e.message).includes('404') ? 'Completa tu Expediente Digital primero.' : ''}
        <button class="ll-btn ll-btn--ghost ll-btn--sm" style="margin-left:8px" onclick="evaluarCompatibilidadExpediente('${codigo}', ${!!forzar})">Reintentar</button>
      </div>
    </div>`;
  }
}

async function quitarSeguimiento(codigo) {
  // Sin confirmación — el botón ya es explícito ("✕")
  try {
    await sbRest(
      `seguimiento_procesos?user_id=eq.${currentUser.id}&proceso_codigo=eq.${codigo}`,
      'DELETE', null, { 'Prefer': 'return=minimal' }
    );
    seguimientoData = seguimientoData.filter(d =>
      (d.proceso?.codigo_proceso || d.proceso_codigo) !== codigo
    );
    renderSeguimiento();
    actualizarBtnSeguir(codigo);
    mostrarToast('Proceso eliminado del seguimiento');
  } catch(e) {
    mostrarToast('Error al quitar');
  }
}

function estaEnSeguimiento(codigo) {
  return seguimientoData.some(d =>
    (d.proceso?.codigo_proceso || d.proceso_codigo) === codigo
  );
}

async function actualizarBtnSeguir(codigo) {
  const btn = document.getElementById(`btn-seguir-${codigo}`);
  if (!btn) return;

  // Si seguimientoData está vacío, consultar Supabase directamente
  if (seguimientoData.length === 0 && currentUser) {
    try {
      const data = await sbRest(
        `seguimiento_procesos?user_id=eq.${currentUser.id}&proceso_codigo=eq.${codigo}`
      );
      const siguiendo = Array.isArray(data) && data.length > 0;
      btn.className = siguiendo
        ? 'll-btn ll-btn--secondary ll-btn--block btn-seguir siguiendo'
        : 'll-btn ll-btn--primary ll-btn--block btn-seguir no-siguiendo';
      btn.innerHTML  = siguiendo ? 'Siguiendo proceso' : 'Seguir proceso';
      return;
    } catch(e) {}
  }

  const siguiendo = estaEnSeguimiento(codigo);
  btn.className = siguiendo
    ? 'll-btn ll-btn--secondary ll-btn--block btn-seguir siguiendo'
    : 'll-btn ll-btn--primary ll-btn--block btn-seguir no-siguiendo';
  btn.innerHTML  = siguiendo ? 'Siguiendo proceso' : 'Seguir proceso';
}

function abrirModalEstado(codigo, estadoActual) {
  segModalCodigo = codigo;
  document.querySelectorAll('.seg-estado-opcion').forEach(el => {
    const onclick = el.getAttribute('onclick') || '';
    el.classList.toggle('selected', onclick.includes(`'${estadoActual}'`));
  });
  document.getElementById('seg-estado-modal').classList.add('visible');
  document.getElementById('seg-overlay').style.display = 'block';
}

function cerrarModalEstado() {
  document.getElementById('seg-estado-modal').classList.remove('visible');
  document.getElementById('seg-overlay').style.display = 'none';
  segModalCodigo = null;
}

async function cambiarEstado(nuevoEstado) {
  if (!segModalCodigo) return;
  try {
    await sbRest(
      `seguimiento_procesos?user_id=eq.${currentUser.id}&proceso_codigo=eq.${segModalCodigo}`,
      'PATCH', { estado: nuevoEstado }, { 'Prefer': 'return=minimal' }
    );
    const item = seguimientoData.find(d =>
      (d.proceso?.codigo_proceso || d.proceso_codigo) === segModalCodigo
    );
    if (item) item.estado = nuevoEstado;
    renderSeguimiento();
    cerrarModalEstado();
    mostrarToast(`Estado: ${ESTADOS_CONFIG[nuevoEstado]?.label}`);
  } catch(e) {
    mostrarToast('Error actualizando estado');
  }
}

function mostrarToast(msg) {
  let t = document.getElementById('ll-toast');
  if (!t) {
    t = document.createElement('div');
    t.id = 'll-toast';
    t.style.cssText = `position:fixed;bottom:80px;left:50%;transform:translateX(-50%);
      background:var(--color-surface,#1F2937);color:var(--color-text-primary,#F9FAFB);
      padding:10px 18px;border-radius:20px;border:1px solid var(--color-border,rgba(255,255,255,0.1));
      font-size:13px;font-weight:600;z-index:9000;white-space:nowrap;
      box-shadow:0 4px 16px rgba(0,0,0,0.4);opacity:0;transition:opacity 0.2s;
      font-family:var(--font-sans,'Inter',sans-serif);pointer-events:none;`;
    document.body.appendChild(t);
  }
  t.textContent = msg;
  t.style.opacity = '1';
  clearTimeout(t._timer);
  t._timer = setTimeout(() => { t.style.opacity = '0'; }, 2500);
}

// ── BADGE DE NOTIFICACIONES ──
function mostrarBadge() {
  const badge = document.getElementById('notif-badge');
  if (badge) badge.style.display = 'block';
  // Persistir en localStorage para que sobreviva si el usuario cierra la app
  localStorage.setItem('ll_notif_badge', '1');
}

function limpiarBadge() {
  const badge = document.getElementById('notif-badge');
  if (badge) badge.style.display = 'none';
  localStorage.removeItem('ll_notif_badge');
}

// Restaurar badge si había notificaciones pendientes al abrir la app
(function restaurarBadge() {
  if (localStorage.getItem('ll_notif_badge') === '1') {
    // Esperar a que el DOM esté listo
    const check = () => {
      const badge = document.getElementById('notif-badge');
      if (badge) { badge.style.display = 'block'; }
      else setTimeout(check, 200);
    };
    check();
  }
})();


// ══════════════════════════════════════════════
// CHECKLIST DE DOCUMENTOS
// ══════════════════════════════════════════════

// Estado en memoria: { "CODIGO-legal_0": true, ... }
let checklistEstado = {};

function renderChecklistHTML(analisis, codigo) {
  const checklist = analisis?.checklist_categorizado;
  const enSeguimiento = estaEnSeguimiento(codigo);

  if (!checklist || (!checklist.legal?.length && !checklist.tecnica?.length && !checklist.financiera?.length)) {
    const msgTexto = analisis ? 'El análisis no incluye checklist de documentos.' : 'Sin análisis para este proceso. El análisis se solicita automáticamente al seguir el proceso.';
    return `<div class="checklist-card">
      <div class="checklist-header"><div class="checklist-titulo">📋 Checklist de documentos</div></div>
      <div class="checklist-sin-analisis">
        ${msgTexto}
      </div>
    </div>`;
  }

  const secciones = [
    { key: 'legal',      icon: '',      label: 'Legal',      items: checklist.legal      || [] },
    { key: 'tecnica',    icon: '',      label: 'Técnica',    items: checklist.tecnica    || [] },
    { key: 'financiera', icon: '',      label: 'Financiera', items: checklist.financiera || [] },
  ];

  const totalItems = secciones.reduce((s, sec) => s + sec.items.length, 0);
  const totalCheck = secciones.reduce((s, sec) =>
    s + sec.items.filter((_, i) => checklistEstado[`${codigo}-${sec.key}_${i}`]).length, 0);
  const pct = totalItems ? Math.round(totalCheck / totalItems * 100) : 0;

  const seccionesHTML = secciones.map(sec => {
    if (!sec.items.length) return '';
    const marcados = sec.items.filter((_, i) => checklistEstado[`${codigo}-${sec.key}_${i}`]).length;
    const completo = marcados === sec.items.length;
    return `
    <div class="checklist-seccion" id="cl-sec-${codigo}-${sec.key}">
      <div class="checklist-seccion-header" onclick="toggleSeccionChecklist('${codigo}','${sec.key}')">
        <div class="checklist-seccion-titulo">
          <span>${sec.icon}</span><span>${sec.label}</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px">
          <span class="checklist-seccion-badge ${completo?'completo':''}">${marcados}/${sec.items.length}</span>
          <span class="checklist-chevron">▼</span>
        </div>
      </div>
      <div class="checklist-items">
        ${sec.items.map((item, i) => {
          const key = `${codigo}-${sec.key}_${i}`;
          const marcado = checklistEstado[key] || false;
          const subsanable = item.es_subsanable;
          return `<div class="checklist-item ${marcado?'marcado':''}" id="cli-${key}" onclick="toggleCheckItem('${codigo}','${sec.key}',${i},${enSeguimiento})">
            <input type="checkbox" ${marcado?'checked':''} onclick="event.stopPropagation();toggleCheckItem('${codigo}','${sec.key}',${i},${enSeguimiento})">
            <div class="checklist-item-texto">
              <div class="checklist-item-nombre">${item.documento}</div>
              <div class="checklist-item-meta">
                ${subsanable === true  ? '<span class="tag-subsanable">Subsanable</span>' : ''}
                ${subsanable === false ? '<span class="tag-no-subsanable">No subsanable</span>' : ''}
                ${item.nota ? `<span>${item.nota}</span>` : ''}
              </div>
            </div>
          </div>`;
        }).join('')}
      </div>
    </div>`;
  }).join('');

  return `<div class="checklist-card" id="checklist-${codigo}">
    <div class="checklist-header">
      <div class="checklist-titulo">📋 Checklist de documentos</div>
      <div class="checklist-progreso">
        <div class="checklist-progreso-bar">
          <div class="checklist-progreso-fill" id="cl-fill-${codigo}" style="width:${pct}%"></div>
        </div>
        <div class="checklist-progreso-texto" id="cl-texto-${codigo}">${totalCheck}/${totalItems} listos</div>
      </div>
    </div>
    ${seccionesHTML}
  </div>`;
}

function toggleSeccionChecklist(codigo, secKey) {
  const sec = document.getElementById(`cl-sec-${codigo}-${secKey}`);
  if (sec) sec.classList.toggle('abierta');
}

async function toggleCheckItem(codigo, secKey, idx, guardar) {
  const key = `${codigo}-${secKey}_${idx}`;
  checklistEstado[key] = !checklistEstado[key];

  // Actualizar UI del ítem
  const el = document.getElementById(`cli-${key}`);
  if (el) {
    el.classList.toggle('marcado', checklistEstado[key]);
    const cb = el.querySelector('input[type="checkbox"]');
    if (cb) cb.checked = checklistEstado[key];
  }

  // Actualizar progreso
  actualizarProgresoChecklist(codigo);

  // Guardar en Supabase si está en seguimiento
  if (guardar && currentUser) {
    try {
      await fetch(`${API}/api/checklist/${codigo}`, {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ user_id: currentUser.id, item_key: `${secKey}_${idx}`, completado: checklistEstado[key] })
      });
    } catch(e) { console.error('Error guardando checklist:', e); }
  }
}

function actualizarProgresoChecklist(codigo) {
  // Contar todos los ítems del checklist actual
  const items = document.querySelectorAll(`#checklist-${codigo} .checklist-item`);
  const total = items.length;
  const marcados = document.querySelectorAll(`#checklist-${codigo} .checklist-item.marcado`).length;
  const pct = total ? Math.round(marcados / total * 100) : 0;

  const fill = document.getElementById(`cl-fill-${codigo}`);
  const texto = document.getElementById(`cl-texto-${codigo}`);
  if (fill) fill.style.width = pct + '%';
  if (texto) texto.textContent = `${marcados}/${total} listos`;

  // Actualizar badges de secciones
  ['legal','tecnica','financiera'].forEach(secKey => {
    const sec = document.getElementById(`cl-sec-${codigo}-${secKey}`);
    if (!sec) return;
    const secItems = sec.querySelectorAll('.checklist-item');
    const secMarcados = sec.querySelectorAll('.checklist-item.marcado').length;
    const badge = sec.querySelector('.checklist-seccion-badge');
    if (badge) {
      badge.textContent = `${secMarcados}/${secItems.length}`;
      badge.classList.toggle('completo', secMarcados === secItems.length && secItems.length > 0);
    }
  });
}

async function cargarEstadoChecklist(codigo) {
  if (!currentUser) return;
  try {
    const res = await fetch(`${API}/api/checklist/${codigo}?user_id=${currentUser.id}`);
    const data = await res.json();
    const items = data.items || {};
    // Restaurar estado en memoria
    Object.entries(items).forEach(([itemKey, completado]) => {
      checklistEstado[`${codigo}-${itemKey}`] = completado;
    });
    // Actualizar UI
    Object.entries(items).forEach(([itemKey, completado]) => {
      const el = document.getElementById(`cli-${codigo}-${itemKey}`);
      if (el) {
        el.classList.toggle('marcado', completado);
        const cb = el.querySelector('input[type="checkbox"]');
        if (cb) cb.checked = completado;
      }
    });
    actualizarProgresoChecklist(codigo);
    // Abrir la primera sección automáticamente
    ['legal','tecnica','financiera'].forEach(secKey => {
      const sec = document.getElementById(`cl-sec-${codigo}-${secKey}`);
      if (sec && !document.querySelector(`#cl-sec-${codigo}-${secKey}.abierta`)) {
        if (sec.querySelector('.checklist-items')?.children.length > 0) {
          sec.classList.add('abierta');
          return false;
        }
      }
    });
  } catch(e) { console.error('Error cargando checklist:', e); }
}


// ── OFFLINE MODE ──
const offlineBanner = document.createElement('div');
offlineBanner.id = 'll-offline-banner';
offlineBanner.style.cssText = `
  display:none; position:fixed; top:0; left:0; right:0; z-index:9999;
  background:#D97706; color:white; text-align:center;
  padding:8px 16px; font-size:13px; font-weight:600;
  font-family:'Plus Jakarta Sans',sans-serif;
  padding-top: calc(8px + env(safe-area-inset-top));
`;
offlineBanner.textContent = 'Sin conexión — datos en caché';
document.body.appendChild(offlineBanner);

window.addEventListener('offline', () => {
  offlineBanner.style.display = 'block';
});
window.addEventListener('online', () => {
  offlineBanner.style.display = 'none';
  mostrarToast('Conexión restaurada');
  cargarProcesos(1);
});
// Verificar estado inicial
if (!navigator.onLine) offlineBanner.style.display = 'block';

// ── FORZAR ACTUALIZACIÓN DEL SERVICE WORKER (evita cache viejo en iOS PWA) ──
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.getRegistration().then(reg => {
    if (reg) {
      reg.update().catch(() => {});
      // Si hay un SW esperando, activarlo inmediatamente
      if (reg.waiting) {
        reg.waiting.postMessage({type: 'SKIP_WAITING'});
      }
    }
  }).catch(() => {});
}

// ── RECIBIR MENSAJE DEL SERVICE WORKER (badge desde push en segundo plano) ──
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.addEventListener('message', event => {
    if (event.data?.type === 'SHOW_BADGE') {
      mostrarBadge();
    }
  });
  // Auto-actualización: cuando un Service Worker nuevo toma control,
  // recargar una sola vez para aplicar el deploy más reciente.
  let _swReloaded = false;
  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (_swReloaded) return;
    _swReloaded = true;
    window.location.reload();
  });
}



// ══════════════════════════════════════════════════
// MERCADO DE PROVEEDORES
// ══════════════════════════════════════════════════
let mercadoTipoActivo = '';
let mercadoSubcatActiva = '';
let mercadoDebounceTimer = null;
const TIPOS_MANO_OBRA = ['mano_obra', 'servicios', 'subcontratista'];

const TIPO_LABELS = {
  materiales: 'Materiales', equipos: 'Equipos',
  mano_obra: 'Mano de obra', servicios: 'Servicios', subcontratista: 'Subcontratista'
};

function esSuscriptor() {
  // Reutiliza la variable global de sesión de LicitacionLab
  // Si tienes plan_activo o suscripcion en el perfil del usuario, úsala aquí
  // Por ahora retorna true para no bloquear durante pruebas
  return true;
}

let _mercadoView = 'grid';

function setMercadoView(v) {
  _mercadoView = v;
  const lista = document.getElementById('mercado-lista');
  if (lista) { lista.classList.toggle('list-view', v === 'list'); }
  document.getElementById('mercado-view-grid').style.background = v==='grid' ? 'var(--green-dim)' : 'none';
  document.getElementById('mercado-view-grid').style.color = v==='grid' ? 'var(--green)' : 'var(--muted)';
  document.getElementById('mercado-view-list').style.background = v==='list' ? 'var(--green-dim)' : 'none';
  document.getElementById('mercado-view-list').style.color = v==='list' ? 'var(--green)' : 'var(--muted)';
}

function aplicarFiltrosMercado() { cargarMercado(); }

// ── Buscador Mercado: debounce + sugerencias (FIX: estas funciones no existían) ──
let _mercadoDebounce = null;
let _mercadoProvsCache = [];

function debouncarMercado() {
  clearTimeout(_mercadoDebounce);
  _mercadoDebounce = setTimeout(() => { actualizarSugerenciasMercado(); cargarMercado(); }, 350);
}

function mostrarSugerencias() { actualizarSugerenciasMercado(); }

function ocultarSugerencias() {
  const box = document.getElementById('mercado-suggestions');
  if (box) box.classList.remove('visible');
}

function actualizarSugerenciasMercado() {
  const box = document.getElementById('mercado-suggestions');
  const q = (document.getElementById('mercado-busqueda')?.value || '').trim().toLowerCase();
  if (!box) return;
  if (q.length < 2 || !_mercadoProvsCache.length) { box.classList.remove('visible'); return; }
  const matches = _mercadoProvsCache
    .filter(p => (p.nombre_empresa || '').toLowerCase().includes(q))
    .slice(0, 6);
  if (!matches.length) { box.classList.remove('visible'); return; }
  box.innerHTML = matches.map(p => `<div class="mercado-sug-item">${sanitize(p.nombre_empresa)}</div>`).join('');
  box.querySelectorAll('.mercado-sug-item').forEach((el, i) => {
    el.onmousedown = () => seleccionarSugerenciaMercado(matches[i].nombre_empresa || '');
  });
  box.classList.add('visible');
}

function seleccionarSugerenciaMercado(nombre) {
  const inp = document.getElementById('mercado-busqueda');
  if (inp) inp.value = nombre;
  ocultarSugerencias();
  cargarMercado();
}

// Filtro por chip en la barra móvil del Mercado (sincroniza con los checkboxes)
function filtrarChipMercado(btn) {
  haptic(); // UX v13
  const cont  = document.getElementById('mercado-chips-mobile');
  const chips = cont ? [...cont.querySelectorAll('.mercado-chip')] : [];
  const cat   = btn.dataset.cat;
  if (!cat) {
    // "Todos": limpiar selección
    chips.forEach(c => c.classList.toggle('active', !c.dataset.cat));
    document.querySelectorAll('.mercado-cat-check').forEach(c => c.checked = false);
  } else {
    btn.classList.toggle('active');
    const activo = btn.classList.contains('active');
    document.querySelectorAll(`.mercado-cat-check[value="${cat}"]`).forEach(c => c.checked = activo);
    const algunActivo = chips.some(c => c.dataset.cat && c.classList.contains('active'));
    const todosBtn = chips.find(c => !c.dataset.cat);
    if (todosBtn) todosBtn.classList.toggle('active', !algunActivo);
  }
  cargarMercado();
}

function abrirFiltrosMobile() {
  const drawer = document.getElementById('mercado-filtros-drawer');
  if (drawer) { drawer.classList.add('open'); document.body.style.overflow = 'hidden'; }
}

function cerrarFiltrosMobile() {
  const drawer = document.getElementById('mercado-filtros-drawer');
  if (drawer) { drawer.classList.remove('open'); document.body.style.overflow = ''; }
}

function sincronizarFiltrosMobile() {
  // Sincronizar checkboxes del drawer con los del sidebar desktop
  document.querySelectorAll('.mercado-cat-check-m').forEach(chk => {
    const val = chk.value;
    const desktop = document.querySelector(`.mercado-cat-check:not(.mercado-cat-check-m)[value="${val}"]`);
    if (desktop) desktop.checked = chk.checked;
  });
}

function toggleVerMasCats() {
  const extra = document.getElementById('mercado-cats-extra');
  const btn   = document.getElementById('mercado-ver-mas-cats');
  if (!extra) return;
  const visible = extra.style.display === 'flex';
  extra.style.display = visible ? 'none' : 'flex';
  if (btn) btn.textContent = visible ? '+ Ver las 14 categorías' : '- Menos categorías';
}

function limpiarFiltrosMercado() {
  document.querySelectorAll('.mercado-cat-check').forEach(c => c.checked = false);
  document.querySelectorAll('.mercado-zona-check').forEach(c => c.checked = false);
  const radios = document.querySelectorAll('input[name="mercado-verificado"]');
  if (radios[0]) radios[0].checked = true;
  const rc = document.getElementById('mercado-rating-check');
  if (rc) rc.checked = false;
  document.getElementById('mercado-busqueda').value = '';
  cargarMercado();
}

// ── Render de tarjetas de proveedores (Mercado) ──
const _PROV_TIPO_LABEL = {
  materiales: 'Materiales',
  equipos: 'Equipos',
  mano_obra: 'Mano de obra',
  servicios: 'Servicios',
  subcontratista: 'Subcontratista',
  otros: 'Otros',
};

function _provStars(rating) {
  const r = Math.round((Number(rating) || 0));
  let s = '';
  for (let i = 1; i <= 5; i++) s += (i <= r) ? '★' : '☆';
  return s;
}

function _provMapUrl(p) {
  if (p.latitud && p.longitud) {
    return `https://www.google.com/maps/search/?api=1&query=${p.latitud},${p.longitud}`;
  }
  if (p.direccion) {
    return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(p.direccion)}`;
  }
  return '';
}

function renderProveedores(provs) {
  const lista = document.getElementById('mercado-lista');
  if (!lista) return;

  if (!provs || !provs.length) {
    lista.innerHTML = `<div class="mercado-empty">
      <div style="font-size:30px;margin-bottom:10px;">🔍</div>
      <div style="font-weight:800;color:var(--text);margin-bottom:4px;">No se encontraron proveedores</div>
      <div style="font-size:13px;">Prueba quitando filtros o cambiando la búsqueda.</div>
    </div>`;
    return;
  }

  lista.innerHTML = provs.map(p => {
    const nombre = sanitize(p.nombre_empresa || 'Sin nombre');
    const inicial = (p.nombre_empresa || '?').trim().charAt(0).toUpperCase();

    // Logo
    const logo = p.logo_url
      ? `<img class="prov-logo" src="${safeUrl(p.logo_url)}" alt="" onerror="this.outerHTML='<div class=&quot;prov-logo-placeholder&quot;>${inicial}</div>'">`
      : `<div class="prov-logo-placeholder">${inicial}</div>`;

    // Verificado
    const verif = p.verificado
      ? `<span class="prov-verificado" title="RNC verificado contra DGII"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>Verificado</span>`
      : '';

    // Tipo badge
    const tipoKey = (p.tipo || '').toLowerCase();
    const tipoLabel = _PROV_TIPO_LABEL[tipoKey] || (p.tipo ? sanitize(p.tipo) : '');
    const tipoBadge = tipoLabel
      ? `<span class="prov-tipo-badge prov-tipo-${sanitize(tipoKey)}">${tipoLabel}</span>`
      : '';

    // Descripción
    const desc = p.descripcion
      ? `<div class="prov-desc">${sanitize(p.descripcion)}</div>`
      : '';

    // Categorías
    const cats = (p.proveedor_categorias || [])
      .map(c => c.categoria_nombre)
      .filter(Boolean)
      .slice(0, 4);
    const catsHtml = cats.length
      ? `<div class="prov-cats">${cats.map(c => `<span class="prov-cat-chip">${sanitize(c)}</span>`).join('')}</div>`
      : '';

    // Meta (zona, dirección, rating)
    const zonas = Array.isArray(p.zona_geografica) ? p.zona_geografica : (p.zona_geografica ? [p.zona_geografica] : []);
    const metaRows = [];
    if (zonas.length) {
      metaRows.push(`<div class="prov-meta-row">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg>
        <span>${sanitize(zonas.join(', '))}</span></div>`);
    }
    if (p.direccion) {
      metaRows.push(`<div class="prov-meta-row">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
        <span>${sanitize(p.direccion)}</span></div>`);
    }
    if (p.rating_promedio || p.num_resenas) {
      const rating = Number(p.rating_promedio || 0).toFixed(1);
      const resenas = p.num_resenas ? ` · ${p.num_resenas} reseña${p.num_resenas == 1 ? '' : 's'}` : '';
      metaRows.push(`<div class="prov-meta-row">
        <span style="color:#F59E0B;letter-spacing:1px;">${_provStars(p.rating_promedio)}</span>
        <span>${rating}${resenas}</span></div>`);
    }
    const metaHtml = metaRows.length ? `<div class="prov-meta">${metaRows.join('')}</div>` : '';

    // Redes sociales
    const social = [];
    if (p.website) social.push(`<a class="prov-social-btn" href="${safeUrl(p.website)}" target="_blank" rel="noopener" title="Sitio web">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/></svg></a>`);
    if (p.instagram) social.push(`<a class="prov-social-btn" href="${safeUrl(p.instagram)}" target="_blank" rel="noopener" title="Instagram">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="20" rx="5"/><circle cx="12" cy="12" r="4"/><circle cx="17.5" cy="6.5" r="1" fill="currentColor" stroke="none"/></svg></a>`);
    if (p.facebook) social.push(`<a class="prov-social-btn" href="${safeUrl(p.facebook)}" target="_blank" rel="noopener" title="Facebook">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 2h-3a5 5 0 00-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 011-1h3z"/></svg></a>`);
    const socialHtml = social.length ? `<div class="prov-social">${social.join('')}</div>` : '';

    // Acciones
    const waNum = p.whatsapp || p.telefono || '';
    const actions = [];
    if (waNum) {
      actions.push(`<button class="prov-btn-wa" onclick="contactarProveedor('${p.id}','whatsapp','${sanitize(waNum)}')">WhatsApp</button>`);
    }
    if (p.telefono) {
      actions.push(`<a class="prov-btn-call" href="tel:${sanitize(String(p.telefono).replace(/[^\d+]/g,''))}" style="text-decoration:none;" onclick="contactarProveedor('${p.id}','telefono','${sanitize(p.telefono)}')">Llamar</a>`);
    }
    const mapUrl = _provMapUrl(p);
    if (mapUrl) {
      actions.push(`<a class="prov-btn-map" href="${safeUrl(mapUrl)}" target="_blank" rel="noopener" title="Ver en mapa">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg></a>`);
    }
    const actionsHtml = actions.length ? `<div class="prov-actions">${actions.join('')}</div>` : '';

    return `<div class="prov-card">
      <div class="prov-card-header">
        ${logo}
        <div class="prov-info">
          <div class="prov-nombre">${nombre}${verif}</div>
          ${tipoBadge}
        </div>
      </div>
      <div class="prov-card-body">
        ${desc}
        ${catsHtml}
        ${metaHtml}
        ${socialHtml}
      </div>
      ${actionsHtml}
    </div>`;
  }).join('');
}

async function cargarMercado() {
  const lista = document.getElementById('mercado-lista');
  if (!lista) return;
  lista.innerHTML = skeletonCards(4);

  try {
    const busqueda = document.getElementById('mercado-busqueda')?.value.trim() || '';
    const orden    = document.getElementById('mercado-orden')?.value || 'rating';

    // Recoger filtros del sidebar
    const catChecks  = [...document.querySelectorAll('.mercado-cat-check:checked')].map(c => c.value);
    const zonaChecks = [...document.querySelectorAll('.mercado-zona-check:checked')].map(c => c.value);
    const verificado = document.querySelector('input[name="mercado-verificado"]:checked')?.value || '';
    const ratingMin  = document.getElementById('mercado-rating-check')?.checked ? '4' : '';

    const params = new URLSearchParams({ page_size: '50' });
    if (busqueda)       params.set('busqueda', busqueda);
    if (catChecks[0])   params.set('tipo', catChecks[0]);   // primer tipo seleccionado
    if (zonaChecks[0])  params.set('zona', zonaChecks[0]);  // primera zona seleccionada
    if (verificado === 'true') params.set('verificado', 'true');
    if (ratingMin)      params.set('rating_min', ratingMin);

    const res  = await fetch(`${API}/api/mercado/proveedores?${params}`, {
      headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('sb_token') || '') }
    });
    const data = await res.json();
    let provs  = data.proveedores || [];

    // Cache para sugerencias del buscador (solo cuando no hay búsqueda activa)
    if (!busqueda && provs.length) _mercadoProvsCache = provs;

    // Filtrar por zonas adicionales si hay más de una marcada
    if (zonaChecks.length > 1) {
      provs = provs.filter(p =>
        zonaChecks.some(z => (p.zona_geografica || []).includes(z))
      );
    }
    // Filtrar por categorías adicionales si hay más de una marcada
    if (catChecks.length > 1) {
      provs = provs.filter(p =>
        catChecks.some(cat => p.tipo === cat)
      );
    }

    // Ordenar localmente
    if (orden === 'nombre') provs.sort((a,b) => (a.nombre_empresa||'').localeCompare(b.nombre_empresa||''));
    else if (orden === 'recientes') provs.sort((a,b) => new Date(b.created_at) - new Date(a.created_at));
    // rating ya viene ordenado desde el backend

    // Actualizar contadores
    const badge = document.getElementById('mercado-total-badge');
    const label = document.getElementById('mercado-resultados-label');
    const filtrosActivos = [...catChecks, ...zonaChecks, busqueda].filter(Boolean);
    if (badge) badge.textContent = `${provs.length} proveedores`;
    if (label) {
      label.textContent = provs.length
        ? `${provs.length} proveedores${filtrosActivos.length ? ' · filtrado por ' + filtrosActivos.join(', ') : ''}`
        : '';
    }

    lista.classList.toggle('list-view', _mercadoView === 'list');
    renderProveedores(provs);
  } catch(e) {
    const errMsg = e?.message || String(e) || 'Error desconocido';
    lista.innerHTML = `<div class="mercado-empty" style="padding:20px;color:var(--muted);font-size:13px;">
      <div style="font-size:18px;margin-bottom:8px;">⚠️</div>
      <div style="font-weight:700;margin-bottom:4px;">Error al cargar proveedores</div>
      <div style="font-size:12px;opacity:.7;">${errMsg}</div>
    </div>`;
  }
}


async function contactarProveedor(proveedorId, tipo, numero) {
  // Registrar contacto en analytics
  try {
    fetch(`${API}/api/mercado/contactos`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ proveedor_id: proveedorId, tipo_contacto: tipo })
    });
  } catch(e) {}
  // FIX: solo abrir WhatsApp si el contacto es por WhatsApp.
  // Antes el botón "Llamar" (enlace tel:) también abría wa.me.
  if (tipo !== 'whatsapp') return;
  const num = numero.replace(/\D/g, '');
  const msg = encodeURIComponent('Hola, los contacto desde LicitacionLab. ¿Pueden enviarme cotización?');
  window.open(`https://wa.me/${num}?text=${msg}`, '_blank');
}

// ── Formulario de registro ──
let pfSubcatsSeleccionadas = [];


function seleccionarTipo(btn) {
  document.querySelectorAll('.pf-tipo-btn').forEach(b => b.classList.remove('selected'));
  btn.classList.add('selected');
  document.getElementById('pf-tipo').value = btn.dataset.tipo;
  pfSubcatsSeleccionadas = [];
  // Mostrar el input "Otros" solo cuando se elige esa categoría
  const otrosWrap = document.getElementById('pf-tipo-otros-wrap');
  if (otrosWrap) {
    if (btn.dataset.tipo === 'otros') {
      otrosWrap.style.display = 'block';
      const inp = document.getElementById('pf-tipo-otros');
      if (inp) setTimeout(() => inp.focus(), 50);
    } else {
      otrosWrap.style.display = 'none';
      const inp = document.getElementById('pf-tipo-otros');
      if (inp) inp.value = '';
    }
  }
  renderSubcats(btn.dataset.tipo);
}

function renderSubcats(tipo) {
  const wrap = document.getElementById('pf-subcats-wrap');
  const grid = document.getElementById('pf-subcats-grid');
  const lbl  = document.getElementById('pf-subcats-label');
  const cfg  = SUBCATEGORIAS[tipo];
  if (!cfg) { wrap.classList.remove('visible'); return; }
  lbl.textContent = cfg.label;
  grid.innerHTML = cfg.items.map(item =>
    `<button class="pf-subcat-chip" onclick="toggleSubcat(this,'${item}')">${item}</button>`
  ).join('');
  wrap.classList.add('visible');
}

function toggleSubcat(el, nombre) {
  el.classList.toggle('selected');
  if (el.classList.contains('selected')) {
    if (!pfSubcatsSeleccionadas.includes(nombre)) pfSubcatsSeleccionadas.push(nombre);
  } else {
    pfSubcatsSeleccionadas = pfSubcatsSeleccionadas.filter(s => s !== nombre);
  }
}

// ── Provincias desplegable ──
function toggleZonasPanel() {
  const panel   = document.getElementById('zonas-panel');
  const trigger = document.getElementById('zonas-trigger');
  const arrow   = document.getElementById('zonas-arrow');
  const isOpen  = panel.classList.contains('open');
  panel.classList.toggle('open', !isOpen);
  trigger.classList.toggle('open', !isOpen);
  arrow.style.transform = isOpen ? '' : 'rotate(180deg)';
}

function toggleZonaPill(el) {
  const esTodo = el.dataset.zona === 'Todo el país';
  if (esTodo) {
    const allSelected = el.classList.contains('selected');
    document.querySelectorAll('#zonas-panel .zona-pill').forEach(p => {
      p.classList.toggle('selected', !allSelected);
    });
  } else {
    el.classList.toggle('selected');
    // Desmarcar "Todo el país" si deseleccionamos algo
    const todoPill = document.querySelector('.zona-pill-todo');
    if (todoPill && !el.classList.contains('selected')) todoPill.classList.remove('selected');
  }
  actualizarContadorZonas();
}

function actualizarContadorZonas() {
  const seleccionadas = [...document.querySelectorAll('#zonas-panel .zona-pill.selected')]
    .filter(p => p.dataset.zona !== 'Todo el país');
  const count   = document.getElementById('zonas-count');
  const texto   = document.getElementById('zonas-trigger-text');
  const todoSel = document.querySelector('.zona-pill-todo')?.classList.contains('selected');
  if (todoSel || seleccionadas.length === 0) {
    if (todoSel) {
      texto.textContent = 'Todo el país';
      count.style.display = 'none';
    } else {
      texto.textContent = 'Seleccionar provincias...';
      count.style.display = 'none';
    }
  } else {
    texto.textContent = seleccionadas.length === 1
      ? seleccionadas[0].dataset.zona
      : `${seleccionadas[0].dataset.zona} y ${seleccionadas.length - 1} más`;
    count.textContent = seleccionadas.length;
    count.style.display = 'inline-block';
  }
}

// ── RNC Verificación ──
let rncVerificado = false;

function resetRncStatus() {
  rncVerificado = false;
  const st = document.getElementById('rnc-status');
  if (st) { st.textContent = ''; st.className = 'rnc-status'; }
}

async function verificarRNC() {
  const rnc = document.getElementById('pf-rnc').value.replace(/[-\s]/g, '').trim();
  const st  = document.getElementById('rnc-status');
  if (!rnc || rnc.length < 9) {
    st.textContent = 'Ingresa el RNC completo (9 u 11 dígitos)';
    st.className = 'rnc-status err';
    return;
  }
  st.textContent = 'Consultando DGII...';
  st.className = 'rnc-status loading';
  try {
    const res  = await fetch(`https://api.digital.gob.do/v3/contribuyentes/${rnc}`);
    const data = await res.json();
    if (data && data.nombre) {
      rncVerificado = true;
      st.textContent = `✓ Verificado: ${data.nombre}`;
      st.className = 'rnc-status ok';
      // Pre-llenar el nombre si está vacío
      const nomInput = document.getElementById('pf-nombre');
      if (nomInput && !nomInput.value.trim()) nomInput.value = data.nombre;
    } else {
      st.textContent = 'RNC no encontrado en DGII';
      st.className = 'rnc-status err';
    }
  } catch(e) {
    st.textContent = 'Error al consultar DGII. Continúa sin verificar.';
    st.className = 'rnc-status err';
  }
}

// Verificación automática al salir del campo RNC (si hay 9 u 11 dígitos)
async function autoVerificarRNC() {
  const inp = document.getElementById('pf-rnc');
  if (!inp) return;
  const rnc = inp.value.replace(/[-\s]/g, '').trim();
  // Solo dispara si tiene la longitud exacta de RNC dominicano y aún no se verificó
  if ((rnc.length === 9 || rnc.length === 11) && /^\d+$/.test(rnc) && !rncVerificado) {
    await verificarRNC();
  }
}

let pfLogoBase64 = null;

function previsualizarLogo(input) {
  const file = input.files[0];
  if (!file) return;
  if (file.size > 1048576) { mostrarToast('La imagen debe ser menor de 1MB'); input.value=''; return; }
  const reader = new FileReader();
  reader.onload = e => {
    pfLogoBase64 = e.target.result; // data:image/...;base64,...
    const preview = document.getElementById('pf-logo-preview');
    preview.innerHTML = `<img src="${e.target.result}" style="width:100%;height:100%;object-fit:cover;border-radius:10px">`;
  };
  reader.readAsDataURL(file);
}

function abrirFormProveedor() {
  try {
  // Reset completo del form
  ['pf-nombre','pf-descripcion','pf-whatsapp','pf-rnc','pf-email','pf-direccion','pf-horario','pf-instagram','pf-facebook','pf-website','pf-latitud','pf-longitud','pf-tipo-otros'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
  document.querySelectorAll('.pf-tipo-btn').forEach(b => b.classList.remove('selected'));
  const pfTipo = document.getElementById('pf-tipo');
  if (pfTipo) pfTipo.value = '';
  const tipoOtrosWrap = document.getElementById('pf-tipo-otros-wrap');
  if (tipoOtrosWrap) tipoOtrosWrap.style.display = 'none';
  const subcatsWrap = document.getElementById('pf-subcats-wrap');
  if (subcatsWrap) subcatsWrap.classList.remove('visible');
  document.querySelectorAll('#zonas-panel .zona-pill').forEach(p => p.classList.remove('selected'));
  actualizarContadorZonas();
  const zonasPanel = document.getElementById('zonas-panel');
  if (zonasPanel && zonasPanel.classList.contains('open')) toggleZonasPanel();
  pfSubcatsSeleccionadas = [];
  rncVerificado = false;
  pfLogoBase64 = null;
  const logoPreview = document.getElementById('pf-logo-preview');
  if (logoPreview) logoPreview.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>';
  const logoFile = document.getElementById('pf-logo-file');
  if (logoFile) logoFile.value = '';
  resetRncStatus();
  const msg = document.getElementById('pf-msg');
  if (msg) msg.style.display = 'none';
  } catch(e) { console.error('abrirFormProveedor reset error:', e); }
  // Mostrar overlay y sheet siempre, aunque el reset falle
  const overlay = document.getElementById('mercado-form-overlay');
  const sheet   = document.getElementById('mercado-form-sheet');
  if (overlay) overlay.style.display = 'block';
  if (sheet) { sheet.classList.add('visible'); sheet.scrollTop = 0; }
}

function cerrarFormProveedor() {
  document.getElementById('mercado-form-overlay').style.display = 'none';
  document.getElementById('mercado-form-sheet').classList.remove('visible');
}

// ── RED DE SEGURIDAD: nunca quedar atrapado en un panel ──
// ESC (teclado) cierra CUALQUIER panel/modal abierto, en orden de prioridad
document.addEventListener('keydown', (e) => {
  if (e.key !== 'Escape') return;
  try {
    const sheet = document.getElementById('mercado-form-sheet');
    if (sheet?.classList.contains('visible')) { cerrarFormProveedor(); return; }
    const filtros = document.getElementById('mercado-filtros-drawer');
    if (filtros?.classList.contains('open')) { cerrarFiltrosMobile(); return; }
    const pricing = document.getElementById('modal-pricing');
    if (pricing && pricing.style.display === 'block') { cerrarPricing(); return; }
    const segModal = document.getElementById('seg-estado-modal');
    if (segModal && segModal.style.transform === 'translateY(0px)') { cerrarModalEstado?.(); return; }
    const drawer = document.getElementById('drawer');
    if (drawer?.classList.contains('open')) { closeDrawer?.(); return; }
    const tutorial = document.getElementById('onb-tutorial-overlay');
    if (tutorial && !tutorial.classList.contains('oculto')) { cerrarTutorial(false); return; }
    const wizard = document.getElementById('onb-overlay');
    if (wizard && !wizard.classList.contains('oculto')) { saltarOnboarding(); return; }
  } catch(err) {}
});

// ── RED GLOBAL DE ERRORES JS: ningún error rompe la app en silencio ──
// Si algo explota, el usuario ve un toast en vez de una pantalla congelada.
(function initRedErrores() {
  let _ultimoToastError = 0;
  function _avisarError() {
    const ahora = Date.now();
    if (ahora - _ultimoToastError < 8000) return; // máx 1 aviso cada 8s
    _ultimoToastError = ahora;
    try { mostrarToast('⚠️ Algo salió mal — desliza hacia abajo para refrescar', 'error'); } catch(e) {}
  }
  window.addEventListener('error', (e) => {
    console.error('🔴 Error global:', e.message, e.filename, e.lineno);
    _avisarError();
  });
  window.addEventListener('unhandledrejection', (e) => {
    console.error('🔴 Promesa rechazada:', e.reason);
    _avisarError();
  });
})();

// ── FETCH CON TIMEOUT GLOBAL: ningún botón se queda en "cargando" infinito ──
// Toda petición que tarde más de 45s se aborta y dispara el catch del llamador,
// que ya muestra su mensaje de error correspondiente.
(function initFetchTimeout() {
  const _fetchOriginal = window.fetch.bind(window);
  window.fetch = function(recurso, opciones = {}) {
    if (opciones.signal) return _fetchOriginal(recurso, opciones); // respeta señales propias
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(new DOMException('Timeout de 45s', 'TimeoutError')), 45000);
    return _fetchOriginal(recurso, { ...opciones, signal: ctrl.signal })
      .finally(() => clearTimeout(timer));
  };
})();

// Deslizar hacia la derecha sobre el sheet de proveedor también lo cierra (gesto iOS)
(function initSwipeCierreSheet() {
  const sheet = document.getElementById('mercado-form-sheet');
  if (!sheet) return;
  let x0 = null, y0 = null;
  sheet.addEventListener('touchstart', (e) => {
    x0 = e.touches[0].clientX; y0 = e.touches[0].clientY;
  }, { passive: true });
  sheet.addEventListener('touchend', (e) => {
    if (x0 === null) return;
    const dx = e.changedTouches[0].clientX - x0;
    const dy = Math.abs(e.changedTouches[0].clientY - y0);
    if (dx > 90 && dy < 60) cerrarFormProveedor();  // swipe derecha franco
    x0 = y0 = null;
  }, { passive: true });
})();

async function enviarRegistroProveedor() {
  const btn    = document.getElementById('pf-submit-btn');
  const msg    = document.getElementById('pf-msg');
  const nombre = document.getElementById('pf-nombre').value.trim();
  const tipo   = document.getElementById('pf-tipo').value;
  const wa     = document.getElementById('pf-whatsapp').value.trim();
  const tipoOtros = (document.getElementById('pf-tipo-otros')?.value || '').trim();

  if (!nombre || !tipo || !wa) {
    msg.style.display = 'block';
    msg.style.background = '#FEE2E2';
    msg.style.color = '#DC2626';
    msg.textContent = 'Completa: nombre, tipo de proveedor y WhatsApp';
    document.getElementById('mercado-form-sheet').scrollTop = 0;
    return;
  }

  // Si eligió "Otros", exige que escriba qué tipo
  if (tipo === 'otros' && !tipoOtros) {
    msg.style.display = 'block';
    msg.style.background = '#FEE2E2';
    msg.style.color = '#DC2626';
    msg.textContent = 'Especifica qué tipo de proveedor eres (campo "Otros")';
    document.getElementById('pf-tipo-otros')?.focus();
    return;
  }

  const todoSel = document.querySelector('.zona-pill-todo')?.classList.contains('selected');
  const zonas = todoSel
    ? ['Todo el país']
    : [...document.querySelectorAll('#zonas-panel .zona-pill.selected')]
        .filter(p => p.dataset.zona !== 'Todo el país')
        .map(p => p.dataset.zona);

  btn.disabled = true;
  btn.textContent = 'Registrando...';
  msg.style.display = 'none';

  try {
    const lat = parseFloat(document.getElementById('pf-latitud').value) || null;
    const lng = parseFloat(document.getElementById('pf-longitud').value) || null;

    // Si tipo es "otros", añade el texto libre a categorias y a descripcion
    const descripcionBase = document.getElementById('pf-descripcion').value.trim();
    const descripcionFinal = (tipo === 'otros' && tipoOtros)
      ? (descripcionBase ? `[${tipoOtros}] ${descripcionBase}` : `[${tipoOtros}]`)
      : descripcionBase;
    const categoriasFinal = (tipo === 'otros' && tipoOtros)
      ? [...pfSubcatsSeleccionadas, tipoOtros]
      : pfSubcatsSeleccionadas;

    const res = await fetch(`${API}/api/mercado/proveedores`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        nombre_empresa:  nombre,
        tipo,
        tipo_otros:      tipo === 'otros' ? tipoOtros : null,
        descripcion:     descripcionFinal,
        whatsapp:        wa,
        telefono:        wa,
        rnc:             document.getElementById('pf-rnc').value.trim(),
        email_contacto:  document.getElementById('pf-email').value.trim(),
        zona_geografica: zonas,
        categorias:      categoriasFinal,
        rnc_verificado:  rncVerificado,
        logo_base64:     pfLogoBase64,
        direccion:       document.getElementById('pf-direccion').value.trim(),
        horario:         document.getElementById('pf-horario').value.trim(),
        instagram:       document.getElementById('pf-instagram').value.trim().replace('@',''),
        facebook:        document.getElementById('pf-facebook').value.trim(),
        website:         document.getElementById('pf-website').value.trim(),
        latitud:         lat,
        longitud:        lng,
      })
    });
    const data = await res.json();
    if (data.ok) {
      cerrarFormProveedor();
      mostrarToast('Proveedor registrado exitosamente');
      cargarMercado();
    } else {
      throw new Error(data.detail || 'Error desconocido');
    }
  } catch(e) {
    msg.style.display = 'block';
    msg.style.background = '#FEE2E2';
    msg.style.color = '#DC2626';
    msg.textContent = `Error: ${e.message}`;
  } finally {
    btn.disabled = false;
    btn.textContent = 'Registrar proveedor';
  }
}

// ── DESCARGA PDF ARTÍCULOS ────────────────────────────────
async function descargarArticulosPDF(codigo) {
  try {
    mostrarToast('Generando PDF...');
    const res  = await fetch(`${API}/api/procesos/${codigo}`);
    const data = await res.json();
    const p    = data.proceso;
    const arts = data.articulos || [];

    const { jsPDF } = window.jspdf;
    const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });

    // ── Encabezado ──
    doc.setFillColor(23, 47, 70);
    doc.rect(0, 0, 210, 28, 'F');
    doc.setTextColor(46, 204, 113);
    doc.setFontSize(13); doc.setFont('helvetica', 'bold');
    doc.text('LicitacionLab', 14, 11);
    doc.setTextColor(143, 174, 201);
    doc.setFontSize(8); doc.setFont('helvetica', 'normal');
    doc.text('Inteligencia en Compras Públicas — República Dominicana', 14, 17);

    // ── Info del proceso ──
    doc.setTextColor(240, 248, 255);
    doc.setFontSize(10); doc.setFont('helvetica', 'bold');
    const titulo = (p.titulo || codigo).substring(0, 90);
    const tituloLines = doc.splitTextToSize(titulo, 182);
    doc.text(tituloLines, 14, 24);

    let y = 34;
    doc.setTextColor(30, 30, 30);
    doc.setFontSize(8); doc.setFont('helvetica', 'normal');
    doc.text(`Código: ${p.codigo_proceso}`, 14, y);
    doc.text(`Institución: ${p.unidad_compra || '—'}`, 80, y);
    const monto = p.monto_estimado ? `RD$ ${Number(p.monto_estimado).toLocaleString('es-DO')}` : '—';
    doc.text(`Monto estimado: ${monto}`, 14, y + 5);
    const fCierre = p.fecha_fin_recepcion_ofertas ? new Date(p.fecha_fin_recepcion_ofertas).toLocaleDateString('es-DO') : '—';
    doc.text(`Cierre de ofertas: ${fCierre}`, 80, y + 5);
    y += 14;

    // ── Tabla de artículos ──
    doc.setFontSize(9); doc.setFont('helvetica', 'bold');
    doc.setTextColor(46, 204, 113);
    doc.text(`ARTÍCULOS DEL PROCESO (${arts.length})`, 14, y);
    y += 4;

    const rows = arts.map(a => [
      a.descripcion_articulo || a.descripcion_usuario || 'Sin descripción',
      `${a.cantidad || '—'} ${a.unidad_medida || ''}`.trim(),
      a.clase_unspsc || '—',
      a.precio_total_estimado ? 'RD$' + Number(a.precio_total_estimado).toLocaleString('es-DO') : '—'
    ]);

    doc.autoTable({
      startY: y,
      head: [['Descripción', 'Cantidad', 'UNSPSC', 'Precio']],
      body: rows,
      styles: { fontSize: 8, cellPadding: 3, textColor: [30,30,30] },
      headStyles: { fillColor: [23,47,70], textColor: [46,204,113], fontStyle: 'bold', fontSize: 8 },
      alternateRowStyles: { fillColor: [245,248,252] },
      columnStyles: {
        0: { cellWidth: 90 },
        1: { cellWidth: 28, halign: 'center' },
        2: { cellWidth: 28, halign: 'center' },
        3: { cellWidth: 36, halign: 'right' }
      },
      margin: { left: 14, right: 14 },
    });

    // ── Total ──
    const finalY = doc.lastAutoTable.finalY + 4;
    const total = arts.reduce((s, a) => s + (Number(a.precio_total_estimado) || 0), 0);
    if (total > 0) {
      doc.setFont('helvetica', 'bold'); doc.setFontSize(9);
      doc.setTextColor(30,30,30);
      doc.text('Total estimado:', 140, finalY);
      doc.setTextColor(46, 204, 113);
      doc.text(`RD$ ${total.toLocaleString('es-DO')}`, 196, finalY, { align: 'right' });
    }

    // ── Pie ──
    doc.setFont('helvetica', 'normal'); doc.setFontSize(7);
    doc.setTextColor(150,150,150);
    doc.text(`Generado por LicitacionLab · ${new Date().toLocaleDateString('es-DO')}`, 14, 290);
    doc.text('licitacionlab.com', 196, 290, { align: 'right' });

    doc.save(`${codigo}_articulos.pdf`);
    mostrarToast('PDF descargado');
  } catch(e) {
    console.error('Error PDF:', e);
    mostrarToast('Error generando PDF');
  }
}

// ── DESCARGAR PDF DEL ANÁLISIS IA ────────────────────────
async function descargarAnalisisPDF(codigo) {
  const btn = document.getElementById(`btn-pdf-analisis-${codigo}`);
  if (btn) btn.classList.add('cargando');
  mostrarToast('Generando PDF del análisis...');

  try {
    const res = await fetch(`${API}/analisis/${encodeURIComponent(codigo)}/pdf`);

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Error ${res.status}`);
    }

    // Descargar el blob como archivo
    const blob = await res.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `Analisis_${codigo.replace(/\//g, '-')}.pdf`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    mostrarToast('PDF descargado');
  } catch(e) {
    console.error('Error descargando análisis PDF:', e);
    mostrarToast('Error: ' + (e.message || 'No se pudo generar el PDF'));
  } finally {
    if (btn) btn.classList.remove('cargando');
  }
}

// ── GRÁFICAS CON CANVAS ─────────────────────────────────────
function toggleGraficas() {
  const body    = document.getElementById('seg-graficas-body');
  const chevron = document.getElementById('seg-graficas-chevron');
  if (!body) return;
  const open = body.classList.toggle('visible');
  chevron?.classList.toggle('open', open);
  if (open) renderGraficas();
}

function renderGraficas() {
  if (!seguimientoData?.length) return;
  const todos   = seguimientoData;
  const ganados = todos.filter(d => d.estado === 'adjudicado');
  const noAdj   = todos.filter(d => d.estado === 'no-adj');
  const activos = todos.filter(d => !['adjudicado','no-adj'].includes(d.estado || 'analizando'));

  // Colores CSS var resueltos
  const getVar = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
  const GREEN  = '#22c55e';
  const AMBER  = '#f59e0b';
  const RED    = '#ef4444';
  const BLUE   = '#3b82f6';
  const MUTED  = getVar('--muted') || '#6b7280';
  const BG3    = getVar('--bg3') || '#1e2a1e';
  const TEXT   = getVar('--text') || '#f0fdf4';

  // ── 1. DONA: tasa de éxito ──────────────────────────────
  (function() {
    const cv = document.getElementById('chart-tasa');
    if (!cv) return;
    const W = cv.parentElement.offsetWidth || 300;
    cv.width  = W;
    cv.height = 160;
    const ctx = cv.getContext('2d');
    ctx.clearRect(0, 0, W, 160);

    const vals  = [ganados.length, noAdj.length, activos.length];
    const total = vals.reduce((a,b) => a+b, 0) || 1;
    const colors = [GREEN, RED, AMBER];
    const labels = ['Ganados','No adj.','Activos'];
    const cx = W/2, cy = 80, R = 60, r = 38;

    let start = -Math.PI/2;
    vals.forEach((v, i) => {
      const angle = (v / total) * 2 * Math.PI;
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.arc(cx, cy, R, start, start + angle);
      ctx.closePath();
      ctx.fillStyle = colors[i];
      ctx.fill();
      start += angle;
    });
    // Hueco interior
    ctx.beginPath();
    ctx.arc(cx, cy, r, 0, 2*Math.PI);
    ctx.fillStyle = getVar('--bg2') || '#111a11';
    ctx.fill();
    // Texto central
    const tasa = (ganados.length + noAdj.length) > 0
      ? Math.round((ganados.length / (ganados.length + noAdj.length)) * 100)
      : null;
    ctx.fillStyle = TEXT;
    ctx.textAlign = 'center';
    ctx.font = `bold 22px var(--font-body, sans-serif)`;
    ctx.fillText(tasa !== null ? tasa+'%' : '—', cx, cy + 4);
    ctx.font = `10px var(--font-body, sans-serif)`;
    ctx.fillStyle = MUTED;
    ctx.fillText('win rate', cx, cy + 18);

    // Leyenda
    const ley = document.getElementById('ley-tasa');
    if (ley) {
      ley.innerHTML = labels.map((l,i) => `
        <div class="seg-ley-item">
          <div class="seg-ley-dot" style="background:${colors[i]}"></div>
          <span>${l}: ${vals[i]}</span>
        </div>`).join('');
    }
  })();

  // ── 2. BARRAS: procesos por institución ─────────────────
  (function() {
    const cv = document.getElementById('chart-inst');
    if (!cv) return;
    // Agrupar por institución
    const instMap = {};
    todos.forEach(d => {
      const inst = (d.proceso?.unidad_compra || 'Desconocido').split(' ').slice(0,2).join(' ');
      instMap[inst] = (instMap[inst] || 0) + 1;
    });
    const sorted = Object.entries(instMap).sort((a,b) => b[1]-a[1]).slice(0, 8);
    if (!sorted.length) return;

    const W    = cv.parentElement.offsetWidth || 300;
    const rowHi = 44;
    const H    = sorted.length * rowHi + 10;
    cv.width   = W;
    cv.height  = H;
    const ctx  = cv.getContext('2d');
    ctx.clearRect(0, 0, W, H);

    const maxVal = sorted[0][1];
    const barH   = 18;
    const PADi   = 6;

    sorted.forEach(([inst, cnt], i) => {
      const baseY = i * rowHi + 6;
      // Etiqueta completa arriba a la izquierda
      ctx.fillStyle = MUTED;
      ctx.font = `10px var(--font-body, sans-serif)`;
      ctx.textAlign = 'left';
      ctx.fillText(inst, PADi, baseY + 11);
      // Número a la derecha
      ctx.textAlign = 'right';
      ctx.fillStyle = GREEN;
      ctx.font = `bold 10px var(--font-body, sans-serif)`;
      ctx.fillText(cnt, W - PADi, baseY + 11);
      // Barra fondo
      const barY = baseY + 18;
      const barW = W - PADi * 2;
      ctx.fillStyle = BG3;
      ctx.beginPath();
      ctx.roundRect(PADi, barY, barW, barH, 4);
      ctx.fill();
      // Barra valor
      const w = Math.max(6, (cnt / maxVal) * barW);
      ctx.fillStyle = GREEN;
      ctx.globalAlpha = 0.85;
      ctx.beginPath();
      ctx.roundRect(PADi, barY, w, barH, 4);
      ctx.fill();
      ctx.globalAlpha = 1;
    });
  })();

  // ── 3. LÍNEA: monto ganado en el tiempo ─────────────────
  (function() {
    const cv = document.getElementById('chart-monto');
    if (!cv) return;
    if (!ganados.length) {
      const W = cv.parentElement.offsetWidth || 300;
      cv.width = W; cv.height = 80;
      const ctx = cv.getContext('2d');
      ctx.fillStyle = MUTED;
      ctx.textAlign = 'center';
      ctx.font = '12px var(--font-body, sans-serif)';
      ctx.fillText('Sin contratos adjudicados aún', W/2, 44);
      return;
    }
    // Agrupar por mes
    const meses = {};
    ganados.forEach(d => {
      const fecha = d.proceso?.fecha_fin_recepcion_ofertas || d.created_at;
      if (!fecha) return;
      const key = fecha.slice(0, 7); // YYYY-MM
      meses[key] = (meses[key] || 0) + Number(d.proceso?.monto_estimado || 0);
    });
    const keys = Object.keys(meses).sort();
    if (keys.length < 2) keys.unshift(''); // pad para mostrar algo
    const vals = keys.map(k => meses[k] || 0);
    const maxV = Math.max(...vals) || 1;

    const W    = cv.parentElement.offsetWidth || 300;
    const H    = 160;
    const PAD  = { t:16, b:28, l:10, r:10 };
    cv.width   = W; cv.height = H;
    const ctx  = cv.getContext('2d');
    ctx.clearRect(0, 0, W, H);

    const n   = keys.length;
    const stepX = (W - PAD.l - PAD.r) / Math.max(n-1, 1);
    const pts = keys.map((k, i) => ({
      x: PAD.l + i * stepX,
      y: PAD.t + (1 - vals[i] / maxV) * (H - PAD.t - PAD.b),
    }));

    // Área relleno
    ctx.beginPath();
    ctx.moveTo(pts[0].x, H - PAD.b);
    pts.forEach(p => ctx.lineTo(p.x, p.y));
    ctx.lineTo(pts[pts.length-1].x, H - PAD.b);
    ctx.closePath();
    const grad = ctx.createLinearGradient(0, PAD.t, 0, H - PAD.b);
    grad.addColorStop(0, 'rgba(34,197,94,0.35)');
    grad.addColorStop(1, 'rgba(34,197,94,0.02)');
    ctx.fillStyle = grad;
    ctx.fill();

    // Línea
    ctx.beginPath();
    pts.forEach((p, i) => i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y));
    ctx.strokeStyle = GREEN;
    ctx.lineWidth   = 2;
    ctx.stroke();

    // Puntos
    pts.forEach(p => {
      ctx.beginPath();
      ctx.arc(p.x, p.y, 3, 0, 2*Math.PI);
      ctx.fillStyle = GREEN;
      ctx.fill();
    });

    // Etiquetas eje X
    ctx.fillStyle = MUTED;
    ctx.textAlign = 'center';
    ctx.font = '9px var(--font-body, sans-serif)';
    keys.forEach((k, i) => {
      if (!k) return;
      const [y, m] = k.split('-');
      ctx.fillText(`${m}/${y.slice(2)}`, pts[i].x, H - 6);
    });
  })();

  // ── 4. BARRAS HORIZONTALES: tiempo preparación ──────────
  (function() {
    const cv = document.getElementById('chart-tiempo');
    if (!cv) return;
    const pares = todos
      .filter(d => d.created_at && d.proceso?.fecha_fin_recepcion_ofertas && d.proceso?.titulo)
      .map(d => {
        const dias = Math.round(
          (new Date(d.proceso.fecha_fin_recepcion_ofertas) - new Date(d.created_at)) / 86400000
        );
        return { label: d.proceso.titulo, dias: dias > 0 ? dias : 0 };
      })
      .filter(p => p.dias > 0)
      .sort((a,b) => b.dias - a.dias)
      .slice(0, 7);

    if (!pares.length) {
      const W = cv.parentElement.offsetWidth || 300;
      cv.width = W; cv.height = 60;
      const ctx = cv.getContext('2d');
      ctx.fillStyle = MUTED;
      ctx.textAlign = 'center';
      ctx.font = '12px var(--font-body, sans-serif)';
      ctx.fillText('Sin datos suficientes', W/2, 36);
      return;
    }

    const W    = cv.parentElement.offsetWidth || 300;
    const rowH2 = 44;
    const H    = pares.length * rowH2 + 10;
    cv.width   = W; cv.height = H;
    const ctx  = cv.getContext('2d');
    ctx.clearRect(0, 0, W, H);

    const maxDias = pares[0].dias;
    const barH    = 18;
    const PAD2    = 6;

    pares.forEach(({ label, dias }, i) => {
      const baseY = i * rowH2 + 6;
      // Etiqueta completa arriba
      ctx.fillStyle = MUTED;
      ctx.font = '10px var(--font-body, sans-serif)';
      ctx.textAlign = 'left';
      // Truncar solo si es muy largo (nombre completo del proceso)
      const maxChars = Math.floor((W - 50) / 6);
      const txt = label.length > maxChars ? label.slice(0, maxChars-1)+'…' : label;
      ctx.fillText(txt, PAD2, baseY + 11);
      // Días a la derecha
      const color = dias <= 7 ? RED : dias <= 14 ? AMBER : BLUE;
      ctx.textAlign = 'right';
      ctx.fillStyle = color;
      ctx.font = 'bold 10px var(--font-body, sans-serif)';
      ctx.fillText(dias+'d', W - PAD2, baseY + 11);
      // Barra fondo
      const barY = baseY + 18;
      const barW = W - PAD2 * 2;
      ctx.fillStyle = BG3;
      ctx.beginPath();
      ctx.roundRect(PAD2, barY, barW, barH, 4);
      ctx.fill();
      // Barra valor
      const w = Math.max(6, (dias / maxDias) * barW);
      ctx.fillStyle = color;
      ctx.globalAlpha = 0.8;
      ctx.beginPath();
      ctx.roundRect(PAD2, barY, w, barH, 4);
      ctx.fill();
      ctx.globalAlpha = 1;
    });
  })();
}
// ────────────────────────────────────────────────────────────

// ── HISTORIAL DE ANÁLISIS ───────────────────────────────────
async function cargarPerfilEmpresa() {
  if (!currentUser) return;
  try {
    const res = await fetch(`${API}/api/perfil/empresa?user_id=${currentUser.id}`);
    const data = await res.json();
    window._perfilEmpresa = data.perfil?.onboarding_completado ? data.perfil : null;
  } catch(e) {
    window._perfilEmpresa = null;
  }
}

// ── RE-ANALIZAR CON ENMIENDA ─────────────────────────────────────────────
async function solicitarReanalisis(codigo) {
  const btn = document.getElementById(`btn-reanalizar-${codigo}`);
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Iniciando…'; }

  try {
    const res = await fetch(`${API}/api/procesos/${codigo}/reanalizar`, { method: 'POST' });
    const data = await res.json();

    if (data.status === 'ya_procesando') {
      mostrarToast('⏳ El análisis ya está en proceso', 'info');
      if (btn) { btn.disabled = false; btn.innerHTML = '⚠ Re-analizar con enmienda'; }
      return;
    }
    if (!res.ok) throw new Error(data.detail || 'Error');

    mostrarToast(`⚡ Re-análisis iniciado · Enmienda #${data.enmienda_numero}${data.enmienda_nombre ? ' · ' + data.enmienda_nombre.substring(0, 30) : ''}`, 'success');
    if (btn) btn.style.display = 'none';
    iniciarPollingAnalisis(codigo);

  } catch(e) {
    mostrarToast('Error iniciando re-análisis. Intenta de nuevo.', 'error');
    if (btn) { btn.disabled = false; btn.innerHTML = '⚠ Re-analizar con enmienda'; }
  }
}

async function cargarHistorialAnalisis() {
  const lista = document.getElementById('historial-analisis-lista');
  if (!lista || !currentUser?.id) return;
  lista.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted);font-size:13px"><div class="spinner" style="margin:0 auto 8px"></div>Cargando...</div>';
  try {
    const res  = await fetch(`${API}/api/mis-analisis?user_id=${currentUser.id}`);
    const data = await res.json();
    const items = data.analisis || [];
    if (!items.length) {
      lista.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted);font-size:13px">Sin análisis aún. Sigue un proceso para que LicitacionLab analice su pliego.</div>';
      return;
    }
    const etiquetas = { completado: 'Completado', procesando: 'Analizando', pendiente_analisis: 'En cola', error: 'Error' };
    lista.innerHTML = items.map(a => {
      const fecha = a.actualizado_en ? new Date(a.actualizado_en).toLocaleDateString('es-DO', { day:'2-digit', month:'short', year:'numeric' }) : '—';
      const monto = a.monto_estimado ? (Number(a.monto_estimado) >= 1_000_000 ? `RD$${(Number(a.monto_estimado)/1_000_000).toFixed(1)}M` : `RD$${(Number(a.monto_estimado)/1_000).toFixed(0)}K`) : '';
      const estado = a.estado || 'pendiente_analisis';
      return `
      <div class="historial-card" onclick="verDetalle('${a.proceso_id}')">
        <div class="historial-card-titulo">${sanitize(a.titulo)}</div>
        <div class="historial-card-meta">${sanitize(a.unidad_compra)}${monto ? ' · '+monto : ''}</div>
        ${a.resumen ? `<div class="historial-resumen">${sanitize(a.resumen)}</div>` : ''}
        <div class="historial-card-footer" style="margin-top:8px">
          <span class="historial-card-estado ${estado}">${etiquetas[estado] || estado}</span>
          <span class="historial-card-fecha">${fecha}</span>
        </div>
      </div>`;
    }).join('');
  } catch(e) {
    lista.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted);font-size:13px">Error cargando historial.</div>';
  }
}
// ─────────────────────────────────────────────────────────────

// ── MODAL PRICING ─────────────────────────────────────────
function abrirPricing() {
  const m = document.getElementById('modal-pricing');
  if (m) { m.style.display = 'block'; document.body.style.overflow = 'hidden'; }
}
function cerrarPricing() {
  const m = document.getElementById('modal-pricing');
  if (m) { m.style.display = 'none'; document.body.style.overflow = ''; }
}

// ── BANNER BETA ──────────────────────────────────────────
function cerrarBanner() {
  const b = document.getElementById('beta-banner');
  if (b) b.style.display = 'none';
  localStorage.setItem('beta_banner_cerrado', '1');
}
function initBanner() {
  if (localStorage.getItem('beta_banner_cerrado') === '1') {
    const b = document.getElementById('beta-banner');
    if (b) b.style.display = 'none';
  }
}


// ── ONBOARDING WIZARD ────────────────────────────────────────────────────
let onbPaso = 0;
const ONB_TOTAL = 4;
let onbEspecialidades = [];
let onbModalidades = [];
let onbProvincias = [];
let onbObras = [];
let onbRubros = [];
let onbCategorias = [];

// ── SHEET TUTORIAL (onboarding anterior — 6 slides) ──────────────────────
let tutorialPaso = 0;
const TUTORIAL_TOTAL = 8;

function mostrarTutorial() {
  tutorialPaso = 0;
  document.getElementById('onb-tutorial-overlay').classList.remove('oculto');
  renderTutorialPaso();
}

function cerrarTutorial(abrirPerfil) {
  document.getElementById('onb-tutorial-overlay').classList.add('oculto');
  localStorage.setItem('tutorial_completado', '1');
  _setCookie('tutorial_completado', '1', 365);
  if (abrirPerfil && !localStorage.getItem('onb_completado') && currentUser?.id) {
    setTimeout(mostrarWizardPerfil, 400);
  }
}

function tutorialSiguiente() {
  if (tutorialPaso < TUTORIAL_TOTAL - 1) {
    tutorialPaso++;
    renderTutorialPaso();
  } else {
    cerrarTutorial(true); // al terminar → abre wizard perfil
  }
}

function tutorialAnterior() {
  if (tutorialPaso > 0) { tutorialPaso--; renderTutorialPaso(); }
}

function renderTutorialPaso() {
  for (let i = 0; i < TUTORIAL_TOTAL; i++) {
    const s = document.getElementById('onb-slide-' + i);
    if (s) s.classList.toggle('visible', i === tutorialPaso);
    const d = document.getElementById('onbdot-' + i);
    if (!d) continue;
    d.classList.remove('active', 'done', 'is-active');
    if (i < tutorialPaso)          d.classList.add('done');
    else if (i === tutorialPaso)   d.classList.add('is-active');
  }
  const btn  = document.getElementById('onb-btn-next');
  const skip = document.getElementById('onb-btn-skip');
  const back = document.getElementById('onb-btn-back');
  if (btn)  btn.textContent = tutorialPaso === TUTORIAL_TOTAL - 1 ? '¡Empezar!' : 'Siguiente →';
  if (skip) skip.style.visibility = tutorialPaso === TUTORIAL_TOTAL - 1 ? 'hidden' : 'visible';
  if (back) back.style.display = tutorialPaso > 0 ? 'block' : 'none';
}

// ── WIZARD PERFIL EMPRESA (5 pasos) ──────────────────────────────────────
function mostrarOnboarding() { mostrarWizardPerfil(); } // alias retrocompatible

// ── Filtrar catálogo UNSPSC por búsqueda textual ──────────────
function filtrarCatalogoUnspsc(q) {
  const sel = document.getElementById('onb-cat-select');
  if (!sel) return;
  const texto = q.trim().toLowerCase();
  let primerMatch = null;
  Array.from(sel.options).forEach(opt => {
    if (!opt.value) { opt.style.display = ''; return; }
    const match = !texto || opt.text.toLowerCase().includes(texto) || opt.value.toLowerCase().includes(texto);
    opt.style.display = match ? '' : 'none';
    if (match && !primerMatch) primerMatch = opt.value;
  });
  // Seleccionar primer match automáticamente para que sea clickeable
  const visibles = Array.from(sel.options).filter(o => o.value && o.style.display !== 'none');
  if (visibles.length >= 1) sel.value = visibles[0].value;
  else sel.value = '';
}

function agregarCatBusqueda() {
  // Agrega la categoría seleccionada en el select (después de buscar)
  const sel = document.getElementById('onb-cat-select');
  if (sel && sel.value) {
    agregarCategoriaSelect(sel);
    // Limpiar búsqueda y reset opciones
    const inp = document.getElementById('onb-cat-search');
    if (inp) inp.value = '';
    filtrarCatalogoUnspsc('');
  }
}

// ── Activar notificaciones desde el onboarding ──────────────
async function activarNotifOnboarding() {
  const btn = document.getElementById('onb-push-btn');
  const txt = document.getElementById('onb-push-status-text');
  const card = document.getElementById('onb-push-card');

  // Si ya está activo, desactivar
  if (btn && btn.dataset.activo === '1') {
    await togglePush();
    btn.textContent = 'Activar';
    btn.style.background = 'var(--green)';
    btn.dataset.activo = '0';
    if (txt) txt.textContent = 'Recibirás alertas en tiempo real sobre fechas clave y nuevas licitaciones.';
    if (card) card.style.borderColor = 'var(--border)';
    return;
  }

  if (btn) { btn.textContent = 'Activando…'; btn.disabled = true; }
  try {
    await togglePush();
    // Verificar si quedó activa
    const reg = await navigator.serviceWorker.getRegistration();
    const sub = reg ? await reg.pushManager.getSubscription() : null;
    if (sub) {
      if (btn) { btn.textContent = '✓ Activadas'; btn.style.background = 'var(--green-mid)'; btn.dataset.activo = '1'; btn.disabled = false; }
      if (txt) txt.textContent = 'Notificaciones activas — recibirás alertas de nuevas licitaciones y fechas clave.';
      if (card) card.style.borderColor = 'var(--green-border)';
      mostrarToast('Notificaciones activadas', 'success');
    } else {
      if (btn) { btn.textContent = 'Activar'; btn.disabled = false; }
      if (txt) txt.textContent = 'No se pudo activar. Verifica que tu navegador permita notificaciones.';
    }
  } catch(e) {
    if (btn) { btn.textContent = 'Activar'; btn.disabled = false; }
    console.error('Error activando notif:', e);
  }
}

// Verificar estado push al abrir el paso 5
async function verificarEstadoPushOnb() {
  const btn = document.getElementById('onb-push-btn');
  const txt = document.getElementById('onb-push-status-text');
  const card = document.getElementById('onb-push-card');
  if (!btn) return;
  try {
    const reg = await navigator.serviceWorker.getRegistration();
    const sub = reg ? await reg.pushManager.getSubscription() : null;
    if (sub) {
      btn.textContent = '✓ Activadas'; btn.style.background = 'var(--green-mid)'; btn.dataset.activo = '1';
      if (txt) txt.textContent = 'Notificaciones activas — recibirás alertas de nuevas licitaciones y fechas clave.';
      if (card) card.style.borderColor = 'var(--green-border)';
    }
  } catch(e) {}
}

function mostrarWizardPerfil() {
  onbPaso = 0;
  document.getElementById('onb-overlay').classList.remove('oculto');
  document.querySelectorAll('.dsb-nav-item').forEach(b => b.classList.remove('active'));
  renderOnbPaso();
}

function cerrarOnboarding() {
  document.getElementById('onb-overlay').classList.add('oculto');
}

function saltarOnboarding() {
  cerrarOnboarding();
  _marcarOnbCompletado();
}

function irAPaso(n) {
  onbPaso = n;
  renderOnbPaso();
}

function actualizarSidebarEmpresa() {
  const rs = document.getElementById('onb-razon-social')?.value.trim();
  const el = document.getElementById('onb-sidebar-empresa');
  if (el && rs) el.textContent = `Cuéntanos sobre ${rs}.`;
}

function toggleCheckCard(cardId, checkbox) {
  document.getElementById(cardId)?.classList.toggle('active', checkbox.checked);
}

function toggleEspecialidad(btn, val) {
  if (btn.classList.contains('active')) {
    btn.classList.remove('active');
    onbEspecialidades = onbEspecialidades.filter(e => e !== val);
  } else {
    if (onbEspecialidades.length >= 5) { mostrarToast('Máximo 5 especialidades', 'warning'); return; }
    btn.classList.add('active');
    onbEspecialidades.push(val);
  }
}

function toggleModalidad(btn, val) {
  btn.classList.toggle('active');
  if (btn.classList.contains('active')) onbModalidades.push(val);
  else onbModalidades = onbModalidades.filter(m => m !== val);
}

function toggleRubro(btn, val) {
  btn.classList.toggle('active');
  if (btn.classList.contains('active')) onbRubros.push(val);
  else onbRubros = onbRubros.filter(r => r !== val);
}

function toggleCategoria(btn, codigo, nombre) {
  // mantenemos por compatibilidad aunque ya no se usa con chips
  if (!onbCategorias.find(c => c.codigo === codigo)) {
    onbCategorias.push({ codigo, nombre });
    renderCategoriaTags();
  }
}

// Nueva: agrega categoría desde el <select>
function agregarCategoriaSelect(sel) {
  const val = sel.value;
  if (!val) return;
  const [codigo, nombre] = val.split('|');
  if (!onbCategorias.find(c => c.codigo === codigo)) {
    onbCategorias.push({ codigo, nombre });
    renderCategoriaTags();
  }
  sel.value = ''; // reset select
}

function renderCategoriaTags() {
  const cont = document.getElementById('onb-categorias-tags');
  if (!cont) return;
  cont.innerHTML = onbCategorias.map((c, i) =>
    `<span style="display:inline-flex;align-items:center;gap:5px;background:var(--green-dim);border:1px solid var(--green-border);border-radius:var(--r-full);padding:4px 10px;font-size:12px;font-weight:600;color:var(--green);">
      ${c.codigo.substring(0,2)} · ${c.nombre.split(',')[0].substring(0,28)}
      <button type="button" onclick="quitarCategoria(${i})" style="background:none;border:none;cursor:pointer;color:var(--green);font-size:14px;line-height:1;padding:0 2px;">×</button>
    </span>`
  ).join('');
}

function quitarCategoria(i) {
  onbCategorias.splice(i, 1);
  renderCategoriaTags();
}

// Nueva: agrega provincia desde el <select>
function agregarProvinciaSelect(sel) {
  const val = sel.value;
  if (!val) return;
  if (!onbProvincias.includes(val)) {
    onbProvincias.push(val);
    renderProvinciasTags();
  }
  sel.value = '';
}

function seleccionarTodasProvincias() {
  const todas = ['Distrito Nacional','Santo Domingo','Santiago','La Vega','San Pedro de Macorís',
    'La Romana','San Cristóbal','Puerto Plata','Duarte','La Altagracia','Monte Plata','Espaillat',
    'Barahona','San Juan','Azua','Peravia','Monseñor Nouel','Sánchez Ramírez','Hato Mayor',
    'El Seibo','Valverde','María Trinidad Sánchez','Samaná','Monte Cristi','Dajabón',
    'Santiago Rodríguez','Elías Piña','San José de Ocoa','Independencia','Baoruco',
    'Pedernales','Hermanas Mirabal'];
  onbProvincias = [...todas];
  renderProvinciasTags();
}

function renderProvinciasTags() {
  const cont = document.getElementById('onb-provincias-tags');
  if (!cont) return;
  if (onbProvincias.length === 32) {
    cont.innerHTML = `<span style="display:inline-flex;align-items:center;gap:5px;background:var(--green-dim);border:1px solid var(--green-border);border-radius:var(--r-full);padding:4px 12px;font-size:12px;font-weight:700;color:var(--green);">
      Todo el país ✓ <button type="button" onclick="onbProvincias=[];renderProvinciasTags();" style="background:none;border:none;cursor:pointer;color:var(--green);font-size:14px;padding:0 2px;">×</button>
    </span>`;
    return;
  }
  cont.innerHTML = onbProvincias.map((p, i) =>
    `<span style="display:inline-flex;align-items:center;gap:5px;background:var(--surface);border:1px solid var(--border);border-radius:var(--r-full);padding:4px 10px;font-size:12px;font-weight:600;color:var(--text2);">
      ${p}
      <button type="button" onclick="quitarProvincia(${i})" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:14px;line-height:1;padding:0 2px;">×</button>
    </span>`
  ).join('');
}

function quitarProvincia(i) {
  onbProvincias.splice(i, 1);
  renderProvinciasTags();
}

// Compatibilidad legacy (chips)
function toggleProvincia(btn, val) {
  if (!onbProvincias.includes(val)) onbProvincias.push(val);
  renderProvinciasTags();
}

function toggleTodasProvincias() { seleccionarTodasProvincias(); }

function abrirModalObra() { mostrarFormObraInline(); } // alias compatibilidad

function mostrarFormObraInline() {
  const form = document.getElementById('onb-obra-inline');
  const btn  = document.getElementById('onb-btn-agregar-obra');
  if (!form) return;
  ['obra-nombre','obra-institucion','obra-monto','obra-meses','obra-anio'].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = '';
  });
  form.style.display = 'block';
  if (btn) btn.style.display = 'none';
  document.getElementById('obra-nombre')?.focus();
}

function cancelarObraInline() {
  const form = document.getElementById('onb-obra-inline');
  const btn  = document.getElementById('onb-btn-agregar-obra');
  if (form) form.style.display = 'none';
  if (btn)  btn.style.display  = 'flex';
}

function guardarObraInline() {
  const nombre = document.getElementById('obra-nombre')?.value.trim();
  const inst   = document.getElementById('obra-institucion')?.value.trim();
  const monto  = Number(document.getElementById('obra-monto')?.value);
  const meses  = Number(document.getElementById('obra-meses')?.value);
  const anio   = document.getElementById('obra-anio')?.value;
  if (!nombre) { mostrarToast('Ingresa el nombre de la obra', 'warning'); return; }
  onbObras.push({ nombre, institucion: inst, monto, meses, anio });
  renderObras();
  cancelarObraInline();
}

function cerrarModalObra() { cancelarObraInline(); } // alias

function guardarObra() { guardarObraInline(); } // alias

function renderObras() {
  const lista = document.getElementById('onb-obras-lista');
  if (!lista) return;
  lista.innerHTML = onbObras.map((o, i) => {
    const montoFmt = o.monto >= 1e6 ? `RD$${(o.monto/1e6).toFixed(0)}M` : o.monto ? `RD$${o.monto.toLocaleString()}` : '';
    return `<div class="onb-obra-item">
      <div>
        <div style="font-size:13px;font-weight:700;">${sanitize(o.nombre)}${o.institucion ? ' · ' + sanitize(o.institucion) : ''}</div>
        <div style="font-size:11px;color:var(--muted);">${montoFmt}${o.meses ? ' · ' + o.meses + ' meses' : ''}${o.anio ? ' · ' + o.anio : ''}</div>
      </div>
      <button onclick="eliminarObra(${i})" style="background:none;border:none;color:var(--muted);cursor:pointer;font-size:13px;font-weight:600;font-family:var(--font-body);">Eliminar</button>
    </div>`;
  }).join('');
}

function eliminarObra(i) {
  onbObras.splice(i, 1);
  renderObras();
}

function renderOnbPaso() {
  // Mostrar step correcto
  for (let i = 0; i < ONB_TOTAL; i++) {
    const s = document.getElementById(`onb-step-${i}`);
    if (s) s.style.display = i === onbPaso ? 'block' : 'none';
  }
  // Actualizar nav sidebar
  for (let i = 0; i < ONB_TOTAL; i++) {
    const nav = document.getElementById(`onb-nav-${i}`);
    const num = document.getElementById(`onb-num-${i}`);
    if (!nav || !num) continue;
    nav.classList.remove('active','done');
    if (i < onbPaso) { nav.classList.add('done'); num.textContent = '✓'; }
    else if (i === onbPaso) { nav.classList.add('active'); num.textContent = i+1; }
    else { num.textContent = i+1; }
  }
  // Progreso
  const pct = Math.round(((onbPaso + 1) / ONB_TOTAL) * 100);
  const bar = document.getElementById('onb-progreso-bar');
  const pctEl = document.getElementById('onb-progreso-pct');
  if (bar) bar.style.width = pct + '%';
  if (pctEl) pctEl.textContent = pct + '%';
  // Botones
  const siguiente = document.getElementById('onb-btn-siguiente');
  const atras = document.getElementById('onb-btn-atras');
  const saltar = document.getElementById('onb-btn-saltar');
  if (siguiente) siguiente.textContent = onbPaso === ONB_TOTAL - 1 ? '✅ Guardar perfil' : 'Siguiente →';
  if (atras) atras.style.display = onbPaso > 0 ? 'block' : 'none';
  if (saltar) saltar.style.visibility = onbPaso === ONB_TOTAL - 1 ? 'hidden' : 'visible';
  // Resumen en último paso
  if (onbPaso === ONB_TOTAL - 1) {
    mostrarResumenOnb();
    setTimeout(verificarEstadoPushOnb, 100);
  }
}

function mostrarResumenOnb() {
  const razon = document.getElementById('onb-razon-social')?.value || '—';
  const resumen = document.getElementById('onb-resumen');
  const cont = document.getElementById('onb-resumen-contenido');
  if (resumen) resumen.style.display = 'block';
  const rubrosStr = onbRubros.length ? onbRubros.join(', ') : '—';
  const catStr = onbCategorias.length ? `${onbCategorias.length} categorías seleccionadas` : '—';
  const provStr = onbProvincias.length === 32 ? 'Todo el país' : onbProvincias.length ? `${onbProvincias.length} provincias` : '—';
  if (cont) cont.innerHTML = `
    <b>Empresa:</b> ${sanitize(razon)}<br>
    <b>Rubros:</b> ${rubrosStr}<br>
    <b>Categorías UNSPSC:</b> ${catStr}<br>
    <b>Provincias:</b> ${provStr}<br>
    <b>Obras:</b> ${onbObras.length} registradas
  `;
}

async function onbSiguiente() {
  if (onbPaso < ONB_TOTAL - 1) {
    onbPaso++;
    renderOnbPaso();
  } else {
    await guardarPerfilOnboarding();
  }
}

function onbAnterior() {
  if (onbPaso > 0) { onbPaso--; renderOnbPaso(); }
}

async function guardarPerfilOnboarding() {
  if (!currentUser?.id) { mostrarToast('Error: no hay sesión activa', 'error'); return; }
  const btn = document.getElementById('onb-btn-siguiente');
  if (btn) { btn.disabled = true; btn.textContent = 'Guardando…'; }

  // Parsear montos
  const parseMonto = (v) => {
    if (!v) return null;
    const n = v.replace(/[^\d.]/g, '');
    const mult = v.toUpperCase().includes('M') ? 1e6 : v.toUpperCase().includes('K') ? 1e3 : 1;
    return parseFloat(n) * mult || null;
  };

  const datos = {
    user_id:                currentUser.id,
    razon_social:           document.getElementById('onb-razon-social')?.value.trim() || null,
    email_empresa:          document.getElementById('onb-email-empresa')?.value.trim() || null,
    rnc:                    document.getElementById('onb-rnc')?.value.trim() || null,
    domicilio:              document.getElementById('onb-domicilio')?.value.trim() || null,
    anios_experiencia:      parseInt(document.getElementById('onb-anios')?.value) || null,
    tiene_rpe:              document.getElementById('onb-rpe')?.checked || false,
    tiene_rnc:              document.getElementById('onb-tiene-rnc')?.checked || false,
    tiene_estados_financieros: document.getElementById('onb-estados')?.checked || false,
    rubros:                 onbRubros,
    categorias_unspsc:      onbCategorias,
    modalidades:            onbModalidades,
    provincias:             onbProvincias,
    monto_minimo:           parseMonto(document.getElementById('onb-monto-min')?.value),
    monto_maximo:           parseMonto(document.getElementById('onb-monto-max')?.value),
    plazo_maximo_dias:      parseInt(document.getElementById('onb-plazo')?.value) || null,
    obras_representativas:  onbObras,
    alertas_push:           !!pushSubscription,
    umbral_monto_min:       parseFloat(document.getElementById('onb-umbral-min')?.value) || null,
    onboarding_completado:  true,
    onboarding_paso_actual: 5,
    actualizado_en:         new Date().toISOString(),
  };

  try {
    const res = await fetch(`${API}/api/perfil/empresa`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(datos)
    });
    if (!res.ok) throw new Error('Error guardando');
    window._perfilEmpresa = datos;
    _marcarOnbCompletado();
    cerrarOnboarding();
    mostrarToast('✅ Perfil guardado — el Score IA ahora es personalizado', 'success');
  } catch(e) {
    mostrarToast('Error guardando perfil. Intenta de nuevo.', 'error');
    if (btn) { btn.disabled = false; btn.textContent = '✅ Guardar perfil'; }
  }
}

// Marca el onboarding como completado en localStorage Y cookies
// (iOS borra el localStorage de la PWA — las cookies sobreviven, igual que la sesión)
function _marcarOnbCompletado() {
  localStorage.setItem('onb_completado', '1');
  localStorage.setItem('tutorial_completado', '1');
  _setCookie('onb_completado', '1', 365);
  _setCookie('tutorial_completado', '1', 365);
}

async function initOnboarding() {
  if (!currentUser?.id) return;

  // 1) Rehidratar banderas desde cookies si iOS borró el localStorage
  if (!localStorage.getItem('onb_completado') && _getCookie('onb_completado') === '1') {
    localStorage.setItem('onb_completado', '1');
  }
  if (!localStorage.getItem('tutorial_completado') && _getCookie('tutorial_completado') === '1') {
    localStorage.setItem('tutorial_completado', '1');
  }

  // 2) Atajo local: ya completado en este dispositivo
  if (localStorage.getItem('onb_completado') === '1') return;

  // 3) Fuente de verdad: el servidor. Si el perfil ya está completo en la BD,
  //    NUNCA mostrar el wizard (sobrevive borrado de localStorage y cambio de dispositivo)
  try {
    if (!window._perfilEmpresa) await cargarPerfilEmpresa();
  } catch(e) {}
  if (window._perfilEmpresa?.onboarding_completado) {
    _marcarOnbCompletado();
    return;
  }

  // 4) DESACTIVADO (ago-2026): el wizard ya no se abre solo.
  //    El Expediente Digital es ahora la via principal para completar el perfil;
  //    el wizard sigue disponible manualmente desde el menu (Perfil empresa).
}


// ════════════════════════════════════════════════════════════
// INTELIGENCIA COMPETITIVA
// ════════════════════════════════════════════════════════════
let _icDebounceTimer = null;
let _icEmpresaActual = null;
let _icChartC = null;
let _icChartM = null;

const IC_API = API + '/api/v1/proveedores';


async function cargarSemaforoPago(institucion, intento = 0) {
  const el = document.getElementById('dtab-semaforo-content');
  if (!el) {
    // El panel aún no está en el DOM: reintentar hasta 10 veces (3s en total)
    if (intento < 10) setTimeout(() => cargarSemaforoPago(institucion, intento + 1), 300);
    return;
  }
  if (el.dataset.cargado === '1') return;  // ya está pintado, no repetir
  if (!institucion) { el.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">Institución no identificada.</div>'; return; }
  try {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 12000);
    const r = await fetch('/api/instituciones/semaforo?nombre=' + encodeURIComponent(institucion), { signal: ctl.signal });
    clearTimeout(timer);
    if (!r.ok) throw new Error('HTTP ' + r.status + ' — el backend aún no tiene el endpoint del semáforo. Verifica que main.py esté actualizado en Railway.');
    const d = await r.json();
    if (!d || !d.encontrado) {
      el.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">Sin historial de pagos publicado por la DGCP para esta institución.</div>';
      return;
    }
    const med = Math.round(d.mediana_dias_pago ?? d.promedio_dias_pago ?? 0);
    const color = med <= 30 ? '#16A34A' : med <= 60 ? '#EAB308' : '#DC2626';
    const verdicto = med <= 30 ? 'Paga rápido' : med <= 60 ? 'Paga con demora moderada' : 'Paga lento';
    const fmt = n => (n==null ? '—' : Number(n).toLocaleString('es-DO', {maximumFractionDigits:0}));
    el.innerHTML = `
      <div style="display:flex;align-items:center;gap:14px;margin-bottom:14px;">
        <div style="width:54px;height:54px;border-radius:50%;background:${color}1a;border:2.5px solid ${color};display:flex;align-items:center;justify-content:center;flex-shrink:0;">
          <span style="font-size:17px;font-weight:900;color:${color};font-family:var(--font-mono,monospace);">${med}</span>
        </div>
        <div>
          <div style="font-size:15px;font-weight:800;color:${color};">${verdicto}</div>
          <div style="font-size:12px;color:var(--muted);">Mediana: <b style="color:var(--text);">${med} días</b> para pagar una factura · Periodo ${d.periodo}</div>
        </div>
      </div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px;">
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center;">
          <div style="font-size:16px;font-weight:800;color:var(--text);font-family:var(--font-mono,monospace);">${fmt(d.porc_pagos_20_dias)}%</div>
          <div style="font-size:10px;color:var(--muted);margin-top:2px;">en ≤20 días</div>
        </div>
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center;">
          <div style="font-size:16px;font-weight:800;color:var(--text);font-family:var(--font-mono,monospace);">${fmt(d.porc_pagos_60_dias)}%</div>
          <div style="font-size:10px;color:var(--muted);margin-top:2px;">en ≤60 días</div>
        </div>
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center;">
          <div style="font-size:16px;font-weight:800;color:${(d.porc_fuera_plazo||0)>30?'#DC2626':'var(--text)'};font-family:var(--font-mono,monospace);">${fmt(d.porc_fuera_plazo)}%</div>
          <div style="font-size:10px;color:var(--muted);margin-top:2px;">fuera de plazo</div>
        </div>
      </div>
      <div style="font-size:11px;color:var(--muted);line-height:1.6;">Basado en <b style="color:var(--text);">${fmt(d.cantidad_facturas)}</b> facturas analizadas por la DGCP (InfoPago) · Monto pagado: <b style="color:var(--text);">RD$${fmt(d.monto_total_pagado)}</b>${d.institucion_match ? ` · Institución: ${d.institucion_match}` : ''}</div>`;
    el.dataset.cargado = '1';
  } catch(e) {
    const msg = e.name === 'AbortError' ? 'El servidor tardó demasiado en responder. Intenta de nuevo.' : (e.message || 'No se pudo cargar el historial de pagos.');
    el.innerHTML = `<div style="font-size:13px;color:var(--muted);padding:8px 0;">${msg} <button onclick="cargarSemaforoPago('${(institucion||'').replace(/'/g,"\\'")}')" style="margin-left:8px;background:none;border:1px solid var(--border);border-radius:6px;color:var(--green);font-size:12px;font-weight:700;cursor:pointer;padding:4px 10px;">Reintentar</button></div>`;
  }
}

async function cargarParticipantesProceso(codigo) {
  const el    = document.getElementById('dtab-participantes-content');
  const badge = document.getElementById('dtab-participantes-badge');
  if (!el) return;
  try {
    const res = await fetch(`${API}/api/v1/proveedores/proceso/${encodeURIComponent(codigo)}/oferentes`);
    if (!res.ok) { el.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">Sin datos disponibles.</div>'; return; }
    const data = await res.json();
    const oferentes = data.oferentes || [];
    const total = data.total_oferentes || 0;
    const ganador = data.ganador || null;
    if (badge) badge.textContent = total ? total + ' empresa' + (total !== 1 ? 's' : '') : '';
    if (!total) { el.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">Sin datos de participantes para este proceso.</div>'; return; }
    let h = '';
    if (ganador) h += `<div style="background:rgba(52,199,89,.12);border:1px solid rgba(52,199,89,.25);border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:13px;"><span style="font-weight:700;color:var(--green);">🏆 Ganador:</span><span style="margin-left:6px;">${sanitize(ganador)}</span></div>`;
    h += '<div>';
    oferentes.forEach((o, i) => {
      const monto = o.valor_oferta ? 'RD$ ' + icFmtM(Number(o.valor_oferta)) : '—';
      const adj = (o.es_ganador && o.monto_adjudicado) ? `<div style="font-size:10px;color:var(--muted);">Adj: RD$ ${icFmtM(Number(o.monto_adjudicado))}</div>` : '';
      h += `<div onclick="icVerDashboardPorRnc('${o.rpe}')" style="display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid var(--border);cursor:pointer;" onmouseover="this.style.background='var(--surface2)'" onmouseout="this.style.background='transparent'">
        <div style="flex:1;overflow:hidden;padding-right:8px;"><div style="display:flex;align-items:center;gap:6px;"><span style="font-size:12px;flex-shrink:0;">${o.es_ganador ? '🏆' : '#'+(i+1)}</span><span style="font-size:13px;font-weight:${o.es_ganador?700:500};color:${o.es_ganador?'var(--text)':'var(--text2)'};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${sanitize(o.razon_social||'—')}</span></div><div style="font-size:11px;color:var(--muted);padding-left:18px;">${o.tipo_oferta||''}</div></div>
        <div style="text-align:right;flex-shrink:0;"><div style="font-size:13px;font-weight:700;color:${o.es_ganador?'var(--green)':'var(--text2)'};">${monto}</div>${adj}</div>
      </div>`;
    });
    h += '</div>';
    el.innerHTML = h;
  } catch(e) {
    if (el) el.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">No hay datos disponibles.</div>';
  }
}

function icFmtM(n) {
  if (!n || n === 0) return '0';
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(0) + 'K';
  return Math.round(n).toLocaleString();
}

function icFmtFecha(f) {
  if (!f) return '';
  return f.slice(0, 7);
}

function icRenderEstado(msg) {
  const el = document.getElementById('ic-estado');
  if (el) el.textContent = msg;
}

function icDebounce() {
  clearTimeout(_icDebounceTimer);
  _icDebounceTimer = setTimeout(icBuscar, 350);
}

async function icBuscar() {
  if (!(await verificarAccesoPro('empresas'))) return;
  const q = (document.getElementById('ic-search-input')?.value || '').trim();
  if (q.length < 2) return;
  document.getElementById('ic-resultados').innerHTML = '';
  document.getElementById('ic-dashboard').style.display = 'none';
  document.getElementById('ic-resultados').style.display = 'block';
  icRenderEstado('Buscando...');
  try {
    const res = await fetch(`${IC_API}/buscar?q=${encodeURIComponent(q)}&limit=8`);
    const data = await res.json();
    icRenderResultados(data.resultados || []);
  } catch(e) {
    icRenderEstado('Error al conectar. Verifica tu conexión.');
  }
}

function icRenderResultados(lista) {
  const cont = document.getElementById('ic-resultados');
  if (!lista.length) {
    icRenderEstado('No se encontraron empresas para esa búsqueda.');
    cont.innerHTML = '';
    return;
  }
  icRenderEstado(lista.length + ' empresa(s) encontrada(s). Toca para ver el dashboard.');
  cont.innerHTML = lista.map(e => `
    <div onclick="icVerDashboard('${e.id}')" style="display:flex;justify-content:space-between;align-items:center;padding:12px 14px;background:var(--surface);border:1px solid var(--border);border-radius:var(--r-md);margin-bottom:6px;cursor:pointer;transition:border-color .15s;" onmouseover="this.style.borderColor='var(--green)'" onmouseout="this.style.borderColor='var(--border)'">
      <div>
        <div style="font-size:14px;font-weight:700;color:var(--text);">${e.nombre || '—'}</div>
        <div style="font-size:12px;color:var(--muted);margin-top:2px;">RNC: ${e.rnc || 'No disponible'}</div>
      </div>
      <div style="text-align:right;">
        <div style="font-size:13px;font-weight:700;color:var(--green);">${e.total_contratos || 0} contratos</div>
        <div style="font-size:11px;color:var(--muted);">RD$ ${icFmtM(e.monto_total)}</div>
      </div>
    </div>`).join('');
}

let _icUltimosResultadosHTML = '';

async function icVerDashboard(id) {
  const resCont = document.getElementById('ic-resultados');
  // FIX: guardar los resultados de búsqueda para poder volver a ellos
  if (resCont && resCont.innerHTML.trim()) _icUltimosResultadosHTML = resCont.innerHTML;
  _pushNavState({ tab: 'inteligencia', view: 'dashboard', id });
  resCont.innerHTML = '';
  resCont.style.display = 'none';
  // Ocultar sección win/loss mientras carga nuevo dashboard
  const wlSec = document.getElementById('ic-winloss-section');
  if (wlSec) wlSec.style.display = 'none';
  icRenderEstado('Cargando...');
  try {
    const res = await fetch(`${IC_API}/${id}/dashboard`);
    const data = await res.json();
    _icEmpresaActual = data;
    icRenderDashboard(data);
    icRenderEstado('');
    // Cargar inteligencia Win/Loss en paralelo sin bloquear el dashboard
    icCargarInteligencia(id);
  } catch(e) {
    icRenderEstado('Error al cargar el dashboard.');
  }
}

function icRenderDashboard(data) {
  const emp = data.empresa;
  document.getElementById('ic-d-nombre').textContent = emp.nombre || '—';
  document.getElementById('ic-d-rnc').textContent = emp.rnc ? 'RNC ' + emp.rnc : '';
  document.getElementById('ic-d-provincia').textContent = emp.provincia || '';
  document.getElementById('ic-d-estado-rpe').textContent = emp.estado_rpe ? 'Estado: ' + emp.estado_rpe : '';

  // Badges de contacto
  const email = emp.correo_comercial || emp.correo_contacto || '';
  const tel   = emp.telefono_comercial || emp.telefono_contacto || '';
  const telHref = tel ? 'tel:+1' + String(tel).replace(/[^0-9]/g, '').replace(/^1/, '') : '';
  let badges = '';
  if (email) badges += `<a href="mailto:${sanitize(email)}" style="text-decoration:none;"><span style="font-size:11px;font-weight:700;padding:3px 10px;background:var(--green-dim);color:var(--green);border:1px solid var(--green-border);border-radius:var(--r-full);cursor:pointer;display:inline-flex;align-items:center;gap:4px;">✉ ${sanitize(email)}</span></a> `;
  if (tel)   badges += `<a href="${telHref}" style="text-decoration:none;"><span style="font-size:11px;font-weight:700;padding:3px 10px;background:var(--green-dim);color:var(--green);border:1px solid var(--green-border);border-radius:var(--r-full);cursor:pointer;display:inline-flex;align-items:center;gap:4px;">☎ ${sanitize(tel)}</span></a>`;
  document.getElementById('ic-d-contacto').innerHTML = badges;

  // KPIs
  const k = data.kpis;
  document.getElementById('ic-kpi-contratos').textContent = (k.total_contratos || 0).toLocaleString();
  const rango = [k.primer_contrato, k.ultimo_contrato].filter(Boolean).map(icFmtFecha).join(' → ');
  document.getElementById('ic-kpi-rango').textContent = rango;
  document.getElementById('ic-kpi-monto').textContent = 'RD$ ' + icFmtM(k.monto_total);
  const prom = k.total_contratos > 0 ? k.monto_total / k.total_contratos : 0;
  document.getElementById('ic-kpi-promedio').textContent = 'RD$ ' + icFmtM(prom);
  document.getElementById('ic-kpi-contacto').textContent = emp.nombre_contacto || email || 'No disponible';
  document.getElementById('ic-kpi-contacto-pos').textContent = emp.posicion_contacto || '';

  // Charts con Chart.js
  icRenderChartCanvas('ic-chart-contratos', data.por_anio, 'cantidad_contratos', false);
  icRenderChartCanvas('ic-chart-montos', data.por_anio, 'monto_total', true);

  // Instituciones
  const instEl = document.getElementById('ic-instituciones');
  if (data.top_instituciones && data.top_instituciones.length) {
    instEl.innerHTML = data.top_instituciones.map(i => `
      <div style="display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid var(--border);font-size:13px;">
        <span style="color:var(--text2);flex:1;padding-right:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${i.nombre || '—'}</span>
        <div style="text-align:right;flex-shrink:0;">
          <span style="font-weight:700;color:var(--green);">${i.cantidad_contratos}</span>
          <div style="font-size:11px;color:var(--muted);">RD$ ${icFmtM(i.monto_total)}</div>
        </div>
      </div>`).join('');
  } else {
    instEl.innerHTML = '<div style="font-size:13px;color:var(--muted);">Sin datos</div>';
  }

  // Últimos contratos
  const contEl = document.getElementById('ic-contratos');
  if (data.ultimos_contratos && data.ultimos_contratos.length) {
    contEl.innerHTML = data.ultimos_contratos.map(c => `
      <div onclick="icVerProceso('${c.ocid || ''}')" style="display:flex;justify-content:space-between;padding:9px 0;border-bottom:1px solid var(--border);font-size:13px;cursor:pointer;" onmouseover="this.style.background='var(--surface2)'" onmouseout="this.style.background='transparent'">
        <div style="flex:1;padding-right:8px;overflow:hidden;">
          <div style="color:var(--text);font-weight:600;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;">${c.titulo || 'Sin título'}</div>
          <div style="display:flex;gap:8px;align-items:center;margin-top:3px;">
            <span style="font-size:10px;font-family:var(--font-mono);color:var(--muted);">${c.ocid || ''}</span>
            ${c.institucion ? `<span style="font-size:10px;color:var(--muted);">· ${c.institucion}</span>` : ''}
          </div>
        </div>
        <div style="text-align:right;flex-shrink:0;">
          <div style="font-weight:700;color:var(--green);">RD$ ${icFmtM(c.monto)}</div>
          <div style="font-size:11px;color:var(--muted);">${icFmtFecha(c.fecha)}</div>
          <div style="font-size:10px;color:var(--green);margin-top:2px;">Ver proceso →</div>
        </div>
      </div>`).join('');
  } else {
    contEl.innerHTML = '<div style="font-size:13px;color:var(--muted);">Sin contratos registrados</div>';
  }

  document.getElementById('ic-dashboard').style.display = 'block';
}

function icRenderChartCanvas(canvasId, datos, campo, esMonto) {
  const cv = document.getElementById(canvasId);
  if (!cv || !datos || !datos.length) return;
  // Destruir chart anterior si existe
  const existente = Chart.getChart(cv);
  if (existente) existente.destroy();

  const isDark = !document.documentElement.getAttribute('data-theme') || document.documentElement.getAttribute('data-theme') !== 'light';
  const GREEN  = isDark ? '#16A34A' : '#0F7A3E';
  const MUTED  = isDark ? '#737373' : '#737373';
  const GRID   = isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)';

  const labels = datos.map(d => String(d.anio));
  const vals   = datos.map(d => esMonto ? Math.round(d[campo] / 1e6) : d[campo]);

  cv.parentElement.style.height = '160px';
  cv.parentElement.style.position = 'relative';

  new Chart(cv, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data: vals,
        backgroundColor: GREEN,
        borderRadius: 5,
        borderSkipped: false,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => esMonto ? 'RD$ ' + ctx.raw + 'M' : ctx.raw + ' contratos'
          }
        }
      },
      scales: {
        x: {
          ticks: { color: MUTED, font: { size: 11 } },
          grid: { display: false },
          border: { display: false }
        },
        y: {
          ticks: {
            color: MUTED, font: { size: 10 },
            callback: v => esMonto ? v + 'M' : v
          },
          grid: { color: GRID },
          border: { display: false }
        }
      }
    }
  });
}

function icVerProceso(ocid) {
  if (!ocid) return;
  // El identificador puede ser OCID o codigo_proceso — el backend resuelve ambos
  const codigo = ocid;
  if (typeof verDetalle === 'function') {
    switchTab2('dashboard');
    setTimeout(() => verDetalle(codigo), 100);
  } else {
    mostrarToast('Código: ' + (codigo || ocid));
  }
}

async function icCargarInteligencia(empresa_id) {
  const seccion  = document.getElementById('ic-winloss-section');
  const loading  = document.getElementById('ic-winloss-loading');
  if (!seccion || !loading) return;

  seccion.style.display = 'none';
  loading.style.display = 'block';

  try {
    const res  = await fetch(`${IC_API}/${empresa_id}/inteligencia`);
    const data = await res.json();

    const wl   = data.winloss || {};
    const comp = data.competidores_frecuentes || [];

    // KPIs Win/Loss
    const wr = wl.win_rate_pct ?? 0;
    document.getElementById('ic-wl-winrate').textContent  = wr + '%';
    document.getElementById('ic-wl-ganados').textContent  = (wl.procesos_ganados  ?? 0).toLocaleString();
    document.getElementById('ic-wl-perdidos').textContent = (wl.procesos_perdidos ?? 0).toLocaleString();

    // Barra
    const total = wl.total_con_resultado ?? 0;
    document.getElementById('ic-wl-total').textContent = total.toLocaleString() + ' procesos';
    document.getElementById('ic-wl-barra').style.width  = Math.min(wr, 100) + '%';

    // Competidores
    const compEl = document.getElementById('ic-competidores');
    if (comp.length) {
      compEl.innerHTML = comp.map((c, i) => `
        <div onclick="icVerDashboardPorRnc('${c.rpe}')" style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;cursor:pointer;" onmouseover="this.style.background='var(--surface2)'" onmouseout="this.style.background='transparent'">
          <div style="flex:1;overflow:hidden;">
            <div style="font-weight:600;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${c.nombre || '—'}</div>
            <div style="font-size:11px;color:var(--muted);margin-top:1px;">${c.coincidencias} coincidencias</div>
          </div>
          <div style="text-align:right;flex-shrink:0;padding-left:8px;">
            <div style="font-size:12px;font-weight:700;color:${c.veces_ganador > 0 ? 'var(--green)' : 'var(--muted)'};">${c.veces_ganador} ganados</div>
            <div style="font-size:10px;color:var(--muted);">${c.win_rate_vs_ti}% win</div>
          </div>
        </div>`).join('');
    } else {
      compEl.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">Sin competidores identificados aún.</div>';
    }

    loading.style.display  = 'none';
    seccion.style.display  = 'block';

  } catch(e) {
    loading.style.display  = 'none';
    // Si falla silenciosamente no rompe el dashboard existente
    console.warn('Win/Loss no disponible:', e);
  }
}

async function icVerDashboardPorRnc(rnc) {
  if (!rnc) return;
  icRenderEstado('Cargando empresa...');
  document.getElementById('ic-dashboard').style.display = 'none';
  try {
    const res  = await fetch(`${IC_API}/buscar?q=${encodeURIComponent(rnc)}&limit=1`);
    const data = await res.json();
    if (data.resultados && data.resultados.length) {
      icVerDashboard(data.resultados[0].id);
    } else {
      icRenderEstado('No se encontró la empresa.');
    }
  } catch(e) {
    icRenderEstado('Error al cargar empresa.');
  }
}

function icMostrarResultados() {
  const dash = document.getElementById('ic-dashboard');
  const res  = document.getElementById('ic-resultados');
  if (dash) dash.style.display = 'none';
  if (res) {
    res.style.display = 'block';
    // FIX: restaurar los resultados de la búsqueda anterior (antes se borraban)
    if (_icUltimosResultadosHTML) res.innerHTML = _icUltimosResultadosHTML;
  }
  icRenderEstado(_icUltimosResultadosHTML ? '' : 'Busca una empresa por nombre o RNC para ver su historial.');
}

function icVolverBusqueda() {
  // Si el estado actual del historial es el dashboard, usar back() para
  // mantener consistencia con la flecha atrás del navegador/PWA
  if (history.state && history.state.view === 'dashboard') { history.back(); return; }
  icMostrarResultados();
}
// ════════════════════════════════════════════════════════════
