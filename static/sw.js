const CACHE_NAME = 'licitacionlab-v3';
const CACHE_URLS = [
  '/',
  '/frontend/index.html',
  '/frontend/manifest.json',
  '/static/icon-72.png',
  '/static/icon-96.png',
  '/static/icon-128.png',
  '/static/icon-144.png',
  '/static/icon-152.png',
  '/static/icon-192.png',
  '/static/icon-384.png',
  '/static/icon-512.png',
];

// ── INSTALL: cachear recursos estáticos ──
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return cache.addAll(CACHE_URLS).catch(err => {
        // No fallar si algún recurso estático no existe
        console.warn('Cache parcial:', err);
      });
    })
  );
  self.skipWaiting();
});

// ── ACTIVATE: limpiar caches viejos ──
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// ── FETCH: servir desde caché si está offline ──
self.addEventListener('fetch', event => {
  // Solo interceptar GET, no APIs ni Supabase
  if (event.request.method !== 'GET') return;
  const url = event.request.url;
  if (url.includes('/api/') || url.includes('supabase.co') || url.includes('fonts.googleapis')) return;

  event.respondWith(
    fetch(event.request)
      .then(response => {
        // Actualizar cache si la respuesta es válida
        if (response && response.status === 200 && response.type === 'basic') {
          const cloned = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, cloned));
        }
        return response;
      })
      .catch(() => {
        // Sin conexión — servir desde caché
        return caches.match(event.request).then(cached => {
          if (cached) return cached;
          // Fallback: página principal si es navegación
          if (event.request.mode === 'navigate') {
            return caches.match('/') || caches.match('/frontend/index.html');
          }
        });
      })
  );
});

// ── PUSH: mostrar notificación y encender badge ──
self.addEventListener('push', function(event) {
  const data = event.data
    ? (()=>{ try { return event.data.json(); } catch(e) { return { title: 'LicitacionLab', body: event.data.text() }; } })()
    : {};

  // Encender badge en la app si está abierta
  event.waitUntil(
    Promise.all([
      self.registration.showNotification(data.title || 'LicitacionLab', {
        body: data.body || 'Nueva licitación disponible',
        icon: data.icon || '/static/icon-192.png',
        badge: data.badge || '/static/icon-72.png',
        data: { url: data.url || '/' },
        requireInteraction: false,
      }),
      // Notificar a la app para que muestre el badge
      self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clients => {
        clients.forEach(client => client.postMessage({ type: 'SHOW_BADGE' }));
      })
    ])
  );
});

// ── NOTIFICATION CLICK: abrir app en el proceso ──
self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  const targetUrl = event.notification.data?.url || '/';

  // Construir URL absoluta correcta apuntando al frontend
  const appBase = self.registration.scope; // ej: https://licitacionlab.railway.app/
  // Si la URL ya es absoluta, usarla; si no, construirla desde la raíz
  let fullUrl;
  try {
    fullUrl = new URL(targetUrl, appBase).href;
  } catch(e) {
    fullUrl = appBase;
  }

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(windowClients) {
      // Buscar si ya hay una ventana de la app abierta
      for (let client of windowClients) {
        try {
          const clientOrigin = new URL(client.url).origin;
          const targetOrigin = new URL(fullUrl).origin;
          if (clientOrigin === targetOrigin) {
            // Enfocar la ventana existente y navegar al proceso
            return client.focus().then(function(focusedClient) {
              return focusedClient.navigate(fullUrl);
            });
          }
        } catch(e) {}
      }
      // No hay ventana abierta — abrir nueva
      return clients.openWindow(fullUrl);
    })
  );
});
