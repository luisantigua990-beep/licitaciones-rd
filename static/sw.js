self.addEventListener('push', function(event) {
    const data = event.data
        ? (()=>{ try { return event.data.json(); } catch(e) { return { title: 'LicitacionLab', body: event.data.text() }; } })()
        : {};

    event.waitUntil(
        self.registration.showNotification(data.title || 'LicitacionLab', {
            body: data.body || 'Nueva licitación disponible',
            icon: data.icon || '/static/icon-192.png',
            badge: data.badge || '/static/icon-72.png',
            data: { url: data.url || '/' },
            requireInteraction: false,
        })
    );
});

self.addEventListener('notificationclick', function(event) {
    event.notification.close();
    const targetUrl = event.notification.data.url || '/';

    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(windowClients) {
            // Si la app ya está abierta, hacer focus y navegar al proceso
            for (let client of windowClients) {
                const clientUrl = new URL(client.url);
                const targetParsed = new URL(targetUrl, self.location.origin);
                if (clientUrl.origin === targetParsed.origin) {
                    return client.focus().then(function(focusedClient) {
                        return focusedClient.navigate(targetUrl);
                    });
                }
            }
            // Si no hay ventana abierta, abrir nueva directamente en el proceso
            return clients.openWindow(targetUrl);
        })
    );
});
