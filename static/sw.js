self.addEventListener('push', function(event) {
    const data = event.data ? (()=>{try{return event.data.json()}catch(e){return {title:'LicitacionLab',body:event.data.text()}}})() : {};
    event.waitUntil(
        self.registration.showNotification(data.title || 'LicitacionLab', {
            body: data.body || 'Nueva licitación disponible',
            icon: data.icon || '',
            data: { url: data.url || '/' }
        })
    );
});

self.addEventListener('notificationclick', function(event) {
    event.notification.close();
    event.waitUntil(clients.openWindow(event.notification.data.url));
});
